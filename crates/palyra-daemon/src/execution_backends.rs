use std::collections::BTreeSet;

use palyra_common::feature_rollouts::{
    FeatureRolloutSetting, FeatureRolloutSource, EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV,
    EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV, EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV,
};
use palyra_common::runtime_preview::RuntimePreviewMode;
use palyra_sandbox::{current_backend_capabilities, current_backend_kind};
use palyra_workerd::{WorkerFleetPolicy, WorkerFleetSnapshot};
use serde::{Deserialize, Serialize};

use crate::{
    config::{FeatureRolloutsConfig, NetworkedWorkersConfig},
    node_runtime::RegisteredNodeRecord,
    sandbox_runner::{process_runner_executor_name, SandboxProcessRunnerPolicy},
};

const NODE_HEALTHY_AFTER_MS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendPreference {
    #[default]
    Automatic,
    LocalSandbox,
    DesktopNode,
    NetworkedWorker,
    SshTunnel,
}

impl ExecutionBackendPreference {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::LocalSandbox => "local_sandbox",
            Self::DesktopNode => "desktop_node",
            Self::NetworkedWorker => "networked_worker",
            Self::SshTunnel => "ssh_tunnel",
        }
    }

    #[must_use]
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Automatic => "Automatic",
            Self::LocalSandbox => "Local sandbox",
            Self::DesktopNode => "Desktop node",
            Self::NetworkedWorker => "Networked worker",
            Self::SshTunnel => "SSH tunnel",
        }
    }

    #[must_use]
    pub(crate) const fn description(self) -> &'static str {
        match self {
            Self::Automatic => {
                "Keep the current default behavior and stay on the daemon host unless a preview backend is explicitly selected."
            }
            Self::LocalSandbox => {
                "Run work on the daemon host and keep sandbox/process-runner guardrails local."
            }
            Self::DesktopNode => {
                "Hand work off to a paired first-party desktop node when a healthy node is available."
            }
            Self::NetworkedWorker => {
                "Run work on an attested ephemeral worker with proxy-mediated egress and scoped artifact transport."
            }
            Self::SshTunnel => {
                "Use an operator-established SSH tunnel for remote control-plane access and remote operator workflows."
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendState {
    Available,
    Degraded,
    Disabled,
}

impl ExecutionBackendState {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Degraded => "degraded",
            Self::Disabled => "disabled",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendInventoryRecord {
    pub(crate) backend_id: String,
    pub(crate) label: String,
    pub(crate) state: ExecutionBackendState,
    pub(crate) selectable: bool,
    pub(crate) selected_by_default: bool,
    pub(crate) description: String,
    pub(crate) operator_summary: String,
    pub(crate) executor_label: Option<String>,
    pub(crate) rollout_flag: Option<String>,
    pub(crate) rollout_source: Option<FeatureRolloutSource>,
    pub(crate) rollout_enabled: bool,
    pub(crate) capabilities: Vec<String>,
    pub(crate) tradeoffs: Vec<String>,
    pub(crate) requires_attestation: bool,
    pub(crate) requires_egress_proxy: bool,
    pub(crate) workspace_scope_mode: String,
    pub(crate) artifact_transport: String,
    pub(crate) cleanup_strategy: String,
    pub(crate) active_node_count: usize,
    pub(crate) total_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendResolution {
    pub(crate) requested: ExecutionBackendPreference,
    pub(crate) resolved: ExecutionBackendPreference,
    pub(crate) fallback_used: bool,
    pub(crate) reason_code: String,
    pub(crate) approval_required: bool,
    pub(crate) reason: String,
}

pub(crate) fn parse_execution_backend_preference(
    raw: &str,
    field_name: &str,
) -> Result<ExecutionBackendPreference, String> {
    let normalized = raw.trim().to_ascii_lowercase();
    let preference = match normalized.as_str() {
        "" | "automatic" | "auto" => ExecutionBackendPreference::Automatic,
        "local_sandbox" | "local" | "sandbox" => ExecutionBackendPreference::LocalSandbox,
        "desktop_node" | "node" | "remote_node" => ExecutionBackendPreference::DesktopNode,
        "networked_worker" | "networked" | "worker" | "remote_worker" => {
            ExecutionBackendPreference::NetworkedWorker
        }
        "ssh_tunnel" | "ssh" | "tunnel" => ExecutionBackendPreference::SshTunnel,
        _ => {
            return Err(format!(
                "{field_name} must be one of automatic, local_sandbox, desktop_node, networked_worker, ssh_tunnel"
            ));
        }
    };
    Ok(preference)
}

pub(crate) fn parse_optional_execution_backend_preference(
    raw: Option<&str>,
    field_name: &str,
) -> Result<Option<ExecutionBackendPreference>, String> {
    raw.map(|value| parse_execution_backend_preference(value, field_name)).transpose()
}

#[allow(dead_code)]
#[must_use]
pub(crate) fn build_execution_backend_inventory(
    policy: &SandboxProcessRunnerPolicy,
    nodes: &[RegisteredNodeRecord],
    now_unix_ms: i64,
    feature_rollouts: &FeatureRolloutsConfig,
    networked_workers: &NetworkedWorkersConfig,
) -> Vec<ExecutionBackendInventoryRecord> {
    build_execution_backend_inventory_with_worker_state(
        policy,
        nodes,
        now_unix_ms,
        feature_rollouts,
        networked_workers,
        WorkerFleetSnapshot::default(),
        &WorkerFleetPolicy::default(),
    )
}

#[must_use]
pub(crate) fn build_execution_backend_inventory_with_worker_state(
    policy: &SandboxProcessRunnerPolicy,
    nodes: &[RegisteredNodeRecord],
    now_unix_ms: i64,
    feature_rollouts: &FeatureRolloutsConfig,
    networked_workers: &NetworkedWorkersConfig,
    worker_snapshot: WorkerFleetSnapshot,
    worker_policy: &WorkerFleetPolicy,
) -> Vec<ExecutionBackendInventoryRecord> {
    let healthy_nodes = nodes
        .iter()
        .filter(|node| {
            now_unix_ms.saturating_sub(node.last_seen_at_unix_ms.max(0)) <= NODE_HEALTHY_AFTER_MS
        })
        .collect::<Vec<_>>();
    build_execution_backend_inventory_with_rollout(
        policy,
        nodes.len(),
        healthy_nodes.as_slice(),
        feature_rollouts.execution_backend_remote_node,
        feature_rollouts.execution_backend_networked_worker,
        feature_rollouts.networked_workers,
        feature_rollouts.execution_backend_ssh_tunnel,
        networked_workers,
        worker_snapshot,
        worker_policy,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_execution_backend_inventory_with_rollout(
    policy: &SandboxProcessRunnerPolicy,
    total_nodes: usize,
    healthy_nodes: &[&RegisteredNodeRecord],
    remote_node_rollout: FeatureRolloutSetting,
    networked_worker_rollout: FeatureRolloutSetting,
    networked_workers_runtime_rollout: FeatureRolloutSetting,
    ssh_tunnel_rollout: FeatureRolloutSetting,
    networked_workers: &NetworkedWorkersConfig,
    worker_snapshot: WorkerFleetSnapshot,
    worker_policy: &WorkerFleetPolicy,
) -> Vec<ExecutionBackendInventoryRecord> {
    vec![
        local_sandbox_inventory_record(policy),
        desktop_node_inventory_record(total_nodes, healthy_nodes, remote_node_rollout),
        networked_worker_inventory_record(
            networked_worker_rollout,
            networked_workers_runtime_rollout,
            networked_workers,
            worker_snapshot,
            worker_policy,
        ),
        ssh_tunnel_inventory_record(ssh_tunnel_rollout),
    ]
}

pub(crate) fn validate_execution_backend_selection(
    preference: ExecutionBackendPreference,
    inventory: &[ExecutionBackendInventoryRecord],
) -> Result<(), String> {
    if matches!(preference, ExecutionBackendPreference::Automatic) {
        return Ok(());
    }
    let record = inventory
        .iter()
        .find(|entry| entry.backend_id == preference.as_str())
        .ok_or_else(|| format!("execution backend '{}' is not available", preference.as_str()))?;
    if record.selectable {
        return Ok(());
    }
    Err(format!(
        "execution backend '{}' cannot be selected: {}",
        preference.as_str(),
        record.operator_summary
    ))
}

#[must_use]
pub(crate) fn resolve_execution_backend(
    preference: ExecutionBackendPreference,
    inventory: &[ExecutionBackendInventoryRecord],
) -> ExecutionBackendResolution {
    let local_record = inventory
        .iter()
        .find(|entry| entry.backend_id == ExecutionBackendPreference::LocalSandbox.as_str());
    let requested_record = inventory.iter().find(|entry| entry.backend_id == preference.as_str());
    if matches!(preference, ExecutionBackendPreference::Automatic) {
        if let Some(record) = local_record {
            return ExecutionBackendResolution {
                requested: preference,
                resolved: ExecutionBackendPreference::LocalSandbox,
                fallback_used: false,
                reason_code: "backend.default.local_sandbox".to_owned(),
                approval_required: false,
                reason: if record.selectable {
                    "Automatic keeps execution on the daemon host until an operator explicitly opts into a preview backend."
                        .to_owned()
                } else {
                    format!(
                        "Automatic prefers the daemon-host backend; the current local posture is degraded: {}",
                        record.operator_summary
                    )
                },
            };
        }
        return ExecutionBackendResolution {
            requested: preference,
            resolved: ExecutionBackendPreference::Automatic,
            fallback_used: false,
            reason_code: "backend.inventory.missing".to_owned(),
            approval_required: false,
            reason: "No execution backend inventory is available.".to_owned(),
        };
    }

    if let Some(record) = requested_record {
        if record.selectable {
            return ExecutionBackendResolution {
                requested: preference,
                resolved: preference,
                fallback_used: false,
                reason_code: format!("backend.available.{}", preference.as_str()),
                approval_required: !matches!(preference, ExecutionBackendPreference::LocalSandbox),
                reason: record.operator_summary.clone(),
            };
        }
    }

    if let Some(record) = local_record.filter(|entry| entry.selectable) {
        return ExecutionBackendResolution {
            requested: preference,
            resolved: ExecutionBackendPreference::LocalSandbox,
            fallback_used: true,
            reason_code: format!("backend.fallback.{}", preference.as_str()),
            approval_required: false,
            reason: format!(
                "Requested backend '{}' is not selectable right now; falling back to local_sandbox. {}",
                preference.as_str(),
                record.operator_summary
            ),
        };
    }

    let fallback = inventory.iter().find(|entry| entry.selectable);
    if let Some(record) = fallback {
        let resolved = parse_execution_backend_preference(record.backend_id.as_str(), "backend_id")
            .unwrap_or(ExecutionBackendPreference::Automatic);
        return ExecutionBackendResolution {
            requested: preference,
            resolved,
            fallback_used: true,
            reason_code: format!("backend.fallback.{}", record.backend_id),
            approval_required: !matches!(resolved, ExecutionBackendPreference::LocalSandbox),
            reason: format!(
                "Requested backend '{}' is not selectable; falling back to '{}'. {}",
                preference.as_str(),
                record.backend_id,
                record.operator_summary
            ),
        };
    }

    ExecutionBackendResolution {
        requested: preference,
        resolved: preference,
        fallback_used: false,
        reason_code: format!("backend.unavailable.{}", preference.as_str()),
        approval_required: !matches!(preference, ExecutionBackendPreference::LocalSandbox),
        reason: format!(
            "Requested backend '{}' is currently unavailable and no fallback backend is selectable.",
            preference.as_str()
        ),
    }
}

fn local_sandbox_inventory_record(
    policy: &SandboxProcessRunnerPolicy,
) -> ExecutionBackendInventoryRecord {
    let backend_kind = current_backend_kind();
    let backend_capabilities = current_backend_capabilities();
    let process_runner_summary = if policy.enabled {
        format!(
            "Process runner is enabled with executor '{}' and tier '{}'.",
            process_runner_executor_name(policy),
            policy.tier.as_str()
        )
    } else {
        "Process runner is disabled by runtime policy; local daemon-host execution remains the conservative default."
            .to_owned()
    };
    let operator_summary = if matches!(backend_kind.as_str(), "unsupported") {
        format!(
            "{} Tier-C isolation is unavailable on this platform, so preview backends should be treated conservatively.",
            process_runner_summary
        )
    } else {
        format!(
            "{} Tier-C backend '{}', runtime_network_isolation={}, host_allowlists={}.",
            process_runner_summary,
            backend_kind.as_str(),
            backend_capabilities.runtime_network_isolation,
            backend_capabilities.host_allowlists
        )
    };
    let mut capabilities = vec!["daemon_host_execution".to_owned(), "workspace_patch".to_owned()];
    if policy.enabled {
        capabilities.push("sandbox_process_runner".to_owned());
    }
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::LocalSandbox.as_str().to_owned(),
        label: ExecutionBackendPreference::LocalSandbox.label().to_owned(),
        state: if matches!(backend_kind.as_str(), "unsupported") {
            ExecutionBackendState::Degraded
        } else {
            ExecutionBackendState::Available
        },
        selectable: true,
        selected_by_default: true,
        description: ExecutionBackendPreference::LocalSandbox.description().to_owned(),
        operator_summary,
        executor_label: Some(process_runner_executor_name(policy)),
        rollout_flag: None,
        rollout_source: None,
        rollout_enabled: true,
        capabilities,
        tradeoffs: vec![
            "Most conservative default posture".to_owned(),
            "Cannot satisfy first-party desktop-native capability requests by itself".to_owned(),
        ],
        requires_attestation: false,
        requires_egress_proxy: false,
        workspace_scope_mode: "daemon_workspace_root".to_owned(),
        artifact_transport: "direct_local_filesystem".to_owned(),
        cleanup_strategy: "process_exit_and_workspace_scope_validation".to_owned(),
        active_node_count: 0,
        total_node_count: 0,
    }
}

fn desktop_node_inventory_record(
    total_nodes: usize,
    healthy_nodes: &[&RegisteredNodeRecord],
    rollout: FeatureRolloutSetting,
) -> ExecutionBackendInventoryRecord {
    let capabilities = aggregate_node_capabilities(healthy_nodes);
    let (state, selectable, operator_summary) = if !rollout.enabled {
        (
            ExecutionBackendState::Disabled,
            false,
            format!(
                "Preview backend is disabled. Set {}=1 and keep at least one paired desktop node healthy before selecting it.",
                EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV
            ),
        )
    } else if !healthy_nodes.is_empty() {
        (
            ExecutionBackendState::Available,
            true,
            format!(
                "{} healthy desktop node(s) are available for first-party node handoff.",
                healthy_nodes.len()
            ),
        )
    } else if total_nodes > 0 {
        (
            ExecutionBackendState::Degraded,
            false,
            format!(
                "{} desktop node(s) are registered but none are healthy enough for selection.",
                total_nodes
            ),
        )
    } else {
        (
            ExecutionBackendState::Disabled,
            false,
            "Preview backend is enabled, but no paired desktop node has registered yet.".to_owned(),
        )
    };
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::DesktopNode.as_str().to_owned(),
        label: ExecutionBackendPreference::DesktopNode.label().to_owned(),
        state,
        selectable,
        selected_by_default: false,
        description: ExecutionBackendPreference::DesktopNode.description().to_owned(),
        operator_summary,
        executor_label: None,
        rollout_flag: Some(EXECUTION_BACKEND_REMOTE_NODE_ROLLOUT_ENV.to_owned()),
        rollout_source: Some(rollout.source),
        rollout_enabled: rollout.enabled,
        capabilities,
        tradeoffs: vec![
            "Supports first-party desktop capabilities and local mediation flows".to_owned(),
            "Depends on node heartbeat, pairing trust, and explicit rollout opt-in".to_owned(),
        ],
        requires_attestation: true,
        requires_egress_proxy: false,
        workspace_scope_mode: "paired_node_workspace_contract".to_owned(),
        artifact_transport: "node_rpc_transfer".to_owned(),
        cleanup_strategy: "node_disconnect_or_run_completion_cleanup".to_owned(),
        active_node_count: healthy_nodes.len(),
        total_node_count: total_nodes,
    }
}

fn networked_worker_inventory_record(
    rollout: FeatureRolloutSetting,
    runtime_rollout: FeatureRolloutSetting,
    networked_workers: &NetworkedWorkersConfig,
    worker_snapshot: WorkerFleetSnapshot,
    worker_policy: &WorkerFleetPolicy,
) -> ExecutionBackendInventoryRecord {
    let (state, selectable, operator_summary) = if matches!(
        networked_workers.mode,
        RuntimePreviewMode::Disabled
    ) {
        (
            ExecutionBackendState::Disabled,
            false,
            "Networked workers runtime is disabled. Set networked_workers.mode to preview_only or enabled before advertising remote execution."
                .to_owned(),
        )
    } else if !rollout.enabled {
        (
            ExecutionBackendState::Disabled,
            false,
            format!(
                "Preview backend is disabled. Set {}=1 before attested worker registration can advertise networked execution.",
                EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV
            ),
        )
    } else if matches!(networked_workers.mode, RuntimePreviewMode::Enabled)
        && !runtime_rollout.enabled
    {
        (
            ExecutionBackendState::Disabled,
            false,
            "Networked workers runtime is pinned to enabled mode, but its dedicated rollout flag is still off."
                .to_owned(),
        )
    } else if worker_snapshot.attested_workers > 0 {
        (
            ExecutionBackendState::Available,
            true,
            format!(
                "{} attested worker(s) are registered with proxy-bound egress and ephemeral lease support.",
                worker_snapshot.attested_workers
            ),
        )
    } else if worker_snapshot.registered_workers > 0 {
        (
            ExecutionBackendState::Degraded,
            false,
            format!(
                "{} worker(s) registered, but none passed attestation requirements for execution.",
                worker_snapshot.registered_workers
            ),
        )
    } else {
        (
            ExecutionBackendState::Degraded,
            false,
            "Preview backend is enabled, but no attested worker has registered yet.".to_owned(),
        )
    };
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::NetworkedWorker.as_str().to_owned(),
        label: ExecutionBackendPreference::NetworkedWorker.label().to_owned(),
        state,
        selectable,
        selected_by_default: false,
        description: ExecutionBackendPreference::NetworkedWorker.description().to_owned(),
        operator_summary,
        executor_label: Some("networked_worker".to_owned()),
        rollout_flag: Some(EXECUTION_BACKEND_NETWORKED_WORKER_ROLLOUT_ENV.to_owned()),
        rollout_source: Some(rollout.source),
        rollout_enabled: rollout.enabled,
        capabilities: vec![
            "attested_remote_execution".to_owned(),
            "proxy_mediated_egress".to_owned(),
            "scoped_artifact_transport".to_owned(),
        ],
        tradeoffs: vec![
            "Requires explicit worker attestation plus cleanup verification before use".to_owned(),
            format!(
                "Worker leases stay ephemeral with ttl<={}ms and fail closed on cleanup gaps",
                worker_policy.max_ttl_ms
            ),
        ],
        requires_attestation: true,
        requires_egress_proxy: worker_policy.attestation.require_egress_proxy,
        workspace_scope_mode: "ephemeral_scoped_mount".to_owned(),
        artifact_transport: "manifest_attested_bundle_transfer".to_owned(),
        cleanup_strategy: "lease_ttl_reap_with_fail_closed_cleanup".to_owned(),
        active_node_count: worker_snapshot.attested_workers,
        total_node_count: worker_snapshot.registered_workers,
    }
}

fn ssh_tunnel_inventory_record(rollout: FeatureRolloutSetting) -> ExecutionBackendInventoryRecord {
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::SshTunnel.as_str().to_owned(),
        label: ExecutionBackendPreference::SshTunnel.label().to_owned(),
        state: if rollout.enabled {
            ExecutionBackendState::Available
        } else {
            ExecutionBackendState::Disabled
        },
        selectable: rollout.enabled,
        selected_by_default: false,
        description: ExecutionBackendPreference::SshTunnel.description().to_owned(),
        operator_summary: if rollout.enabled {
            "Preview backend is enabled. Operators must still establish an explicit SSH forward before relying on remote control-plane flows."
                .to_owned()
        } else {
            format!(
                "Preview backend is disabled. Set {}=1 before advertising SSH tunnel workflows.",
                EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV
            )
        },
        executor_label: None,
        rollout_flag: Some(EXECUTION_BACKEND_SSH_TUNNEL_ROLLOUT_ENV.to_owned()),
        rollout_source: Some(rollout.source),
        rollout_enabled: rollout.enabled,
        capabilities: vec![
            "verified_remote_dashboard_access".to_owned(),
            "operator_handoff".to_owned(),
        ],
        tradeoffs: vec![
            "Useful for explicit remote operator access and controlled handoff".to_owned(),
            "Requires manual tunnel setup and does not replace sandbox or node trust boundaries"
                .to_owned(),
        ],
        requires_attestation: false,
        requires_egress_proxy: false,
        workspace_scope_mode: "operator_managed_remote_scope".to_owned(),
        artifact_transport: "out_of_band_operator_tunnel".to_owned(),
        cleanup_strategy: "operator_managed_tunnel_teardown".to_owned(),
        active_node_count: 0,
        total_node_count: 0,
    }
}

fn aggregate_node_capabilities(nodes: &[&RegisteredNodeRecord]) -> Vec<String> {
    let mut capabilities = BTreeSet::<String>::new();
    for node in nodes {
        for capability in &node.capabilities {
            if capability.available {
                capabilities.insert(capability.name.clone());
            }
        }
    }
    if capabilities.is_empty() {
        return vec!["paired_desktop_capabilities".to_owned()];
    }
    capabilities.into_iter().take(6).collect()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use palyra_common::feature_rollouts::FeatureRolloutSource;
    use palyra_common::runtime_preview::RuntimePreviewMode;
    use palyra_workerd::{WorkerFleetPolicy, WorkerFleetSnapshot};

    use crate::config::NetworkedWorkersConfig;
    use crate::sandbox_runner::{
        EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
    };

    use super::{
        build_execution_backend_inventory_with_rollout, resolve_execution_backend,
        validate_execution_backend_selection, ExecutionBackendPreference, ExecutionBackendState,
        FeatureRolloutSetting,
    };

    fn test_policy() -> SandboxProcessRunnerPolicy {
        SandboxProcessRunnerPolicy {
            enabled: true,
            tier: SandboxProcessRunnerTier::C,
            workspace_root: PathBuf::from("."),
            allowed_executables: vec!["cargo".to_owned()],
            allow_interpreters: false,
            egress_enforcement_mode: EgressEnforcementMode::Preflight,
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: 1_000,
            memory_limit_bytes: 1_048_576,
            max_output_bytes: 1_048_576,
        }
    }

    #[test]
    fn automatic_resolution_prefers_local_sandbox() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let resolution =
            resolve_execution_backend(ExecutionBackendPreference::Automatic, &inventory);
        assert_eq!(resolution.resolved, ExecutionBackendPreference::LocalSandbox);
        assert!(!resolution.fallback_used);
    }

    #[test]
    fn preview_backend_selection_rejects_disabled_rollout() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let error = validate_execution_backend_selection(
            ExecutionBackendPreference::DesktopNode,
            &inventory,
        )
        .expect_err("disabled preview backend should be rejected");
        assert!(error.contains("desktop_node"), "unexpected error: {error}");
    }

    #[test]
    fn preview_backend_resolution_falls_back_to_local_sandbox() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let resolution =
            resolve_execution_backend(ExecutionBackendPreference::DesktopNode, &inventory);
        assert_eq!(resolution.resolved, ExecutionBackendPreference::LocalSandbox);
        assert!(resolution.fallback_used);
    }

    #[test]
    fn preview_backend_inventory_is_degraded_without_healthy_nodes() {
        let networked_workers = NetworkedWorkersConfig::default();
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            1,
            &[],
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot::default(),
            &WorkerFleetPolicy::default(),
        );
        let desktop_node = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::DesktopNode.as_str())
            .expect("desktop node backend should exist");
        assert_eq!(desktop_node.state, ExecutionBackendState::Degraded);
        assert!(desktop_node.rollout_enabled);
        assert_eq!(desktop_node.rollout_source, Some(FeatureRolloutSource::Config));
        assert!(!desktop_node.selectable);
    }

    #[test]
    fn networked_worker_inventory_is_available_only_with_attested_workers() {
        let networked_workers = NetworkedWorkersConfig {
            mode: RuntimePreviewMode::PreviewOnly,
            ..NetworkedWorkersConfig::default()
        };
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot {
                registered_workers: 1,
                attested_workers: 1,
                active_leases: 0,
                orphaned_workers: 0,
            },
            &WorkerFleetPolicy::default(),
        );
        let networked_worker = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::NetworkedWorker.as_str())
            .expect("networked worker backend should exist");
        assert_eq!(networked_worker.state, ExecutionBackendState::Available);
        assert!(networked_worker.selectable);
        assert!(networked_worker.requires_attestation);
        assert!(networked_worker.requires_egress_proxy);
    }

    #[test]
    fn networked_worker_inventory_requires_runtime_rollout_when_enabled_mode_is_pinned() {
        let networked_workers = NetworkedWorkersConfig {
            mode: RuntimePreviewMode::Enabled,
            ..NetworkedWorkersConfig::default()
        };
        let inventory = build_execution_backend_inventory_with_rollout(
            &test_policy(),
            0,
            &[],
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::from_config(true),
            FeatureRolloutSetting::default(),
            FeatureRolloutSetting::default(),
            &networked_workers,
            WorkerFleetSnapshot {
                registered_workers: 1,
                attested_workers: 1,
                active_leases: 0,
                orphaned_workers: 0,
            },
            &WorkerFleetPolicy::default(),
        );
        let networked_worker = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::NetworkedWorker.as_str())
            .expect("networked worker backend should exist");
        assert_eq!(networked_worker.state, ExecutionBackendState::Disabled);
        assert!(!networked_worker.selectable);
        assert!(networked_worker.operator_summary.contains("dedicated rollout flag"));
    }
}
