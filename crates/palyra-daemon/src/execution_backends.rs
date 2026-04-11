use std::collections::BTreeSet;
use std::env;

use palyra_sandbox::{current_backend_capabilities, current_backend_kind};
use serde::{Deserialize, Serialize};

use crate::{
    node_runtime::RegisteredNodeRecord,
    sandbox_runner::{process_runner_executor_name, SandboxProcessRunnerPolicy},
};

pub(crate) const ENV_REMOTE_NODE_BACKEND_ENABLED: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_REMOTE_NODE";
pub(crate) const ENV_SSH_TUNNEL_BACKEND_ENABLED: &str =
    "PALYRA_EXPERIMENTAL_EXECUTION_BACKEND_SSH_TUNNEL";
const NODE_HEALTHY_AFTER_MS: i64 = 5 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ExecutionBackendPreference {
    #[default]
    Automatic,
    LocalSandbox,
    DesktopNode,
    SshTunnel,
}

impl ExecutionBackendPreference {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Automatic => "automatic",
            Self::LocalSandbox => "local_sandbox",
            Self::DesktopNode => "desktop_node",
            Self::SshTunnel => "ssh_tunnel",
        }
    }

    #[must_use]
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Automatic => "Automatic",
            Self::LocalSandbox => "Local sandbox",
            Self::DesktopNode => "Desktop node",
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
    pub(crate) rollout_enabled: bool,
    pub(crate) capabilities: Vec<String>,
    pub(crate) tradeoffs: Vec<String>,
    pub(crate) active_node_count: usize,
    pub(crate) total_node_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExecutionBackendResolution {
    pub(crate) requested: ExecutionBackendPreference,
    pub(crate) resolved: ExecutionBackendPreference,
    pub(crate) fallback_used: bool,
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
        "ssh_tunnel" | "ssh" | "tunnel" => ExecutionBackendPreference::SshTunnel,
        _ => {
            return Err(format!(
                "{field_name} must be one of automatic, local_sandbox, desktop_node, ssh_tunnel"
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

#[must_use]
pub(crate) fn build_execution_backend_inventory(
    policy: &SandboxProcessRunnerPolicy,
    nodes: &[RegisteredNodeRecord],
    now_unix_ms: i64,
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
        parse_backend_flag(ENV_REMOTE_NODE_BACKEND_ENABLED),
        parse_backend_flag(ENV_SSH_TUNNEL_BACKEND_ENABLED),
    )
}

fn build_execution_backend_inventory_with_rollout(
    policy: &SandboxProcessRunnerPolicy,
    total_nodes: usize,
    healthy_nodes: &[&RegisteredNodeRecord],
    remote_node_enabled: bool,
    ssh_tunnel_enabled: bool,
) -> Vec<ExecutionBackendInventoryRecord> {
    vec![
        local_sandbox_inventory_record(policy),
        desktop_node_inventory_record(total_nodes, healthy_nodes, remote_node_enabled),
        ssh_tunnel_inventory_record(ssh_tunnel_enabled),
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
            reason: "No execution backend inventory is available.".to_owned(),
        };
    }

    if let Some(record) = requested_record {
        if record.selectable {
            return ExecutionBackendResolution {
                requested: preference,
                resolved: preference,
                fallback_used: false,
                reason: record.operator_summary.clone(),
            };
        }
    }

    if let Some(record) = local_record.filter(|entry| entry.selectable) {
        return ExecutionBackendResolution {
            requested: preference,
            resolved: ExecutionBackendPreference::LocalSandbox,
            fallback_used: true,
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
        rollout_enabled: true,
        capabilities,
        tradeoffs: vec![
            "Most conservative default posture".to_owned(),
            "Cannot satisfy first-party desktop-native capability requests by itself".to_owned(),
        ],
        active_node_count: 0,
        total_node_count: 0,
    }
}

fn desktop_node_inventory_record(
    total_nodes: usize,
    healthy_nodes: &[&RegisteredNodeRecord],
    rollout_enabled: bool,
) -> ExecutionBackendInventoryRecord {
    let capabilities = aggregate_node_capabilities(healthy_nodes);
    let (state, selectable, operator_summary) = if !rollout_enabled {
        (
            ExecutionBackendState::Disabled,
            false,
            format!(
                "Preview backend is disabled. Set {}=1 and keep at least one paired desktop node healthy before selecting it.",
                ENV_REMOTE_NODE_BACKEND_ENABLED
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
        rollout_flag: Some(ENV_REMOTE_NODE_BACKEND_ENABLED.to_owned()),
        rollout_enabled,
        capabilities,
        tradeoffs: vec![
            "Supports first-party desktop capabilities and local mediation flows".to_owned(),
            "Depends on node heartbeat, pairing trust, and explicit rollout opt-in".to_owned(),
        ],
        active_node_count: healthy_nodes.len(),
        total_node_count: total_nodes,
    }
}

fn ssh_tunnel_inventory_record(rollout_enabled: bool) -> ExecutionBackendInventoryRecord {
    ExecutionBackendInventoryRecord {
        backend_id: ExecutionBackendPreference::SshTunnel.as_str().to_owned(),
        label: ExecutionBackendPreference::SshTunnel.label().to_owned(),
        state: if rollout_enabled {
            ExecutionBackendState::Available
        } else {
            ExecutionBackendState::Disabled
        },
        selectable: rollout_enabled,
        selected_by_default: false,
        description: ExecutionBackendPreference::SshTunnel.description().to_owned(),
        operator_summary: if rollout_enabled {
            "Preview backend is enabled. Operators must still establish an explicit SSH forward before relying on remote control-plane flows."
                .to_owned()
        } else {
            format!(
                "Preview backend is disabled. Set {}=1 before advertising SSH tunnel workflows.",
                ENV_SSH_TUNNEL_BACKEND_ENABLED
            )
        },
        executor_label: None,
        rollout_flag: Some(ENV_SSH_TUNNEL_BACKEND_ENABLED.to_owned()),
        rollout_enabled,
        capabilities: vec![
            "verified_remote_dashboard_access".to_owned(),
            "operator_handoff".to_owned(),
        ],
        tradeoffs: vec![
            "Useful for explicit remote operator access and controlled handoff".to_owned(),
            "Requires manual tunnel setup and does not replace sandbox or node trust boundaries"
                .to_owned(),
        ],
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

fn parse_backend_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::sandbox_runner::{
        EgressEnforcementMode, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
    };

    use super::{
        build_execution_backend_inventory_with_rollout, resolve_execution_backend,
        validate_execution_backend_selection, ExecutionBackendPreference, ExecutionBackendState,
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
        let inventory =
            build_execution_backend_inventory_with_rollout(&test_policy(), 0, &[], false, false);
        let resolution =
            resolve_execution_backend(ExecutionBackendPreference::Automatic, &inventory);
        assert_eq!(resolution.resolved, ExecutionBackendPreference::LocalSandbox);
        assert!(!resolution.fallback_used);
    }

    #[test]
    fn preview_backend_selection_rejects_disabled_rollout() {
        let inventory =
            build_execution_backend_inventory_with_rollout(&test_policy(), 0, &[], false, false);
        let error = validate_execution_backend_selection(
            ExecutionBackendPreference::DesktopNode,
            &inventory,
        )
        .expect_err("disabled preview backend should be rejected");
        assert!(error.contains("desktop_node"), "unexpected error: {error}");
    }

    #[test]
    fn preview_backend_resolution_falls_back_to_local_sandbox() {
        let inventory =
            build_execution_backend_inventory_with_rollout(&test_policy(), 0, &[], false, true);
        let resolution =
            resolve_execution_backend(ExecutionBackendPreference::DesktopNode, &inventory);
        assert_eq!(resolution.resolved, ExecutionBackendPreference::LocalSandbox);
        assert!(resolution.fallback_used);
    }

    #[test]
    fn preview_backend_inventory_is_degraded_without_healthy_nodes() {
        let inventory =
            build_execution_backend_inventory_with_rollout(&test_policy(), 1, &[], true, false);
        let desktop_node = inventory
            .iter()
            .find(|entry| entry.backend_id == ExecutionBackendPreference::DesktopNode.as_str())
            .expect("desktop node backend should exist");
        assert_eq!(desktop_node.state, ExecutionBackendState::Degraded);
        assert!(desktop_node.rollout_enabled);
        assert!(!desktop_node.selectable);
    }
}
