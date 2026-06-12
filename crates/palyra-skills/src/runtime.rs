//! Projection of validated manifests into runtime capability grants and
//! deny-by-default policy bindings/requests.
//!
//! Outputs are sorted and deduplicated so downstream policy evaluation and
//! contract snapshots stay deterministic.

use std::collections::BTreeMap;

use palyra_plugins_runtime::CapabilityGrantSet;
use palyra_policy::PolicyRequest;

use crate::models::{SkillCapabilityGrantSnapshot, SkillManifest, SkillPolicyBinding};

/// Flattens manifest capabilities into the runtime grant snapshot.
///
/// Secret keys are emitted as `<scope>/<key>`; only filesystem *write* roots
/// become storage prefixes (read roots are not part of the runtime grant set).
pub fn capability_grants_from_manifest(manifest: &SkillManifest) -> SkillCapabilityGrantSnapshot {
    let mut secret_keys = manifest
        .capabilities
        .secrets
        .iter()
        .flat_map(|scope| {
            scope.key_names.iter().map(|key| format!("{}/{}", scope.scope, key)).collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    secret_keys.sort();
    secret_keys.dedup();

    SkillCapabilityGrantSnapshot {
        http_hosts: dedupe_sorted(manifest.capabilities.http_egress_allowlist.as_slice()),
        secret_keys,
        storage_prefixes: dedupe_sorted(manifest.capabilities.filesystem.write_roots.as_slice()),
        // Channel grants are intentionally not projected into this persisted
        // snapshot; extension capability grants carry node/channel capabilities.
        channels: Vec::new(),
    }
}

/// Derives deduplicated policy bindings: one `tool.execute` binding per tool
/// plus one approval-required binding per used capability class.
///
/// Capability bindings always set `requires_approval = true`; tool bindings
/// inherit the tool's risk flags.
#[must_use]
pub fn policy_bindings_from_manifest(manifest: &SkillManifest) -> Vec<SkillPolicyBinding> {
    let mut bindings = manifest
        .entrypoints
        .tools
        .iter()
        .map(|tool| SkillPolicyBinding {
            action: "tool.execute".to_owned(),
            resource: format!("tool:{}", tool.id),
            requires_approval: tool.risk.default_sensitive || tool.risk.requires_approval,
        })
        .collect::<Vec<_>>();

    let capability_resource = format!("skill:{}", manifest.skill_id);
    if !manifest.capabilities.http_egress_allowlist.is_empty() {
        bindings.push(SkillPolicyBinding {
            action: "skill.capability.http.egress".to_owned(),
            resource: capability_resource.clone(),
            requires_approval: true,
        });
    }
    if !manifest.capabilities.filesystem.write_roots.is_empty() {
        bindings.push(SkillPolicyBinding {
            action: "skill.capability.filesystem.write".to_owned(),
            resource: capability_resource.clone(),
            requires_approval: true,
        });
    }
    if !manifest.capabilities.secrets.is_empty() {
        bindings.push(SkillPolicyBinding {
            action: "skill.capability.vault.read".to_owned(),
            resource: capability_resource.clone(),
            requires_approval: true,
        });
    }
    if !manifest.capabilities.device_capabilities.is_empty()
        || !manifest.capabilities.node_capabilities.is_empty()
    {
        bindings.push(SkillPolicyBinding {
            action: "skill.capability.device.use".to_owned(),
            resource: capability_resource,
            requires_approval: true,
        });
    }

    let mut deduped = BTreeMap::new();
    for binding in bindings {
        deduped.insert(
            (binding.action.clone(), binding.resource.clone(), binding.requires_approval),
            binding,
        );
    }
    deduped.into_values().collect()
}

/// Converts the manifest's policy bindings into evaluable policy requests
/// with the skill itself (`skill:<id>`) as principal.
#[must_use]
pub fn policy_requests_from_manifest(manifest: &SkillManifest) -> Vec<PolicyRequest> {
    let principal = format!("skill:{}", manifest.skill_id);
    policy_bindings_from_manifest(manifest)
        .into_iter()
        .map(|binding| PolicyRequest {
            principal: principal.clone(),
            action: binding.action,
            resource: binding.resource,
        })
        .collect()
}

impl SkillCapabilityGrantSnapshot {
    /// Converts the snapshot into the plugin runtime's canonicalized grant set.
    #[must_use]
    pub fn to_runtime_capability_grants(&self) -> CapabilityGrantSet {
        CapabilityGrantSet {
            http_hosts: self.http_hosts.clone(),
            secret_keys: self.secret_keys.clone(),
            storage_prefixes: self.storage_prefixes.clone(),
            channels: self.channels.clone(),
        }
        .canonicalized()
    }
}

fn dedupe_sorted(values: &[String]) -> Vec<String> {
    let mut normalized = values
        .iter()
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}
