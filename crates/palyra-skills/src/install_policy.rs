//! Fail-closed install policy for skills, plugins, and external extension packages.
//!
//! The evaluator is pure: callers supply the artifact digest, manifest, trust
//! pins, capability grants, and optional operator-hook metadata, and receive a
//! bounded decision plus a journal-ready audit event.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::models::{SkillManifest, SkillPluginCapabilityRequirement, SkillPluginRiskClass};

/// Kind of extension package being installed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionInstallSubjectKind {
    Skill,
    Plugin,
    ExternalPackage,
}

/// Host-owned install policy pins and risk thresholds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionInstallPolicy {
    #[serde(default)]
    pub trusted_publisher_keys: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub package_digest_allowlist: Vec<String>,
    #[serde(default)]
    pub repo_allowlist: Vec<String>,
    #[serde(default)]
    pub local_path_allowlist: Vec<String>,
    #[serde(default)]
    pub capability_profile_allowlist: Vec<SkillPluginCapabilityRequirement>,
    #[serde(default = "default_max_operator_hook_timeout_ms")]
    pub max_operator_hook_timeout_ms: u64,
}

impl Default for ExtensionInstallPolicy {
    fn default() -> Self {
        Self {
            trusted_publisher_keys: BTreeMap::new(),
            package_digest_allowlist: Vec::new(),
            repo_allowlist: Vec::new(),
            local_path_allowlist: Vec::new(),
            capability_profile_allowlist: Vec::new(),
            max_operator_hook_timeout_ms: default_max_operator_hook_timeout_ms(),
        }
    }
}

/// Inputs for one install-policy decision.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionInstallPolicyRequest {
    pub subject_kind: ExtensionInstallSubjectKind,
    pub actor: String,
    pub manifest: SkillManifest,
    pub package_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publisher_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_repo: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_local_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator_hook_timeout_ms: Option<u64>,
    #[serde(default)]
    pub requested_capabilities: Vec<SkillPluginCapabilityRequirement>,
}

/// Final install decision.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionInstallDecisionKind {
    Allow,
    Deny,
}

/// Install risk summary shown to operators before grants are persisted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionInstallRiskSummary {
    pub risk: SkillPluginRiskClass,
    pub secret_scope_count: usize,
    pub outbound_host_count: usize,
    pub storage_prefix_count: usize,
    pub event_subscription_count: usize,
    pub required_capability_count: usize,
    pub optional_capability_count: usize,
}

/// One install-policy finding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionInstallPolicyFinding {
    pub code: String,
    pub message: String,
    pub fix_hint: String,
}

/// Journal-ready audit event for install decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionInstallAuditEvent {
    pub event_kind: String,
    pub actor: String,
    pub skill_id: String,
    pub publisher: String,
    pub package_digest: String,
    pub manifest_digest: String,
    pub decision: ExtensionInstallDecisionKind,
    pub grants: Vec<SkillPluginCapabilityRequirement>,
    pub risk: ExtensionInstallRiskSummary,
}

/// Complete install-policy result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionInstallPolicyDecision {
    pub schema_version: u32,
    pub accepted: bool,
    pub decision: ExtensionInstallDecisionKind,
    pub risk: ExtensionInstallRiskSummary,
    pub findings: Vec<ExtensionInstallPolicyFinding>,
    pub audit_event: ExtensionInstallAuditEvent,
}

/// Evaluates install policy without side effects.
#[must_use]
pub fn evaluate_extension_install_policy(
    policy: &ExtensionInstallPolicy,
    request: &ExtensionInstallPolicyRequest,
) -> ExtensionInstallPolicyDecision {
    let mut findings = Vec::new();
    if request.actor.trim().is_empty() {
        findings.push(finding(
            "install.actor_missing",
            "install actor is missing",
            "provide the authenticated operator principal",
        ));
    }
    if !digest_allowed(request.package_digest.as_str(), policy.package_digest_allowlist.as_slice())
        && !publisher_key_allowed(
            request.manifest.publisher.as_str(),
            request.publisher_key.as_deref(),
            &policy.trusted_publisher_keys,
        )
        && !source_allowed(request.source_repo.as_deref(), policy.repo_allowlist.as_slice())
        && !source_allowed(
            request.source_local_path.as_deref(),
            policy.local_path_allowlist.as_slice(),
        )
    {
        findings.push(finding(
            "install.trust_pin_missing",
            "no publisher key, package digest, repository, or local path trust pin matched",
            "pin the publisher key or exact package digest before installing",
        ));
    }
    if let Some(timeout_ms) = request.operator_hook_timeout_ms {
        if timeout_ms > policy.max_operator_hook_timeout_ms {
            findings.push(finding(
                "install.operator_hook_timeout_exceeded",
                "operator install hook timeout exceeds host policy",
                "reduce the hook timeout or update host policy deliberately",
            ));
        }
    }
    for capability in &request.requested_capabilities {
        if !capability_allowed(capability, policy.capability_profile_allowlist.as_slice()) {
            findings.push(finding(
                "install.capability_not_allowlisted",
                format!(
                    "requested capability {:?}:{} is not allowlisted",
                    capability.class, capability.value
                )
                .as_str(),
                "add the capability to an approved profile or remove it from the install request",
            ));
        }
    }
    if request.manifest.operator.plugin.risk == SkillPluginRiskClass::Privileged
        && request.requested_capabilities.is_empty()
    {
        findings.push(finding(
            "install.privileged_without_grants",
            "privileged plugin install did not enumerate requested capabilities",
            "include every requested grant in the install request",
        ));
    }
    let risk = risk_summary(&request.manifest);
    let accepted = findings.is_empty();
    let decision = if accepted {
        ExtensionInstallDecisionKind::Allow
    } else {
        ExtensionInstallDecisionKind::Deny
    };
    let audit_event = ExtensionInstallAuditEvent {
        event_kind: "extension.install_policy_decision".to_owned(),
        actor: request.actor.clone(),
        skill_id: request.manifest.skill_id.clone(),
        publisher: request.manifest.publisher.clone(),
        package_digest: request.package_digest.clone(),
        manifest_digest: manifest_digest(&request.manifest),
        decision,
        grants: request.requested_capabilities.clone(),
        risk: risk.clone(),
    };
    ExtensionInstallPolicyDecision {
        schema_version: 1,
        accepted,
        decision,
        risk,
        findings,
        audit_event,
    }
}

/// Builds the risk summary directly from manifest plugin metadata.
#[must_use]
pub fn risk_summary(manifest: &SkillManifest) -> ExtensionInstallRiskSummary {
    let plugin = &manifest.operator.plugin;
    ExtensionInstallRiskSummary {
        risk: plugin.risk,
        secret_scope_count: plugin.secret_scopes.len(),
        outbound_host_count: plugin.outbound_hosts.len(),
        storage_prefix_count: plugin.storage_prefixes.len(),
        event_subscription_count: plugin.event_subscriptions.len(),
        required_capability_count: plugin.required_capabilities.len(),
        optional_capability_count: plugin.optional_capabilities.len(),
    }
}

fn digest_allowed(digest: &str, allowlist: &[String]) -> bool {
    let normalized = digest.trim().to_ascii_lowercase();
    !normalized.is_empty()
        && allowlist.iter().any(|allowed| allowed.trim().to_ascii_lowercase() == normalized)
}

fn publisher_key_allowed(
    publisher: &str,
    key: Option<&str>,
    trusted: &BTreeMap<String, Vec<String>>,
) -> bool {
    let Some(key) = key.map(|key| key.trim().to_ascii_lowercase()).filter(|key| !key.is_empty())
    else {
        return false;
    };
    trusted
        .get(publisher)
        .is_some_and(|keys| keys.iter().any(|trusted| trusted.trim().to_ascii_lowercase() == key))
}

fn source_allowed(source: Option<&str>, allowlist: &[String]) -> bool {
    let Some(source) = source.map(str::trim).filter(|source| !source.is_empty()) else {
        return false;
    };
    allowlist.iter().any(|allowed| allowed.trim() == source)
}

fn capability_allowed(
    capability: &SkillPluginCapabilityRequirement,
    allowlist: &[SkillPluginCapabilityRequirement],
) -> bool {
    allowlist.iter().any(|allowed| allowed == capability)
}

fn manifest_digest(manifest: &SkillManifest) -> String {
    let bytes = serde_json::to_vec(manifest).unwrap_or_else(|_| b"null".to_vec());
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn finding(code: &str, message: &str, fix_hint: &str) -> ExtensionInstallPolicyFinding {
    ExtensionInstallPolicyFinding {
        code: code.to_owned(),
        message: message.to_owned(),
        fix_hint: fix_hint.to_owned(),
    }
}

fn default_max_operator_hook_timeout_ms() -> u64 {
    5_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse_manifest_toml, SkillPluginCapabilityClass};

    fn manifest() -> SkillManifest {
        parse_manifest_toml(
            r#"
manifest_version = 2
skill_id = "acme.plugin"
name = "Plugin"
version = "1.0.0"
publisher = "acme"

[entrypoints]
[[entrypoints.tools]]
id = "acme.echo"
name = "echo"
description = "Echo payload"
input_schema = { type = "object", properties = { text = { type = "string" } } }
output_schema = { type = "object", properties = { echo = { type = "string" } } }

[compat]
required_protocol_major = 1
min_palyra_version = "0.1.0"

[operator]
display_name = "Plugin"

[operator.plugin]
plugin_id = "acme.plugin"
abi_major = 1
risk = "privileged"
default_tool_id = "acme.echo"
default_module_path = "modules/plugin.wasm"
default_entrypoint = "run"
outbound_hosts = ["api.example.com"]
secret_scopes = ["skill:acme.plugin"]

[[operator.plugin.required_capabilities]]
class = "http_host"
value = "api.example.com"
"#,
        )
        .expect("manifest should parse")
    }

    #[test]
    fn install_policy_allows_pinned_digest_and_capability() {
        let capability = SkillPluginCapabilityRequirement {
            class: SkillPluginCapabilityClass::HttpHost,
            value: "api.example.com".to_owned(),
        };
        let policy = ExtensionInstallPolicy {
            package_digest_allowlist: vec!["sha256:abc".to_owned()],
            capability_profile_allowlist: vec![capability.clone()],
            ..ExtensionInstallPolicy::default()
        };
        let request = ExtensionInstallPolicyRequest {
            subject_kind: ExtensionInstallSubjectKind::Plugin,
            actor: "user:test".to_owned(),
            manifest: manifest(),
            package_digest: "sha256:abc".to_owned(),
            publisher_key: None,
            source_repo: None,
            source_local_path: None,
            operator_hook_timeout_ms: Some(1_000),
            requested_capabilities: vec![capability],
        };

        let decision = evaluate_extension_install_policy(&policy, &request);

        assert!(decision.accepted);
        assert_eq!(decision.decision, ExtensionInstallDecisionKind::Allow);
        assert_eq!(decision.audit_event.actor, "user:test");
        assert_eq!(decision.risk.outbound_host_count, 1);
    }

    #[test]
    fn install_policy_denies_missing_trust_pin_and_hook_timeout() {
        let request = ExtensionInstallPolicyRequest {
            subject_kind: ExtensionInstallSubjectKind::Plugin,
            actor: "user:test".to_owned(),
            manifest: manifest(),
            package_digest: "sha256:untrusted".to_owned(),
            publisher_key: None,
            source_repo: None,
            source_local_path: None,
            operator_hook_timeout_ms: Some(10_000),
            requested_capabilities: Vec::new(),
        };

        let decision =
            evaluate_extension_install_policy(&ExtensionInstallPolicy::default(), &request);

        assert!(!decision.accepted);
        assert!(decision
            .findings
            .iter()
            .any(|finding| finding.code == "install.trust_pin_missing"));
        assert!(decision
            .findings
            .iter()
            .any(|finding| finding.code == "install.operator_hook_timeout_exceeded"));
    }
}
