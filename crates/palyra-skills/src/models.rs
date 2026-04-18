use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::constants::{
    default_manifest_version, default_operator_config_schema_version, default_quota_fuel_budget,
    default_quota_max_memory, default_quota_timeout_ms, DEFAULT_SKILL_AUDIT_MAX_EXPORTED_FUNCTIONS,
    DEFAULT_SKILL_AUDIT_MAX_MODULE_BYTES,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillManifest {
    #[serde(default = "default_manifest_version")]
    pub manifest_version: u32,
    pub skill_id: String,
    pub name: String,
    pub version: String,
    pub publisher: String,
    pub entrypoints: SkillEntrypoints,
    #[serde(default)]
    pub capabilities: SkillCapabilities,
    pub compat: SkillCompat,
    #[serde(default)]
    pub integrity: SkillIntegrity,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub builder: Option<SkillBuilderMetadata>,
    #[serde(default)]
    pub operator: SkillOperatorMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillEntrypoints {
    pub tools: Vec<SkillToolEntrypoint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillToolEntrypoint {
    pub id: String,
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,
    #[serde(default)]
    pub risk: SkillToolRisk,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillToolRisk {
    #[serde(default)]
    pub default_sensitive: bool,
    #[serde(default)]
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillCapabilities {
    #[serde(default)]
    pub filesystem: SkillFilesystemCapabilities,
    #[serde(default)]
    pub http_egress_allowlist: Vec<String>,
    #[serde(default)]
    pub secrets: Vec<SkillSecretScope>,
    #[serde(default)]
    pub device_capabilities: Vec<String>,
    #[serde(default)]
    pub node_capabilities: Vec<String>,
    #[serde(default)]
    pub quotas: SkillQuotaConfig,
    #[serde(default)]
    pub wildcard_opt_in: SkillWildcardOptIn,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillFilesystemCapabilities {
    #[serde(default)]
    pub read_roots: Vec<String>,
    #[serde(default)]
    pub write_roots: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillSecretScope {
    pub scope: String,
    #[serde(default)]
    pub key_names: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillWildcardOptIn {
    #[serde(default)]
    pub filesystem: bool,
    #[serde(default)]
    pub http_egress: bool,
    #[serde(default)]
    pub secrets: bool,
    #[serde(default)]
    pub device: bool,
    #[serde(default)]
    pub node: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillQuotaConfig {
    #[serde(default = "default_quota_timeout_ms")]
    pub wall_clock_timeout_ms: u64,
    #[serde(default = "default_quota_fuel_budget")]
    pub fuel_budget: u64,
    #[serde(default = "default_quota_max_memory")]
    pub max_memory_bytes: u64,
}

impl Default for SkillQuotaConfig {
    fn default() -> Self {
        Self {
            wall_clock_timeout_ms: default_quota_timeout_ms(),
            fuel_budget: default_quota_fuel_budget(),
            max_memory_bytes: default_quota_max_memory(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillCompat {
    #[serde(alias = "min_protocol_major")]
    pub required_protocol_major: u32,
    #[serde(alias = "min_runtime_version")]
    pub min_palyra_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillIntegrity {
    #[serde(default)]
    pub files: Vec<SkillIntegrityEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillIntegrityEntry {
    pub path: String,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillBuilderMetadata {
    pub experimental: bool,
    pub source_kind: String,
    pub source_ref: String,
    pub rollout_flag: String,
    #[serde(default)]
    pub review_status: String,
    pub checklist: SkillBuilderChecklist,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillBuilderChecklist {
    pub capability_declaration_path: String,
    pub provenance_path: String,
    pub test_harness_path: String,
    #[serde(default)]
    pub review_notes: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillOperatorMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub docs_url: Option<String>,
    #[serde(default)]
    pub plugin: SkillPluginMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<SkillConfigContract>,
}

impl SkillOperatorMetadata {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.display_name.is_none()
            && self.summary.is_none()
            && self.description.is_none()
            && self.categories.is_empty()
            && self.tags.is_empty()
            && self.docs_url.is_none()
            && self.plugin.is_empty()
            && self.config.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillPluginMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_tool_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_module_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_entrypoint: Option<String>,
}

impl SkillPluginMetadata {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.default_tool_id.is_none()
            && self.default_module_path.is_none()
            && self.default_entrypoint.is_none()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillConfigContract {
    #[serde(default = "default_operator_config_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub required: Vec<String>,
    #[serde(default)]
    pub properties: BTreeMap<String, SkillConfigProperty>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillConfigProperty {
    #[serde(rename = "type")]
    pub value_type: SkillConfigValueType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(default)]
    pub redacted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_values: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillConfigValueType {
    String,
    Integer,
    Number,
    Boolean,
    StringList,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillManifestWarningSeverity {
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillManifestWarning {
    pub code: String,
    pub severity: SkillManifestWarningSeverity,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillArtifactSignature {
    pub algorithm: String,
    pub publisher: String,
    pub key_id: String,
    pub public_key_base64: String,
    pub payload_sha256: String,
    pub signature_base64: String,
    pub signed_at_unix_ms: i64,
}

#[derive(Clone)]
pub struct SkillArtifactBuildRequest {
    pub manifest_toml: String,
    pub modules: Vec<ArtifactFile>,
    pub assets: Vec<ArtifactFile>,
    pub sbom_cyclonedx_json: Vec<u8>,
    pub provenance_json: Vec<u8>,
    pub signing_key: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillArtifactBuildOutput {
    pub artifact_bytes: Vec<u8>,
    pub manifest: SkillManifest,
    pub payload_sha256: String,
    pub signature: SkillArtifactSignature,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillTrustStore {
    #[serde(default)]
    pub trusted_publishers: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub tofu_publishers: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustDecision {
    Allowlisted,
    TofuPinned,
    TofuNewlyPinned,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillCapabilityGrantSnapshot {
    pub http_hosts: Vec<String>,
    pub secret_keys: Vec<String>,
    pub storage_prefixes: Vec<String>,
    pub channels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillPolicyBinding {
    pub action: String,
    pub resource: String,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillVerificationAuditEvent {
    pub event_kind: String,
    pub skill_id: String,
    pub publisher: String,
    pub version: String,
    pub payload_sha256: String,
    pub trust_decision: TrustDecision,
    pub verified_at_unix_ms: i64,
    pub policy_bindings: Vec<SkillPolicyBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillVerificationReport {
    pub accepted: bool,
    pub trust_decision: TrustDecision,
    pub payload_sha256: String,
    pub manifest: SkillManifest,
    #[serde(default)]
    pub manifest_warnings: Vec<SkillManifestWarning>,
    pub capability_grants: SkillCapabilityGrantSnapshot,
    pub policy_bindings: Vec<SkillPolicyBinding>,
    pub audit_event: SkillVerificationAuditEvent,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillAuditCheckStatus {
    Pass,
    Warn,
    Fail,
    Skipped,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillAuditSeverity {
    Info,
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillSecurityAuditCheck {
    pub check_id: String,
    pub status: SkillAuditCheckStatus,
    pub severity: SkillAuditSeverity,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillSecurityAuditPolicy {
    pub max_module_bytes: u64,
    pub max_exported_functions: usize,
}

impl Default for SkillSecurityAuditPolicy {
    fn default() -> Self {
        Self {
            max_module_bytes: DEFAULT_SKILL_AUDIT_MAX_MODULE_BYTES,
            max_exported_functions: DEFAULT_SKILL_AUDIT_MAX_EXPORTED_FUNCTIONS,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillSecurityAuditReport {
    pub skill_id: String,
    pub version: String,
    pub publisher: String,
    pub accepted: bool,
    pub passed: bool,
    pub should_quarantine: bool,
    pub trust_decision: TrustDecision,
    pub payload_sha256: String,
    pub generated_at_unix_ms: i64,
    pub policy: SkillSecurityAuditPolicy,
    #[serde(default)]
    pub manifest_warnings: Vec<SkillManifestWarning>,
    pub checks: Vec<SkillSecurityAuditCheck>,
    pub quarantine_reasons: Vec<String>,
    pub vulnerability_scan: SkillSecurityAuditCheck,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedArtifact {
    pub(crate) manifest: SkillManifest,
    pub(crate) signature: SkillArtifactSignature,
    pub(crate) payload_sha256: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SkillArtifactInspection {
    pub manifest: SkillManifest,
    pub signature: SkillArtifactSignature,
    pub payload_sha256: String,
    pub manifest_warnings: Vec<SkillManifestWarning>,
    pub entries: BTreeMap<String, Vec<u8>>,
}
