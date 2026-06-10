//! Serde data model for skill manifests, signatures, trust stores, verification
//! reports, and security-audit results.
//!
//! Field names and enum value spellings are wire/CLI contracts pinned by golden
//! fixtures and persisted artifacts — never rename or re-case them. Manifest
//! structs use `deny_unknown_fields` so unknown input fails closed.

use std::collections::BTreeMap;

use palyra_plugins_sdk::TypedPluginContractDeclaration;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::constants::{
    default_manifest_version, default_operator_config_schema_version, default_quota_fuel_budget,
    default_quota_max_memory, default_quota_timeout_ms, DEFAULT_SKILL_AUDIT_MAX_EXPORTED_FUNCTIONS,
    DEFAULT_SKILL_AUDIT_MAX_MODULE_BYTES,
};

/// Parsed `skill.toml` manifest describing identity, entrypoints, capabilities,
/// compatibility, and operator metadata for one skill.
///
/// Construct via [`crate::parse_manifest_toml`], which also enforces the
/// validation rules that raw deserialization cannot express (identifier
/// grammar, namespacing, wildcard opt-ins, quota floors).
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

/// Tool entrypoints exposed by a skill; validation requires at least one.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SkillEntrypoints {
    pub tools: Vec<SkillToolEntrypoint>,
}

/// One callable tool declared by the manifest, with JSON I/O schemas and risk flags.
///
/// Tool ids must be namespaced under the publisher (`<publisher>.<name>`).
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

/// Risk flags for one tool; either flag forces approval in policy bindings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillToolRisk {
    #[serde(default)]
    pub default_sensitive: bool,
    #[serde(default)]
    pub requires_approval: bool,
}

/// Least-privilege capability declarations requested by a skill.
///
/// Wildcard values (`*`) in any list are rejected during validation unless the
/// matching [`SkillWildcardOptIn`] flag is set, keeping broad grants an
/// explicit, auditable choice.
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

/// Relative filesystem roots a skill may read from or write to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillFilesystemCapabilities {
    #[serde(default)]
    pub read_roots: Vec<String>,
    #[serde(default)]
    pub write_roots: Vec<String>,
}

/// Vault secret keys requested within one scope
/// (`global`, `principal:<id>`, `channel:<id>`, or `skill:<id>`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillSecretScope {
    pub scope: String,
    #[serde(default)]
    pub key_names: Vec<String>,
}

/// Per-capability-class opt-in flags that unlock wildcard (`*`) values.
///
/// Defaults to all-false so wildcard grants always require an explicit
/// manifest declaration; the security audit additionally quarantines wildcard
/// use unless its policy allows it.
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

/// Execution quotas (wall clock, fuel, memory) applied to skill module runs.
///
/// Validation requires non-zero timeout/fuel and at least 64 KiB of memory.
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

/// Host compatibility range declared by the skill.
///
/// The serde aliases keep manifests written against the v1 field names
/// (`min_protocol_major`, `min_runtime_version`) parseable; serialization
/// always emits the current names.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillCompat {
    #[serde(alias = "min_protocol_major")]
    pub required_protocol_major: u32,
    #[serde(alias = "min_runtime_version")]
    pub min_palyra_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_palyra_version: Option<String>,
}

/// Per-file SHA-256 integrity manifest, populated by the packager and
/// cross-checked against actual entry contents during verification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillIntegrity {
    #[serde(default)]
    pub files: Vec<SkillIntegrityEntry>,
}

/// SHA-256 digest (lowercase hex) of one artifact payload entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillIntegrityEntry {
    pub path: String,
    pub sha256: String,
}

/// Metadata attached to skills generated by the experimental builder pipeline.
///
/// `experimental` must stay `true`; builder outputs only leave review through
/// the lifecycle gates, never by editing this flag.
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

/// Review checklist artifact paths required for builder-generated skills.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillBuilderChecklist {
    pub capability_declaration_path: String,
    pub provenance_path: String,
    pub test_harness_path: String,
    #[serde(default)]
    pub review_notes: String,
}

/// Operator-facing display metadata (manifest v2+): names, docs, plugin
/// defaults, and the optional configuration contract.
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
    /// Returns `true` when no operator metadata field is set.
    ///
    /// Legacy (v1) manifests must be empty here; validation rejects v1
    /// manifests that carry operator metadata.
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

/// Default plugin wiring (tool, module, entrypoint) and typed contract
/// declarations surfaced to the plugin runtime.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillPluginMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_tool_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_module_path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_entrypoint: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub contracts: Vec<TypedPluginContractDeclaration>,
}

impl SkillPluginMetadata {
    /// Returns `true` when no plugin default or contract declaration is set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.default_tool_id.is_none()
            && self.default_module_path.is_none()
            && self.default_entrypoint.is_none()
            && self.contracts.is_empty()
    }
}

/// Operator configuration contract: required keys plus typed property schema.
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

/// One operator-config property: declared type, optional default and labels.
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
    /// Marks the value as secret so console/doctor output masks it.
    #[serde(default)]
    pub redacted: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_values: Vec<String>,
}

/// Value types accepted for operator-config properties.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillConfigValueType {
    String,
    Integer,
    Number,
    Boolean,
    StringList,
}

/// Severity of a non-fatal manifest finding.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillManifestWarningSeverity {
    Warning,
    Error,
}

/// Non-fatal manifest finding (e.g. legacy version, missing operator metadata)
/// surfaced through verification reports and audit checks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillManifestWarning {
    pub code: String,
    pub severity: SkillManifestWarningSeverity,
    pub message: String,
}

/// Detached signature document stored as `signature.json` inside the artifact.
///
/// The Ed25519 signature is computed over the ASCII hex `payload_sha256`
/// digest, which itself covers every artifact entry except `signature.json`.
/// The embedded public key is self-asserted; authenticity is established only
/// by the trust-store check in [`crate::verify_skill_artifact`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillArtifactSignature {
    pub algorithm: String,
    pub publisher: String,
    /// Short key identifier (`ed25519:<hex>`); informational, not a trust input.
    pub key_id: String,
    pub public_key_base64: String,
    pub payload_sha256: String,
    pub signature_base64: String,
    pub signed_at_unix_ms: i64,
}

/// Inputs for [`crate::build_signed_skill_artifact`].
///
/// INTENTIONAL: no `Debug`/`Serialize` derives — `signing_key` is private key
/// material and must never reach logs or serialized output.
#[derive(Clone)]
pub struct SkillArtifactBuildRequest {
    pub manifest_toml: String,
    pub modules: Vec<ArtifactFile>,
    pub assets: Vec<ArtifactFile>,
    pub sbom_cyclonedx_json: Vec<u8>,
    pub provenance_json: Vec<u8>,
    /// Raw Ed25519 secret key; parse text encodings with
    /// [`crate::parse_ed25519_signing_key`] first.
    pub signing_key: [u8; 32],
}

/// One module or asset file to package, addressed by artifact-relative path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactFile {
    pub path: String,
    pub bytes: Vec<u8>,
}

/// Result of a successful artifact build: bytes, normalized manifest,
/// canonical payload digest, and the embedded signature document.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SkillArtifactBuildOutput {
    pub artifact_bytes: Vec<u8>,
    pub manifest: SkillManifest,
    pub payload_sha256: String,
    pub signature: SkillArtifactSignature,
}

/// Publisher trust state: operator-curated allowlist plus TOFU pins.
///
/// An allowlist entry for a publisher takes precedence over (and disables) the
/// TOFU path for that publisher. Keys are lowercase hex Ed25519 public keys.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct SkillTrustStore {
    /// Publisher id -> explicitly allowlisted verifying keys.
    #[serde(default)]
    pub trusted_publishers: BTreeMap<String, Vec<String>>,
    /// Publisher id -> single key pinned on first use.
    #[serde(default)]
    pub tofu_publishers: BTreeMap<String, String>,
}

/// How a publisher key was accepted during verification.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TrustDecision {
    /// Key matched the operator-curated allowlist.
    Allowlisted,
    /// Key matched an existing trust-on-first-use pin.
    TofuPinned,
    /// Key was pinned for the first time during this verification.
    TofuNewlyPinned,
}

/// Flattened capability grants derived from a manifest, in the shape consumed
/// by the plugin runtime broker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillCapabilityGrantSnapshot {
    pub http_hosts: Vec<String>,
    pub secret_keys: Vec<String>,
    pub storage_prefixes: Vec<String>,
    pub channels: Vec<String>,
}

/// One deny-by-default policy binding (action on resource) derived from a
/// manifest tool or capability declaration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillPolicyBinding {
    pub action: String,
    pub resource: String,
    pub requires_approval: bool,
}

/// Journal event recorded when an artifact passes verification.
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

/// Successful verification outcome with derived grants and policy bindings.
///
/// Only produced when every check passed (`accepted` is always `true`);
/// failures surface as [`crate::SkillPackagingError`] instead of a report.
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

/// Outcome of one security-audit check; any `Fail` quarantines the artifact.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillAuditCheckStatus {
    Pass,
    Warn,
    Fail,
    Skipped,
}

/// Severity attached to a security-audit check result.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SkillAuditSeverity {
    Info,
    Warning,
    Error,
}

/// One named security-audit check with status, message, and optional details.
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

/// Operator-tunable limits and overrides for the security audit.
///
/// Defaults are restrictive: device and wildcard capabilities quarantine
/// unless explicitly allowed here.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillSecurityAuditPolicy {
    #[serde(default = "default_skill_audit_max_module_bytes")]
    pub max_module_bytes: u64,
    #[serde(default = "default_skill_audit_max_exported_functions")]
    pub max_exported_functions: usize,
    #[serde(default)]
    pub allow_device_capabilities: bool,
    #[serde(default)]
    pub allow_wildcard_capabilities: bool,
}

fn default_skill_audit_max_module_bytes() -> u64 {
    DEFAULT_SKILL_AUDIT_MAX_MODULE_BYTES
}

fn default_skill_audit_max_exported_functions() -> usize {
    DEFAULT_SKILL_AUDIT_MAX_EXPORTED_FUNCTIONS
}

impl Default for SkillSecurityAuditPolicy {
    fn default() -> Self {
        Self {
            max_module_bytes: DEFAULT_SKILL_AUDIT_MAX_MODULE_BYTES,
            max_exported_functions: DEFAULT_SKILL_AUDIT_MAX_EXPORTED_FUNCTIONS,
            allow_device_capabilities: false,
            allow_wildcard_capabilities: false,
        }
    }
}

/// Full security-audit result for one artifact.
///
/// `should_quarantine` is fail-closed: it is `true` whenever any check failed
/// (`passed == false`), so callers can never enable a failing artifact by
/// reading only one flag.
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

/// Signature- and integrity-checked artifact contents (internal verify result).
#[derive(Debug, Clone)]
pub(crate) struct ParsedArtifact {
    pub(crate) manifest: SkillManifest,
    pub(crate) signature: SkillArtifactSignature,
    pub(crate) payload_sha256: String,
}

/// Verified artifact view exposed to installers: manifest, signature, warnings,
/// and the decoded entries keyed by normalized path.
///
/// Produced only after signature and integrity checks pass, so `entries`
/// contents are safe to extract.
#[derive(Debug, Clone, PartialEq)]
pub struct SkillArtifactInspection {
    pub manifest: SkillManifest,
    pub signature: SkillArtifactSignature,
    pub payload_sha256: String,
    pub manifest_warnings: Vec<SkillManifestWarning>,
    pub entries: BTreeMap<String, Vec<u8>>,
}
