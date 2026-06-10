//! Extension lifecycle layer above raw skill artifacts: registry records,
//! capability grant diffs, compatibility/doctor preflight, enable/eval gates,
//! and self-improvement rollback planning.
//!
//! Lifecycle transitions are an explicit allowlisted state machine
//! (`is_lifecycle_transition_allowed`), and quarantine release requires a
//! reviewer distinct from the acting principal (four-eyes rule).

use std::collections::{BTreeMap, BTreeSet};

use palyra_common::{build_metadata, CANONICAL_PROTOCOL_MAJOR};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::artifact::now_unix_ms;
use crate::error::SkillPackagingError;
use crate::manifest::assert_runtime_compatibility;
use crate::models::{
    SkillArtifactInspection, SkillAuditCheckStatus, SkillCapabilityGrantSnapshot, SkillManifest,
    SkillSecurityAuditPolicy, SkillTrustStore,
};
use crate::runtime::capability_grants_from_manifest;
use crate::{audit_skill_artifact_security, inspect_skill_artifact};

/// Stable package kind used by the extension lifecycle registry.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionPackageKind {
    Skill,
    Plugin,
    GeneratedSkill,
}

/// Durable lifecycle status for extension packages.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionPackageStatus {
    Installed,
    Verified,
    Quarantined,
    Enabled,
    Disabled,
    Failed,
    RolledBack,
    Removed,
}

/// Source descriptor for an extension artifact or registry record.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionPackageSource {
    pub kind: String,
    pub reference: String,
}

impl ExtensionPackageSource {
    /// Builds a deterministic local-artifact source descriptor.
    #[must_use]
    pub fn local_artifact(reference: impl Into<String>) -> Self {
        Self { kind: "local_artifact".to_owned(), reference: reference.into() }
    }
}

/// Machine-readable extension compatibility issue.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionCompatibilityIssue {
    pub code: String,
    pub message: String,
    pub repair_hint: String,
}

/// Compatibility result for manifest schema, ABI/protocol and host range checks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionCompatibilityResult {
    pub compatible: bool,
    pub manifest_version: u32,
    pub required_protocol_major: u32,
    pub current_protocol_major: u32,
    pub min_host_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_host_version: Option<String>,
    pub current_host_version: String,
    #[serde(default)]
    pub issues: Vec<ExtensionCompatibilityIssue>,
}

/// Capability classes exposed through the extension grant model.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum ExtensionCapabilityClass {
    Network,
    Secret,
    StorageRead,
    StorageWrite,
    Channel,
    Device,
}

impl ExtensionCapabilityClass {
    /// Returns the policy action string evaluated for grants of this class.
    #[must_use]
    pub fn policy_action(self) -> &'static str {
        match self {
            Self::Network => "extension.capability.network.egress",
            Self::Secret => "extension.capability.secrets.read",
            Self::StorageRead => "extension.capability.storage.read",
            Self::StorageWrite => "extension.capability.storage.write",
            Self::Channel => "extension.capability.channel.use",
            Self::Device => "extension.capability.device.use",
        }
    }
}

/// One least-privilege capability grant requested or granted for an extension.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub struct ExtensionCapabilityGrant {
    pub class: ExtensionCapabilityClass,
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub policy_action: String,
}

impl ExtensionCapabilityGrant {
    /// Builds a grant with `policy_action` derived from the capability class.
    #[must_use]
    pub fn new(
        class: ExtensionCapabilityClass,
        value: impl Into<String>,
        scope: Option<String>,
    ) -> Self {
        Self { class, value: value.into(), scope, policy_action: class.policy_action().to_owned() }
    }
}

/// Capability diff used by doctor and enable preflight flows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionCapabilityDiff {
    pub valid: bool,
    #[serde(default)]
    pub requested: Vec<ExtensionCapabilityGrant>,
    #[serde(default)]
    pub granted: Vec<ExtensionCapabilityGrant>,
    #[serde(default)]
    pub missing: Vec<ExtensionCapabilityGrant>,
    #[serde(default)]
    pub excess: Vec<ExtensionCapabilityGrant>,
    #[serde(default)]
    pub policy_actions: Vec<String>,
}

/// Durable registry record for an installed or discovered extension package.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionPackageRegistryRecord {
    pub package_id: String,
    pub kind: ExtensionPackageKind,
    pub manifest_hash: String,
    pub version: String,
    pub source: ExtensionPackageSource,
    pub status: ExtensionPackageStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    #[serde(default)]
    pub grants: Vec<ExtensionCapabilityGrant>,
    pub compatibility: ExtensionCompatibilityResult,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
}

/// Minimal registry abstraction shared by skill/plugin lifecycle surfaces.
pub trait ExtensionPackageRegistry {
    /// Returns all known packages in deterministic order.
    fn list_packages(&self) -> Vec<ExtensionPackageRegistryRecord>;

    /// Looks up one package by stable package id.
    fn get_package(&self, package_id: &str) -> Option<ExtensionPackageRegistryRecord>;

    /// Inserts or replaces a package record.
    fn upsert_package(&mut self, record: ExtensionPackageRegistryRecord);

    /// Applies a validated lifecycle transition and returns the updated record.
    ///
    /// # Errors
    /// Returns [`SkillPackagingError::ExtensionLifecycle`] when the package is
    /// unknown or the transition is rejected by
    /// [`apply_extension_lifecycle_transition`].
    fn transition_package(
        &mut self,
        request: ExtensionLifecycleTransitionRequest,
    ) -> Result<ExtensionPackageRegistryRecord, SkillPackagingError>;
}

/// Deterministic in-memory registry implementation used by tests and adapters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct InMemoryExtensionPackageRegistry {
    #[serde(default)]
    pub records: BTreeMap<String, ExtensionPackageRegistryRecord>,
}

impl ExtensionPackageRegistry for InMemoryExtensionPackageRegistry {
    fn list_packages(&self) -> Vec<ExtensionPackageRegistryRecord> {
        self.records.values().cloned().collect()
    }

    fn get_package(&self, package_id: &str) -> Option<ExtensionPackageRegistryRecord> {
        self.records.get(package_id).cloned()
    }

    fn upsert_package(&mut self, record: ExtensionPackageRegistryRecord) {
        self.records.insert(record.package_id.clone(), record);
    }

    fn transition_package(
        &mut self,
        request: ExtensionLifecycleTransitionRequest,
    ) -> Result<ExtensionPackageRegistryRecord, SkillPackagingError> {
        let current = self.records.get(&request.package_id).cloned().ok_or_else(|| {
            SkillPackagingError::ExtensionLifecycle(format!(
                "extension package '{}' is not registered",
                request.package_id
            ))
        })?;
        let updated = apply_extension_lifecycle_transition(&current, &request)?;
        self.records.insert(updated.package_id.clone(), updated.clone());
        Ok(updated)
    }
}

/// Lifecycle transition request with policy/audit context.
///
/// Callers are responsible for authenticating and authorizing these principals before constructing
/// the request. The lifecycle helper still validates that supplied principals are non-empty and
/// that quarantine enablement is approved by a distinct reviewer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionLifecycleTransitionRequest {
    pub package_id: String,
    pub target_status: ExtensionPackageStatus,
    pub actor_principal: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_by: Option<String>,
    pub requested_at_unix_ms: i64,
}

/// Contract-test fixture for extension ABI stability checks.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionContractFixture {
    pub fixture_id: String,
    pub package_kind: ExtensionPackageKind,
    pub manifest_version: u32,
    pub required_protocol_major: u32,
    pub min_host_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_host_version: Option<String>,
    #[serde(default)]
    pub expected_reason_codes: Vec<String>,
}

/// Contract-test outcome for extension API compatibility gates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionContractTestOutcome {
    pub fixture_id: String,
    pub passed: bool,
    #[serde(default)]
    pub observed_reason_codes: Vec<String>,
}

/// Self-improvement candidate promoted only through review, scaffold and eval gates.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SelfImprovementCandidate {
    pub candidate_id: String,
    #[serde(default)]
    pub source_refs: Vec<String>,
    pub rationale: String,
    pub risk: String,
    #[serde(default)]
    pub expected_capabilities: Vec<ExtensionCapabilityGrant>,
    #[serde(default)]
    pub tests: Vec<String>,
    pub sensitivity: String,
}

/// Evaluation fixture for generated skills and other self-improvement artifacts.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillEvalFixture {
    pub fixture_id: String,
    pub label: String,
    #[serde(default)]
    pub expected_outputs: BTreeMap<String, String>,
    #[serde(default)]
    pub required_capabilities: Vec<ExtensionCapabilityGrant>,
    #[serde(default)]
    pub regression_labels: Vec<String>,
}

/// Outcome recorded by the deterministic skill eval harness.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillEvalOutcome {
    pub fixture_id: String,
    pub passed: bool,
    #[serde(default)]
    pub failed_checks: Vec<String>,
    #[serde(default)]
    pub flaky_signal: bool,
    #[serde(default)]
    pub provider_usage: BTreeMap<String, u64>,
    #[serde(default)]
    pub artifact_refs: Vec<String>,
}

/// Durable summary for one eval harness run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillEvalRunRecord {
    pub schema_version: u32,
    pub eval_run_id: String,
    pub package_id: String,
    pub status: String,
    #[serde(default)]
    pub outcomes: Vec<SkillEvalOutcome>,
    #[serde(default)]
    pub regression_labels: Vec<String>,
    #[serde(default)]
    pub provider_usage: BTreeMap<String, u64>,
    #[serde(default)]
    pub artifact_refs: Vec<String>,
    pub recorded_at_unix_ms: i64,
}

/// Reproducible scaffold signing/eval plan for generated skills.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SkillScaffoldArtifactPlan {
    pub schema_version: u32,
    pub package_id: String,
    pub manifest_path: String,
    pub capability_declaration_path: String,
    pub provenance_path: String,
    pub test_harness_path: String,
    pub artifact_status: String,
    pub signature_status: String,
    pub quarantine_status: ExtensionPackageStatus,
    pub reproducibility_key: String,
    #[serde(default)]
    pub audit_events: Vec<String>,
}

/// Enable gate result for self-improvement rollout decisions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionEnableGate {
    pub allowed: bool,
    #[serde(default)]
    pub reasons: Vec<String>,
}

/// Rollback decision for self-improvement lifecycle failures.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SelfImprovementRollbackPlan {
    pub package_id: String,
    pub target_status: ExtensionPackageStatus,
    pub policy_action: String,
    pub audit_reason: String,
}

/// Read-only doctor report for extension package preflight.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionDoctorReport {
    pub schema_version: u32,
    pub package_id: String,
    pub kind: ExtensionPackageKind,
    pub status: String,
    #[serde(default)]
    pub reason_codes: Vec<String>,
    #[serde(default)]
    pub repair_hints: Vec<String>,
    pub registry_record: ExtensionPackageRegistryRecord,
    pub compatibility: ExtensionCompatibilityResult,
    pub capability_diff: ExtensionCapabilityDiff,
    pub security: ExtensionSecurityPreflight,
    pub runtime_available: bool,
    pub checked_at_unix_ms: i64,
}

/// Security subset of the extension doctor report.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ExtensionSecurityPreflight {
    pub accepted: bool,
    pub passed: bool,
    pub should_quarantine: bool,
    pub signed: bool,
    #[serde(default)]
    pub failed_checks: Vec<String>,
    #[serde(default)]
    pub warning_checks: Vec<String>,
    #[serde(default)]
    pub quarantine_reasons: Vec<String>,
}

/// Builds a deterministic package id for a skill extension.
#[must_use]
pub fn skill_extension_package_id(skill_id: &str, version: &str) -> String {
    format!("skill:{skill_id}@{version}")
}

/// Projects a skill manifest into extension capability grants.
#[must_use]
pub fn extension_capability_grants_from_skill_manifest(
    manifest: &SkillManifest,
) -> Vec<ExtensionCapabilityGrant> {
    let mut grants = Vec::new();
    for host in &manifest.capabilities.http_egress_allowlist {
        grants.push(ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::Network,
            host.trim().to_ascii_lowercase(),
            None,
        ));
    }
    for secret in &manifest.capabilities.secrets {
        for key in &secret.key_names {
            grants.push(ExtensionCapabilityGrant::new(
                ExtensionCapabilityClass::Secret,
                key.trim().to_owned(),
                Some(secret.scope.trim().to_owned()),
            ));
        }
    }
    for path in &manifest.capabilities.filesystem.read_roots {
        grants.push(ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::StorageRead,
            path.trim().to_owned(),
            None,
        ));
    }
    for path in &manifest.capabilities.filesystem.write_roots {
        grants.push(ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::StorageWrite,
            path.trim().to_owned(),
            None,
        ));
    }
    for channel in &manifest.capabilities.node_capabilities {
        grants.push(ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::Channel,
            channel.trim().to_owned(),
            None,
        ));
    }
    for capability in &manifest.capabilities.device_capabilities {
        grants.push(ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::Device,
            capability.trim().to_owned(),
            None,
        ));
    }
    canonicalize_grants(grants)
}

/// Evaluates manifest compatibility with the current Palyra host.
#[must_use]
pub fn extension_compatibility_from_skill_manifest(
    manifest: &SkillManifest,
) -> ExtensionCompatibilityResult {
    let mut issues = Vec::new();
    if let Err(error) = assert_runtime_compatibility(&manifest.compat) {
        issues.push(compatibility_issue_from_error(&error));
    }
    ExtensionCompatibilityResult {
        compatible: issues.is_empty(),
        manifest_version: manifest.manifest_version,
        required_protocol_major: manifest.compat.required_protocol_major,
        current_protocol_major: CANONICAL_PROTOCOL_MAJOR,
        min_host_version: manifest.compat.min_palyra_version.clone(),
        max_host_version: manifest.compat.max_palyra_version.clone(),
        current_host_version: build_metadata().version.to_owned(),
        issues,
    }
}

/// Computes the missing/excess capability grant diff for enable preflight.
#[must_use]
pub fn diff_extension_capability_grants(
    requested: &[ExtensionCapabilityGrant],
    granted: &[ExtensionCapabilityGrant],
) -> ExtensionCapabilityDiff {
    let requested = canonicalize_grants(requested.to_vec());
    let granted = canonicalize_grants(granted.to_vec());
    let requested_set = requested.iter().cloned().collect::<BTreeSet<_>>();
    let granted_set = granted.iter().cloned().collect::<BTreeSet<_>>();
    let missing = requested_set.difference(&granted_set).cloned().collect::<Vec<_>>();
    let excess = granted_set.difference(&requested_set).cloned().collect::<Vec<_>>();
    let policy_actions = requested
        .iter()
        .map(|grant| grant.policy_action.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    ExtensionCapabilityDiff {
        valid: missing.is_empty() && excess.is_empty(),
        requested,
        granted,
        missing,
        excess,
        policy_actions,
    }
}

/// Projects an inspected skill artifact into an extension registry record.
#[must_use]
pub fn extension_record_from_skill_artifact(
    inspection: &SkillArtifactInspection,
    source: ExtensionPackageSource,
    status: ExtensionPackageStatus,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
) -> ExtensionPackageRegistryRecord {
    let package_id = skill_extension_package_id(
        inspection.manifest.skill_id.as_str(),
        inspection.manifest.version.as_str(),
    );
    let kind = if inspection.manifest.builder.is_some() {
        ExtensionPackageKind::GeneratedSkill
    } else {
        ExtensionPackageKind::Skill
    };
    ExtensionPackageRegistryRecord {
        package_id,
        kind,
        manifest_hash: inspection.payload_sha256.clone(),
        version: inspection.manifest.version.clone(),
        source,
        status,
        status_reason: None,
        grants: extension_capability_grants_from_skill_manifest(&inspection.manifest),
        compatibility: extension_compatibility_from_skill_manifest(&inspection.manifest),
        created_at_unix_ms,
        updated_at_unix_ms,
    }
}

/// Runs read-only package integrity, manifest, ABI, grants and security preflight.
///
/// An empty `explicit_grants` slice means "grant exactly what the manifest
/// requests"; otherwise the supplied grants are diffed against the manifest.
///
/// # Errors
/// Returns any verification, trust, or audit error from
/// [`inspect_skill_artifact`] and [`audit_skill_artifact_security`]; policy
/// failures that produce a report are encoded in `status`/`reason_codes`
/// rather than returned as errors.
pub fn extension_doctor_for_skill_artifact(
    artifact_bytes: &[u8],
    source: ExtensionPackageSource,
    trust_store: &mut SkillTrustStore,
    allow_tofu: bool,
    explicit_grants: &[ExtensionCapabilityGrant],
    audit_policy: &SkillSecurityAuditPolicy,
) -> Result<ExtensionDoctorReport, SkillPackagingError> {
    let checked_at_unix_ms = now_unix_ms();
    let inspection = inspect_skill_artifact(artifact_bytes)?;
    let audit =
        audit_skill_artifact_security(artifact_bytes, trust_store, allow_tofu, audit_policy)?;
    let requested_grants = extension_capability_grants_from_skill_manifest(&inspection.manifest);
    let granted = if explicit_grants.is_empty() {
        requested_grants.clone()
    } else {
        explicit_grants.to_vec()
    };
    let capability_diff = diff_extension_capability_grants(requested_grants.as_slice(), &granted);
    let status = if audit.should_quarantine {
        ExtensionPackageStatus::Quarantined
    } else if audit.passed && capability_diff.valid {
        ExtensionPackageStatus::Verified
    } else {
        ExtensionPackageStatus::Failed
    };
    let registry_record = extension_record_from_skill_artifact(
        &inspection,
        source,
        status,
        checked_at_unix_ms,
        checked_at_unix_ms,
    );
    let security = ExtensionSecurityPreflight {
        accepted: audit.accepted,
        passed: audit.passed,
        should_quarantine: audit.should_quarantine,
        // Unsigned or signature-invalid artifacts error out of the audit above,
        // so any report that reaches this point describes a signed artifact.
        signed: true,
        failed_checks: audit
            .checks
            .iter()
            .filter(|check| check.status == SkillAuditCheckStatus::Fail)
            .map(|check| check.check_id.clone())
            .collect(),
        warning_checks: audit
            .checks
            .iter()
            .filter(|check| check.status == SkillAuditCheckStatus::Warn)
            .map(|check| check.check_id.clone())
            .collect(),
        quarantine_reasons: audit.quarantine_reasons.clone(),
    };
    let compatibility = registry_record.compatibility.clone();
    let (reason_codes, repair_hints) =
        doctor_reason_codes_and_hints(&compatibility, &capability_diff, &security);
    let report_status = if reason_codes.is_empty() { "ready" } else { "blocked" }.to_owned();
    Ok(ExtensionDoctorReport {
        schema_version: 1,
        package_id: registry_record.package_id.clone(),
        kind: registry_record.kind,
        status: report_status,
        reason_codes,
        repair_hints,
        registry_record,
        compatibility,
        capability_diff,
        security,
        runtime_available: true,
        checked_at_unix_ms,
    })
}

/// Applies a lifecycle transition without losing audit context.
///
/// Gate order: identity and audit fields (package id, reason, principals)
/// first, then the transition allowlist, then the enable-specific checks
/// (compatibility, and the four-eyes rule for quarantined packages — the
/// approving reviewer must differ from the acting principal).
///
/// # Errors
/// Returns [`SkillPackagingError::ExtensionLifecycle`] when any of the gates
/// above rejects the request.
pub fn apply_extension_lifecycle_transition(
    current: &ExtensionPackageRegistryRecord,
    request: &ExtensionLifecycleTransitionRequest,
) -> Result<ExtensionPackageRegistryRecord, SkillPackagingError> {
    if current.package_id != request.package_id {
        return Err(SkillPackagingError::ExtensionLifecycle(format!(
            "transition package_id '{}' does not match record '{}'",
            request.package_id, current.package_id
        )));
    }
    if request.reason.trim().is_empty() {
        return Err(SkillPackagingError::ExtensionLifecycle(
            "extension lifecycle transition reason cannot be empty".to_owned(),
        ));
    }
    let actor_principal =
        normalize_lifecycle_principal(request.actor_principal.as_str(), "actor_principal")?;
    let approved_by = request
        .approved_by
        .as_deref()
        .map(|principal| normalize_lifecycle_principal(principal, "approved_by"))
        .transpose()?;
    if !is_lifecycle_transition_allowed(current.status, request.target_status) {
        return Err(SkillPackagingError::ExtensionLifecycle(format!(
            "invalid extension lifecycle transition {:?} -> {:?}",
            current.status, request.target_status
        )));
    }
    if request.target_status == ExtensionPackageStatus::Enabled {
        if !current.compatibility.compatible {
            return Err(SkillPackagingError::ExtensionLifecycle(
                "extension cannot be enabled with incompatible manifest".to_owned(),
            ));
        }
        if current.status == ExtensionPackageStatus::Quarantined {
            let Some(approved_by) = approved_by.as_deref() else {
                return Err(SkillPackagingError::ExtensionLifecycle(
                    "enabling a quarantined extension requires approved_by".to_owned(),
                ));
            };
            if approved_by == actor_principal {
                return Err(SkillPackagingError::ExtensionLifecycle(
                    "enabling a quarantined extension requires approval from a distinct reviewer"
                        .to_owned(),
                ));
            }
        }
    }
    let mut updated = current.clone();
    updated.status = request.target_status;
    updated.status_reason = Some(request.reason.trim().to_owned());
    updated.updated_at_unix_ms = request.requested_at_unix_ms;
    Ok(updated)
}

fn normalize_lifecycle_principal(
    value: &str,
    field: &'static str,
) -> Result<String, SkillPackagingError> {
    let principal = value.trim();
    if principal.is_empty() {
        return Err(SkillPackagingError::ExtensionLifecycle(format!(
            "extension lifecycle {field} cannot be empty",
        )));
    }
    Ok(principal.to_owned())
}

/// Runs extension ABI fixture checks against the current host.
///
/// A fixture passes when the observed compatibility reason codes match its
/// `expected_reason_codes` exactly (including order).
#[must_use]
pub fn evaluate_extension_contract_fixture(
    fixture: &ExtensionContractFixture,
) -> ExtensionContractTestOutcome {
    let issues = compatibility_issues_for_raw_ranges(
        fixture.required_protocol_major,
        fixture.min_host_version.as_str(),
        fixture.max_host_version.as_deref(),
    );
    let observed_reason_codes = issues.iter().map(|issue| issue.code.clone()).collect::<Vec<_>>();
    ExtensionContractTestOutcome {
        fixture_id: fixture.fixture_id.clone(),
        passed: observed_reason_codes == fixture.expected_reason_codes,
        observed_reason_codes,
    }
}

/// Evaluates deterministic skill fixtures and capability requirements.
#[must_use]
pub fn evaluate_skill_fixture(
    fixture: &SkillEvalFixture,
    actual_outputs: &BTreeMap<String, String>,
    granted_capabilities: &[ExtensionCapabilityGrant],
    artifact_refs: Vec<String>,
) -> SkillEvalOutcome {
    let mut failed_checks = Vec::new();
    for (key, expected) in &fixture.expected_outputs {
        if actual_outputs.get(key) != Some(expected) {
            failed_checks.push(format!("expected_output_mismatch:{key}"));
        }
    }
    let capability_diff = diff_extension_capability_grants(
        fixture.required_capabilities.as_slice(),
        granted_capabilities,
    );
    for grant in capability_diff.missing {
        failed_checks.push(format!("missing_capability:{:?}:{}", grant.class, grant.value));
    }
    SkillEvalOutcome {
        fixture_id: fixture.fixture_id.clone(),
        passed: failed_checks.is_empty(),
        failed_checks,
        flaky_signal: false,
        provider_usage: BTreeMap::new(),
        artifact_refs,
    }
}

/// Records deterministic eval outcomes and aggregates provider usage/artifact refs.
///
/// Run status precedence: no outcomes -> `missing`, any flaky signal ->
/// `flaky` (even if everything passed), all passed -> `passed`, otherwise
/// `failed`. Flakiness outranks success because a flaky eval cannot gate a
/// rollout.
#[must_use]
pub fn skill_eval_run_record(
    eval_run_id: impl Into<String>,
    package_id: impl Into<String>,
    outcomes: Vec<SkillEvalOutcome>,
    regression_labels: Vec<String>,
    recorded_at_unix_ms: i64,
) -> SkillEvalRunRecord {
    let mut provider_usage = BTreeMap::<String, u64>::new();
    let mut artifact_refs = BTreeSet::<String>::new();
    for outcome in &outcomes {
        for (key, value) in &outcome.provider_usage {
            *provider_usage.entry(key.clone()).or_default() += *value;
        }
        for artifact_ref in &outcome.artifact_refs {
            artifact_refs.insert(artifact_ref.clone());
        }
    }
    let status = if outcomes.is_empty() {
        "missing"
    } else if outcomes.iter().any(|outcome| outcome.flaky_signal) {
        "flaky"
    } else if outcomes.iter().all(|outcome| outcome.passed) {
        "passed"
    } else {
        "failed"
    }
    .to_owned();
    SkillEvalRunRecord {
        schema_version: 1,
        eval_run_id: eval_run_id.into(),
        package_id: package_id.into(),
        status,
        outcomes,
        regression_labels: regression_labels
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect(),
        provider_usage,
        artifact_refs: artifact_refs.into_iter().collect(),
        recorded_at_unix_ms,
    }
}

/// Blocks extension enablement when eval or compatibility gates failed.
#[must_use]
pub fn extension_enable_gate(
    record: &ExtensionPackageRegistryRecord,
    eval_outcomes: &[SkillEvalOutcome],
) -> ExtensionEnableGate {
    let mut reasons = Vec::new();
    if !record.compatibility.compatible {
        reasons.push("compatibility_failed".to_owned());
    }
    if record.status == ExtensionPackageStatus::Quarantined {
        reasons.push("package_quarantined".to_owned());
    }
    if eval_outcomes.is_empty() {
        reasons.push("eval_missing".to_owned());
    }
    for outcome in eval_outcomes {
        if !outcome.passed {
            reasons.push(format!("eval_failed:{}", outcome.fixture_id));
        }
        if outcome.flaky_signal {
            reasons.push(format!("eval_flaky:{}", outcome.fixture_id));
        }
    }
    ExtensionEnableGate { allowed: reasons.is_empty(), reasons }
}

/// Builds the signed-artifact preparation plan for a quarantined scaffold.
#[must_use]
pub fn skill_scaffold_artifact_plan(
    package_id: impl Into<String>,
    manifest_path: impl Into<String>,
    capability_declaration_path: impl Into<String>,
    provenance_path: impl Into<String>,
    test_harness_path: impl Into<String>,
    reproducibility_key: impl Into<String>,
) -> SkillScaffoldArtifactPlan {
    SkillScaffoldArtifactPlan {
        schema_version: 1,
        package_id: package_id.into(),
        manifest_path: manifest_path.into(),
        capability_declaration_path: capability_declaration_path.into(),
        provenance_path: provenance_path.into(),
        test_harness_path: test_harness_path.into(),
        artifact_status: "prepared_for_signing".to_owned(),
        signature_status: "unsigned_requires_operator_signature".to_owned(),
        quarantine_status: ExtensionPackageStatus::Quarantined,
        reproducibility_key: reproducibility_key.into(),
        audit_events: vec![
            "skill.scaffolded".to_owned(),
            "skill.quarantined".to_owned(),
            "skill.eval_required".to_owned(),
        ],
    }
}

/// Plans a fail-closed rollback or disable action after self-improvement regression.
#[must_use]
pub fn plan_self_improvement_rollback(
    record: &ExtensionPackageRegistryRecord,
    eval_outcomes: &[SkillEvalOutcome],
) -> SelfImprovementRollbackPlan {
    let has_regression =
        eval_outcomes.iter().any(|outcome| !outcome.passed || outcome.flaky_signal);
    SelfImprovementRollbackPlan {
        package_id: record.package_id.clone(),
        target_status: if has_regression {
            ExtensionPackageStatus::RolledBack
        } else {
            ExtensionPackageStatus::Disabled
        },
        policy_action: "extension.self_improvement.rollback".to_owned(),
        audit_reason: if has_regression {
            "self_improvement_regression_gate_failed".to_owned()
        } else {
            "operator_requested_disable".to_owned()
        },
    }
}

/// Builds self-improvement metadata from a generic review payload.
///
/// `source_refs` and `tests` are deduplicated and sorted; expected
/// capabilities are canonicalized like manifest-derived grants.
///
/// # Errors
/// Returns [`SkillPackagingError::ManifestValidation`] when a required text
/// field (`rationale`, `risk`, `sensitivity`) is missing or empty, and
/// [`SkillPackagingError::ExtensionLifecycle`] when an expected-capability
/// entry is malformed.
pub fn extract_self_improvement_candidate(
    candidate_id: impl Into<String>,
    payload: &Value,
) -> Result<SelfImprovementCandidate, SkillPackagingError> {
    let candidate_id = candidate_id.into();
    let rationale = required_payload_text(payload, "rationale")?;
    let risk = required_payload_text(payload, "risk")?;
    let sensitivity = required_payload_text(payload, "sensitivity")?;
    let source_refs = payload
        .get("source_refs")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let tests = payload
        .get("tests")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let expected_capabilities = payload
        .get("expected_capabilities")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .map(parse_expected_capability_payload)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SelfImprovementCandidate {
        candidate_id,
        source_refs,
        rationale,
        risk,
        expected_capabilities: canonicalize_grants(expected_capabilities),
        tests,
        sensitivity,
    })
}

/// Converts legacy skill grant snapshots into extension capability grants.
#[must_use]
pub fn extension_grants_from_skill_snapshot(
    snapshot: &SkillCapabilityGrantSnapshot,
) -> Vec<ExtensionCapabilityGrant> {
    let mut grants = Vec::new();
    for host in &snapshot.http_hosts {
        grants.push(ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::Network,
            host.clone(),
            None,
        ));
    }
    for secret in &snapshot.secret_keys {
        // Snapshot secret keys are encoded as "<scope>/<key>"; entries without
        // a separator are treated as unscoped keys.
        let (scope, key) = match secret.split_once('/') {
            Some((scope, key)) => (Some(scope.to_owned()), key.to_owned()),
            None => (None, secret.clone()),
        };
        grants.push(ExtensionCapabilityGrant::new(ExtensionCapabilityClass::Secret, key, scope));
    }
    for prefix in &snapshot.storage_prefixes {
        grants.push(ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::StorageWrite,
            prefix.clone(),
            None,
        ));
    }
    for channel in &snapshot.channels {
        grants.push(ExtensionCapabilityGrant::new(
            ExtensionCapabilityClass::Channel,
            channel.clone(),
            None,
        ));
    }
    canonicalize_grants(grants)
}

fn compatibility_issue_from_error(error: &SkillPackagingError) -> ExtensionCompatibilityIssue {
    match error {
        SkillPackagingError::UnsupportedProtocolMajor { requested, current } => {
            ExtensionCompatibilityIssue {
                code: "abi_required_protocol_major_unsupported".to_owned(),
                message: format!(
                    "extension requires protocol major {requested}, current host supports {current}"
                ),
                repair_hint: "upgrade Palyra before installing this extension".to_owned(),
            }
        }
        SkillPackagingError::UnsupportedRuntimeVersion { requested, current } => {
            ExtensionCompatibilityIssue {
                code: "host_below_min_palyra_version".to_owned(),
                message: format!(
                    "extension requires host >= {requested}, current host is {current}"
                ),
                repair_hint: "upgrade Palyra or install an older extension version".to_owned(),
            }
        }
        SkillPackagingError::RuntimeVersionAboveSupportedMaximum { supported_max, current } => {
            ExtensionCompatibilityIssue {
                code: "host_above_max_palyra_version".to_owned(),
                message: format!(
                    "extension supports host <= {supported_max}, current host is {current}"
                ),
                repair_hint: "install a newer extension version for this Palyra host".to_owned(),
            }
        }
        other => ExtensionCompatibilityIssue {
            code: "manifest_compatibility_failed".to_owned(),
            message: other.to_string(),
            repair_hint: "inspect and repair the extension manifest compatibility section"
                .to_owned(),
        },
    }
}

fn compatibility_issues_for_raw_ranges(
    required_protocol_major: u32,
    min_host_version: &str,
    max_host_version: Option<&str>,
) -> Vec<ExtensionCompatibilityIssue> {
    let compat = crate::models::SkillCompat {
        required_protocol_major,
        min_palyra_version: min_host_version.to_owned(),
        max_palyra_version: max_host_version.map(ToOwned::to_owned),
    };
    match assert_runtime_compatibility(&compat) {
        Ok(()) => Vec::new(),
        Err(error) => vec![compatibility_issue_from_error(&error)],
    }
}

fn doctor_reason_codes_and_hints(
    compatibility: &ExtensionCompatibilityResult,
    capability_diff: &ExtensionCapabilityDiff,
    security: &ExtensionSecurityPreflight,
) -> (Vec<String>, Vec<String>) {
    let mut reason_codes = BTreeSet::new();
    let mut repair_hints = BTreeSet::new();
    for issue in &compatibility.issues {
        reason_codes.insert(issue.code.clone());
        repair_hints.insert(issue.repair_hint.clone());
    }
    if !capability_diff.missing.is_empty() {
        reason_codes.insert("missing_capability_grant".to_owned());
        repair_hints.insert(
            "grant only the missing capabilities or keep the extension disabled".to_owned(),
        );
    }
    if !capability_diff.excess.is_empty() {
        reason_codes.insert("excess_capability_grant".to_owned());
        repair_hints
            .insert("remove grants that are not declared by the extension manifest".to_owned());
    }
    if !security.passed {
        reason_codes.insert("security_audit_failed".to_owned());
        repair_hints
            .insert("inspect failed security checks before enabling the extension".to_owned());
    }
    if security.should_quarantine {
        reason_codes.insert("quarantine_required".to_owned());
        repair_hints
            .insert("leave the package quarantined until operator review completes".to_owned());
    }
    (reason_codes.into_iter().collect(), repair_hints.into_iter().collect())
}

// Allowlisted state machine: any pair missing here is rejected, so new states
// or edges must be added deliberately. Note the deliberate asymmetries —
// Removed and Failed are near-terminal, and quarantine release still passes
// the four-eyes check in `apply_extension_lifecycle_transition`.
fn is_lifecycle_transition_allowed(
    current: ExtensionPackageStatus,
    target: ExtensionPackageStatus,
) -> bool {
    use ExtensionPackageStatus::{
        Disabled, Enabled, Failed, Installed, Quarantined, Removed, RolledBack, Verified,
    };
    matches!(
        (current, target),
        (Installed, Verified)
            | (Installed, Quarantined)
            | (Installed, Failed)
            | (Verified, Enabled)
            | (Verified, Disabled)
            | (Verified, Quarantined)
            | (Enabled, Disabled)
            | (Enabled, Quarantined)
            | (Enabled, RolledBack)
            | (Disabled, Enabled)
            | (Disabled, Removed)
            | (Quarantined, Enabled)
            | (Quarantined, Disabled)
            | (Quarantined, Removed)
            | (Failed, Removed)
            | (RolledBack, Disabled)
            | (RolledBack, Removed)
    )
}

// Trims values/scopes, re-derives policy_action from the class (so a
// deserialized grant cannot smuggle a mismatched action), drops empty values,
// and sort-deduplicates via BTreeSet for deterministic output.
fn canonicalize_grants(grants: Vec<ExtensionCapabilityGrant>) -> Vec<ExtensionCapabilityGrant> {
    grants
        .into_iter()
        .filter(|grant| !grant.value.trim().is_empty())
        .map(|mut grant| {
            grant.value = grant.value.trim().to_owned();
            grant.scope = grant.scope.and_then(|scope| {
                let trimmed = scope.trim();
                (!trimmed.is_empty()).then(|| trimmed.to_owned())
            });
            grant.policy_action = grant.class.policy_action().to_owned();
            grant
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExpectedCapabilityPayload {
    class: ExtensionCapabilityClass,
    value: String,
    #[serde(default)]
    scope: Option<String>,
}

fn parse_expected_capability_payload(
    payload: &Value,
) -> Result<ExtensionCapabilityGrant, SkillPackagingError> {
    let parsed =
        serde_json::from_value::<ExpectedCapabilityPayload>(payload.clone()).map_err(|error| {
            SkillPackagingError::ExtensionLifecycle(format!(
                "invalid expected self-improvement capability: {error}"
            ))
        })?;
    if parsed.value.trim().is_empty() {
        return Err(SkillPackagingError::ExtensionLifecycle(
            "expected self-improvement capability value cannot be empty".to_owned(),
        ));
    }
    Ok(ExtensionCapabilityGrant::new(parsed.class, parsed.value, parsed.scope))
}

fn required_payload_text(
    payload: &Value,
    field: &'static str,
) -> Result<String, SkillPackagingError> {
    payload
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            SkillPackagingError::ManifestValidation(format!(
                "self-improvement candidate payload missing {field}"
            ))
        })
}

/// Provides a stable JSON snapshot for registry contract tests.
///
/// Records are sorted by `package_id` so the snapshot is order-independent.
///
/// # Errors
/// Returns [`SkillPackagingError::Serialization`] when a record fails JSON
/// serialization.
pub fn extension_registry_snapshot(
    records: &[ExtensionPackageRegistryRecord],
) -> Result<Value, SkillPackagingError> {
    let mut records = records.to_vec();
    records.sort_by(|left, right| left.package_id.cmp(&right.package_id));
    serde_json::to_value(json!({
        "schema_version": 1,
        "records": records,
    }))
    .map_err(|error| SkillPackagingError::Serialization(error.to_string()))
}

/// Convenience helper for the legacy skill grant snapshot.
#[must_use]
pub fn skill_manifest_extension_grants(manifest: &SkillManifest) -> Vec<ExtensionCapabilityGrant> {
    extension_grants_from_skill_snapshot(&capability_grants_from_manifest(manifest))
}
