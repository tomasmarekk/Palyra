//! Embedded release evidence for the built-in feature-rollout registry.
//!
//! This module pins the authored manifest to its JSON Schema, resolves shared
//! evidence profiles, and rejects promotion claims that lack qualifying proof.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::OnceLock,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

mod state_validation;

use state_validation::validate_promotion_state_matrix;

pub(super) const FEATURE_ROLLOUT_PROMOTION_SCHEMA_VERSION: u32 = 1;
pub(super) const FEATURE_ROLLOUT_PROMOTION_SCHEMA_ID: &str = "palyra.feature-rollout-promotions.v1";
const BUILTIN_ROLLOUT_COUNT: usize = 39;
const MAX_EVIDENCE_PROFILES: usize = 64;
const MAX_LIST_ITEMS: usize = 32;
const MAX_IDENTIFIER_BYTES: usize = 96;
const MAX_TEXT_BYTES: usize = 1_024;

const BUILTIN_MANIFEST_JSON: &str =
    include_str!("../../../../infra/release/feature-rollout-promotions.json");
const BUILTIN_SCHEMA_JSON: &str =
    include_str!("../../../../schemas/json/common/feature-rollout-promotion-manifest.v1.json");

static BUILTIN_MANIFEST: OnceLock<
    Result<FeatureRolloutPromotionManifest, FeatureRolloutPromotionManifestError>,
> = OnceLock::new();

/// Returns the validated built-in promotion manifest.
///
/// `expected_rollouts` is the capability and owner projection from the runtime
/// descriptor registry. The call fails if either source adds, removes, or
/// reassigns a rollout without updating the other source in the same change.
///
/// # Errors
/// Returns [`FeatureRolloutPromotionManifestError`] when the embedded JSON,
/// schema digest, evidence, or runtime descriptor projection is invalid.
pub(super) fn builtin_feature_rollout_promotion_manifest(
    expected_rollouts: &[(&str, &str)],
) -> Result<&'static FeatureRolloutPromotionManifest, FeatureRolloutPromotionManifestError> {
    let manifest = match BUILTIN_MANIFEST.get_or_init(|| {
        parse_feature_rollout_promotion_manifest(BUILTIN_MANIFEST_JSON, BUILTIN_SCHEMA_JSON)
    }) {
        Ok(manifest) => manifest,
        Err(error) => return Err(error.clone()),
    };
    validate_expected_rollouts(manifest, expected_rollouts)?;
    Ok(manifest)
}

/// Parses and validates a promotion manifest against the supplied schema.
///
/// This entry point is intentionally data-only: it validates test references
/// but never executes commands authored in release metadata.
///
/// # Errors
/// Returns [`FeatureRolloutPromotionManifestError`] for malformed JSON, schema
/// drift, unresolved evidence, or an invalid promotion transition.
pub(super) fn parse_feature_rollout_promotion_manifest(
    manifest_json: &str,
    schema_json: &str,
) -> Result<FeatureRolloutPromotionManifest, FeatureRolloutPromotionManifestError> {
    let manifest: FeatureRolloutPromotionManifest =
        serde_json::from_str(manifest_json).map_err(|error| {
            FeatureRolloutPromotionManifestError::Parse {
                document: "feature rollout promotion manifest",
                reason: error.to_string(),
            }
        })?;
    validate_manifest(&manifest, schema_json)?;
    Ok(manifest)
}

/// Failure returned while loading or validating rollout promotion evidence.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(super) enum FeatureRolloutPromotionManifestError {
    /// The JSON document could not be decoded into its strict wire type.
    #[error("failed to parse {document}: {reason}")]
    Parse {
        /// Stable document label suitable for operator diagnostics.
        document: &'static str,
        /// Parser detail with no runtime or credential payloads.
        reason: String,
    },
    /// A decoded field violated a bounded or cross-field invariant.
    #[error("invalid feature rollout promotion manifest at {path}: {reason}")]
    Invalid {
        /// JSON-like path to the invalid value.
        path: String,
        /// Stable explanation of the violated invariant.
        reason: String,
    },
}

/// Strict wire model for the versioned release manifest.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FeatureRolloutPromotionManifest {
    pub(super) schema_version: u32,
    pub(super) schema_id: String,
    pub(super) schema_sha256: String,
    pub(super) evidence_profiles: BTreeMap<String, PromotionEvidenceProfile>,
    pub(super) rollouts: Vec<FeatureRolloutPromotion>,
}

impl FeatureRolloutPromotionManifest {
    /// Returns a rollout by its canonical capability identifier.
    pub(super) fn rollout(&self, capability: &str) -> Option<&FeatureRolloutPromotion> {
        self.rollouts.iter().find(|rollout| rollout.capability == capability)
    }

    /// Resolves every rollout to its shared evidence profile in manifest order.
    ///
    /// # Errors
    /// Returns [`FeatureRolloutPromotionManifestError`] if a caller constructs
    /// an unvalidated value whose evidence reference does not exist.
    pub(super) fn resolved_rollouts(
        &self,
    ) -> Result<Vec<ResolvedFeatureRolloutPromotion<'_>>, FeatureRolloutPromotionManifestError>
    {
        self.rollouts
            .iter()
            .map(|rollout| {
                let evidence =
                    self.evidence_profiles.get(&rollout.evidence_profile).ok_or_else(|| {
                        invalid(
                            format!("rollouts.{}.evidence_profile", rollout.capability),
                            format!("unknown evidence profile {}", rollout.evidence_profile),
                        )
                    })?;
                Ok(ResolvedFeatureRolloutPromotion { rollout, evidence })
            })
            .collect()
    }
}

/// A rollout paired with its resolved, reusable release evidence.
#[derive(Debug, Clone, Copy)]
pub(super) struct ResolvedFeatureRolloutPromotion<'a> {
    pub(super) rollout: &'a FeatureRolloutPromotion,
    pub(super) evidence: &'a PromotionEvidenceProfile,
}

/// Release posture for one canonical rollout capability.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FeatureRolloutPromotion {
    pub(super) capability: String,
    pub(super) owner_component: String,
    pub(super) contract_availability: ContractAvailability,
    pub(super) execution_completeness: ExecutionCompleteness,
    pub(super) promotion_state: PromotionState,
    pub(super) support_maturity: SupportMaturity,
    pub(super) lifecycle: RolloutLifecycle,
    pub(super) evidence_profile: String,
    pub(super) promotion_blockers: Vec<String>,
    pub(super) replacement: Option<String>,
    pub(super) removal_date: Option<String>,
    pub(super) removal_condition: String,
    pub(super) shadow_side_effect_posture: ShadowSideEffectPosture,
}

/// Availability of an authored contract independently of runtime execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ContractAvailability {
    DescriptorOnly,
    ApiAvailable,
    RuntimeAvailable,
    Blocked,
}

impl ContractAvailability {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::DescriptorOnly => "descriptor_only",
            Self::ApiAvailable => "api_available",
            Self::RuntimeAvailable => "runtime_available",
            Self::Blocked => "blocked",
        }
    }
}

/// Completeness of the implementation independently of its promotion state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ExecutionCompleteness {
    NotImplemented,
    Partial,
    Complete,
}

impl ExecutionCompleteness {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::NotImplemented => "not_implemented",
            Self::Partial => "partial",
            Self::Complete => "complete",
        }
    }
}

/// Declared rollout stage; actual hot-path use is reported separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum PromotionState {
    ContractOnly,
    Shadow,
    Canary,
    GatedProduction,
    Stable,
}

impl PromotionState {
    /// Returns the stable JSON spelling used by diagnostics and golden files.
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::ContractOnly => "contract_only",
            Self::Shadow => "shadow",
            Self::Canary => "canary",
            Self::GatedProduction => "gated_production",
            Self::Stable => "stable",
        }
    }
}

/// Operator support commitment for a capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SupportMaturity {
    Unsupported,
    Experimental,
    Preview,
    Supported,
    Deprecated,
    Retired,
}

impl SupportMaturity {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Unsupported => "unsupported",
            Self::Experimental => "experimental",
            Self::Preview => "preview",
            Self::Supported => "supported",
            Self::Deprecated => "deprecated",
            Self::Retired => "retired",
        }
    }
}

/// Lifecycle state used to drive deprecation and retirement checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum RolloutLifecycle {
    Active,
    Deprecated,
    Retired,
}

impl RolloutLifecycle {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deprecated => "deprecated",
            Self::Retired => "retired",
        }
    }
}

/// Whether a shadow implementation is safe to evaluate without side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ShadowSideEffectPosture {
    NotApplicable,
    SideEffectFree,
    NotQualified,
}

/// Shared evidence required by one or more rollout entries.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PromotionEvidenceProfile {
    pub(super) required_test_refs: Vec<String>,
    pub(super) sli: PromotionServiceLevelIndicator,
    pub(super) rollback: PromotionRollbackPlan,
    pub(super) compatibility_commitment: String,
    pub(super) legacy_removal_condition: String,
    pub(super) direct_hot_path_test_ref: Option<String>,
    pub(super) no_hidden_fallback_test_ref: Option<String>,
}

/// Service-level qualification attached to a promotion profile.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PromotionServiceLevelIndicator {
    pub(super) indicator: String,
    pub(super) objective: String,
    pub(super) window: String,
}

/// Operator rollback mechanism and its observable verification.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PromotionRollbackPlan {
    pub(super) mechanism: String,
    pub(super) verification: String,
}

fn validate_manifest(
    manifest: &FeatureRolloutPromotionManifest,
    schema_json: &str,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    if manifest.schema_version != FEATURE_ROLLOUT_PROMOTION_SCHEMA_VERSION {
        return Err(invalid(
            "schema_version",
            format!(
                "expected {}, found {}",
                FEATURE_ROLLOUT_PROMOTION_SCHEMA_VERSION, manifest.schema_version
            ),
        ));
    }
    if manifest.schema_id != FEATURE_ROLLOUT_PROMOTION_SCHEMA_ID {
        return Err(invalid(
            "schema_id",
            format!("expected {FEATURE_ROLLOUT_PROMOTION_SCHEMA_ID}, found {}", manifest.schema_id),
        ));
    }
    validate_schema_hash(&manifest.schema_sha256, schema_json)?;

    if manifest.evidence_profiles.is_empty()
        || manifest.evidence_profiles.len() > MAX_EVIDENCE_PROFILES
    {
        return Err(invalid(
            "evidence_profiles",
            format!(
                "expected 1..={MAX_EVIDENCE_PROFILES} profiles, found {}",
                manifest.evidence_profiles.len()
            ),
        ));
    }
    if manifest.rollouts.len() != BUILTIN_ROLLOUT_COUNT {
        return Err(invalid(
            "rollouts",
            format!(
                "expected exactly {BUILTIN_ROLLOUT_COUNT} entries, found {}",
                manifest.rollouts.len()
            ),
        ));
    }

    for (profile_id, evidence) in &manifest.evidence_profiles {
        validate_identifier(format!("evidence_profiles.{profile_id}"), profile_id)?;
        validate_evidence_profile(profile_id, evidence)?;
    }

    let mut capabilities = BTreeSet::new();
    for rollout in &manifest.rollouts {
        validate_rollout(manifest, rollout)?;
        if !capabilities.insert(rollout.capability.as_str()) {
            return Err(invalid(
                format!("rollouts.{}.capability", rollout.capability),
                "duplicate capability",
            ));
        }
    }
    Ok(())
}

fn validate_schema_hash(
    declared_hash: &str,
    schema_json: &str,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    if declared_hash.len() != 64
        || !declared_hash.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(invalid("schema_sha256", "expected a lowercase 64-character SHA-256 digest"));
    }
    let schema: Value = serde_json::from_str(schema_json).map_err(|error| {
        FeatureRolloutPromotionManifestError::Parse {
            document: "feature rollout promotion schema",
            reason: error.to_string(),
        }
    })?;
    let canonical_schema = canonical_json(&schema)?;
    let actual_hash = hex::encode(Sha256::digest(canonical_schema.as_bytes()));
    if declared_hash != actual_hash {
        return Err(invalid(
            "schema_sha256",
            format!("expected canonical schema digest {actual_hash}, found {declared_hash}"),
        ));
    }
    Ok(())
}

fn canonical_json(value: &Value) -> Result<String, FeatureRolloutPromotionManifestError> {
    let mut output = String::new();
    write_canonical_json(value, &mut output)?;
    Ok(output)
}

fn write_canonical_json(
    value: &Value,
    output: &mut String,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => output.push_str(&value.to_string()),
        Value::String(value) => output.push_str(&json_string(value)?),
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                write_canonical_json(value, output)?;
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort_unstable();
            for (index, key) in keys.into_iter().enumerate() {
                if index != 0 {
                    output.push(',');
                }
                output.push_str(&json_string(key)?);
                output.push(':');
                if let Some(value) = values.get(key) {
                    write_canonical_json(value, output)?;
                } else {
                    return Err(invalid(
                        "schema",
                        "canonical object traversal lost an existing key",
                    ));
                }
            }
            output.push('}');
        }
    }
    Ok(())
}

fn json_string(value: &str) -> Result<String, FeatureRolloutPromotionManifestError> {
    serde_json::to_string(value).map_err(|error| FeatureRolloutPromotionManifestError::Parse {
        document: "canonical feature rollout promotion schema",
        reason: error.to_string(),
    })
}

fn validate_evidence_profile(
    profile_id: &str,
    evidence: &PromotionEvidenceProfile,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    let path = format!("evidence_profiles.{profile_id}");
    validate_unique_text_list(
        format!("{path}.required_test_refs"),
        &evidence.required_test_refs,
        1,
    )?;
    validate_text(format!("{path}.sli.indicator"), &evidence.sli.indicator)?;
    validate_text(format!("{path}.sli.objective"), &evidence.sli.objective)?;
    validate_text(format!("{path}.sli.window"), &evidence.sli.window)?;
    validate_text(format!("{path}.rollback.mechanism"), &evidence.rollback.mechanism)?;
    validate_text(format!("{path}.rollback.verification"), &evidence.rollback.verification)?;
    validate_text(format!("{path}.compatibility_commitment"), &evidence.compatibility_commitment)?;
    validate_text(format!("{path}.legacy_removal_condition"), &evidence.legacy_removal_condition)?;
    if let Some(reference) = &evidence.direct_hot_path_test_ref {
        validate_text(format!("{path}.direct_hot_path_test_ref"), reference)?;
    }
    if let Some(reference) = &evidence.no_hidden_fallback_test_ref {
        validate_text(format!("{path}.no_hidden_fallback_test_ref"), reference)?;
    }
    Ok(())
}

fn validate_rollout(
    manifest: &FeatureRolloutPromotionManifest,
    rollout: &FeatureRolloutPromotion,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    let path = format!("rollouts.{}", rollout.capability);
    validate_identifier(format!("{path}.capability"), &rollout.capability)?;
    validate_text(format!("{path}.owner_component"), &rollout.owner_component)?;
    validate_identifier(format!("{path}.evidence_profile"), &rollout.evidence_profile)?;
    validate_unique_text_list(
        format!("{path}.promotion_blockers"),
        &rollout.promotion_blockers,
        0,
    )?;
    if let Some(replacement) = &rollout.replacement {
        validate_text(format!("{path}.replacement"), replacement)?;
    }
    if let Some(removal_date) = &rollout.removal_date {
        validate_calendar_date(format!("{path}.removal_date"), removal_date)?;
    }
    validate_text(format!("{path}.removal_condition"), &rollout.removal_condition)?;

    let evidence = manifest.evidence_profiles.get(&rollout.evidence_profile).ok_or_else(|| {
        invalid(
            format!("{path}.evidence_profile"),
            format!("unknown evidence profile {}", rollout.evidence_profile),
        )
    })?;

    validate_contract_consistency(&path, rollout)?;
    validate_lifecycle(&path, rollout)?;
    validate_promotion_state_matrix(&path, rollout)?;
    validate_promotion_evidence(&path, rollout, evidence)
}

fn validate_contract_consistency(
    path: &str,
    rollout: &FeatureRolloutPromotion,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    if rollout.contract_availability == ContractAvailability::Blocked
        && (rollout.promotion_state != PromotionState::ContractOnly
            || rollout.support_maturity != SupportMaturity::Unsupported)
    {
        return Err(invalid(
            format!("{path}.contract_availability"),
            "blocked contracts must remain contract_only and unsupported",
        ));
    }
    if rollout.contract_availability == ContractAvailability::DescriptorOnly
        && rollout.execution_completeness == ExecutionCompleteness::Complete
    {
        return Err(invalid(
            format!("{path}.execution_completeness"),
            "descriptor-only contracts cannot claim complete execution",
        ));
    }
    if matches!(
        rollout.contract_availability,
        ContractAvailability::ApiAvailable | ContractAvailability::RuntimeAvailable
    ) && rollout.execution_completeness == ExecutionCompleteness::NotImplemented
    {
        return Err(invalid(
            format!("{path}.execution_completeness"),
            "available API and runtime contracts require at least partial execution",
        ));
    }
    Ok(())
}

fn validate_lifecycle(
    path: &str,
    rollout: &FeatureRolloutPromotion,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    match rollout.lifecycle {
        RolloutLifecycle::Active => {
            if matches!(
                rollout.support_maturity,
                SupportMaturity::Deprecated | SupportMaturity::Retired
            ) {
                return Err(invalid(
                    format!("{path}.support_maturity"),
                    "an active lifecycle cannot advertise deprecated or retired support",
                ));
            }
        }
        RolloutLifecycle::Deprecated => {
            if rollout.support_maturity != SupportMaturity::Deprecated {
                return Err(invalid(
                    format!("{path}.support_maturity"),
                    "a deprecated lifecycle requires deprecated support maturity",
                ));
            }
            if rollout.replacement.is_none() {
                return Err(invalid(
                    format!("{path}.replacement"),
                    "a deprecated rollout requires a replacement",
                ));
            }
            if rollout.removal_date.is_none() && rollout.removal_condition.trim().is_empty() {
                return Err(invalid(
                    format!("{path}.removal_condition"),
                    "a deprecated rollout requires a removal date or condition",
                ));
            }
        }
        RolloutLifecycle::Retired => {
            if rollout.support_maturity != SupportMaturity::Retired {
                return Err(invalid(
                    format!("{path}.support_maturity"),
                    "a retired lifecycle requires retired support maturity",
                ));
            }
            if matches!(
                rollout.promotion_state,
                PromotionState::GatedProduction | PromotionState::Stable
            ) {
                return Err(invalid(
                    format!("{path}.promotion_state"),
                    "a retired rollout cannot remain on an active production promotion stage",
                ));
            }
        }
    }
    Ok(())
}

fn validate_promotion_evidence(
    path: &str,
    rollout: &FeatureRolloutPromotion,
    evidence: &PromotionEvidenceProfile,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    if !matches!(rollout.promotion_state, PromotionState::GatedProduction | PromotionState::Stable)
    {
        return Ok(());
    }
    if rollout.execution_completeness != ExecutionCompleteness::Complete {
        return Err(invalid(
            format!("{path}.execution_completeness"),
            "gated and stable promotion requires complete execution",
        ));
    }
    if rollout.contract_availability != ContractAvailability::RuntimeAvailable {
        return Err(invalid(
            format!("{path}.contract_availability"),
            "gated and stable promotion requires runtime availability",
        ));
    }
    if rollout.lifecycle != RolloutLifecycle::Active {
        return Err(invalid(
            format!("{path}.lifecycle"),
            "gated and stable promotion requires an active lifecycle",
        ));
    }
    if rollout.promotion_state == PromotionState::Stable {
        if rollout.support_maturity != SupportMaturity::Supported {
            return Err(invalid(
                format!("{path}.support_maturity"),
                "stable promotion requires supported maturity",
            ));
        }
        if !rollout.promotion_blockers.is_empty() {
            return Err(invalid(
                format!("{path}.promotion_blockers"),
                "stable promotion cannot retain blockers",
            ));
        }
    }

    let direct_reference = evidence.direct_hot_path_test_ref.as_deref().ok_or_else(|| {
        invalid(
            format!("{path}.evidence.direct_hot_path_test_ref"),
            "gated and stable promotion requires a direct hot-path proof",
        )
    })?;
    let fallback_reference = evidence.no_hidden_fallback_test_ref.as_deref().ok_or_else(|| {
        invalid(
            format!("{path}.evidence.no_hidden_fallback_test_ref"),
            "gated and stable promotion requires a no-hidden-fallback proof",
        )
    })?;
    for (field, reference) in [
        ("direct_hot_path_test_ref", direct_reference),
        ("no_hidden_fallback_test_ref", fallback_reference),
    ] {
        if !evidence.required_test_refs.iter().any(|required| required == reference) {
            return Err(invalid(
                format!("{path}.evidence.{field}"),
                "proof reference must also appear in required_test_refs",
            ));
        }
    }

    for (field, value) in [
        ("sli.indicator", evidence.sli.indicator.as_str()),
        ("sli.objective", evidence.sli.objective.as_str()),
        ("sli.window", evidence.sli.window.as_str()),
        ("rollback.mechanism", evidence.rollback.mechanism.as_str()),
        ("rollback.verification", evidence.rollback.verification.as_str()),
        ("compatibility_commitment", evidence.compatibility_commitment.as_str()),
        ("legacy_removal_condition", evidence.legacy_removal_condition.as_str()),
        ("removal_condition", rollout.removal_condition.as_str()),
    ] {
        validate_qualified_text(format!("{path}.evidence.{field}"), value)?;
    }
    Ok(())
}

fn validate_expected_rollouts(
    manifest: &FeatureRolloutPromotionManifest,
    expected_rollouts: &[(&str, &str)],
) -> Result<(), FeatureRolloutPromotionManifestError> {
    if expected_rollouts.len() != BUILTIN_ROLLOUT_COUNT {
        return Err(invalid(
            "runtime_descriptors",
            format!(
                "expected exactly {BUILTIN_ROLLOUT_COUNT} descriptors, found {}",
                expected_rollouts.len()
            ),
        ));
    }

    let mut expected = BTreeMap::new();
    for (capability, owner) in expected_rollouts {
        validate_identifier("runtime_descriptors.capability", capability)?;
        validate_text("runtime_descriptors.owner_component", owner)?;
        if expected.insert(*capability, *owner).is_some() {
            return Err(invalid(
                "runtime_descriptors.capability",
                format!("duplicate capability {capability}"),
            ));
        }
    }
    for rollout in &manifest.rollouts {
        let owner = expected.get(rollout.capability.as_str()).ok_or_else(|| {
            invalid(
                format!("rollouts.{}.capability", rollout.capability),
                "capability is absent from the runtime descriptor registry",
            )
        })?;
        if *owner != rollout.owner_component {
            return Err(invalid(
                format!("rollouts.{}.owner_component", rollout.capability),
                format!("expected {owner}, found {}", rollout.owner_component),
            ));
        }
    }
    Ok(())
}

fn validate_unique_text_list(
    path: impl Into<String>,
    values: &[String],
    minimum_items: usize,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    let path = path.into();
    if values.len() < minimum_items || values.len() > MAX_LIST_ITEMS {
        return Err(invalid(
            &path,
            format!("expected {minimum_items}..={MAX_LIST_ITEMS} entries, found {}", values.len()),
        ));
    }
    let mut unique = BTreeSet::new();
    for (index, value) in values.iter().enumerate() {
        validate_text(format!("{path}[{index}]"), value)?;
        if !unique.insert(value.as_str()) {
            return Err(invalid(format!("{path}[{index}]"), "duplicate entry"));
        }
    }
    Ok(())
}

fn validate_identifier(
    path: impl Into<String>,
    value: &str,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    let path = path.into();
    if value.is_empty() || value.len() > MAX_IDENTIFIER_BYTES {
        return Err(invalid(
            path,
            format!("identifier must contain 1..={MAX_IDENTIFIER_BYTES} ASCII bytes"),
        ));
    }
    let mut bytes = value.bytes();
    let valid_first = bytes.next().is_some_and(|byte| byte.is_ascii_lowercase());
    if !valid_first
        || !bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid(path, "identifier must match ^[a-z][a-z0-9_]*$"));
    }
    Ok(())
}

fn validate_text(
    path: impl Into<String>,
    value: &str,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    let path = path.into();
    if value.trim().is_empty() || value.len() > MAX_TEXT_BYTES {
        return Err(invalid(
            path,
            format!("text must contain 1..={MAX_TEXT_BYTES} bytes and non-whitespace content"),
        ));
    }
    Ok(())
}

fn validate_qualified_text(
    path: impl Into<String>,
    value: &str,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    let path = path.into();
    validate_text(&path, value)?;
    let normalized = value.trim().to_ascii_lowercase();
    const PLACEHOLDERS: &[&str] =
        &["todo", "tbd", "placeholder", "unknown", "fill me", "not defined", "n/a"];
    if PLACEHOLDERS.iter().any(|placeholder| normalized.contains(placeholder)) {
        return Err(invalid(path, "qualification evidence cannot contain placeholder text"));
    }
    Ok(())
}

fn validate_calendar_date(
    path: impl Into<String>,
    value: &str,
) -> Result<(), FeatureRolloutPromotionManifestError> {
    let path = path.into();
    let mut segments = value.split('-');
    let (Some(year), Some(month), Some(day), None) =
        (segments.next(), segments.next(), segments.next(), segments.next())
    else {
        return Err(invalid(path, "date must use YYYY-MM-DD format"));
    };
    if year.len() != 4 || month.len() != 2 || day.len() != 2 {
        return Err(invalid(path, "date must use YYYY-MM-DD format"));
    }
    let year = year.parse::<u32>().map_err(|_| invalid(&path, "date year is invalid"))?;
    let month = month.parse::<u32>().map_err(|_| invalid(&path, "date month is invalid"))?;
    let day = day.parse::<u32>().map_err(|_| invalid(&path, "date day is invalid"))?;
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => return Err(invalid(path, "date month is outside 01..=12")),
    };
    if day == 0 || day > max_day {
        return Err(invalid(path, "date day is outside the selected month"));
    }
    Ok(())
}

const fn is_leap_year(year: u32) -> bool {
    year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
}

fn invalid(
    path: impl Into<String>,
    reason: impl Into<String>,
) -> FeatureRolloutPromotionManifestError {
    FeatureRolloutPromotionManifestError::Invalid { path: path.into(), reason: reason.into() }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use super::{
        builtin_feature_rollout_promotion_manifest, canonical_json,
        parse_feature_rollout_promotion_manifest, FeatureRolloutPromotionManifestError,
        PromotionState, BUILTIN_MANIFEST_JSON, BUILTIN_SCHEMA_JSON,
    };

    const BASH_RUNTIME_CONTRACT_GATE: &str =
        include_str!("../../../../scripts/test/check-runtime-contract-snapshots.sh");
    const POWERSHELL_RUNTIME_CONTRACT_GATE: &str =
        include_str!("../../../../scripts/test/check-runtime-contract-snapshots.ps1");

    const EXPECTED_ROLLOUTS: &[(&str, &str)] = &[
        ("dynamic_tool_builder", "skills/tool runtime"),
        ("context_engine", "application/context_engine"),
        ("execution_backend_remote_node", "execution backends"),
        ("execution_backend_networked_worker", "workerd/execution backends"),
        ("execution_backend_docker", "execution backends"),
        ("execution_backend_ssh_tunnel", "execution backends"),
        ("safety_boundary", "safety"),
        ("execution_gate_pipeline_v2", "execution gate"),
        ("agent_harness_runtime", "application/agent_harness"),
        ("inline_runtime_hooks", "hooks/runtime loop"),
        ("tool_result_middleware", "tool runtime"),
        ("session_queue_policy", "session lifecycle"),
        ("pruning_policy_matrix", "memory/context pruning"),
        ("retrieval_dual_path", "memory/retrieval"),
        ("auxiliary_executor", "agent delegation"),
        ("flow_orchestration", "flow orchestration"),
        ("delivery_arbitration", "channel delivery"),
        ("replay_capture", "replay"),
        ("networked_workers", "workerd/execution backends"),
        ("tool_repair", "run stream/tool repair"),
        ("provider_stream_normalizer", "model provider streaming"),
        ("provider_recovery", "model provider recovery"),
        ("terminal_sessions", "process runtime"),
        ("browser_rescue", "browserd/browser rescue"),
        ("browser_resilience", "browserd/session recovery"),
        ("audio_pipeline", "application/audio pipeline"),
        ("computer_use", "workerd/computer use"),
        ("semantic_memory_consolidation", "memory/semantic consolidation"),
        ("lsp_service", "code intelligence"),
        ("advisor_fanout", "advisors"),
        ("acp_runtime", "acp runtime"),
        ("channel_turn_kernel", "channel router"),
        ("agent_plan_state", "agent plan state"),
        ("objective_judge", "objective judge"),
        ("verification_runtime", "verification runtime"),
        ("progress_drafts", "progress drafts"),
        ("compaction_safeguard", "session compaction"),
        ("provider_backed_evidence_compaction", "session compaction"),
        ("attack_surface_audit", "security audit"),
    ];

    #[test]
    fn builtin_promotion_manifest_is_valid() {
        let manifest = builtin_feature_rollout_promotion_manifest(EXPECTED_ROLLOUTS)
            .expect("built-in manifest must satisfy its release contract");

        assert_eq!(manifest.rollouts.len(), EXPECTED_ROLLOUTS.len());
        assert_eq!(manifest.resolved_rollouts().expect("evidence must resolve").len(), 39);
        for promotion in manifest.resolved_rollouts().expect("evidence must resolve") {
            if !matches!(
                promotion.rollout.promotion_state,
                PromotionState::GatedProduction | PromotionState::Stable
            ) {
                continue;
            }
            let direct_reference = promotion
                .evidence
                .direct_hot_path_test_ref
                .as_deref()
                .expect("production direct hot-path proof must be present");
            let fallback_reference = promotion
                .evidence
                .no_hidden_fallback_test_ref
                .as_deref()
                .expect("production no-hidden-fallback proof must be present");
            validate_runtime_contract_proof_pair(
                direct_reference,
                fallback_reference,
                BASH_RUNTIME_CONTRACT_GATE,
                POWERSHELL_RUNTIME_CONTRACT_GATE,
            )
            .unwrap_or_else(|error| panic!("production proof gate mismatch: {error}"));
        }
    }

    #[test]
    fn promotion_state_wire_spellings_are_stable() {
        let cases = [
            ("contract_only", PromotionState::ContractOnly),
            ("shadow", PromotionState::Shadow),
            ("canary", PromotionState::Canary),
            ("gated_production", PromotionState::GatedProduction),
            ("stable", PromotionState::Stable),
        ];

        for (wire, expected) in cases {
            let parsed: PromotionState =
                serde_json::from_value(json!(wire)).expect("golden promotion state must parse");
            assert_eq!(parsed, expected);
            assert_eq!(parsed.as_str(), wire);
            assert_eq!(serde_json::to_value(parsed).expect("state must serialize"), json!(wire));
        }
    }

    #[test]
    fn every_promotion_state_passes_manifest_cross_field_validation() {
        for state in ["contract_only", "shadow", "canary", "gated_production", "stable"] {
            let mut manifest = builtin_manifest_value();
            let rollout = rollout_mut(&mut manifest, "compaction_safeguard");
            rollout["promotion_state"] = json!(state);
            rollout["shadow_side_effect_posture"] =
                json!(if state == "shadow" { "side_effect_free" } else { "not_applicable" });
            if state == "stable" {
                rollout["support_maturity"] = json!("supported");
                rollout["promotion_blockers"] = json!([]);
            }

            parse_feature_rollout_promotion_manifest(
                &serde_json::to_string(&manifest).expect("fixture must serialize"),
                BUILTIN_SCHEMA_JSON,
            )
            .unwrap_or_else(|error| panic!("{state} fixture must validate: {error}"));
        }
    }

    #[test]
    fn stable_promotion_without_fallback_proof_is_rejected() {
        let mut manifest = builtin_manifest_value();
        let rollout = rollout_mut(&mut manifest, "compaction_safeguard");
        rollout["promotion_state"] = json!("stable");
        rollout["support_maturity"] = json!("supported");
        rollout["promotion_blockers"] = json!([]);
        manifest["evidence_profiles"]["compaction_gated"]
            .as_object_mut()
            .expect("profile fixture must be an object")
            .remove("no_hidden_fallback_test_ref");

        let error = parse_feature_rollout_promotion_manifest(
            &serde_json::to_string(&manifest).expect("fixture must serialize"),
            BUILTIN_SCHEMA_JSON,
        )
        .expect_err("stable promotion without fallback proof must fail");

        assert!(error.to_string().contains("no-hidden-fallback proof"));
    }

    #[test]
    fn canary_without_runtime_execution_is_rejected() {
        let mut manifest = builtin_manifest_value();
        let rollout = rollout_mut(&mut manifest, "compaction_safeguard");
        rollout["promotion_state"] = json!("canary");
        rollout["contract_availability"] = json!("descriptor_only");
        rollout["execution_completeness"] = json!("not_implemented");

        let error = parse_manifest_value(&manifest)
            .expect_err("canary promotion without a runtime implementation must fail");

        assert!(error.to_string().contains("canary promotion requires runtime availability"));
    }

    #[test]
    fn shadow_without_runtime_availability_is_rejected() {
        let mut manifest = builtin_manifest_value();
        let rollout = rollout_mut(&mut manifest, "compaction_safeguard");
        rollout["promotion_state"] = json!("shadow");
        rollout["contract_availability"] = json!("api_available");
        rollout["execution_completeness"] = json!("partial");
        rollout["shadow_side_effect_posture"] = json!("side_effect_free");

        let error = parse_manifest_value(&manifest)
            .expect_err("shadow promotion without runtime availability must fail");

        assert!(error.to_string().contains("shadow promotion requires runtime availability"));
    }

    #[test]
    fn gated_production_with_unsupported_maturity_is_rejected() {
        let mut manifest = builtin_manifest_value();
        let rollout = rollout_mut(&mut manifest, "compaction_safeguard");
        rollout["support_maturity"] = json!("unsupported");

        let error = parse_manifest_value(&manifest)
            .expect_err("gated production cannot advertise unsupported maturity");

        assert!(error
            .to_string()
            .contains("gated_production promotion requires preview or supported maturity"));
    }

    #[test]
    fn active_contract_only_with_supported_maturity_is_rejected() {
        let mut manifest = builtin_manifest_value();
        let rollout = rollout_mut(&mut manifest, "compaction_safeguard");
        rollout["promotion_state"] = json!("contract_only");
        rollout["support_maturity"] = json!("supported");

        let error = parse_manifest_value(&manifest)
            .expect_err("active contract-only promotion cannot advertise supported maturity");

        assert!(error.to_string().contains(
            "active contract_only promotion permits unsupported, experimental, or preview support"
        ));
    }

    #[test]
    fn retired_canary_is_rejected() {
        let mut manifest = builtin_manifest_value();
        let rollout = rollout_mut(&mut manifest, "compaction_safeguard");
        rollout["promotion_state"] = json!("canary");
        rollout["lifecycle"] = json!("retired");
        rollout["support_maturity"] = json!("retired");

        let error =
            parse_manifest_value(&manifest).expect_err("retired canary promotion must fail");

        assert!(error.to_string().contains("canary promotion requires an active lifecycle"));
    }

    #[test]
    fn proof_gate_rejects_substring_only_matches() {
        let error = validate_runtime_contract_proof_pair(
            "cargo test -p palyra-daemon gateway::tests::feature --locked -- --exact",
            "cargo test -p palyra-daemon gateway::tests::rollout --locked -- --exact",
            "feature rollout direct hot-path proof",
            "feature rollout no-hidden-fallback proof",
        )
        .expect_err("labels containing test-filter substrings must not count as executed proofs");

        assert!(error.contains("exact Bash command"));
    }

    #[test]
    fn proof_gate_rejects_duplicate_direct_and_fallback_references() {
        let reference = "cargo test -p palyra-daemon gateway::tests::one_proof --locked -- --exact";
        let error = validate_runtime_contract_proof_pair(
            reference,
            reference,
            BASH_RUNTIME_CONTRACT_GATE,
            POWERSHELL_RUNTIME_CONTRACT_GATE,
        )
        .expect_err("one test cannot prove both direct and fallback behavior");

        assert!(error.contains("must be distinct"));
    }

    #[test]
    fn schema_hash_uses_recursive_key_sorted_compact_json() {
        let manifest = builtin_feature_rollout_promotion_manifest(EXPECTED_ROLLOUTS)
            .expect("built-in manifest must validate");
        let schema: Value =
            serde_json::from_str(BUILTIN_SCHEMA_JSON).expect("built-in schema must parse");
        let reordered = json!({"z": [schema.clone()], "a": {"second": 2, "first": 1}});
        let equivalent = json!({"a": {"first": 1, "second": 2}, "z": [schema]});

        assert_eq!(
            canonical_json(&reordered).expect("fixture must canonicalize"),
            canonical_json(&equivalent).expect("fixture must canonicalize")
        );
        assert_eq!(manifest.schema_sha256.len(), 64);
    }

    #[test]
    fn descriptor_owner_drift_is_rejected() {
        let mut expected = EXPECTED_ROLLOUTS.to_vec();
        expected[0].1 = "wrong owner";

        let error = builtin_feature_rollout_promotion_manifest(&expected)
            .expect_err("owner drift must fail closed");

        assert!(matches!(error, FeatureRolloutPromotionManifestError::Invalid { .. }));
        assert!(error.to_string().contains("owner_component"));
    }

    fn builtin_manifest_value() -> Value {
        serde_json::from_str(BUILTIN_MANIFEST_JSON).expect("built-in manifest fixture must parse")
    }

    fn parse_manifest_value(
        manifest: &Value,
    ) -> Result<super::FeatureRolloutPromotionManifest, FeatureRolloutPromotionManifestError> {
        parse_feature_rollout_promotion_manifest(
            &serde_json::to_string(manifest).expect("manifest fixture must serialize"),
            BUILTIN_SCHEMA_JSON,
        )
    }

    fn rollout_mut<'a>(manifest: &'a mut Value, capability: &str) -> &'a mut Value {
        manifest["rollouts"]
            .as_array_mut()
            .expect("rollout fixture must be an array")
            .iter_mut()
            .find(|rollout| rollout["capability"] == capability)
            .expect("fixture capability must exist")
    }

    fn validate_runtime_contract_proof_pair(
        direct_reference: &str,
        fallback_reference: &str,
        bash_gate: &str,
        powershell_gate: &str,
    ) -> Result<(), String> {
        if direct_reference == fallback_reference {
            return Err("production direct and fallback proofs must be distinct".to_owned());
        }
        for reference in [direct_reference, fallback_reference] {
            let filter = cargo_test_filter(reference)?;
            let bash_command =
                format!(r#""$CARGO_BIN" test -p palyra-daemon {filter} --locked -- --exact"#);
            if !contains_exact_trimmed_line(bash_gate, bash_command.as_str()) {
                return Err(format!(
                    "runtime-contract gate is missing exact Bash command for {reference}"
                ));
            }
            let powershell_args = format!(
                r#"-CargoArgs @("test", "-p", "palyra-daemon", "{filter}", "--locked", "--", "--exact")"#,
            );
            if !contains_exact_trimmed_line(powershell_gate, powershell_args.as_str()) {
                return Err(format!(
                    "runtime-contract gate is missing exact PowerShell command for {reference}"
                ));
            }
        }
        Ok(())
    }

    fn contains_exact_trimmed_line(document: &str, expected: &str) -> bool {
        document.lines().any(|line| line.trim() == expected)
    }

    fn cargo_test_filter(reference: &str) -> Result<&str, String> {
        let parts = reference.split_ascii_whitespace().collect::<Vec<_>>();
        if parts.len() != 8
            || parts[..4] != ["cargo", "test", "-p", "palyra-daemon"]
            || parts[5..] != ["--locked", "--", "--exact"]
            || !parts[4].starts_with("gateway::tests::")
        {
            return Err(format!(
                "production proof must be one fixed locked daemon test command: {reference}"
            ));
        }
        Ok(parts[4])
    }
}
