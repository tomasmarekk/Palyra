//! Fail-closed retirement contract for legacy runtime generation selectors.
//!
//! The embedded manifest keeps durable readers distinct from executable
//! admission. It also time-bounds accepted config inputs without turning a
//! deprecated flag into hidden runtime authority.

use std::collections::BTreeSet;

use palyra_common::feature_rollouts::FeatureRolloutSource;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    application::runtime_kernel_v2::profile_resolver::ResolvedRuntimeProfileV1,
    config::FeatureRolloutsConfig,
};

const LEGACY_RETIREMENT_SCHEMA_VERSION: u32 = 1;
const CORE_RUNTIME_CONTRACT_VERSION: &str = "runtime-contracts.v15";
const REQUIRED_RETIRED_GENERATION_FLAGS: [&str; 6] = [
    "feature_rollouts.context_engine",
    "feature_rollouts.provider_stream_normalizer",
    "feature_rollouts.provider_recovery",
    "feature_rollouts.session_queue_policy",
    "feature_rollouts.replay_capture",
    "feature_rollouts.delivery_arbitration",
];
const REQUIRED_DURABLE_ROUTES: [&str; 3] =
    ["run_stream_legacy_authority", "legacy_orchestrator_tape_reader", "pre_v2_journal_migration"];

const BUILTIN_MANIFEST_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../infra/release/legacy-retirement.json"
));

/// Supported posture for a retained legacy route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LegacyRouteDisposition {
    CompatibilityOnly,
    DurableReader,
}

/// One bounded compatibility surface retained after V2 becomes the default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyRouteRecord {
    pub(crate) route_id: String,
    pub(crate) disposition: LegacyRouteDisposition,
    pub(crate) replacement: String,
    pub(crate) new_run_admission: bool,
    pub(crate) preserves_durable_data: bool,
    pub(crate) removal_condition: String,
    pub(crate) owner: String,
    pub(crate) evidence_refs: Vec<String>,
}

/// Operator-visible migration guidance for one accepted deprecated input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConfigDeprecationNotice {
    pub(crate) key: String,
    pub(crate) scope: String,
    pub(crate) replacement: String,
    pub(crate) accepted_for_read: bool,
    pub(crate) removal_condition: String,
    pub(crate) reason_code: String,
}

/// A source module removed after its production replacement became canonical.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RemovedLegacyModule {
    pub(crate) path: String,
    pub(crate) replacement: String,
}

/// Static size and legacy-start budget enforced by the architecture gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OrchestrationBudget {
    pub(crate) path: String,
    pub(crate) max_lines: u32,
    pub(crate) allowed_legacy_start_sites: u32,
}

/// Canonical retirement inventory for runtime branches and config inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyRetirementManifest {
    pub(crate) schema_version: u32,
    pub(crate) runtime_contract_version: String,
    pub(crate) replacement_profile: String,
    pub(crate) generated_from: String,
    pub(crate) as_of: String,
    pub(crate) removal_deadline: String,
    pub(crate) legacy_routes: Vec<LegacyRouteRecord>,
    pub(crate) config_deprecations: Vec<ConfigDeprecationNotice>,
    pub(crate) independent_high_risk_flags: Vec<String>,
    pub(crate) removed_modules: Vec<RemovedLegacyModule>,
    pub(crate) orchestration_budget: OrchestrationBudget,
    pub(crate) release_rollback_only: bool,
}

/// One stable reason why the retirement contract blocks release.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LegacyRetirementIssue {
    pub(crate) code: &'static str,
    pub(crate) subject: String,
}

/// Release decision derived from the retirement manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct LegacyRetirementQualification {
    pub(crate) schema_version: u32,
    pub(crate) qualified: bool,
    pub(crate) reason_code: &'static str,
    pub(crate) issues: Vec<LegacyRetirementIssue>,
}

/// Parses the repository-owned retirement manifest.
///
/// # Errors
/// Returns a stable serialization error when the embedded contract drifts.
pub(crate) fn builtin_legacy_retirement_manifest(
) -> Result<LegacyRetirementManifest, serde_json::Error> {
    serde_json::from_str(BUILTIN_MANIFEST_JSON)
}

/// Evaluates the complete retirement inventory without inspecting host state.
#[must_use]
pub(crate) fn evaluate_legacy_retirement(
    manifest: &LegacyRetirementManifest,
) -> LegacyRetirementQualification {
    let mut issues = Vec::new();
    if manifest.schema_version != LEGACY_RETIREMENT_SCHEMA_VERSION {
        issue(
            &mut issues,
            "runtime.retirement.schema_version_unsupported",
            manifest.schema_version.to_string(),
        );
    }
    if manifest.runtime_contract_version != CORE_RUNTIME_CONTRACT_VERSION {
        issue(
            &mut issues,
            "runtime.retirement.contract_version_mismatch",
            manifest.runtime_contract_version.clone(),
        );
    }
    if manifest.replacement_profile != "v2" || !manifest.release_rollback_only {
        issue(
            &mut issues,
            "runtime.retirement.rollback_contract_invalid",
            manifest.replacement_profile.clone(),
        );
    }
    if manifest.removal_deadline.trim().is_empty()
        || manifest.orchestration_budget.max_lines > 11_000
        || manifest.orchestration_budget.allowed_legacy_start_sites != 1
    {
        issue(
            &mut issues,
            "runtime.retirement.boundary_budget_invalid",
            manifest.orchestration_budget.path.clone(),
        );
    }

    validate_routes(manifest, &mut issues);
    validate_deprecations(manifest, &mut issues);
    validate_removed_modules(manifest, &mut issues);

    let qualified = issues.is_empty();
    LegacyRetirementQualification {
        schema_version: LEGACY_RETIREMENT_SCHEMA_VERSION,
        qualified,
        reason_code: if qualified {
            "runtime.retirement.qualified"
        } else {
            "runtime.retirement.release_blocked"
        },
        issues,
    }
}

/// Builds the redacted operator projection, including only configured notices.
#[must_use]
pub(crate) fn build_legacy_retirement_snapshot(
    profile: &ResolvedRuntimeProfileV1,
    feature_rollouts: &FeatureRolloutsConfig,
) -> Value {
    let Ok(manifest) = builtin_legacy_retirement_manifest() else {
        return invalid_manifest_snapshot();
    };
    let qualification = evaluate_legacy_retirement(&manifest);
    let profile = serde_json::to_value(profile).unwrap_or(Value::Null);
    let configured_profile = profile.get("profile").and_then(Value::as_str);
    let configured_deprecations =
        configured_deprecation_notices(&manifest, feature_rollouts, configured_profile);

    serde_json::json!({
        "schema_version": qualification.schema_version,
        "qualified": qualification.qualified,
        "reason_code": qualification.reason_code,
        "issues": qualification.issues,
        "replacement_profile": manifest.replacement_profile,
        "configured_profile": configured_profile,
        "new_legacy_run_admission": false,
        "release_rollback_only": manifest.release_rollback_only,
        "removal_deadline": manifest.removal_deadline,
        "retained_legacy_route_count": manifest.legacy_routes.len(),
        "durable_reader_count": manifest
            .legacy_routes
            .iter()
            .filter(|route| route.disposition == LegacyRouteDisposition::DurableReader)
            .count(),
        "configured_deprecation_notices": configured_deprecations,
        "independent_high_risk_flags": manifest.independent_high_risk_flags,
    })
}

fn validate_routes(manifest: &LegacyRetirementManifest, issues: &mut Vec<LegacyRetirementIssue>) {
    let mut observed = BTreeSet::new();
    for route in &manifest.legacy_routes {
        if !observed.insert(route.route_id.as_str()) {
            issue(issues, "runtime.retirement.route_duplicate", route.route_id.clone());
        }
        if route.new_run_admission
            || !route.preserves_durable_data
            || route.replacement.trim().is_empty()
            || route.removal_condition.trim().is_empty()
            || route.owner.trim().is_empty()
            || route.evidence_refs.is_empty()
        {
            issue(issues, "runtime.retirement.route_incomplete", route.route_id.clone());
        }
    }
    for required in REQUIRED_DURABLE_ROUTES {
        if !observed.contains(required) {
            issue(issues, "runtime.retirement.route_missing", required.to_owned());
        }
    }
}

fn validate_deprecations(
    manifest: &LegacyRetirementManifest,
    issues: &mut Vec<LegacyRetirementIssue>,
) {
    let mut observed = BTreeSet::new();
    for notice in &manifest.config_deprecations {
        if !observed.insert(notice.key.as_str()) {
            issue(issues, "runtime.retirement.config_duplicate", notice.key.clone());
        }
        if !notice.accepted_for_read
            || notice.replacement.trim().is_empty()
            || notice.removal_condition.trim().is_empty()
            || !notice.reason_code.starts_with("runtime.")
        {
            issue(issues, "runtime.retirement.config_notice_invalid", notice.key.clone());
        }
    }
    for required in REQUIRED_RETIRED_GENERATION_FLAGS {
        if !observed.contains(required) {
            issue(issues, "runtime.retirement.config_notice_missing", required.to_owned());
        }
    }
}

fn validate_removed_modules(
    manifest: &LegacyRetirementManifest,
    issues: &mut Vec<LegacyRetirementIssue>,
) {
    let removed =
        manifest.removed_modules.iter().map(|module| module.path.as_str()).collect::<BTreeSet<_>>();
    for required in ["application/release_hardening.rs", "application/runtime_boundary_metrics.rs"]
    {
        if !removed.contains(required) {
            issue(issues, "runtime.retirement.removed_module_missing", required.to_owned());
        }
    }
}

fn configured_deprecation_notices(
    manifest: &LegacyRetirementManifest,
    config: &FeatureRolloutsConfig,
    configured_profile: Option<&str>,
) -> Vec<ConfigDeprecationNotice> {
    let mut configured = [
        ("feature_rollouts.context_engine", config.context_engine.source),
        ("feature_rollouts.provider_stream_normalizer", config.provider_stream_normalizer.source),
        ("feature_rollouts.provider_recovery", config.provider_recovery.source),
        ("feature_rollouts.session_queue_policy", config.session_queue_policy.source),
        ("feature_rollouts.replay_capture", config.replay_capture.source),
        ("feature_rollouts.delivery_arbitration", config.delivery_arbitration.source),
    ]
    .into_iter()
    .filter_map(|(key, source)| (source != FeatureRolloutSource::Default).then_some(key))
    .collect::<BTreeSet<_>>();
    if configured_profile == Some("legacy") {
        configured.insert("runtime_kernel.profile=legacy");
    }

    manifest
        .config_deprecations
        .iter()
        .filter(|notice| configured.contains(notice.key.as_str()))
        .cloned()
        .collect()
}

fn issue(issues: &mut Vec<LegacyRetirementIssue>, code: &'static str, subject: String) {
    issues.push(LegacyRetirementIssue { code, subject });
}

fn invalid_manifest_snapshot() -> Value {
    serde_json::json!({
        "schema_version": LEGACY_RETIREMENT_SCHEMA_VERSION,
        "qualified": false,
        "reason_code": "runtime.retirement.manifest_invalid",
        "issues": [{
            "code": "runtime.retirement.manifest_invalid",
            "subject": "canonical_manifest",
        }],
        "new_legacy_run_admission": false,
        "release_rollback_only": true,
    })
}

#[cfg(test)]
mod tests {
    use palyra_common::feature_rollouts::FeatureRolloutSetting;

    use crate::{
        application::runtime_kernel_v2::profile_resolver::RuntimeProfileResolver,
        config::{FeatureRolloutsConfig, RuntimeKernelConfig},
    };

    use super::*;

    #[test]
    fn builtin_manifest_qualifies_complete_retirement_inventory() {
        let manifest =
            builtin_legacy_retirement_manifest().expect("embedded manifest should parse");
        let qualification = evaluate_legacy_retirement(&manifest);

        assert!(qualification.qualified, "{:?}", qualification.issues);
        assert_eq!(qualification.reason_code, "runtime.retirement.qualified");
        assert!(manifest.legacy_routes.iter().all(|route| !route.new_run_admission));
        assert!(manifest.legacy_routes.iter().all(|route| route.preserves_durable_data));
    }

    #[test]
    fn admitted_legacy_route_blocks_release() {
        let mut manifest =
            builtin_legacy_retirement_manifest().expect("embedded manifest should parse");
        manifest.legacy_routes[0].new_run_admission = true;

        let qualification = evaluate_legacy_retirement(&manifest);

        assert!(!qualification.qualified);
        assert!(qualification
            .issues
            .iter()
            .any(|issue| issue.code == "runtime.retirement.route_incomplete"));
    }

    #[test]
    fn diagnostics_report_only_explicit_generation_flag_notices() {
        let rollouts = FeatureRolloutsConfig {
            context_engine: FeatureRolloutSetting::from_config(true),
            provider_recovery: FeatureRolloutSetting::from_env(true),
            ..FeatureRolloutsConfig::default()
        };
        let resolver = RuntimeProfileResolver::resolve(
            &RuntimeKernelConfig::default(),
            &FeatureRolloutsConfig::default(),
            None,
        )
        .expect("default runtime profile should resolve");

        let snapshot = build_legacy_retirement_snapshot(resolver.diagnostics(), &rollouts);

        assert_eq!(snapshot["qualified"], true);
        assert_eq!(snapshot["new_legacy_run_admission"], false);
        let notices = snapshot["configured_deprecation_notices"]
            .as_array()
            .expect("deprecation notices should be an array");
        assert_eq!(notices.len(), 2);
        assert!(notices.iter().all(|notice| {
            notice["reason_code"] == "runtime.config.generation_flag_deprecated"
        }));
    }

    #[test]
    fn legacy_profile_diagnostics_include_the_new_session_deprecation() {
        let config = RuntimeKernelConfig {
            profile: crate::config::RuntimeKernelProfile::Legacy,
            ..RuntimeKernelConfig::default()
        };
        let resolver =
            RuntimeProfileResolver::resolve(&config, &FeatureRolloutsConfig::default(), None)
                .expect("legacy compatibility profile should remain readable");

        let snapshot = build_legacy_retirement_snapshot(
            resolver.diagnostics(),
            &FeatureRolloutsConfig::default(),
        );
        let notices = snapshot["configured_deprecation_notices"]
            .as_array()
            .expect("deprecation notices should be an array");

        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0]["key"], "runtime_kernel.profile=legacy");
        assert_eq!(notices[0]["reason_code"], "runtime.legacy.new_session_retired");
    }
}
