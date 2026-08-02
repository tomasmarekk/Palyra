//! Evidence-derived support and SLI contract for the production core runtime.
//!
//! The embedded pack joins release gates, maturity floors, runbook drills,
//! compatibility commitments, and support-bundle redaction without granting a
//! manifest any runtime authority.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;

const STABLE_EVIDENCE_SCHEMA_VERSION: u32 = 1;
const STABLE_EVIDENCE_CONTRACT_ID: &str = "palyra.stable-core-evidence.v1";
const CORE_RUNTIME_CONTRACT_VERSION: &str = "runtime-contracts.v15";
const REQUIRED_CAPABILITIES: [&str; 7] = [
    "runtime_kernel_v2",
    "provider_recovery",
    "continuity_safe_resume",
    "objective_loop",
    "managed_coding_runtime",
    "work_graph",
    "mcp_persistent_runtime",
];
const REQUIRED_SLI_METRICS: [&str; 7] = [
    "core_run_completion_success_rate_bps",
    "core_recovery_success_rate_bps",
    "core_interrupt_latency_p99_ms",
    "core_duplicate_confirmed_effect_total",
    "core_cleanup_success_rate_bps",
    "mcp_reconnect_latency_p99_ms",
    "core_resource_pressure_rejection_rate_bps",
];
const REQUIRED_RUNBOOKS: [&str; 7] = [
    "continuity_recovery_blocked",
    "plugin_quarantine",
    "mcp_outage",
    "lsp_crash",
    "pty_orphan",
    "work_graph_stale_claim",
    "core_release_rollback",
];
const DISALLOWED_HIGH_CARDINALITY_LABELS: [&str; 8] = [
    "run_id",
    "session_id",
    "trace_id",
    "user_id",
    "principal",
    "server_id",
    "workspace_path",
    "error_message",
];
const REQUIRED_SUPPORT_CHECKS: [&str; 5] = [
    "metadata_only_runtime_evidence",
    "secret_and_credential_redaction",
    "prompt_and_tool_payload_redaction",
    "absolute_path_redaction",
    "bounded_low_cardinality_labels",
];

const BUILTIN_EVIDENCE_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../infra/release/stable-core-evidence.json"
));
const ALERT_FIXTURE_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../qa/fixtures/core-runtime-alert-thresholds.v1.json"
));
const RUNBOOK_DRILL_FIXTURE_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../qa/fixtures/core-runtime-runbook-drill.v1.json"
));

/// Supported production maturity for a core capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CoreCapabilityMaturity {
    GatedProduction,
    Stable,
}

impl CoreCapabilityMaturity {
    const fn rank(self) -> u8 {
        match self {
            Self::GatedProduction => 1,
            Self::Stable => 2,
        }
    }
}

/// Evidence and support commitment for one production core capability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StableCapabilityEvidence {
    pub(crate) capability_id: String,
    pub(crate) maturity: CoreCapabilityMaturity,
    pub(crate) minimum_maturity: CoreCapabilityMaturity,
    pub(crate) owner_component: String,
    pub(crate) owner_signoff: String,
    pub(crate) evidence_status: String,
    pub(crate) direct_hot_path: bool,
    pub(crate) no_hidden_fallback: bool,
    pub(crate) default_for_new_runs: bool,
    pub(crate) required_gate_refs: Vec<String>,
    pub(crate) compatibility_commitment: String,
    pub(crate) rollback_control: String,
    pub(crate) rollback_preserves_durable_data: bool,
    pub(crate) rollback_repeats_confirmed_side_effects: bool,
    pub(crate) runbook_ids: Vec<String>,
    pub(crate) promotion_blockers: Vec<String>,
}

/// Comparison direction for an alert threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SliDirection {
    AtLeast,
    AtMost,
}

/// Low-cardinality release SLI and its alert thresholds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CoreRuntimeSliDefinition {
    pub(crate) metric_id: String,
    pub(crate) direction: SliDirection,
    pub(crate) target: u64,
    pub(crate) warning: u64,
    pub(crate) critical: u64,
    pub(crate) window: String,
    pub(crate) allowed_labels: Vec<String>,
    pub(crate) reason_code: String,
}

/// Runbook section and synthetic-drill status pinned by release evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StableRunbookRecord {
    pub(crate) runbook_id: String,
    pub(crate) section: String,
    pub(crate) synthetic_drill: String,
    pub(crate) reason_code: String,
}

/// One required support-bundle safety check.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SupportBundleChecklistItem {
    pub(crate) check_id: String,
    pub(crate) required: bool,
    pub(crate) validation_ref: String,
}

/// Closure evidence for compatibility-only legacy surfaces.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyRetirementEvidence {
    pub(crate) manifest_ref: String,
    pub(crate) status: String,
    pub(crate) new_legacy_run_admission: bool,
    pub(crate) durable_compatibility_reads_preserved: bool,
}

/// Canonical release evidence and support contract for the core runtime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StableCapabilityEvidencePack {
    pub(crate) schema_version: u32,
    pub(crate) contract_id: String,
    pub(crate) runtime_contract_version: String,
    pub(crate) as_of: String,
    pub(crate) release_support_posture: String,
    pub(crate) capabilities: Vec<StableCapabilityEvidence>,
    pub(crate) sli_definitions: Vec<CoreRuntimeSliDefinition>,
    pub(crate) runbooks: Vec<StableRunbookRecord>,
    pub(crate) support_bundle_checklist: Vec<SupportBundleChecklistItem>,
    pub(crate) legacy_retirement_evidence: LegacyRetirementEvidence,
}

/// One bounded reason why stable-core release qualification is blocked.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StableEvidenceIssue {
    pub(crate) code: &'static str,
    pub(crate) subject: String,
}

/// Evidence-derived stable-core release decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct StableEvidenceQualification {
    pub(crate) schema_version: u32,
    pub(crate) qualified: bool,
    pub(crate) reason_code: &'static str,
    pub(crate) issues: Vec<StableEvidenceIssue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AlertState {
    Healthy,
    Warning,
    Critical,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertThresholdFixture {
    schema_version: u32,
    cases: Vec<AlertThresholdCase>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AlertThresholdCase {
    metric_id: String,
    observed: u64,
    expected: AlertState,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunbookDrillFixture {
    schema_version: u32,
    incidents: Vec<SyntheticIncident>,
    support_bundle_sample: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SyntheticIncident {
    incident_id: String,
    runbook_id: String,
    evidence_fields: Vec<String>,
    contains_raw_secret: bool,
    rollback_preserves_durable_data: bool,
    result: String,
}

/// Parses the repository-owned stable-core evidence pack.
///
/// # Errors
/// Returns a serialization error when the embedded contract drifts.
pub(crate) fn builtin_stable_capability_evidence_pack(
) -> Result<StableCapabilityEvidencePack, serde_json::Error> {
    serde_json::from_str(BUILTIN_EVIDENCE_JSON)
}

/// Evaluates stable maturity, SLI, drill, redaction, and retirement evidence.
#[must_use]
pub(crate) fn evaluate_stable_capability_evidence(
    pack: &StableCapabilityEvidencePack,
) -> StableEvidenceQualification {
    let mut issues = Vec::new();
    if pack.schema_version != STABLE_EVIDENCE_SCHEMA_VERSION
        || pack.contract_id != STABLE_EVIDENCE_CONTRACT_ID
        || pack.runtime_contract_version != CORE_RUNTIME_CONTRACT_VERSION
    {
        issue(&mut issues, "core.stable.contract_mismatch", pack.contract_id.clone());
    }
    if pack.as_of.trim().is_empty() || pack.release_support_posture != "production" {
        issue(
            &mut issues,
            "core.stable.support_posture_invalid",
            pack.release_support_posture.clone(),
        );
    }

    validate_capabilities(pack, &mut issues);
    validate_sli_definitions(pack, &mut issues);
    validate_runbooks(pack, &mut issues);
    validate_support_checklist(pack, &mut issues);
    validate_legacy_retirement(pack, &mut issues);
    validate_alert_fixture(pack, &mut issues);
    validate_runbook_drill_fixture(pack, &mut issues);

    let qualified = issues.is_empty();
    StableEvidenceQualification {
        schema_version: STABLE_EVIDENCE_SCHEMA_VERSION,
        qualified,
        reason_code: if qualified {
            "core.stable.release_qualified"
        } else {
            "core.stable.release_blocked"
        },
        issues,
    }
}

/// Builds the bounded diagnostics and support-bundle projection.
#[must_use]
pub(crate) fn build_stable_core_evidence_snapshot() -> Value {
    let Ok(pack) = builtin_stable_capability_evidence_pack() else {
        return invalid_pack_snapshot();
    };
    let qualification = evaluate_stable_capability_evidence(&pack);
    let stable = pack
        .capabilities
        .iter()
        .filter(|capability| capability.maturity == CoreCapabilityMaturity::Stable)
        .map(|capability| capability.capability_id.as_str())
        .collect::<Vec<_>>();
    let gated_production = pack
        .capabilities
        .iter()
        .filter(|capability| capability.maturity == CoreCapabilityMaturity::GatedProduction)
        .map(|capability| capability.capability_id.as_str())
        .collect::<Vec<_>>();
    let sli = pack
        .sli_definitions
        .iter()
        .map(|definition| {
            serde_json::json!({
                "metric_id": definition.metric_id,
                "direction": definition.direction,
                "target": definition.target,
                "warning": definition.warning,
                "critical": definition.critical,
                "window": definition.window,
                "allowed_labels": definition.allowed_labels,
                "reason_code": definition.reason_code,
            })
        })
        .collect::<Vec<_>>();

    serde_json::json!({
        "schema_version": qualification.schema_version,
        "contract_id": pack.contract_id,
        "qualified": qualification.qualified,
        "reason_code": qualification.reason_code,
        "issues": qualification.issues,
        "release_support_posture": pack.release_support_posture,
        "capability_count": pack.capabilities.len(),
        "stable_capabilities": stable,
        "gated_production_capabilities": gated_production,
        "p0_blocker_count": pack
            .capabilities
            .iter()
            .map(|capability| capability.promotion_blockers.len())
            .sum::<usize>(),
        "sli_definitions": sli,
        "runbook_count": pack.runbooks.len(),
        "synthetic_runbooks_passed": pack
            .runbooks
            .iter()
            .filter(|runbook| runbook.synthetic_drill == "passed")
            .count(),
        "support_bundle_check_count": pack.support_bundle_checklist.len(),
        "legacy_retirement_closed": pack.legacy_retirement_evidence.status == "passed"
            && !pack.legacy_retirement_evidence.new_legacy_run_admission
            && pack.legacy_retirement_evidence.durable_compatibility_reads_preserved,
        "redaction_level": "metadata_only",
    })
}

fn validate_capabilities(
    pack: &StableCapabilityEvidencePack,
    issues: &mut Vec<StableEvidenceIssue>,
) {
    let mut observed = BTreeSet::new();
    for capability in &pack.capabilities {
        if !observed.insert(capability.capability_id.as_str()) {
            issue(issues, "core.stable.capability_duplicate", capability.capability_id.clone());
        }
        if capability.maturity.rank() < capability.minimum_maturity.rank() {
            issue(issues, "core.stable.maturity_downgrade", capability.capability_id.clone());
        }
        if capability.owner_component.trim().is_empty()
            || capability.owner_signoff != "@tomasmarekk"
            || capability.evidence_status != "passed"
            || !capability.direct_hot_path
            || !capability.no_hidden_fallback
            || !capability.default_for_new_runs
            || capability.required_gate_refs.is_empty()
            || capability.compatibility_commitment.trim().is_empty()
            || capability.rollback_control.trim().is_empty()
            || !capability.rollback_preserves_durable_data
            || capability.rollback_repeats_confirmed_side_effects
            || capability.runbook_ids.is_empty()
            || !capability.promotion_blockers.is_empty()
        {
            issue(
                issues,
                "core.stable.capability_evidence_incomplete",
                capability.capability_id.clone(),
            );
        }
    }
    for required in REQUIRED_CAPABILITIES {
        if !observed.contains(required) {
            issue(issues, "core.stable.capability_missing", required.to_owned());
        }
    }
}

fn validate_sli_definitions(
    pack: &StableCapabilityEvidencePack,
    issues: &mut Vec<StableEvidenceIssue>,
) {
    let disallowed = DISALLOWED_HIGH_CARDINALITY_LABELS.into_iter().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    for sli in &pack.sli_definitions {
        if !observed.insert(sli.metric_id.as_str()) {
            issue(issues, "core.stable.sli_duplicate", sli.metric_id.clone());
        }
        let ordered = match sli.direction {
            SliDirection::AtLeast => sli.target >= sli.warning && sli.warning >= sli.critical,
            SliDirection::AtMost => sli.target <= sli.warning && sli.warning <= sli.critical,
        };
        if !ordered
            || sli.window.trim().is_empty()
            || sli.allowed_labels.is_empty()
            || sli.allowed_labels.len() > 4
            || sli.allowed_labels.iter().any(|label| disallowed.contains(label.as_str()))
            || !sli.reason_code.starts_with("core.sli.")
        {
            issue(issues, "core.stable.sli_invalid", sli.metric_id.clone());
        }
    }
    for required in REQUIRED_SLI_METRICS {
        if !observed.contains(required) {
            issue(issues, "core.stable.sli_missing", required.to_owned());
        }
    }
}

fn validate_runbooks(pack: &StableCapabilityEvidencePack, issues: &mut Vec<StableEvidenceIssue>) {
    let mut observed = BTreeSet::new();
    for runbook in &pack.runbooks {
        if !observed.insert(runbook.runbook_id.as_str()) {
            issue(issues, "core.stable.runbook_duplicate", runbook.runbook_id.clone());
        }
        if runbook.section.trim().is_empty()
            || runbook.synthetic_drill != "passed"
            || !runbook.reason_code.starts_with("core.runbook.")
        {
            issue(issues, "core.stable.runbook_unqualified", runbook.runbook_id.clone());
        }
    }
    for required in REQUIRED_RUNBOOKS {
        if !observed.contains(required) {
            issue(issues, "core.stable.runbook_missing", required.to_owned());
        }
    }
    for capability in &pack.capabilities {
        for runbook_id in &capability.runbook_ids {
            if !observed.contains(runbook_id.as_str()) {
                issue(
                    issues,
                    "core.stable.capability_runbook_missing",
                    capability.capability_id.clone(),
                );
            }
        }
    }
}

fn validate_support_checklist(
    pack: &StableCapabilityEvidencePack,
    issues: &mut Vec<StableEvidenceIssue>,
) {
    let mut observed = BTreeSet::new();
    for check in &pack.support_bundle_checklist {
        if !observed.insert(check.check_id.as_str()) {
            issue(issues, "core.stable.support_check_duplicate", check.check_id.clone());
        }
        if !check.required || check.validation_ref.trim().is_empty() {
            issue(issues, "core.stable.support_check_incomplete", check.check_id.clone());
        }
    }
    for required in REQUIRED_SUPPORT_CHECKS {
        if !observed.contains(required) {
            issue(issues, "core.stable.support_check_missing", required.to_owned());
        }
    }
}

fn validate_legacy_retirement(
    pack: &StableCapabilityEvidencePack,
    issues: &mut Vec<StableEvidenceIssue>,
) {
    let retirement = &pack.legacy_retirement_evidence;
    if retirement.manifest_ref != "infra/release/legacy-retirement.json"
        || retirement.status != "passed"
        || retirement.new_legacy_run_admission
        || !retirement.durable_compatibility_reads_preserved
    {
        issue(issues, "core.stable.legacy_retirement_open", retirement.manifest_ref.clone());
    }
}

fn validate_alert_fixture(
    pack: &StableCapabilityEvidencePack,
    issues: &mut Vec<StableEvidenceIssue>,
) {
    let Ok(fixture) = serde_json::from_str::<AlertThresholdFixture>(ALERT_FIXTURE_JSON) else {
        issue(issues, "core.stable.alert_fixture_invalid", "parse_error".to_owned());
        return;
    };
    if fixture.schema_version != STABLE_EVIDENCE_SCHEMA_VERSION {
        issue(issues, "core.stable.alert_fixture_invalid", fixture.schema_version.to_string());
    }
    let mut tested = BTreeSet::new();
    for case in fixture.cases {
        let Some(definition) =
            pack.sli_definitions.iter().find(|sli| sli.metric_id == case.metric_id)
        else {
            issue(issues, "core.stable.alert_fixture_unknown_metric", case.metric_id);
            continue;
        };
        tested.insert(definition.metric_id.as_str());
        if classify_alert(definition, case.observed) != case.expected {
            issue(issues, "core.stable.alert_fixture_mismatch", definition.metric_id.clone());
        }
    }
    for required in REQUIRED_SLI_METRICS {
        if !tested.contains(required) {
            issue(issues, "core.stable.alert_fixture_missing_metric", required.to_owned());
        }
    }
}

fn validate_runbook_drill_fixture(
    pack: &StableCapabilityEvidencePack,
    issues: &mut Vec<StableEvidenceIssue>,
) {
    let Ok(fixture) = serde_json::from_str::<RunbookDrillFixture>(RUNBOOK_DRILL_FIXTURE_JSON)
    else {
        issue(issues, "core.stable.runbook_drill_invalid", "parse_error".to_owned());
        return;
    };
    if fixture.schema_version != STABLE_EVIDENCE_SCHEMA_VERSION {
        issue(issues, "core.stable.runbook_drill_invalid", fixture.schema_version.to_string());
    }
    let mut drilled = BTreeSet::new();
    for incident in fixture.incidents {
        drilled.insert(incident.runbook_id.clone());
        if incident.incident_id.trim().is_empty()
            || incident.evidence_fields.is_empty()
            || incident.contains_raw_secret
            || !incident.rollback_preserves_durable_data
            || incident.result != "passed"
        {
            issue(issues, "core.stable.runbook_drill_failed", incident.runbook_id);
        }
    }
    for runbook in &pack.runbooks {
        if !drilled.contains(runbook.runbook_id.as_str()) {
            issue(issues, "core.stable.runbook_drill_missing", runbook.runbook_id.clone());
        }
    }
    if !support_bundle_fixture_is_redacted(&fixture.support_bundle_sample) {
        issue(
            issues,
            "core.stable.support_bundle_redaction_failed",
            "synthetic_fixture".to_owned(),
        );
    }
}

const fn classify_alert(definition: &CoreRuntimeSliDefinition, observed: u64) -> AlertState {
    match definition.direction {
        SliDirection::AtLeast => {
            if observed >= definition.target {
                AlertState::Healthy
            } else if observed < definition.critical {
                AlertState::Critical
            } else {
                AlertState::Warning
            }
        }
        SliDirection::AtMost => {
            if observed <= definition.target {
                AlertState::Healthy
            } else if observed >= definition.critical {
                AlertState::Critical
            } else {
                AlertState::Warning
            }
        }
    }
}

fn support_bundle_fixture_is_redacted(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().all(|(key, child)| {
            let sensitive = matches!(
                key.as_str(),
                "credential"
                    | "prompt"
                    | "tool_payload"
                    | "workspace_path"
                    | "secret"
                    | "token"
                    | "password"
            );
            (!sensitive || child.as_str() == Some("<redacted>"))
                && support_bundle_fixture_is_redacted(child)
        }),
        Value::Array(values) => values.iter().all(support_bundle_fixture_is_redacted),
        Value::String(raw) => {
            !raw.contains("vault://")
                && !raw.contains("sk-")
                && !raw.contains("C:\\")
                && !raw.contains("/home/")
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => true,
    }
}

fn issue(issues: &mut Vec<StableEvidenceIssue>, code: &'static str, subject: String) {
    issues.push(StableEvidenceIssue { code, subject });
}

fn invalid_pack_snapshot() -> Value {
    serde_json::json!({
        "schema_version": STABLE_EVIDENCE_SCHEMA_VERSION,
        "contract_id": STABLE_EVIDENCE_CONTRACT_ID,
        "qualified": false,
        "reason_code": "core.stable.manifest_invalid",
        "issues": [{
            "code": "core.stable.manifest_invalid",
            "subject": "canonical_evidence_pack",
        }],
        "p0_blocker_count": 1,
        "redaction_level": "metadata_only",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_evidence_pack_qualifies() {
        let pack =
            builtin_stable_capability_evidence_pack().expect("stable evidence pack should parse");
        let qualification = evaluate_stable_capability_evidence(&pack);

        assert!(qualification.qualified, "{:?}", qualification.issues);
        assert_eq!(qualification.reason_code, "core.stable.release_qualified");
        assert!(pack.capabilities.iter().all(|capability| {
            capability.maturity.rank() >= capability.minimum_maturity.rank()
        }));
    }

    #[test]
    fn maturity_downgrade_blocks_release() {
        let mut pack =
            builtin_stable_capability_evidence_pack().expect("stable evidence pack should parse");
        let runtime = pack
            .capabilities
            .iter_mut()
            .find(|capability| capability.capability_id == "runtime_kernel_v2")
            .expect("runtime kernel evidence should exist");
        runtime.maturity = CoreCapabilityMaturity::GatedProduction;

        let qualification = evaluate_stable_capability_evidence(&pack);

        assert!(!qualification.qualified);
        assert!(qualification.issues.iter().any(|issue| {
            issue.code == "core.stable.maturity_downgrade" && issue.subject == "runtime_kernel_v2"
        }));
    }

    #[test]
    fn synthetic_runbook_and_alert_fixtures_are_enforced() {
        let pack =
            builtin_stable_capability_evidence_pack().expect("stable evidence pack should parse");
        let fixture: RunbookDrillFixture =
            serde_json::from_str(RUNBOOK_DRILL_FIXTURE_JSON).expect("runbook fixture should parse");
        let alerts: AlertThresholdFixture =
            serde_json::from_str(ALERT_FIXTURE_JSON).expect("alert fixture should parse");

        assert_eq!(fixture.incidents.len(), REQUIRED_RUNBOOKS.len());
        assert_eq!(alerts.cases.len(), REQUIRED_SLI_METRICS.len() * 2);
        assert!(support_bundle_fixture_is_redacted(&fixture.support_bundle_sample));
        assert!(evaluate_stable_capability_evidence(&pack).qualified);
    }

    #[test]
    fn stable_support_bundle_fixture_rejects_raw_sensitive_data() {
        let fixture: RunbookDrillFixture =
            serde_json::from_str(RUNBOOK_DRILL_FIXTURE_JSON).expect("runbook fixture should parse");
        assert!(support_bundle_fixture_is_redacted(&fixture.support_bundle_sample));

        let mut unsafe_sample = fixture.support_bundle_sample;
        unsafe_sample["credential"] = Value::String("sk-raw-secret".to_owned());

        assert!(!support_bundle_fixture_is_redacted(&unsafe_sample));
    }

    #[test]
    fn diagnostics_snapshot_is_bounded_and_release_qualified() {
        let snapshot = build_stable_core_evidence_snapshot();

        assert_eq!(snapshot["qualified"], true);
        assert_eq!(snapshot["reason_code"], "core.stable.release_qualified");
        assert_eq!(snapshot["p0_blocker_count"], 0);
        assert_eq!(snapshot["capability_count"], REQUIRED_CAPABILITIES.len());
        assert_eq!(snapshot["runbook_count"], REQUIRED_RUNBOOKS.len());
        assert_eq!(snapshot["redaction_level"], "metadata_only");
    }
}
