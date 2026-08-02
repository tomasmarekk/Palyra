//! Versioned security conformance qualification for the core runtime.
//!
//! The evaluator binds the shared adversarial corpus, bounded fuzz surfaces,
//! and negative authorization tests into one fail-closed release decision.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

const SECURITY_CONFORMANCE_SCHEMA_VERSION: u32 = 1;
const CORE_RUNTIME_CONTRACT_VERSION: &str = "runtime-contracts.v16";
const MIN_CRITICAL_ATTACK_SCENARIOS: u32 = 15;

const BUILTIN_REPORT_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../infra/release/core-security-conformance.json"
));

/// Evidence for one executable adversarial suite.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecuritySuiteEvidence {
    pub(crate) suite_id: String,
    pub(crate) scenario_count: u32,
    pub(crate) runner: String,
    pub(crate) evidence_ref: String,
    pub(crate) passed: bool,
}

/// One bounded parser or lifecycle surface covered by fuzzing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecurityFuzzTargetEvidence {
    pub(crate) target_id: String,
    pub(crate) input_cap_bytes: u64,
    pub(crate) evidence_ref: String,
    pub(crate) bounded: bool,
}

/// One fail-closed authorization or isolation boundary regression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NegativeBoundaryEvidence {
    pub(crate) boundary_id: String,
    pub(crate) test_ref: String,
    pub(crate) expected_reason_code: String,
    pub(crate) passed: bool,
}

/// Canonical core security evidence consumed by the stable release gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SecurityConformanceReport {
    pub(crate) schema_version: u32,
    pub(crate) runtime_contract_version: String,
    pub(crate) as_of: String,
    pub(crate) attack_corpus_ref: String,
    pub(crate) suites: Vec<SecuritySuiteEvidence>,
    pub(crate) fuzz_targets: Vec<SecurityFuzzTargetEvidence>,
    pub(crate) negative_boundaries: Vec<NegativeBoundaryEvidence>,
    pub(crate) unresolved_blockers: Vec<String>,
    pub(crate) uses_production_secrets: bool,
}

/// Stable failure raised while parsing the repository-owned report.
#[derive(Debug, thiserror::Error)]
pub(crate) enum SecurityConformanceEvidenceError {
    /// The embedded JSON does not match the closed Rust contract.
    #[error("core security conformance report is invalid")]
    InvalidReport(#[source] serde_json::Error),
}

/// One redacted reason why security qualification did not pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SecurityQualificationIssue {
    pub(crate) code: &'static str,
    pub(crate) subject: String,
}

/// Release decision derived from the complete security report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct SecurityConformanceQualification {
    pub(crate) schema_version: u32,
    pub(crate) qualified: bool,
    pub(crate) reason_code: &'static str,
    pub(crate) issues: Vec<SecurityQualificationIssue>,
}

/// Parses the repository-owned security conformance report.
///
/// # Errors
/// Returns [`SecurityConformanceEvidenceError`] when the embedded evidence
/// does not match the closed schema.
pub(crate) fn builtin_security_conformance_report(
) -> Result<SecurityConformanceReport, SecurityConformanceEvidenceError> {
    serde_json::from_str(BUILTIN_REPORT_JSON)
        .map_err(SecurityConformanceEvidenceError::InvalidReport)
}

/// Evaluates adversarial, fuzz, and negative-boundary release invariants.
#[must_use]
pub(crate) fn evaluate_security_conformance(
    report: &SecurityConformanceReport,
) -> SecurityConformanceQualification {
    let mut issues = Vec::new();

    if report.schema_version != SECURITY_CONFORMANCE_SCHEMA_VERSION {
        blocker(
            &mut issues,
            "core_security.schema_version_unsupported",
            report.schema_version.to_string(),
        );
    }
    if report.runtime_contract_version != CORE_RUNTIME_CONTRACT_VERSION {
        blocker(
            &mut issues,
            "core_security.runtime_contract_mismatch",
            report.runtime_contract_version.clone(),
        );
    }
    if report.attack_corpus_ref != "fixtures/security/critical_attack_scenarios.json" {
        blocker(
            &mut issues,
            "core_security.attack_corpus_unpinned",
            "critical_attack_scenarios".to_owned(),
        );
    }
    if report.uses_production_secrets {
        blocker(
            &mut issues,
            "core_security.production_secret_fixture",
            "security_conformance".to_owned(),
        );
    }

    validate_suites(report, &mut issues);
    validate_fuzz_targets(report, &mut issues);
    validate_negative_boundaries(report, &mut issues);

    for blocker_id in &report.unresolved_blockers {
        blocker(&mut issues, "core_security.unresolved_blocker", blocker_id.clone());
    }

    let qualified = issues.is_empty();
    SecurityConformanceQualification {
        schema_version: SECURITY_CONFORMANCE_SCHEMA_VERSION,
        qualified,
        reason_code: if qualified {
            "core_security.qualified"
        } else {
            "core_security.release_blocked"
        },
        issues,
    }
}

fn validate_suites(
    report: &SecurityConformanceReport,
    issues: &mut Vec<SecurityQualificationIssue>,
) {
    let mut observed = BTreeSet::new();
    let mut scenario_count = 0_u32;
    for suite in &report.suites {
        if !observed.insert(suite.suite_id.as_str()) {
            blocker(issues, "core_security.suite_duplicate", suite.suite_id.clone());
        }
        if suite.scenario_count == 0
            || suite.runner.trim().is_empty()
            || suite.evidence_ref.trim().is_empty()
            || !suite.passed
        {
            blocker(issues, "core_security.suite_incomplete", suite.suite_id.clone());
        }
        scenario_count = scenario_count.saturating_add(suite.scenario_count);
    }
    for required in ["safety", "egress", "worker_attestation"] {
        if !observed.contains(required) {
            blocker(issues, "core_security.suite_missing", required.to_owned());
        }
    }
    if scenario_count < MIN_CRITICAL_ATTACK_SCENARIOS {
        blocker(issues, "core_security.attack_corpus_too_small", scenario_count.to_string());
    }
}

fn validate_fuzz_targets(
    report: &SecurityConformanceReport,
    issues: &mut Vec<SecurityQualificationIssue>,
) {
    let mut observed = BTreeSet::new();
    for target in &report.fuzz_targets {
        if !observed.insert(target.target_id.as_str()) {
            blocker(issues, "core_security.fuzz_target_duplicate", target.target_id.clone());
        }
        if target.input_cap_bytes == 0 || target.evidence_ref.trim().is_empty() || !target.bounded {
            blocker(issues, "core_security.fuzz_target_unbounded", target.target_id.clone());
        }
    }
    for required in [
        "plugin_contract_parser",
        "provider_transcript_parser",
        "transcript_projection",
        "pty_process_input_parser",
    ] {
        if !observed.contains(required) {
            blocker(issues, "core_security.fuzz_target_missing", required.to_owned());
        }
    }
}

fn validate_negative_boundaries(
    report: &SecurityConformanceReport,
    issues: &mut Vec<SecurityQualificationIssue>,
) {
    let mut observed = BTreeSet::new();
    for boundary in &report.negative_boundaries {
        if !observed.insert(boundary.boundary_id.as_str()) {
            blocker(
                issues,
                "core_security.negative_boundary_duplicate",
                boundary.boundary_id.clone(),
            );
        }
        if boundary.test_ref.trim().is_empty()
            || boundary.expected_reason_code.trim().is_empty()
            || !boundary.passed
        {
            blocker(
                issues,
                "core_security.negative_boundary_incomplete",
                boundary.boundary_id.clone(),
            );
        }
    }
    for required in [
        "sandbox_escape",
        "approval_bypass",
        "cross_session_access",
        "stale_generation",
        "vault_reference",
        "egress_proxy",
    ] {
        if !observed.contains(required) {
            blocker(issues, "core_security.negative_boundary_missing", required.to_owned());
        }
    }
}

fn blocker(issues: &mut Vec<SecurityQualificationIssue>, code: &'static str, subject: String) {
    issues.push(SecurityQualificationIssue { code, subject });
}

/// Builds the redacted security decision exposed by diagnostics.
#[must_use]
pub(crate) fn build_security_conformance_snapshot() -> serde_json::Value {
    let Ok(report) = builtin_security_conformance_report() else {
        return serde_json::json!({
            "schema_version": SECURITY_CONFORMANCE_SCHEMA_VERSION,
            "qualified": false,
            "reason_code": "core_security.evidence_invalid",
            "issues": [{
                "code": "core_security.evidence_invalid",
                "subject": "canonical_report",
            }],
        });
    };
    let qualification = evaluate_security_conformance(&report);
    let scenario_count =
        report.suites.iter().fold(0_u32, |total, suite| total.saturating_add(suite.scenario_count));

    serde_json::json!({
        "schema_version": qualification.schema_version,
        "qualified": qualification.qualified,
        "reason_code": qualification.reason_code,
        "issues": qualification.issues,
        "runtime_contract_version": report.runtime_contract_version,
        "as_of": report.as_of,
        "attack_scenario_count": scenario_count,
        "fuzz_target_count": report.fuzz_targets.len(),
        "negative_boundary_count": report.negative_boundaries.len(),
        "unresolved_blocker_count": report.unresolved_blockers.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_security_conformance_is_release_qualified() {
        let report =
            builtin_security_conformance_report().expect("built-in security report must parse");
        let qualification = evaluate_security_conformance(&report);

        assert!(qualification.qualified, "unexpected issues: {:?}", qualification.issues);
        assert_eq!(qualification.reason_code, "core_security.qualified");
    }

    #[test]
    fn missing_fuzz_surface_and_open_blocker_fail_closed() {
        let mut report =
            builtin_security_conformance_report().expect("built-in security report must parse");
        report.fuzz_targets.retain(|target| target.target_id != "plugin_contract_parser");
        report.unresolved_blockers.push("SEC-001".to_owned());

        let qualification = evaluate_security_conformance(&report);

        assert!(!qualification.qualified);
        assert!(qualification
            .issues
            .iter()
            .any(|issue| issue.code == "core_security.fuzz_target_missing"));
        assert!(qualification
            .issues
            .iter()
            .any(|issue| issue.code == "core_security.unresolved_blocker"));
    }

    #[test]
    fn diagnostics_snapshot_is_bounded_and_qualified() {
        let snapshot = build_security_conformance_snapshot();

        assert_eq!(snapshot["qualified"], true);
        assert_eq!(snapshot["reason_code"], "core_security.qualified");
        assert_eq!(snapshot["attack_scenario_count"], 15);
        assert_eq!(snapshot["fuzz_target_count"], 4);
        assert_eq!(snapshot["negative_boundary_count"], 6);
    }
}
