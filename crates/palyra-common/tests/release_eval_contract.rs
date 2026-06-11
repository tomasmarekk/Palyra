//! Contract tests for the release-eval gate against the golden inventory fixture:
//! required suites, regression detection, and path-safe replay bundle filenames.

use anyhow::{Context, Result};
use palyra_common::release_evals::{
    ensure_release_eval_report_passed, evaluate_release_eval_manifest, parse_release_eval_manifest,
    release_eval_replay_bundle_filename, required_release_eval_protocol_inventory,
    ReleaseEvalStatus, REQUIRED_RELEASE_SUITES,
};

const RELEASE_EVAL_FIXTURE: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/golden/release_eval_inventory.json");

#[test]
fn release_eval_fixture_covers_all_required_suites_and_inventory() -> Result<()> {
    let manifest = load_manifest()?;
    let output = evaluate_release_eval_manifest(&manifest);

    ensure_release_eval_report_passed(&output.report)?;
    assert_eq!(output.report.summary.suites_total, REQUIRED_RELEASE_SUITES.len());
    assert_eq!(output.report.summary.generated_replay_bundles, output.report.summary.cases_total);

    let contracts = manifest
        .inventory
        .protocols
        .iter()
        .map(|entry| entry.contract.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for required in required_release_eval_protocol_inventory() {
        assert!(contracts.contains(required), "missing protocol inventory entry: {required}");
    }
    Ok(())
}

#[test]
fn release_eval_gate_fails_when_assertion_regresses() -> Result<()> {
    let mut manifest = load_manifest()?;
    let assertion = manifest
        .suites
        .first_mut()
        .and_then(|suite| suite.cases.first_mut())
        .and_then(|case| case.assertions.first_mut())
        .context("fixture should include at least one assertion")?;
    assertion.passed = false;
    assertion.actual = "regressed".to_owned();

    let output = evaluate_release_eval_manifest(&manifest);
    let error = ensure_release_eval_report_passed(&output.report)
        .expect_err("failed assertion must fail release gate");
    assert!(error.to_string().contains("release eval gate failed"), "unexpected error: {error:#}");
    Ok(())
}

#[test]
fn release_eval_rejects_path_like_case_ids_without_generating_bundle() -> Result<()> {
    let mut manifest = load_manifest()?;
    let case = manifest
        .suites
        .first_mut()
        .and_then(|suite| suite.cases.first_mut())
        .context("fixture should include at least one case")?;
    case.case_id = "../escaped".to_owned();

    let output = evaluate_release_eval_manifest(&manifest);
    let case_report = output
        .report
        .suites
        .first()
        .and_then(|suite| suite.cases.first())
        .context("fixture should emit a report for the first case")?;

    assert_eq!(case_report.status, ReleaseEvalStatus::Failed);
    assert!(
        case_report.issues.iter().any(|issue| issue.code == "case_id_path_segment_required"),
        "expected case_id path segment issue, got {case_report:#?}"
    );
    assert!(
        output.replay_bundles.iter().all(|bundle| bundle.case_id != "../escaped"),
        "invalid case_id must not produce a writable replay bundle"
    );
    Ok(())
}

#[test]
fn release_eval_replay_bundle_filename_rejects_path_segments() {
    assert_eq!(
        release_eval_replay_bundle_filename("provider_runtime_matrix").unwrap(),
        "provider_runtime_matrix.json"
    );

    for case_id in
        ["", " ../escaped", "../escaped", "nested/escaped", r"nested\escaped", "C:escaped"]
    {
        assert!(
            release_eval_replay_bundle_filename(case_id).is_err(),
            "case_id {case_id:?} should not be accepted as a replay bundle filename"
        );
    }
}

fn load_manifest() -> Result<palyra_common::release_evals::ReleaseEvalManifest> {
    let bytes = std::fs::read(RELEASE_EVAL_FIXTURE)
        .with_context(|| format!("failed to read {}", RELEASE_EVAL_FIXTURE))?;
    parse_release_eval_manifest(bytes.as_slice())
}
