//! Contract tests for the release-eval gate against the golden inventory fixture:
//! required suites, regression detection, and path-safe replay bundle filenames.

use anyhow::{Context, Result};
use palyra_common::release_evals::{
    build_palyra_trajectory_export, build_regression_eval_pack_index,
    build_release_eval_maturity_scorecard, ensure_release_eval_report_passed,
    evaluate_release_eval_manifest, parse_release_eval_manifest,
    release_eval_replay_bundle_filename, required_release_eval_protocol_inventory,
    RegressionEvalPackReasonCode, ReleaseEvalMaturityDecision, ReleaseEvalMaturityReasonCode,
    ReleaseEvalMaturityScorecard, ReleaseEvalStatus, ReleaseFlakyMark, ReleaseGateVerdict,
    REQUIRED_RELEASE_SUITES,
};

const RELEASE_EVAL_FIXTURE: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures/golden/release_eval_inventory.json");

#[test]
fn release_eval_fixture_covers_all_required_suites_and_inventory() -> Result<()> {
    let manifest = load_manifest()?;
    let output = evaluate_release_eval_manifest(&manifest);

    ensure_release_eval_report_passed(&output.report)?;
    assert_eq!(output.report.gate_verdict, ReleaseGateVerdict::Pass);
    assert_eq!(output.report.summary.suites_total, REQUIRED_RELEASE_SUITES.len());
    assert_eq!(output.report.summary.generated_replay_bundles, output.report.summary.cases_total);
    assert_eq!(
        output.report.compatibility_matrix.entries.len(),
        output.report.protocol_inventory.protocols.len()
    );

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
    assert_eq!(output.report.gate_verdict, ReleaseGateVerdict::Fail);
    let error = ensure_release_eval_report_passed(&output.report)
        .expect_err("failed assertion must fail release gate");
    assert!(error.to_string().contains("release eval gate failed"), "unexpected error: {error:#}");
    Ok(())
}

#[test]
fn release_eval_marks_flaky_cases_for_manual_review_without_silent_pass() -> Result<()> {
    let mut manifest = load_manifest()?;
    let case = manifest
        .suites
        .first_mut()
        .and_then(|suite| suite.cases.first_mut())
        .context("fixture should include at least one case")?;
    case.deterministic = false;
    case.flaky = Some(ReleaseFlakyMark {
        reason: "provider latency trend is intentionally monitored".to_owned(),
        trend_metric: "release_eval.provider_latency_p95_ms".to_owned(),
    });

    let output = evaluate_release_eval_manifest(&manifest);

    ensure_release_eval_report_passed(&output.report)?;
    assert_eq!(output.report.status, ReleaseEvalStatus::Passed);
    assert_eq!(output.report.gate_verdict, ReleaseGateVerdict::ManualReview);
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

#[test]
fn release_eval_maturity_scorecard_promotes_golden_fixture() -> Result<()> {
    let manifest = load_manifest()?;
    let output = evaluate_release_eval_manifest(&manifest);
    let scorecard = build_release_eval_maturity_scorecard(&output.report, 0);

    assert_eq!(scorecard.decision, ReleaseEvalMaturityDecision::Promote);
    assert_eq!(scorecard.overall_score_bps, 10_000);
    assert_eq!(scorecard.reason_codes, vec![ReleaseEvalMaturityReasonCode::AllGatesPassed]);
    assert_eq!(scorecard.categories.len(), 6);

    let encoded = serde_json::to_vec(&scorecard)?;
    let decoded: ReleaseEvalMaturityScorecard = serde_json::from_slice(encoded.as_slice())?;
    assert_eq!(decoded, scorecard);
    Ok(())
}

#[test]
fn release_eval_maturity_scorecard_blocks_regressed_report() -> Result<()> {
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
    let scorecard = build_release_eval_maturity_scorecard(&output.report, 0);

    assert_eq!(scorecard.decision, ReleaseEvalMaturityDecision::Block);
    assert!(scorecard.reason_codes.contains(&ReleaseEvalMaturityReasonCode::GateFailed));
    assert!(scorecard.reason_codes.contains(&ReleaseEvalMaturityReasonCode::CoverageIncomplete));
    Ok(())
}

#[test]
fn release_eval_trajectory_export_is_digest_only_metadata() -> Result<()> {
    let manifest = load_manifest()?;
    let output = evaluate_release_eval_manifest(&manifest);
    let export = build_palyra_trajectory_export(&output, 0);

    assert_eq!(export.summary.runs_total, output.replay_bundles.len());
    assert_eq!(export.report_status, ReleaseEvalStatus::Passed);
    assert!(export.runs.iter().all(|run| run.bundle_sha256.is_some()));

    let encoded = serde_json::to_string(&export)?;
    assert!(!encoded.contains("normalized_user_input"));
    assert!(!encoded.contains("\"tape_events\":"));
    assert!(!encoded.contains("config_snapshot"));
    Ok(())
}

#[test]
fn release_eval_regression_pack_index_flags_missing_replay_bundles() -> Result<()> {
    let manifest = load_manifest()?;
    let mut output = evaluate_release_eval_manifest(&manifest);
    output.replay_bundles.clear();

    let index = build_regression_eval_pack_index(&output, 0);

    assert_eq!(index.summary.packs_ready, 0);
    assert_eq!(index.summary.packs_blocked, index.summary.packs_total);
    assert!(index.packs.iter().all(|pack| pack
        .reason_codes
        .contains(&RegressionEvalPackReasonCode::MissingReplayBundle)));
    Ok(())
}

fn load_manifest() -> Result<palyra_common::release_evals::ReleaseEvalManifest> {
    let bytes = std::fs::read(RELEASE_EVAL_FIXTURE)
        .with_context(|| format!("failed to read {}", RELEASE_EVAL_FIXTURE))?;
    parse_release_eval_manifest(bytes.as_slice())
}
