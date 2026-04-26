use anyhow::{Context, Result};
use palyra_common::release_evals::{
    ensure_release_eval_report_passed, evaluate_release_eval_manifest, parse_release_eval_manifest,
    required_release_eval_protocol_inventory, REQUIRED_RELEASE_SUITES,
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

fn load_manifest() -> Result<palyra_common::release_evals::ReleaseEvalManifest> {
    let bytes = std::fs::read(RELEASE_EVAL_FIXTURE)
        .with_context(|| format!("failed to read {}", RELEASE_EVAL_FIXTURE))?;
    parse_release_eval_manifest(bytes.as_slice())
}
