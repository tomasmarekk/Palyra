use std::collections::{BTreeMap, BTreeSet};

use palyra_common::qa_fault_injection::{
    ensure_continuity_campaign_passed, run_continuity_campaign, ContinuityCampaignStatus,
    CONTINUITY_CAMPAIGN_BOUNDARIES, CONTINUITY_CAMPAIGN_REPORT_SCHEMA_VERSION,
    CONTINUITY_CAMPAIGN_SCENARIOS,
};

#[test]
fn continuity_campaign_contract_covers_the_required_cartesian_matrix() {
    let report = run_continuity_campaign();
    ensure_continuity_campaign_passed(&report).expect("required continuity matrix should pass");

    assert_eq!(report.schema_version, CONTINUITY_CAMPAIGN_REPORT_SCHEMA_VERSION);
    assert_eq!(report.status, ContinuityCampaignStatus::Passed);
    assert!(report.redacted);
    assert_eq!(report.qualification_evidence_refs.len(), 8);
    assert!(report
        .qualification_evidence_refs
        .iter()
        .any(|evidence| evidence.evidence_ref == "gate:qa/suites/fault_smoke.yaml"));
    assert_eq!(
        report.cases.len(),
        CONTINUITY_CAMPAIGN_SCENARIOS.len() * CONTINUITY_CAMPAIGN_BOUNDARIES.len()
    );

    let expected_scenarios = CONTINUITY_CAMPAIGN_SCENARIOS.iter().copied().collect::<BTreeSet<_>>();
    let expected_boundaries =
        CONTINUITY_CAMPAIGN_BOUNDARIES.iter().copied().collect::<BTreeSet<_>>();
    let mut actual = BTreeMap::new();
    for case in &report.cases {
        actual.insert((case.scenario, case.boundary), case);
        assert!(case.injected_fault.injected);
        assert!(case.invariants.iter().all(|invariant| invariant.passed));
        assert_eq!(case.duplicate_side_effect_count, 0);
        assert_eq!(case.duplicate_confirmed_delivery_count, 0);
    }

    for scenario in expected_scenarios {
        for boundary in &expected_boundaries {
            assert!(
                actual.contains_key(&(scenario, *boundary)),
                "missing {scenario:?} at {boundary:?}"
            );
        }
    }
}
