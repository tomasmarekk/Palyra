use std::path::PathBuf;

use anyhow::{Context, Result};
use palyra_cli::workflow_regression::{
    covered_acceptance_scenarios_for_profile, load_compat_release_readiness,
    load_workflow_regression_manifest, repo_root_from_manifest_dir,
    required_acceptance_scenarios_for_profile, validate_compat_release_readiness,
    validate_workflow_regression_manifest, CompatReleaseReadinessChecklist,
    WorkflowRegressionManifest, WorkflowRegressionScenario,
};
use palyra_common::runtime_preview::RuntimeAcceptanceScenario;

#[test]
fn workflow_regression_manifest_and_compat_checklist_remain_consistent() -> Result<()> {
    let (_, manifest, checklist) = load_regression_assets()?;
    assert_eq!(checklist.matrix_manifest, "infra/release/workflow-regression-matrix.json");
    assert!(manifest.profiles.contains_key("fast"));
    assert!(manifest.profiles.contains_key("full"));
    Ok(())
}

#[test]
fn queued_input_lifecycle_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "queued_input_lifecycle_contract",
        RuntimeAcceptanceScenario::QueuedInputLifecycle,
    )
}

#[test]
fn pruning_decision_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "pruning_decision_contract",
        RuntimeAcceptanceScenario::PruningDecision,
    )
}

#[test]
fn dual_path_retrieval_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "dual_path_retrieval_contract",
        RuntimeAcceptanceScenario::DualPathRetrieval,
    )
}

#[test]
fn preflight_checkpoint_pair_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "preflight_checkpoint_pair_contract",
        RuntimeAcceptanceScenario::PreflightCheckpointPair,
    )
}

#[test]
fn child_progress_merge_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "child_progress_merge_contract",
        RuntimeAcceptanceScenario::ChildProgressMerge,
    )
}

#[test]
fn flow_transitions_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "flow_transitions_contract",
        RuntimeAcceptanceScenario::FlowTransitions,
    )
}

#[test]
fn delivery_arbitration_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "delivery_arbitration_contract",
        RuntimeAcceptanceScenario::DeliveryArbitration,
    )
}

#[test]
fn networked_worker_preview_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "networked_worker_preview_contract",
        RuntimeAcceptanceScenario::NetworkedWorkerPreview,
    )
}

fn load_regression_assets(
) -> Result<(PathBuf, WorkflowRegressionManifest, CompatReleaseReadinessChecklist)> {
    let repo_root = repo_root_from_manifest_dir()?;
    let manifest_path =
        repo_root.join("infra").join("release").join("workflow-regression-matrix.json");
    let checklist_path =
        repo_root.join("infra").join("release").join("compat-hardening-readiness.json");

    let manifest = load_workflow_regression_manifest(manifest_path.as_path())?;
    validate_workflow_regression_manifest(&manifest)?;

    let checklist = load_compat_release_readiness(checklist_path.as_path())?;
    validate_compat_release_readiness(&checklist, &manifest, repo_root.as_path())?;

    Ok((repo_root, manifest, checklist))
}

fn assert_runtime_acceptance_contract(
    scenario_id: &str,
    acceptance: RuntimeAcceptanceScenario,
) -> Result<()> {
    let (_, manifest, checklist) = load_regression_assets()?;

    let manifest_acceptance = manifest
        .runtime_acceptance_scenarios
        .iter()
        .find(|entry| entry.id == acceptance.as_str())
        .with_context(|| {
            format!(
                "runtime acceptance catalog should contain canonical scenario '{}'",
                acceptance.as_str()
            )
        })?;
    assert_eq!(manifest_acceptance.label, acceptance.label());
    assert_eq!(manifest_acceptance.summary, acceptance.summary());
    assert_eq!(manifest_acceptance.capability, acceptance.capability().as_str());
    assert_eq!(manifest_acceptance.required_profiles, vec!["fast".to_owned(), "full".to_owned()]);
    assert_eq!(
        manifest_acceptance.fixture_keys,
        acceptance.required_fixture_keys().iter().copied().map(str::to_owned).collect::<Vec<_>>()
    );

    let scenario =
        manifest.scenarios.iter().find(|entry| entry.id == scenario_id).with_context(|| {
            format!("workflow regression manifest should contain '{scenario_id}'")
        })?;
    assert_contract_scenario_wiring(scenario, scenario_id, acceptance);

    let fast_required = required_acceptance_scenarios_for_profile(&manifest, "fast");
    let fast_covered = covered_acceptance_scenarios_for_profile(&manifest, "fast");
    let full_required = required_acceptance_scenarios_for_profile(&manifest, "full");
    let full_covered = covered_acceptance_scenarios_for_profile(&manifest, "full");
    assert!(fast_required.contains(acceptance.as_str()));
    assert!(fast_covered.contains(acceptance.as_str()));
    assert!(full_required.contains(acceptance.as_str()));
    assert!(full_covered.contains(acceptance.as_str()));

    for evidence_id in ["workflow_regression_fast", "workflow_regression_full"] {
        let evidence =
            checklist.evidence.iter().find(|entry| entry.id == evidence_id).with_context(|| {
                format!("readiness checklist should contain evidence '{evidence_id}'")
            })?;
        assert!(
            evidence.must_pass_scenarios.iter().any(|entry| entry == scenario_id),
            "evidence '{evidence_id}' should require scenario '{scenario_id}'"
        );
    }

    Ok(())
}

fn assert_contract_scenario_wiring(
    scenario: &WorkflowRegressionScenario,
    scenario_id: &str,
    acceptance: RuntimeAcceptanceScenario,
) {
    assert_eq!(scenario.category, "contract");
    assert_eq!(scenario.profiles, vec!["fast".to_owned(), "full".to_owned()]);
    assert_eq!(scenario.subsystems, vec![acceptance.capability().as_str().to_owned()]);
    assert!(!scenario.chaos);
    assert_eq!(scenario.acceptance_scenarios, vec![acceptance.as_str().to_owned()]);
    assert_eq!(scenario.command, contract_command(scenario_id));
}

fn contract_command(test_name: &str) -> Vec<String> {
    [
        "cargo",
        "test",
        "-p",
        "palyra-cli",
        "--test",
        "workflow_regression_contract",
        "--locked",
        test_name,
        "--",
        "--exact",
        "--test-threads=1",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}
