//! Pins consistency between the workflow regression manifest and the compat
//! release-readiness checklist, plus the wiring of each runtime acceptance contract
//! scenario to its CLI command.

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

const TASK01_HARNESS_SUBSYSTEMS: &[&str] = &[
    "harness_path_runtime",
    "harness_tool_output_streaming",
    "harness_resume_state",
    "harness_run_loop_recovery",
    "harness_background_handoff",
    "harness_runtime_risk_posture",
    "harness_workspace_recovery",
    "harness_redaction_image_observation",
];

const TASK01_HARNESS_SCENARIOS: &[(&str, &str)] = &[
    ("harness_unrestricted_path_runtime", "harness_path_runtime"),
    ("harness_tool_output_bounds", "harness_tool_output_streaming"),
    ("harness_process_progress_events", "harness_tool_output_streaming"),
    ("harness_resume_checkpoint_state", "harness_resume_state"),
    ("harness_run_loop_phase_timeout", "harness_run_loop_recovery"),
    ("harness_background_handoff", "harness_background_handoff"),
    ("harness_docker_risk_metadata", "harness_runtime_risk_posture"),
    ("harness_package_risk_metadata", "harness_runtime_risk_posture"),
    ("harness_workspace_root_stability", "harness_workspace_recovery"),
    ("harness_public_fixture_redaction", "harness_redaction_image_observation"),
    ("harness_image_observation", "harness_redaction_image_observation"),
    ("harness_image_observe_no_approval", "harness_redaction_image_observation"),
];

const AGENTIC_FAILURE_REPORT_FIELDS: &[&str] = &["subsystem", "event_ids", "health_findings"];

#[test]
fn workflow_regression_manifest_and_compat_checklist_remain_consistent() -> Result<()> {
    let (_, manifest, checklist) = load_regression_assets()?;
    assert_eq!(checklist.matrix_manifest, "infra/release/workflow-regression-matrix.json");
    assert!(manifest.profiles.contains_key("fast"));
    assert!(manifest.profiles.contains_key("full"));
    Ok(())
}

#[test]
fn task01_harness_regression_matrix_contract() -> Result<()> {
    let (_, manifest, checklist) = load_regression_assets()?;

    for profile_id in ["fast", "full"] {
        let profile = manifest
            .profiles
            .get(profile_id)
            .with_context(|| format!("workflow profile '{profile_id}' should exist"))?;
        for subsystem_id in TASK01_HARNESS_SUBSYSTEMS {
            assert!(
                profile.required_subsystems.iter().any(|entry| entry == subsystem_id),
                "profile '{profile_id}' should require Task 01 subsystem '{subsystem_id}'"
            );
        }
    }

    for (scenario_id, subsystem_id) in TASK01_HARNESS_SCENARIOS {
        let scenario =
            manifest.scenarios.iter().find(|entry| entry.id == *scenario_id).with_context(
                || format!("workflow regression manifest should contain '{scenario_id}'"),
            )?;
        assert_eq!(scenario.category, "harness");
        assert_eq!(scenario.profiles, vec!["fast".to_owned(), "full".to_owned()]);
        assert_eq!(scenario.subsystems, vec![(*subsystem_id).to_owned()]);
        assert!(!scenario.chaos);
        assert!(
            scenario.acceptance_scenarios.is_empty(),
            "Task 01 harness scenarios should not claim runtime-preview acceptance ids"
        );
        assert_exact_unit_test_command_is_fully_qualified(scenario);
    }

    for evidence_id in ["workflow_regression_fast", "workflow_regression_full"] {
        let evidence =
            checklist.evidence.iter().find(|entry| entry.id == evidence_id).with_context(|| {
                format!("readiness checklist should contain evidence '{evidence_id}'")
            })?;
        for subsystem_id in TASK01_HARNESS_SUBSYSTEMS {
            assert!(
                evidence.required_subsystems.iter().any(|entry| entry == subsystem_id),
                "evidence '{evidence_id}' should require Task 01 subsystem '{subsystem_id}'"
            );
        }
        for (scenario_id, _) in TASK01_HARNESS_SCENARIOS {
            assert!(
                evidence.must_pass_scenarios.iter().any(|entry| entry == scenario_id),
                "evidence '{evidence_id}' should require scenario '{scenario_id}'"
            );
        }
    }

    Ok(())
}

#[test]
fn agentic_long_session_compaction_matrix_contract() -> Result<()> {
    assert_agentic_matrix_contract(
        "agentic_long_session_compaction",
        AgenticScenarioExpectation { chaos: false, flaky: false, slow: true },
    )
}

#[test]
fn agentic_availability_tool_disappears_matrix_contract() -> Result<()> {
    assert_agentic_matrix_contract(
        "agentic_availability_tool_disappears",
        AgenticScenarioExpectation { chaos: true, flaky: false, slow: false },
    )
}

#[test]
fn agentic_mcp_schema_drift_matrix_contract() -> Result<()> {
    assert_agentic_matrix_contract(
        "agentic_mcp_schema_drift",
        AgenticScenarioExpectation { chaos: true, flaky: false, slow: false },
    )
}

#[test]
fn agentic_stuck_delegated_run_heartbeat_matrix_contract() -> Result<()> {
    assert_agentic_matrix_contract(
        "agentic_stuck_delegated_run_heartbeat",
        AgenticScenarioExpectation { chaos: true, flaky: false, slow: true },
    )
}

#[test]
fn agentic_sensitive_child_tool_approval_matrix_contract() -> Result<()> {
    assert_agentic_matrix_contract(
        "agentic_sensitive_child_tool_approval",
        AgenticScenarioExpectation { chaos: false, flaky: false, slow: false },
    )
}

#[test]
fn agentic_commitment_reminder_after_restart_matrix_contract() -> Result<()> {
    assert_agentic_matrix_contract(
        "agentic_commitment_reminder_after_restart",
        AgenticScenarioExpectation { chaos: false, flaky: false, slow: true },
    )
}

#[test]
fn agentic_plugin_quarantine_fallback_matrix_contract() -> Result<()> {
    assert_agentic_matrix_contract(
        "agentic_plugin_quarantine_fallback",
        AgenticScenarioExpectation { chaos: true, flaky: false, slow: false },
    )
}

#[test]
fn agentic_learning_rollback_matrix_contract() -> Result<()> {
    assert_agentic_matrix_contract(
        "agentic_learning_rollback",
        AgenticScenarioExpectation { chaos: true, flaky: true, slow: true },
    )
}

#[test]
fn exact_unit_test_commands_use_fully_qualified_names() -> Result<()> {
    let (_, manifest, _) = load_regression_assets()?;

    for scenario in &manifest.scenarios {
        if is_exact_unit_test_command(&scenario.command) {
            assert_exact_unit_test_command_is_fully_qualified(scenario);
        }
    }

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
fn routine_automation_smoke_contract() -> Result<()> {
    assert_runtime_acceptance_contract(
        "routine_automation_smoke_contract",
        RuntimeAcceptanceScenario::RoutineAutomationSmoke,
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

#[derive(Debug, Clone, Copy)]
struct AgenticScenarioExpectation {
    chaos: bool,
    flaky: bool,
    slow: bool,
}

fn assert_agentic_matrix_contract(
    scenario_id: &str,
    expected: AgenticScenarioExpectation,
) -> Result<()> {
    let (_, manifest, _) = load_regression_assets()?;
    let full_profile =
        manifest.profiles.get("full").context("workflow regression full profile should exist")?;
    assert!(
        full_profile.required_subsystems.iter().any(|entry| entry == "agentic_regression_matrix"),
        "full profile should require agentic_regression_matrix"
    );
    let scenario =
        manifest.scenarios.iter().find(|entry| entry.id == scenario_id).with_context(|| {
            format!("workflow regression manifest should contain '{scenario_id}'")
        })?;

    assert_eq!(scenario.category, "agentic");
    assert_eq!(scenario.profiles, vec!["full".to_owned()]);
    assert!(scenario.subsystems.iter().any(|entry| entry == "agentic_regression_matrix"));
    assert_eq!(scenario.chaos, expected.chaos);
    assert_eq!(scenario.flaky, expected.flaky);
    assert_eq!(scenario.slow, expected.slow);
    assert!(scenario.required);
    assert!(scenario.fake_provider);
    assert!(scenario.fake_external_services);
    assert_eq!(
        scenario.failure_report_fields,
        AGENTIC_FAILURE_REPORT_FIELDS.iter().copied().map(str::to_owned).collect::<Vec<_>>()
    );
    assert_eq!(scenario.command, contract_command(&format!("{scenario_id}_matrix_contract")));

    Ok(())
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

fn is_exact_unit_test_command(command: &[String]) -> bool {
    command.iter().any(|entry| entry == "--lib") && command.iter().any(|entry| entry == "--exact")
}

fn assert_exact_unit_test_command_is_fully_qualified(scenario: &WorkflowRegressionScenario) {
    let filter = exact_cargo_test_filter(scenario.command.as_slice())
        .unwrap_or_else(|| panic!("scenario '{}' should declare a cargo test filter", scenario.id));
    assert!(
        filter.starts_with("tests::") || filter.contains("::tests::"),
        "scenario '{}' exact unit-test filter must be fully qualified, got '{filter}'",
        scenario.id
    );
}

fn exact_cargo_test_filter(command: &[String]) -> Option<&str> {
    let test_separator = command.iter().position(|entry| entry == "--").unwrap_or(command.len());
    command
        .iter()
        .take(test_separator)
        .rev()
        .find(|entry| !entry.starts_with('-'))
        .map(String::as_str)
        .filter(|entry| *entry != "test" && *entry != "cargo")
}
