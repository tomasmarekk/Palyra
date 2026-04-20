use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use palyra_common::runtime_preview::{
    runtime_acceptance_fixture_catalog, ALL_RUNTIME_ACCEPTANCE_SCENARIOS,
};
use serde::{Deserialize, Serialize};

pub const WORKFLOW_REGRESSION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRegressionManifest {
    pub schema_version: u32,
    pub release_scope: String,
    pub updated_at: String,
    pub profiles: BTreeMap<String, WorkflowRegressionProfile>,
    pub required_subsystems: Vec<WorkflowRegressionSubsystem>,
    pub runtime_acceptance_fixtures: Vec<WorkflowRegressionFixture>,
    pub runtime_acceptance_scenarios: Vec<WorkflowRegressionAcceptanceScenario>,
    pub setup_steps: Vec<WorkflowRegressionSetupStep>,
    pub scenarios: Vec<WorkflowRegressionScenario>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRegressionProfile {
    pub description: String,
    pub required_subsystems: Vec<String>,
    pub minimum_chaos_scenarios: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRegressionSubsystem {
    pub id: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRegressionSetupStep {
    pub id: String,
    pub label: String,
    pub profiles: Vec<String>,
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRegressionFixture {
    pub id: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRegressionAcceptanceScenario {
    pub id: String,
    pub label: String,
    pub summary: String,
    pub capability: String,
    pub required_profiles: Vec<String>,
    pub fixture_keys: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRegressionScenario {
    pub id: String,
    pub label: String,
    pub category: String,
    pub profiles: Vec<String>,
    pub subsystems: Vec<String>,
    pub chaos: bool,
    #[serde(default)]
    pub acceptance_scenarios: Vec<String>,
    pub command: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatReleaseReadinessChecklist {
    pub schema_version: u32,
    pub release_scope: String,
    pub updated_at: String,
    pub matrix_manifest: String,
    pub rollout_controls: Vec<CompatRolloutControl>,
    pub migration_tracks: Vec<CompatMigrationTrack>,
    pub known_limitations: Vec<CompatKnownLimitation>,
    pub evidence: Vec<CompatEvidenceRequirement>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatRolloutControl {
    pub id: String,
    pub kind: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatMigrationTrack {
    pub id: String,
    pub surface: String,
    pub apply: String,
    pub rollback: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatKnownLimitation {
    pub id: String,
    pub state: String,
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatEvidenceRequirement {
    pub id: String,
    pub kind: CompatEvidenceKind,
    pub summary: String,
    pub validation_mode: CompatValidationMode,
    pub profile: Option<String>,
    pub path: Option<String>,
    #[serde(default)]
    pub must_contain: Vec<String>,
    #[serde(default)]
    pub required_subsystems: Vec<String>,
    #[serde(default)]
    pub must_pass_scenarios: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompatEvidenceKind {
    WorkflowReport,
    SourceContract,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompatValidationMode {
    Runtime,
    Structural,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowRegressionRunReport {
    pub schema_version: u32,
    pub release_scope: String,
    pub manifest_path: String,
    pub checklist_path: String,
    pub profile: String,
    pub started_at_unix_ms: u64,
    pub completed_at_unix_ms: u64,
    pub summary: WorkflowRegressionRunSummary,
    pub coverage: WorkflowRegressionCoverageSummary,
    pub setup_steps: Vec<WorkflowRegressionExecutionRecord>,
    pub scenarios: Vec<WorkflowRegressionExecutionRecord>,
    pub release_checklist: CompatChecklistStatusReport,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowRegressionRunSummary {
    pub setup_total: usize,
    pub setup_failed: usize,
    pub scenario_total: usize,
    pub scenario_passed: usize,
    pub scenario_failed: usize,
    pub scenario_skipped: usize,
    pub chaos_total: usize,
    pub chaos_failed: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowRegressionCoverageSummary {
    pub required_subsystems: Vec<String>,
    pub covered_subsystems: Vec<String>,
    pub missing_subsystems: Vec<String>,
    pub required_acceptance_scenarios: Vec<String>,
    pub covered_acceptance_scenarios: Vec<String>,
    pub missing_acceptance_scenarios: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WorkflowRegressionExecutionRecord {
    pub id: String,
    pub label: String,
    pub category: String,
    pub subsystems: Vec<String>,
    pub chaos: bool,
    pub command: Vec<String>,
    pub status: WorkflowRegressionExecutionStatus,
    pub duration_ms: u64,
    pub log_path: String,
    pub exit_code: Option<i32>,
    pub output_excerpt: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowRegressionExecutionStatus {
    Passed,
    Failed,
    Skipped,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompatChecklistStatusReport {
    pub state: CompatChecklistState,
    pub validated: usize,
    pub pending: usize,
    pub failed: usize,
    pub evidence: Vec<CompatEvidenceStatus>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompatChecklistState {
    Ready,
    ProfileValidatedPendingExternalEvidence,
    NeedsAttention,
}

#[derive(Debug, Clone, Serialize)]
pub struct CompatEvidenceStatus {
    pub id: String,
    pub kind: CompatEvidenceKind,
    pub status: CompatEvidenceStatusKind,
    pub details: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CompatEvidenceStatusKind {
    RuntimeValidated,
    DeclaredContract,
    PendingOtherProfile,
    Failed,
    InvalidReference,
}

pub fn load_workflow_regression_manifest(path: &Path) -> Result<WorkflowRegressionManifest> {
    let manifest_text = fs::read_to_string(path).with_context(|| {
        format!("failed to read workflow regression manifest {}", path.display())
    })?;
    serde_json::from_str(manifest_text.as_str())
        .with_context(|| format!("failed to parse workflow regression manifest {}", path.display()))
}

pub fn load_compat_release_readiness(path: &Path) -> Result<CompatReleaseReadinessChecklist> {
    let checklist_text = fs::read_to_string(path)
        .with_context(|| format!("failed to read compat release checklist {}", path.display()))?;
    serde_json::from_str(checklist_text.as_str())
        .with_context(|| format!("failed to parse compat release checklist {}", path.display()))
}

pub fn validate_workflow_regression_manifest(manifest: &WorkflowRegressionManifest) -> Result<()> {
    ensure_nonempty_value(
        manifest.release_scope.as_str(),
        "workflow regression release scope must not be empty",
    )?;
    ensure_nonempty_value(
        manifest.updated_at.as_str(),
        "workflow regression updated_at must not be empty",
    )?;
    ensure_schema_version(manifest.schema_version, "workflow regression manifest schema_version")?;
    if manifest.profiles.is_empty() {
        anyhow::bail!("workflow regression manifest must define at least one profile");
    }
    if manifest.required_subsystems.is_empty() {
        anyhow::bail!("workflow regression manifest must define at least one required subsystem");
    }
    if manifest.runtime_acceptance_fixtures.is_empty() {
        anyhow::bail!(
            "workflow regression manifest must define shared runtime acceptance fixtures"
        );
    }
    if manifest.runtime_acceptance_scenarios.is_empty() {
        anyhow::bail!(
            "workflow regression manifest must define runtime acceptance minimum scenarios"
        );
    }
    if manifest.scenarios.is_empty() {
        anyhow::bail!("workflow regression manifest must define at least one scenario");
    }

    let profile_ids = manifest.profiles.keys().cloned().collect::<BTreeSet<_>>();
    let subsystem_ids = validate_subsystem_catalog(manifest.required_subsystems.as_slice())?;
    let fixture_catalog_ids = runtime_acceptance_fixture_catalog_ids()?;
    let manifest_fixture_ids = validate_runtime_acceptance_fixtures(
        manifest.runtime_acceptance_fixtures.as_slice(),
        &fixture_catalog_ids,
    )?;
    let runtime_acceptance_ids = validate_runtime_acceptance_catalog(
        manifest.runtime_acceptance_scenarios.as_slice(),
        &profile_ids,
        &manifest_fixture_ids,
    )?;
    validate_setup_steps(manifest.setup_steps.as_slice(), &profile_ids)?;
    validate_scenarios(
        manifest.scenarios.as_slice(),
        &profile_ids,
        &subsystem_ids,
        &runtime_acceptance_ids,
    )?;

    for (profile_id, profile) in &manifest.profiles {
        ensure_nonempty_value(
            profile.description.as_str(),
            format!("workflow regression profile '{profile_id}' description must not be empty"),
        )?;
        if profile.required_subsystems.is_empty() {
            anyhow::bail!(
                "workflow regression profile '{profile_id}' must require at least one subsystem"
            );
        }

        let covered_subsystems = covered_subsystems_for_profile(manifest, profile_id.as_str());
        for subsystem_id in &profile.required_subsystems {
            if !subsystem_ids.contains(subsystem_id.as_str()) {
                anyhow::bail!(
                    "workflow regression profile '{profile_id}' references unknown subsystem '{subsystem_id}'"
                );
            }
            if !covered_subsystems.contains(subsystem_id) {
                anyhow::bail!(
                    "workflow regression profile '{profile_id}' is missing scenario coverage for subsystem '{subsystem_id}'"
                );
            }
        }
        let covered_acceptance_scenarios =
            covered_acceptance_scenarios_for_profile(manifest, profile_id.as_str());
        for acceptance in manifest
            .runtime_acceptance_scenarios
            .iter()
            .filter(|entry| entry.required_profiles.iter().any(|value| value == profile_id))
        {
            if !covered_acceptance_scenarios.contains(acceptance.id.as_str()) {
                anyhow::bail!(
                    "workflow regression profile '{profile_id}' is missing acceptance coverage for scenario '{}'",
                    acceptance.id
                );
            }
        }

        let chaos_count = manifest
            .scenarios
            .iter()
            .filter(|scenario| {
                scenario.chaos && scenario.profiles.iter().any(|value| value == profile_id)
            })
            .count();
        if chaos_count < profile.minimum_chaos_scenarios {
            anyhow::bail!(
                "workflow regression profile '{profile_id}' requires at least {} chaos scenarios, found {}",
                profile.minimum_chaos_scenarios,
                chaos_count
            );
        }
    }

    Ok(())
}

pub fn validate_compat_release_readiness(
    checklist: &CompatReleaseReadinessChecklist,
    manifest: &WorkflowRegressionManifest,
    repo_root: &Path,
) -> Result<()> {
    ensure_nonempty_value(
        checklist.release_scope.as_str(),
        "compat release checklist release_scope must not be empty",
    )?;
    ensure_nonempty_value(
        checklist.updated_at.as_str(),
        "compat release checklist updated_at must not be empty",
    )?;
    ensure_schema_version(checklist.schema_version, "compat release checklist schema_version")?;
    if checklist.release_scope != manifest.release_scope {
        anyhow::bail!(
            "compat release checklist scope '{}' does not match workflow regression scope '{}'",
            checklist.release_scope,
            manifest.release_scope
        );
    }
    if checklist.rollout_controls.is_empty() {
        anyhow::bail!("compat release checklist must define rollout controls");
    }
    if checklist.migration_tracks.is_empty() {
        anyhow::bail!("compat release checklist must define migration tracks");
    }
    if checklist.known_limitations.is_empty() {
        anyhow::bail!("compat release checklist must define known limitations");
    }
    if checklist.evidence.is_empty() {
        anyhow::bail!("compat release checklist must define evidence requirements");
    }

    let manifest_path = resolve_repo_relative_path(repo_root, checklist.matrix_manifest.as_str());
    if !manifest_path.is_file() {
        anyhow::bail!(
            "compat release checklist references missing workflow regression manifest {}",
            manifest_path.display()
        );
    }

    let subsystem_ids =
        manifest.required_subsystems.iter().map(|entry| entry.id.as_str()).collect::<BTreeSet<_>>();
    let scenario_map = manifest
        .scenarios
        .iter()
        .map(|scenario| (scenario.id.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();

    ensure_unique_ids(
        checklist.rollout_controls.iter().map(|entry| entry.id.as_str()),
        "compat rollout control ids",
    )?;
    for control in &checklist.rollout_controls {
        ensure_nonempty_value(
            control.kind.as_str(),
            format!("rollout control '{}' kind must not be empty", control.id),
        )?;
        ensure_nonempty_value(
            control.summary.as_str(),
            format!("rollout control '{}' summary must not be empty", control.id),
        )?;
    }

    ensure_unique_ids(
        checklist.migration_tracks.iter().map(|entry| entry.id.as_str()),
        "compat migration track ids",
    )?;
    for track in &checklist.migration_tracks {
        ensure_nonempty_value(
            track.surface.as_str(),
            format!("migration track '{}' surface must not be empty", track.id),
        )?;
        ensure_nonempty_value(
            track.apply.as_str(),
            format!("migration track '{}' apply instructions must not be empty", track.id),
        )?;
        ensure_nonempty_value(
            track.rollback.as_str(),
            format!("migration track '{}' rollback instructions must not be empty", track.id),
        )?;
    }

    ensure_unique_ids(
        checklist.known_limitations.iter().map(|entry| entry.id.as_str()),
        "compat known limitation ids",
    )?;
    for limitation in &checklist.known_limitations {
        ensure_nonempty_value(
            limitation.state.as_str(),
            format!("known limitation '{}' state must not be empty", limitation.id),
        )?;
        ensure_nonempty_value(
            limitation.summary.as_str(),
            format!("known limitation '{}' summary must not be empty", limitation.id),
        )?;
    }

    ensure_unique_ids(
        checklist.evidence.iter().map(|entry| entry.id.as_str()),
        "compat checklist evidence ids",
    )?;
    for evidence in &checklist.evidence {
        ensure_nonempty_value(
            evidence.summary.as_str(),
            format!("compat checklist evidence '{}' summary must not be empty", evidence.id),
        )?;
        if let Some(profile) = evidence.profile.as_deref() {
            if !manifest.profiles.contains_key(profile) {
                anyhow::bail!(
                    "compat checklist evidence '{}' references unknown workflow profile '{}'",
                    evidence.id,
                    profile
                );
            }
        }
        for subsystem_id in &evidence.required_subsystems {
            if !subsystem_ids.contains(subsystem_id.as_str()) {
                anyhow::bail!(
                    "compat checklist evidence '{}' references unknown subsystem '{}'",
                    evidence.id,
                    subsystem_id
                );
            }
        }
        for scenario_id in &evidence.must_pass_scenarios {
            let Some(scenario) = scenario_map.get(scenario_id.as_str()) else {
                anyhow::bail!(
                    "compat checklist evidence '{}' references unknown scenario '{}'",
                    evidence.id,
                    scenario_id
                );
            };
            if let Some(profile) = evidence.profile.as_deref() {
                if !scenario.profiles.iter().any(|entry| entry == profile) {
                    anyhow::bail!(
                        "compat checklist evidence '{}' requires scenario '{}' for profile '{}', but the scenario does not run in that profile",
                        evidence.id,
                        scenario_id,
                        profile
                    );
                }
            }
        }
        if matches!(evidence.kind, CompatEvidenceKind::SourceContract) {
            let path = evidence.path.as_deref().ok_or_else(|| {
                anyhow::anyhow!(
                    "compat checklist evidence '{}' uses source_contract validation but does not declare a path",
                    evidence.id
                )
            })?;
            let resolved_path = resolve_repo_relative_path(repo_root, path);
            validate_source_contract(
                evidence.id.as_str(),
                resolved_path.as_path(),
                evidence.must_contain.as_slice(),
            )?;
        }
    }

    Ok(())
}

pub fn build_compat_checklist_status(
    checklist: &CompatReleaseReadinessChecklist,
    repo_root: &Path,
    report: &WorkflowRegressionRunReport,
) -> Result<CompatChecklistStatusReport> {
    let scenario_statuses = report
        .scenarios
        .iter()
        .map(|scenario| (scenario.id.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let covered_subsystems =
        report.coverage.covered_subsystems.iter().map(String::as_str).collect::<BTreeSet<_>>();

    let mut evidence = Vec::with_capacity(checklist.evidence.len());
    let mut validated = 0_usize;
    let mut pending = 0_usize;
    let mut failed = 0_usize;

    for item in &checklist.evidence {
        let status = match item.kind {
            CompatEvidenceKind::SourceContract => {
                if let Some(path) = item.path.as_deref() {
                    let resolved_path = resolve_repo_relative_path(repo_root, path);
                    match validate_source_contract(
                        item.id.as_str(),
                        resolved_path.as_path(),
                        item.must_contain.as_slice(),
                    ) {
                        Ok(()) => CompatEvidenceStatus {
                            id: item.id.clone(),
                            kind: item.kind.clone(),
                            status: CompatEvidenceStatusKind::DeclaredContract,
                            details: format!(
                                "validated source contract {}",
                                resolved_path.display()
                            ),
                        },
                        Err(error) => CompatEvidenceStatus {
                            id: item.id.clone(),
                            kind: item.kind.clone(),
                            status: CompatEvidenceStatusKind::InvalidReference,
                            details: error.to_string(),
                        },
                    }
                } else {
                    CompatEvidenceStatus {
                        id: item.id.clone(),
                        kind: item.kind.clone(),
                        status: CompatEvidenceStatusKind::InvalidReference,
                        details: "source contract entry is missing path".to_owned(),
                    }
                }
            }
            CompatEvidenceKind::WorkflowReport => {
                let expected_profile = item.profile.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "compat workflow evidence '{}' must declare a workflow profile",
                        item.id
                    )
                })?;
                if expected_profile != report.profile {
                    CompatEvidenceStatus {
                        id: item.id.clone(),
                        kind: item.kind.clone(),
                        status: CompatEvidenceStatusKind::PendingOtherProfile,
                        details: format!(
                            "report profile '{}' does not satisfy required profile '{}'",
                            report.profile, expected_profile
                        ),
                    }
                } else {
                    let missing_subsystems = item
                        .required_subsystems
                        .iter()
                        .filter(|entry| !covered_subsystems.contains(entry.as_str()))
                        .cloned()
                        .collect::<Vec<_>>();
                    let missing_or_failed_scenarios = item
                        .must_pass_scenarios
                        .iter()
                        .filter(|entry| {
                            scenario_statuses.get(entry.as_str()).is_none_or(|scenario| {
                                scenario.status != WorkflowRegressionExecutionStatus::Passed
                            })
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if report.summary.setup_failed > 0 || report.summary.scenario_failed > 0 {
                        CompatEvidenceStatus {
                            id: item.id.clone(),
                            kind: item.kind.clone(),
                            status: CompatEvidenceStatusKind::Failed,
                            details: format!(
                                "workflow report has {} setup failures and {} scenario failures",
                                report.summary.setup_failed, report.summary.scenario_failed
                            ),
                        }
                    } else if !missing_subsystems.is_empty() {
                        CompatEvidenceStatus {
                            id: item.id.clone(),
                            kind: item.kind.clone(),
                            status: CompatEvidenceStatusKind::Failed,
                            details: format!(
                                "missing subsystem coverage: {}",
                                missing_subsystems.join(", ")
                            ),
                        }
                    } else if !missing_or_failed_scenarios.is_empty() {
                        CompatEvidenceStatus {
                            id: item.id.clone(),
                            kind: item.kind.clone(),
                            status: CompatEvidenceStatusKind::Failed,
                            details: format!(
                                "required scenarios did not pass: {}",
                                missing_or_failed_scenarios.join(", ")
                            ),
                        }
                    } else {
                        CompatEvidenceStatus {
                            id: item.id.clone(),
                            kind: item.kind.clone(),
                            status: CompatEvidenceStatusKind::RuntimeValidated,
                            details: format!(
                                "workflow report '{}' satisfied profile '{}'",
                                report.profile, expected_profile
                            ),
                        }
                    }
                }
            }
        };

        match status.status {
            CompatEvidenceStatusKind::RuntimeValidated
            | CompatEvidenceStatusKind::DeclaredContract => validated = validated.saturating_add(1),
            CompatEvidenceStatusKind::PendingOtherProfile => pending = pending.saturating_add(1),
            CompatEvidenceStatusKind::Failed | CompatEvidenceStatusKind::InvalidReference => {
                failed = failed.saturating_add(1)
            }
        }

        evidence.push(status);
    }

    let state = if failed > 0 {
        CompatChecklistState::NeedsAttention
    } else if pending > 0 {
        CompatChecklistState::ProfileValidatedPendingExternalEvidence
    } else {
        CompatChecklistState::Ready
    };

    Ok(CompatChecklistStatusReport { state, validated, pending, failed, evidence })
}

pub fn covered_subsystems_for_profile(
    manifest: &WorkflowRegressionManifest,
    profile_id: &str,
) -> BTreeSet<String> {
    manifest
        .scenarios
        .iter()
        .filter(|scenario| scenario.profiles.iter().any(|value| value == profile_id))
        .flat_map(|scenario| scenario.subsystems.iter().cloned())
        .collect()
}

pub fn required_acceptance_scenarios_for_profile(
    manifest: &WorkflowRegressionManifest,
    profile_id: &str,
) -> BTreeSet<String> {
    manifest
        .runtime_acceptance_scenarios
        .iter()
        .filter(|scenario| scenario.required_profiles.iter().any(|value| value == profile_id))
        .map(|scenario| scenario.id.clone())
        .collect()
}

pub fn covered_acceptance_scenarios_for_profile(
    manifest: &WorkflowRegressionManifest,
    profile_id: &str,
) -> BTreeSet<String> {
    manifest
        .scenarios
        .iter()
        .filter(|scenario| scenario.profiles.iter().any(|value| value == profile_id))
        .flat_map(|scenario| scenario.acceptance_scenarios.iter().cloned())
        .collect()
}

pub fn repo_root_from_manifest_dir() -> Result<PathBuf> {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .context("failed to resolve workspace root from CARGO_MANIFEST_DIR")
}

pub fn resolve_repo_relative_path(repo_root: &Path, value: &str) -> PathBuf {
    let candidate = PathBuf::from(value);
    if candidate.is_absolute() {
        candidate
    } else {
        repo_root.join(candidate)
    }
}

fn validate_subsystem_catalog(
    subsystems: &[WorkflowRegressionSubsystem],
) -> Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    for subsystem in subsystems {
        ensure_nonempty_value(
            subsystem.id.as_str(),
            "workflow regression subsystem id must not be empty",
        )?;
        ensure_nonempty_value(
            subsystem.summary.as_str(),
            format!("workflow regression subsystem '{}' summary must not be empty", subsystem.id),
        )?;
        if !ids.insert(subsystem.id.clone()) {
            anyhow::bail!("workflow regression subsystem id '{}' is duplicated", subsystem.id);
        }
    }
    Ok(ids)
}

fn validate_setup_steps(
    steps: &[WorkflowRegressionSetupStep],
    profile_ids: &BTreeSet<String>,
) -> Result<()> {
    ensure_unique_ids(
        steps.iter().map(|step| step.id.as_str()),
        "workflow regression setup step ids",
    )?;
    for step in steps {
        ensure_nonempty_value(
            step.label.as_str(),
            format!("workflow regression setup step '{}' label must not be empty", step.id),
        )?;
        validate_profiles(
            profile_ids,
            step.profiles.as_slice(),
            format!("workflow regression setup step '{}'", step.id),
        )?;
        validate_command(
            step.command.as_slice(),
            format!("workflow regression setup step '{}'", step.id),
        )?;
    }
    Ok(())
}

fn validate_scenarios(
    scenarios: &[WorkflowRegressionScenario],
    profile_ids: &BTreeSet<String>,
    subsystem_ids: &BTreeSet<String>,
    runtime_acceptance_ids: &BTreeSet<String>,
) -> Result<()> {
    ensure_unique_ids(
        scenarios.iter().map(|scenario| scenario.id.as_str()),
        "workflow regression scenario ids",
    )?;
    for scenario in scenarios {
        ensure_nonempty_value(
            scenario.label.as_str(),
            format!("workflow regression scenario '{}' label must not be empty", scenario.id),
        )?;
        ensure_nonempty_value(
            scenario.category.as_str(),
            format!("workflow regression scenario '{}' category must not be empty", scenario.id),
        )?;
        validate_profiles(
            profile_ids,
            scenario.profiles.as_slice(),
            format!("workflow regression scenario '{}'", scenario.id),
        )?;
        validate_command(
            scenario.command.as_slice(),
            format!("workflow regression scenario '{}'", scenario.id),
        )?;
        if scenario.subsystems.is_empty() {
            anyhow::bail!(
                "workflow regression scenario '{}' must cover at least one subsystem",
                scenario.id
            );
        }
        for subsystem in &scenario.subsystems {
            if !subsystem_ids.contains(subsystem) {
                anyhow::bail!(
                    "workflow regression scenario '{}' references unknown subsystem '{}'",
                    scenario.id,
                    subsystem
                );
            }
        }
        for acceptance_scenario in &scenario.acceptance_scenarios {
            if !runtime_acceptance_ids.contains(acceptance_scenario) {
                anyhow::bail!(
                    "workflow regression scenario '{}' references unknown runtime acceptance scenario '{}'",
                    scenario.id,
                    acceptance_scenario
                );
            }
        }
    }
    Ok(())
}

fn runtime_acceptance_fixture_catalog_ids() -> Result<BTreeSet<String>> {
    let fixture_catalog = runtime_acceptance_fixture_catalog();
    let fixture_catalog = fixture_catalog.as_object().ok_or_else(|| {
        anyhow::anyhow!("runtime acceptance fixture catalog should serialize as an object")
    })?;
    let mut fixture_ids = BTreeSet::new();
    for key in fixture_catalog.keys() {
        if key == "schema_version" {
            continue;
        }
        fixture_ids.insert(key.clone());
    }
    if fixture_ids.is_empty() {
        anyhow::bail!("runtime acceptance fixture catalog must declare shared fixtures");
    }
    Ok(fixture_ids)
}

fn validate_runtime_acceptance_fixtures(
    fixtures: &[WorkflowRegressionFixture],
    fixture_catalog_ids: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    ensure_unique_ids(
        fixtures.iter().map(|fixture| fixture.id.as_str()),
        "workflow regression fixture ids",
    )?;
    let mut fixture_ids = BTreeSet::new();
    for fixture in fixtures {
        ensure_nonempty_value(
            fixture.summary.as_str(),
            format!("workflow regression fixture '{}' summary must not be empty", fixture.id),
        )?;
        if !fixture_catalog_ids.contains(fixture.id.as_str()) {
            anyhow::bail!(
                "workflow regression fixture '{}' is not published by palyra-common runtime acceptance fixtures",
                fixture.id
            );
        }
        fixture_ids.insert(fixture.id.clone());
    }
    Ok(fixture_ids)
}

fn validate_runtime_acceptance_catalog(
    scenarios: &[WorkflowRegressionAcceptanceScenario],
    profile_ids: &BTreeSet<String>,
    fixture_ids: &BTreeSet<String>,
) -> Result<BTreeSet<String>> {
    ensure_unique_ids(
        scenarios.iter().map(|scenario| scenario.id.as_str()),
        "workflow regression runtime acceptance scenario ids",
    )?;
    let canonical_scenarios = ALL_RUNTIME_ACCEPTANCE_SCENARIOS
        .into_iter()
        .map(|scenario| (scenario.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    let mut scenario_ids = BTreeSet::new();
    for scenario in scenarios {
        ensure_nonempty_value(
            scenario.label.as_str(),
            format!(
                "workflow regression runtime acceptance scenario '{}' label must not be empty",
                scenario.id
            ),
        )?;
        ensure_nonempty_value(
            scenario.summary.as_str(),
            format!(
                "workflow regression runtime acceptance scenario '{}' summary must not be empty",
                scenario.id
            ),
        )?;
        validate_profiles(
            profile_ids,
            scenario.required_profiles.as_slice(),
            format!("workflow regression runtime acceptance scenario '{}'", scenario.id),
        )?;
        let Some(canonical) = canonical_scenarios.get(scenario.id.as_str()).copied() else {
            anyhow::bail!(
                "workflow regression runtime acceptance scenario '{}' is not declared in palyra-common",
                scenario.id
            );
        };
        if scenario.capability != canonical.capability().as_str() {
            anyhow::bail!(
                "workflow regression runtime acceptance scenario '{}' must target capability '{}', found '{}'",
                scenario.id,
                canonical.capability().as_str(),
                scenario.capability
            );
        }
        let expected_fixture_keys = canonical
            .required_fixture_keys()
            .iter()
            .map(|value| value.to_string())
            .collect::<BTreeSet<_>>();
        let actual_fixture_keys = scenario.fixture_keys.iter().cloned().collect::<BTreeSet<_>>();
        if actual_fixture_keys != expected_fixture_keys {
            anyhow::bail!(
                "workflow regression runtime acceptance scenario '{}' fixture keys do not match the canonical shared fixture set",
                scenario.id
            );
        }
        for fixture_id in &scenario.fixture_keys {
            if !fixture_ids.contains(fixture_id) {
                anyhow::bail!(
                    "workflow regression runtime acceptance scenario '{}' references unknown fixture '{}'",
                    scenario.id,
                    fixture_id
                );
            }
        }
        scenario_ids.insert(scenario.id.clone());
    }
    let missing_canonical_scenarios = canonical_scenarios
        .keys()
        .filter(|id| !scenario_ids.contains(**id))
        .cloned()
        .collect::<Vec<_>>();
    if !missing_canonical_scenarios.is_empty() {
        anyhow::bail!(
            "workflow regression manifest is missing canonical runtime acceptance scenarios: {}",
            missing_canonical_scenarios.join(", ")
        );
    }
    Ok(scenario_ids)
}

fn validate_profiles(
    profile_ids: &BTreeSet<String>,
    profiles: &[String],
    owner: impl AsRef<str>,
) -> Result<()> {
    if profiles.is_empty() {
        anyhow::bail!("{} must declare at least one profile", owner.as_ref());
    }
    for profile in profiles {
        if !profile_ids.contains(profile) {
            anyhow::bail!("{} references unknown profile '{}'", owner.as_ref(), profile);
        }
    }
    Ok(())
}

fn validate_command(command: &[String], owner: impl AsRef<str>) -> Result<()> {
    if command.is_empty() {
        anyhow::bail!("{} must declare a command", owner.as_ref());
    }
    for part in command {
        ensure_nonempty_value(part.as_str(), format!("{} command entries", owner.as_ref()))?;
    }
    Ok(())
}

fn validate_source_contract(id: &str, path: &Path, must_contain: &[String]) -> Result<()> {
    if !path.is_file() {
        anyhow::bail!("compat source contract '{}' references missing path {}", id, path.display());
    }
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read source contract {}", path.display()))?;
    for needle in must_contain {
        ensure_nonempty_value(
            needle.as_str(),
            format!("compat source contract '{}' must_contain entries", id),
        )?;
        if !contents.contains(needle) {
            anyhow::bail!(
                "compat source contract '{}' expected '{}' in {}",
                id,
                needle,
                path.display()
            );
        }
    }
    Ok(())
}

fn ensure_schema_version(actual: u32, label: &str) -> Result<()> {
    if actual != WORKFLOW_REGRESSION_SCHEMA_VERSION {
        anyhow::bail!("{label} must be {}, found {}", WORKFLOW_REGRESSION_SCHEMA_VERSION, actual);
    }
    Ok(())
}

fn ensure_nonempty_value(value: &str, message: impl Into<String>) -> Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("{}", message.into());
    }
    Ok(())
}

fn ensure_unique_ids<'a>(ids: impl IntoIterator<Item = &'a str>, label: &str) -> Result<()> {
    let mut seen = BTreeSet::new();
    for id in ids {
        ensure_nonempty_value(id, format!("{label} entries must not be empty"))?;
        if !seen.insert(id.to_owned()) {
            anyhow::bail!("{label} contains duplicate id '{}'", id);
        }
    }
    Ok(())
}
