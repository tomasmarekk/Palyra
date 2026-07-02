//! `palyra qa`: QA Lab scenario manifest validation and discovery.

use crate::*;
use palyra_common::qa_scenarios::{
    parse_qa_scenario_manifest_yaml, QaScenarioManifest, QaScenarioManifestError,
    QaScenarioManifestIssue,
};
use serde::Serialize;

/// Runs a `palyra qa` subcommand.
///
/// # Errors
/// Returns an error when scenario files cannot be discovered, read, parsed, or
/// validated against the QA Lab manifest schema.
pub(crate) fn run_qa(command: QaCommand) -> Result<()> {
    match command {
        QaCommand::Validate { path, json } => run_validate(Path::new(path.as_str()), json),
    }
}

#[derive(Debug, Serialize)]
struct QaValidateReport {
    valid: bool,
    path: String,
    scenario_count: usize,
    scenarios: Vec<QaValidatedScenario>,
}

#[derive(Debug, Serialize)]
struct QaValidatedScenario {
    path: String,
    id: String,
    area: &'static str,
    provider_mode: &'static str,
    deterministic: bool,
    step_count: usize,
    artifact_count: usize,
    maturity_labels: Vec<String>,
    timeout_run_ms: u64,
}

fn run_validate(path: &Path, json: bool) -> Result<()> {
    let scenario_paths = collect_scenario_paths(path)?;
    let mut scenarios = Vec::with_capacity(scenario_paths.len());
    for scenario_path in scenario_paths {
        scenarios.push(validate_scenario_path(scenario_path.as_path())?);
    }

    let report = QaValidateReport {
        valid: true,
        path: path.display().to_string(),
        scenario_count: scenarios.len(),
        scenarios,
    };
    if output::preferred_json(json) {
        return output::print_json_pretty(
            &report,
            "failed to encode QA scenario validation report as JSON",
        );
    }
    println!("qa.validate status=ok path={} scenarios={}", report.path, report.scenario_count);
    for scenario in &report.scenarios {
        println!(
            "qa.scenario.valid id={} area={} provider_mode={} steps={} artifacts={} path={}",
            scenario.id,
            scenario.area,
            scenario.provider_mode,
            scenario.step_count,
            scenario.artifact_count,
            scenario.path
        );
    }
    std::io::stdout().flush().context("stdout flush failed")
}

fn collect_scenario_paths(path: &Path) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        return Ok(vec![path.to_path_buf()]);
    }
    if !path.is_dir() {
        anyhow::bail!("QA scenario path {} is not a file or directory", path.display());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read QA scenario directory {}", path.display()))?
    {
        let entry = entry.with_context(|| {
            format!("failed to read QA scenario directory entry in {}", path.display())
        })?;
        let entry_path = entry.path();
        if entry_path.is_file() && is_yaml_path(entry_path.as_path()) {
            paths.push(entry_path);
        }
    }
    paths.sort();
    if paths.is_empty() {
        anyhow::bail!(
            "QA scenario directory {} does not contain .yaml or .yml files",
            path.display()
        );
    }
    Ok(paths)
}

fn validate_scenario_path(path: &Path) -> Result<QaValidatedScenario> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read QA scenario {}", path.display()))?;
    match parse_qa_scenario_manifest_yaml(text.as_str()) {
        Ok(manifest) => Ok(validated_scenario(path, &manifest)),
        Err(error) => Err(render_manifest_error(path, error)),
    }
}

fn validated_scenario(path: &Path, manifest: &QaScenarioManifest) -> QaValidatedScenario {
    QaValidatedScenario {
        path: path.display().to_string(),
        id: manifest.id.clone(),
        area: manifest.area.as_str(),
        provider_mode: manifest.mode.provider.as_str(),
        deterministic: manifest.mode.deterministic,
        step_count: manifest.steps.len(),
        artifact_count: manifest.artifacts.len(),
        maturity_labels: manifest.maturity.labels.clone(),
        timeout_run_ms: manifest.timeout.run_ms,
    }
}

fn render_manifest_error(path: &Path, error: QaScenarioManifestError) -> anyhow::Error {
    match error.issues() {
        Some(issues) => anyhow::anyhow!(
            "QA scenario validation failed for {}: {}",
            path.display(),
            render_validation_issues(issues)
        ),
        None => anyhow::anyhow!("failed to parse QA scenario {}: {}", path.display(), error),
    }
}

fn render_validation_issues(issues: &[QaScenarioManifestIssue]) -> String {
    issues
        .iter()
        .map(|issue| format!("{} at {}: {}", issue.code, issue.path, issue.message))
        .collect::<Vec<_>>()
        .join("; ")
}

fn is_yaml_path(path: &Path) -> bool {
    path.extension()
        .and_then(OsStr::to_str)
        .is_some_and(|extension| matches!(extension, "yaml" | "yml"))
}
