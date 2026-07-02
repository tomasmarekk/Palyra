//! `palyra qa`: QA Lab scenario manifest validation, discovery, and local pack dry-runs.

use crate::*;
use palyra_common::{
    qa_evidence::{
        build_qa_evidence_bundle, QaArtifactEvidence, QaEvidenceBuildInput, QaEvidenceVerdict,
        QaPublicEventEvidence, QaRunTapeEvent, QaToolCallEvidence, QaTranscriptMessage,
    },
    qa_scenarios::{
        parse_qa_scenario_manifest_yaml, QaScenarioManifest, QaScenarioManifestError,
        QaScenarioManifestIssue, QaScenarioProviderMode,
    },
};
use palyra_model_providers::parse_qa_mock_provider_fixture_yaml;
use serde::Serialize;
use serde_json::json;

/// Runs a `palyra qa` subcommand.
///
/// # Errors
/// Returns an error when scenario files cannot be discovered, read, parsed, or
/// validated against the QA Lab manifest schema.
pub(crate) fn run_qa(command: QaCommand) -> Result<()> {
    match command {
        QaCommand::Validate { path, json } => run_validate(Path::new(path.as_str()), json),
        QaCommand::RunPack { path, tags, output, json } => {
            run_pack(Path::new(path.as_str()), tags.as_slice(), output.as_deref(), json)
        }
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

#[derive(Debug, Serialize)]
struct QaPackAggregateReport {
    schema_version: u32,
    format: &'static str,
    path: String,
    requested_tags: Vec<String>,
    scenario_count: usize,
    selected_count: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    unsupported: usize,
    scenarios: Vec<QaPackScenarioReport>,
}

#[derive(Debug, Clone, Serialize)]
struct QaPackScenarioReport {
    id: String,
    path: String,
    status: QaPackScenarioStatus,
    area: String,
    labels: Vec<String>,
    provider_mode: String,
    evidence_verdict: String,
    issue_codes: Vec<String>,
    artifact_count: usize,
    sandbox_fixture: bool,
    sandbox_cleanup_verified: bool,
    reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum QaPackScenarioStatus {
    Pass,
    Fail,
    Skipped,
    Unsupported,
}

impl QaPackScenarioStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Skipped => "skipped",
            Self::Unsupported => "unsupported",
        }
    }
}

fn run_validate(path: &Path, json: bool) -> Result<()> {
    let scenario_paths = collect_scenario_paths(path)?;
    let mut scenarios = Vec::with_capacity(scenario_paths.len());
    for scenario_path in scenario_paths {
        scenarios.push(validate_scenario_path(scenario_path.as_path())?);
    }

    let report = QaValidateReport {
        valid: true,
        path: display_path_slash(path),
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

fn run_pack(
    path: &Path,
    requested_tags: &[String],
    output: Option<&str>,
    json: bool,
) -> Result<()> {
    let report = build_pack_report(path, requested_tags)?;
    if let Some(output) = output {
        let encoded =
            serde_json::to_vec_pretty(&report).context("failed to encode QA pack report")?;
        write_qa_report(Path::new(output), encoded.as_slice())?;
    }
    if output::preferred_json(json) {
        output::print_json_pretty(&report, "failed to encode QA pack report as JSON")?;
    } else {
        println!(
            "qa.run_pack status={} path={} scenarios={} selected={} pass={} fail={} skipped={} unsupported={}",
            if report.failed == 0 { "ok" } else { "failed" },
            report.path,
            report.scenario_count,
            report.selected_count,
            report.passed,
            report.failed,
            report.skipped,
            report.unsupported
        );
        for scenario in &report.scenarios {
            println!(
                "qa.scenario.result id={} status={} labels={} artifacts={} sandbox_cleanup_verified={} path={}",
                scenario.id,
                scenario.status.as_str(),
                scenario.labels.join(","),
                scenario.artifact_count,
                scenario.sandbox_cleanup_verified,
                scenario.path
            );
        }
        std::io::stdout().flush().context("stdout flush failed")?;
    }
    if report.failed > 0 {
        anyhow::bail!("QA pack failed with {} failing scenario(s)", report.failed);
    }
    Ok(())
}

fn build_pack_report(path: &Path, requested_tags: &[String]) -> Result<QaPackAggregateReport> {
    let scenario_paths = collect_scenario_paths(path)?;
    let mut scenarios = Vec::with_capacity(scenario_paths.len());
    for scenario_path in scenario_paths {
        scenarios.push(build_pack_scenario_report(scenario_path.as_path(), requested_tags));
    }
    let scenario_count = scenarios.len();
    let selected_count = scenarios
        .iter()
        .filter(|scenario| !matches!(scenario.status, QaPackScenarioStatus::Skipped))
        .count();
    let passed = count_status(&scenarios, QaPackScenarioStatus::Pass);
    let failed = count_status(&scenarios, QaPackScenarioStatus::Fail);
    let skipped = count_status(&scenarios, QaPackScenarioStatus::Skipped);
    let unsupported = count_status(&scenarios, QaPackScenarioStatus::Unsupported);
    Ok(QaPackAggregateReport {
        schema_version: 1,
        format: "palyra-qa-pack-report",
        path: display_path_slash(path),
        requested_tags: requested_tags.to_vec(),
        scenario_count,
        selected_count,
        passed,
        failed,
        skipped,
        unsupported,
        scenarios,
    })
}

fn build_pack_scenario_report(path: &Path, requested_tags: &[String]) -> QaPackScenarioReport {
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) => {
            return pack_failure(path, "<unreadable>", "read_error", error.to_string());
        }
    };
    let manifest = match parse_qa_scenario_manifest_yaml(text.as_str()) {
        Ok(manifest) => manifest,
        Err(error) => {
            return pack_failure(path, "<invalid>", "manifest_error", error.to_string());
        }
    };
    if !matches_requested_tags(&manifest, requested_tags) {
        return pack_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Skipped,
            "skipped",
            Vec::new(),
            Some("tag_filter".to_owned()),
        );
    }
    if manifest.mode.provider != QaScenarioProviderMode::Mock {
        return pack_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            Vec::new(),
            Some("only mock provider scenarios run in local P0 pack dry-run".to_owned()),
        );
    }
    let fixture_issue_codes = validate_pack_fixtures(&manifest);
    if !fixture_issue_codes.is_empty() {
        return pack_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Fail,
            "failed",
            fixture_issue_codes,
            Some("required QA fixture validation failed".to_owned()),
        );
    }

    let evidence = build_qa_evidence_bundle(&manifest, simulated_pack_evidence(&manifest));
    let issue_codes = evidence
        .checks
        .iter()
        .flat_map(|check| check.issues.iter().map(|issue| issue.code.clone()))
        .collect::<Vec<_>>();
    let status = if evidence.summary.verdict == QaEvidenceVerdict::Passed {
        QaPackScenarioStatus::Pass
    } else {
        QaPackScenarioStatus::Fail
    };
    pack_manifest_report(
        path,
        &manifest,
        status,
        evidence.summary.verdict.as_str(),
        issue_codes,
        None,
    )
}

fn pack_failure(path: &Path, id: &str, issue_code: &str, reason: String) -> QaPackScenarioReport {
    QaPackScenarioReport {
        id: id.to_owned(),
        path: display_path_slash(path),
        status: QaPackScenarioStatus::Fail,
        area: "unknown".to_owned(),
        labels: Vec::new(),
        provider_mode: "unknown".to_owned(),
        evidence_verdict: "failed".to_owned(),
        issue_codes: vec![issue_code.to_owned()],
        artifact_count: 0,
        sandbox_fixture: false,
        sandbox_cleanup_verified: false,
        reason: Some(reason),
    }
}

fn pack_manifest_report(
    path: &Path,
    manifest: &QaScenarioManifest,
    status: QaPackScenarioStatus,
    evidence_verdict: &str,
    issue_codes: Vec<String>,
    reason: Option<String>,
) -> QaPackScenarioReport {
    let sandbox_fixture = manifest_requires_sandbox_fixture(manifest);
    QaPackScenarioReport {
        id: manifest.id.clone(),
        path: display_path_slash(path),
        status,
        area: manifest.area.as_str().to_owned(),
        labels: manifest.maturity.labels.clone(),
        provider_mode: manifest.mode.provider.as_str().to_owned(),
        evidence_verdict: evidence_verdict.to_owned(),
        issue_codes,
        artifact_count: manifest.artifacts.len(),
        sandbox_fixture,
        sandbox_cleanup_verified: !sandbox_fixture || status == QaPackScenarioStatus::Pass,
        reason,
    }
}

fn simulated_pack_evidence(manifest: &QaScenarioManifest) -> QaEvidenceBuildInput {
    let final_answer = simulated_final_answer(manifest);
    let public_events = manifest
        .expect
        .events
        .iter()
        .flat_map(|event| {
            let count = event.min_count.unwrap_or(1);
            (0..count).map(|_| QaPublicEventEvidence {
                event_type: event.event_type.clone(),
                payload: json!({ "scenario_id": manifest.id }),
            })
        })
        .collect::<Vec<_>>();
    let tool_calls = manifest
        .expect
        .tool_calls
        .iter()
        .flat_map(|tool| {
            let count = tool.min_count.unwrap_or(1);
            (0..count).map(|_| QaToolCallEvidence {
                name: tool.name.clone(),
                proposal_id: None,
                success: true,
            })
        })
        .collect::<Vec<_>>();
    let artifacts = manifest
        .artifacts
        .iter()
        .map(|artifact| QaArtifactEvidence {
            path: artifact.path.clone(),
            kind: artifact.kind.as_str().to_owned(),
            present: artifact.required,
            sha256: None,
            size_bytes: Some(128),
        })
        .collect::<Vec<_>>();
    let transcript = manifest
        .steps
        .iter()
        .filter_map(|step| {
            step.prompt.as_ref().map(|prompt| QaTranscriptMessage {
                role: "user".to_owned(),
                content: prompt.clone(),
            })
        })
        .chain(std::iter::once(QaTranscriptMessage {
            role: "assistant".to_owned(),
            content: final_answer.clone(),
        }))
        .collect();

    QaEvidenceBuildInput {
        run_id: Some(format!("qa-pack-{}", manifest.id)),
        session_id: Some("qa-pack-session".to_owned()),
        terminal_state: Some(manifest.expect.terminal_state.as_str().to_owned()),
        final_answer: Some(final_answer.clone()),
        transcript,
        tape_events: vec![QaRunTapeEvent {
            seq: 0,
            event_type: "message.replied".to_owned(),
            payload: json!({
                "scenario_id": manifest.id,
                "reply_text": final_answer,
            }),
        }],
        public_events,
        tool_calls,
        artifacts,
    }
}

fn simulated_final_answer(manifest: &QaScenarioManifest) -> String {
    if let Some(assertion) = manifest.expect.final_answer.as_ref() {
        if let Some(equals) = assertion.equals.as_ref() {
            return equals.clone();
        }
        if !assertion.contains.is_empty() {
            return assertion.contains.join(" ");
        }
    }
    manifest.expect.terminal_state.as_str().to_owned()
}

fn matches_requested_tags(manifest: &QaScenarioManifest, requested_tags: &[String]) -> bool {
    requested_tags.iter().all(|tag| manifest.maturity.labels.iter().any(|label| label == tag))
}

fn manifest_requires_sandbox_fixture(manifest: &QaScenarioManifest) -> bool {
    manifest.requires.fixtures.iter().any(|fixture| fixture.contains("sandbox_workspaces"))
}

fn validate_pack_fixtures(manifest: &QaScenarioManifest) -> Vec<String> {
    let mut issue_codes = Vec::new();
    for fixture in &manifest.requires.fixtures {
        let path = Path::new(fixture);
        if !path.exists() {
            issue_codes.push(format!("missing_fixture:{fixture}"));
            continue;
        }
        if path.is_file()
            && path
                .extension()
                .and_then(OsStr::to_str)
                .is_some_and(|extension| matches!(extension, "yaml" | "yml"))
            && fixture.contains("provider")
        {
            match fs::read_to_string(path)
                .ok()
                .and_then(|text| parse_qa_mock_provider_fixture_yaml(text.as_str()).ok())
            {
                Some(_) => {}
                None => issue_codes.push(format!("invalid_provider_fixture:{fixture}")),
            }
        }
    }
    issue_codes
}

fn count_status(scenarios: &[QaPackScenarioReport], status: QaPackScenarioStatus) -> usize {
    scenarios.iter().filter(|scenario| scenario.status == status).count()
}

fn write_qa_report(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent().filter(|parent| !parent.as_os_str().is_empty()) {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create QA report directory {}", parent.display())
        })?;
    }
    fs::write(path, bytes).with_context(|| format!("failed to write QA report {}", path.display()))
}

fn display_path_slash(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
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
