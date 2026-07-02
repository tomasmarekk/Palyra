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
use palyra_model_providers::{
    parse_provider_compat_fixture_pack_yaml, parse_qa_mock_provider_fixture_yaml,
    provider_compat_fixture_pack_report, ProviderCompatFixtureError, ProviderCompatFixtureIssue,
    ProviderCompatPackReport,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

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
        QaCommand::ProviderCompat { path, output, json } => {
            run_provider_compat(Path::new(path.as_str()), output.as_deref(), json)
        }
        QaCommand::Gate { suite, output_json, output_markdown, allow_live, json } => run_gate(
            Path::new(suite.as_str()),
            output_json.as_deref(),
            output_markdown.as_deref(),
            allow_live,
            json,
        ),
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

#[derive(Debug, Serialize)]
struct QaProviderCompatReport {
    schema_version: u32,
    format: &'static str,
    path: String,
    pack_count: usize,
    fixture_count: usize,
    category_count: usize,
    missing_categories: Vec<String>,
    packs: Vec<ProviderCompatPackReport>,
}

#[derive(Debug, Clone, Deserialize)]
struct QaSuiteConfig {
    schema_version: u32,
    id: String,
    mode: String,
    scenario_roots: Vec<String>,
    #[serde(default)]
    include_tags: Vec<String>,
    #[serde(default)]
    exclude_tags: Vec<String>,
    #[serde(default)]
    allow_provider_modes: Vec<String>,
    #[serde(default)]
    allow_live_providers: bool,
    #[serde(default)]
    require_p0_green: bool,
    #[serde(default)]
    available_capabilities: Vec<String>,
    #[serde(default)]
    capability_skips: Vec<QaCapabilitySkipConfig>,
    #[serde(default)]
    flaky_policy: QaFlakyPolicyConfig,
    scorecard: QaScorecardConfig,
}

#[derive(Debug, Clone, Deserialize)]
struct QaCapabilitySkipConfig {
    capability: String,
    reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct QaFlakyPolicyConfig {
    max_retries: u32,
    fail_on_flaky: bool,
    require_issue: bool,
}

impl Default for QaFlakyPolicyConfig {
    fn default() -> Self {
        Self { max_retries: 0, fail_on_flaky: true, require_issue: true }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct QaScorecardConfig {
    #[serde(default)]
    baseline_score_bps: Option<u32>,
    #[serde(default)]
    fail_on_required_blockers: bool,
    categories: Vec<QaScorecardCategoryConfig>,
}

#[derive(Debug, Clone, Deserialize)]
struct QaScorecardCategoryConfig {
    id: String,
    label: String,
    #[serde(default)]
    labels: Vec<String>,
    #[serde(default)]
    areas: Vec<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    baseline_score_bps: Option<u32>,
}

#[derive(Debug, Serialize)]
struct QaGateReport {
    schema_version: u32,
    format: &'static str,
    suite_id: String,
    suite_mode: String,
    suite_path: String,
    scenario_roots: Vec<String>,
    decision: QaGateDecision,
    summary: QaGateSummary,
    flaky_policy: QaFlakyPolicyConfig,
    policy_violations: Vec<QaGatePolicyViolation>,
    maturity_scorecard: QaMaturityScorecard,
    scenarios: Vec<QaPackScenarioReport>,
}

#[derive(Debug, Serialize)]
struct QaGateSummary {
    scenario_count: usize,
    selected_count: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    unsupported: usize,
    p0_selected: usize,
    p0_failed: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum QaGateDecision {
    Pass,
    Fail,
}

impl QaGateDecision {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
        }
    }
}

#[derive(Debug, Serialize)]
struct QaGatePolicyViolation {
    code: String,
    scenario_id: Option<String>,
    detail: String,
}

#[derive(Debug, Serialize)]
struct QaMaturityScorecard {
    overall_score_bps: u32,
    baseline_score_bps: Option<u32>,
    trend_delta_bps: Option<i32>,
    blockers: Vec<String>,
    categories: Vec<QaMaturityCategoryScore>,
}

#[derive(Debug, Serialize)]
struct QaMaturityCategoryScore {
    id: String,
    label: String,
    required: bool,
    score_bps: u32,
    baseline_score_bps: Option<u32>,
    trend_delta_bps: Option<i32>,
    total: usize,
    passed: usize,
    failed: usize,
    skipped: usize,
    unsupported: usize,
    blockers: Vec<String>,
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

fn run_provider_compat(path: &Path, output: Option<&str>, json: bool) -> Result<()> {
    let report = build_provider_compat_report(path)?;
    if let Some(output) = output {
        let encoded = serde_json::to_vec_pretty(&report)
            .context("failed to encode provider compatibility report")?;
        write_qa_report(Path::new(output), encoded.as_slice())?;
    }
    if output::preferred_json(json) {
        output::print_json_pretty(
            &report,
            "failed to encode provider compatibility report as JSON",
        )?;
    } else {
        println!(
            "qa.provider_compat status=ok path={} packs={} fixtures={} categories={} missing={}",
            report.path,
            report.pack_count,
            report.fixture_count,
            report.category_count,
            report.missing_categories.len()
        );
        for pack in &report.packs {
            for fixture in &pack.fixtures {
                println!(
                    "qa.provider_compat.fixture id={} category={} failure_class={} recovery_decision={} fail_closed={} recovery_path=\"{}\"",
                    fixture.id,
                    fixture.category,
                    fixture.expected_failure_class,
                    fixture.expected_recovery_decision,
                    fixture.fail_closed,
                    fixture.recovery_path
                );
            }
        }
        std::io::stdout().flush().context("stdout flush failed")?;
    }
    Ok(())
}

fn run_gate(
    suite_path: &Path,
    output_json: Option<&str>,
    output_markdown: Option<&str>,
    allow_live: bool,
    json: bool,
) -> Result<()> {
    let report = build_gate_report(suite_path, allow_live)?;
    if let Some(output) = output_json {
        let encoded =
            serde_json::to_vec_pretty(&report).context("failed to encode QA gate report")?;
        write_qa_report(Path::new(output), encoded.as_slice())?;
    }
    if let Some(output) = output_markdown {
        let markdown = render_gate_markdown(&report);
        write_qa_report(Path::new(output), markdown.as_bytes())?;
    }
    if output::preferred_json(json) {
        output::print_json_pretty(&report, "failed to encode QA gate report as JSON")?;
    } else {
        println!(
            "qa.gate decision={} suite={} scenarios={} selected={} pass={} fail={} skipped={} unsupported={} maturity_score_bps={}",
            report.decision.as_str(),
            report.suite_id,
            report.summary.scenario_count,
            report.summary.selected_count,
            report.summary.passed,
            report.summary.failed,
            report.summary.skipped,
            report.summary.unsupported,
            report.maturity_scorecard.overall_score_bps
        );
        std::io::stdout().flush().context("stdout flush failed")?;
    }
    if report.decision != QaGateDecision::Pass {
        anyhow::bail!(
            "QA gate {} failed with {} failure(s), {} policy violation(s), and {} scorecard blocker(s)",
            report.suite_id,
            report.summary.failed,
            report.policy_violations.len(),
            report.maturity_scorecard.blockers.len()
        );
    }
    Ok(())
}

fn build_gate_report(suite_path: &Path, allow_live: bool) -> Result<QaGateReport> {
    let suite = load_suite_config(suite_path)?;
    let scenario_paths = collect_suite_scenario_paths(&suite)?;
    let available_capabilities =
        suite.available_capabilities.iter().cloned().collect::<BTreeSet<_>>();
    let capability_skip_reasons = suite
        .capability_skips
        .iter()
        .map(|skip| (skip.capability.clone(), skip.reason.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut scenarios = Vec::with_capacity(scenario_paths.len());
    for scenario_path in scenario_paths {
        scenarios.push(build_gate_scenario_report(
            scenario_path.as_path(),
            &suite,
            &available_capabilities,
            &capability_skip_reasons,
            allow_live,
        ));
    }
    let summary = build_gate_summary(&scenarios);
    let mut policy_violations = skip_reason_policy_violations(&scenarios);
    if suite.require_p0_green && summary.p0_failed > 0 {
        policy_violations.push(QaGatePolicyViolation {
            code: "release_p0_not_green".to_owned(),
            scenario_id: None,
            detail: format!(
                "suite requires green P0 scenarios but {} selected P0 scenario(s) failed or were unavailable",
                summary.p0_failed
            ),
        });
    }
    let maturity_scorecard = build_maturity_scorecard(&suite.scorecard, &scenarios);
    let required_scorecard_blockers = suite.scorecard.fail_on_required_blockers
        && maturity_scorecard
            .categories
            .iter()
            .any(|category| category.required && !category.blockers.is_empty());
    let decision =
        if summary.failed == 0 && policy_violations.is_empty() && !required_scorecard_blockers {
            QaGateDecision::Pass
        } else {
            QaGateDecision::Fail
        };
    Ok(QaGateReport {
        schema_version: 1,
        format: "palyra-qa-gate-report",
        suite_id: suite.id,
        suite_mode: suite.mode,
        suite_path: display_path_slash(suite_path),
        scenario_roots: suite.scenario_roots,
        decision,
        summary,
        flaky_policy: suite.flaky_policy,
        policy_violations,
        maturity_scorecard,
        scenarios,
    })
}

fn load_suite_config(path: &Path) -> Result<QaSuiteConfig> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read QA suite {}", path.display()))?;
    let suite = yaml_serde::from_str::<QaSuiteConfig>(text.as_str())
        .with_context(|| format!("failed to parse QA suite {}", path.display()))?;
    validate_suite_config(&suite, path)?;
    Ok(suite)
}

fn validate_suite_config(suite: &QaSuiteConfig, path: &Path) -> Result<()> {
    if suite.schema_version != 1 {
        anyhow::bail!(
            "QA suite {} uses unsupported schema_version {}",
            path.display(),
            suite.schema_version
        );
    }
    if suite.id.trim().is_empty() {
        anyhow::bail!("QA suite {} must define a non-empty id", path.display());
    }
    if suite.scenario_roots.is_empty() {
        anyhow::bail!("QA suite {} must define at least one scenario root", path.display());
    }
    if suite.scorecard.categories.is_empty() {
        anyhow::bail!("QA suite {} must define scorecard categories", path.display());
    }
    for skip in &suite.capability_skips {
        if skip.capability.trim().is_empty() || skip.reason.trim().is_empty() {
            anyhow::bail!(
                "QA suite {} capability skip entries must include capability and reason",
                path.display()
            );
        }
    }
    Ok(())
}

fn collect_suite_scenario_paths(suite: &QaSuiteConfig) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for root in &suite.scenario_roots {
        paths.extend(collect_scenario_paths(Path::new(root))?);
    }
    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        anyhow::bail!("QA suite {} did not resolve any scenario manifests", suite.id);
    }
    Ok(paths)
}

fn build_gate_scenario_report(
    path: &Path,
    suite: &QaSuiteConfig,
    available_capabilities: &BTreeSet<String>,
    capability_skip_reasons: &BTreeMap<String, String>,
    allow_live: bool,
) -> QaPackScenarioReport {
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
    if !matches_requested_tags(&manifest, suite.include_tags.as_slice())
        || matches_excluded_tags(&manifest, suite.exclude_tags.as_slice())
    {
        return pack_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Skipped,
            "skipped",
            Vec::new(),
            Some("suite tag filter".to_owned()),
        );
    }
    if !suite.allow_provider_modes.is_empty()
        && !suite.allow_provider_modes.iter().any(|mode| mode == manifest.mode.provider.as_str())
    {
        return pack_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            Vec::new(),
            Some(format!(
                "provider mode {} is not enabled for suite {}",
                manifest.mode.provider.as_str(),
                suite.id
            )),
        );
    }
    if manifest.mode.provider == QaScenarioProviderMode::Live
        && !(suite.allow_live_providers || allow_live)
    {
        return pack_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            Vec::new(),
            capability_skip_reasons.get("live_provider").cloned(),
        );
    }
    if let Some(missing_capability) = manifest
        .requires
        .capabilities
        .iter()
        .find(|capability| !available_capabilities.contains(capability.as_str()))
    {
        return pack_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            Vec::new(),
            capability_skip_reasons.get(missing_capability).cloned(),
        );
    }
    if manifest.mode.provider != QaScenarioProviderMode::Mock {
        return pack_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            Vec::new(),
            Some("local QA gate dry-run only executes mock-provider scenarios".to_owned()),
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

fn build_gate_summary(scenarios: &[QaPackScenarioReport]) -> QaGateSummary {
    let selected_count = scenarios
        .iter()
        .filter(|scenario| !matches!(scenario.status, QaPackScenarioStatus::Skipped))
        .count();
    let passed = count_status(scenarios, QaPackScenarioStatus::Pass);
    let failed = count_status(scenarios, QaPackScenarioStatus::Fail);
    let skipped = count_status(scenarios, QaPackScenarioStatus::Skipped);
    let unsupported = count_status(scenarios, QaPackScenarioStatus::Unsupported);
    let p0_selected = scenarios
        .iter()
        .filter(|scenario| {
            scenario.labels.iter().any(|label| label == "p0")
                && !matches!(scenario.status, QaPackScenarioStatus::Skipped)
        })
        .count();
    let p0_failed = scenarios
        .iter()
        .filter(|scenario| {
            scenario.labels.iter().any(|label| label == "p0")
                && matches!(
                    scenario.status,
                    QaPackScenarioStatus::Fail | QaPackScenarioStatus::Unsupported
                )
        })
        .count();
    QaGateSummary {
        scenario_count: scenarios.len(),
        selected_count,
        passed,
        failed,
        skipped,
        unsupported,
        p0_selected,
        p0_failed,
    }
}

fn skip_reason_policy_violations(scenarios: &[QaPackScenarioReport]) -> Vec<QaGatePolicyViolation> {
    scenarios
        .iter()
        .filter(|scenario| {
            matches!(
                scenario.status,
                QaPackScenarioStatus::Skipped | QaPackScenarioStatus::Unsupported
            ) && scenario.reason.as_ref().is_none_or(|reason| reason.trim().is_empty())
        })
        .map(|scenario| QaGatePolicyViolation {
            code: "missing_skip_reason".to_owned(),
            scenario_id: Some(scenario.id.clone()),
            detail: "skipped or unavailable scenario must include an explicit reason".to_owned(),
        })
        .collect()
}

fn build_maturity_scorecard(
    config: &QaScorecardConfig,
    scenarios: &[QaPackScenarioReport],
) -> QaMaturityScorecard {
    let categories = config
        .categories
        .iter()
        .map(|category| build_maturity_category_score(category, scenarios))
        .collect::<Vec<_>>();
    let overall_score_bps = if categories.is_empty() {
        0
    } else {
        let sum = categories.iter().map(|category| u64::from(category.score_bps)).sum::<u64>();
        sum.checked_div(categories.len() as u64).unwrap_or(0) as u32
    };
    let blockers = categories
        .iter()
        .flat_map(|category| {
            category.blockers.iter().map(|blocker| format!("{}:{blocker}", category.id))
        })
        .collect::<Vec<_>>();
    QaMaturityScorecard {
        overall_score_bps,
        baseline_score_bps: config.baseline_score_bps,
        trend_delta_bps: config
            .baseline_score_bps
            .map(|baseline| overall_score_bps as i32 - baseline as i32),
        blockers,
        categories,
    }
}

fn build_maturity_category_score(
    config: &QaScorecardCategoryConfig,
    scenarios: &[QaPackScenarioReport],
) -> QaMaturityCategoryScore {
    let matching = scenarios
        .iter()
        .filter(|scenario| category_matches_scenario(config, scenario))
        .collect::<Vec<_>>();
    let total = matching.len();
    let passed =
        matching.iter().filter(|scenario| scenario.status == QaPackScenarioStatus::Pass).count();
    let failed =
        matching.iter().filter(|scenario| scenario.status == QaPackScenarioStatus::Fail).count();
    let skipped =
        matching.iter().filter(|scenario| scenario.status == QaPackScenarioStatus::Skipped).count();
    let unsupported = matching
        .iter()
        .filter(|scenario| scenario.status == QaPackScenarioStatus::Unsupported)
        .count();
    let score_bps = ratio_bps(passed, total);
    let mut blockers = Vec::new();
    if total == 0 {
        blockers.push("coverage_missing".to_owned());
    }
    if failed > 0 {
        blockers.push("scenario_failed".to_owned());
    }
    if unsupported > 0 {
        blockers.push("capability_unavailable".to_owned());
    }
    if skipped > 0 {
        blockers.push("scenario_skipped".to_owned());
    }
    QaMaturityCategoryScore {
        id: config.id.clone(),
        label: config.label.clone(),
        required: config.required,
        score_bps,
        baseline_score_bps: config.baseline_score_bps,
        trend_delta_bps: config
            .baseline_score_bps
            .map(|baseline| score_bps as i32 - baseline as i32),
        total,
        passed,
        failed,
        skipped,
        unsupported,
        blockers,
    }
}

fn ratio_bps(numerator: usize, denominator: usize) -> u32 {
    numerator.checked_mul(10_000).and_then(|value| value.checked_div(denominator)).unwrap_or(0)
        as u32
}

fn category_matches_scenario(
    config: &QaScorecardCategoryConfig,
    scenario: &QaPackScenarioReport,
) -> bool {
    config.labels.iter().any(|label| scenario.labels.iter().any(|value| value == label))
        || config.areas.iter().any(|area| &scenario.area == area)
}

fn render_gate_markdown(report: &QaGateReport) -> String {
    let mut markdown = String::new();
    markdown.push_str(format!("# QA Lab Gate: {}\n\n", report.suite_id).as_str());
    markdown.push_str(format!("- Decision: `{}`\n", report.decision.as_str()).as_str());
    markdown.push_str(format!("- Suite mode: `{}`\n", report.suite_mode).as_str());
    markdown
        .push_str(format!("- Selected scenarios: `{}`\n", report.summary.selected_count).as_str());
    markdown.push_str(format!("- Passed: `{}`\n", report.summary.passed).as_str());
    markdown.push_str(format!("- Failed: `{}`\n", report.summary.failed).as_str());
    markdown.push_str(format!("- Unsupported: `{}`\n", report.summary.unsupported).as_str());
    markdown.push_str(
        format!("- Maturity score: `{}` bps\n\n", report.maturity_scorecard.overall_score_bps)
            .as_str(),
    );
    markdown.push_str("## Maturity Scorecard\n\n");
    markdown.push_str(
        "| Area | Required | Score bps | Trend bps | Total | Pass | Fail | Skip | Unsupported | Blockers |\n",
    );
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n");
    for category in &report.maturity_scorecard.categories {
        let trend = category.trend_delta_bps.map_or("-".to_owned(), |value| value.to_string());
        let blockers = if category.blockers.is_empty() {
            "-".to_owned()
        } else {
            category.blockers.join(", ")
        };
        markdown.push_str(
            format!(
                "| {} | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | `{}` | {} |\n",
                markdown_escape(category.label.as_str()),
                category.required,
                category.score_bps,
                trend,
                category.total,
                category.passed,
                category.failed,
                category.skipped,
                category.unsupported,
                markdown_escape(blockers.as_str())
            )
            .as_str(),
        );
    }
    if !report.policy_violations.is_empty() {
        markdown.push_str("\n## Policy Violations\n\n");
        markdown.push_str("| Code | Scenario | Detail |\n");
        markdown.push_str("| --- | --- | --- |\n");
        for violation in &report.policy_violations {
            markdown.push_str(
                format!(
                    "| `{}` | `{}` | {} |\n",
                    markdown_escape(violation.code.as_str()),
                    violation.scenario_id.as_deref().unwrap_or("-"),
                    markdown_escape(violation.detail.as_str())
                )
                .as_str(),
            );
        }
    }
    markdown.push_str("\n## Scenario Results\n\n");
    markdown.push_str("| Scenario | Status | Area | Labels | Reason |\n");
    markdown.push_str("| --- | --- | --- | --- | --- |\n");
    for scenario in &report.scenarios {
        markdown.push_str(
            format!(
                "| `{}` | `{}` | `{}` | {} | {} |\n",
                markdown_escape(scenario.id.as_str()),
                scenario.status.as_str(),
                markdown_escape(scenario.area.as_str()),
                markdown_escape(scenario.labels.join(", ").as_str()),
                markdown_escape(scenario.reason.as_deref().unwrap_or("-"))
            )
            .as_str(),
        );
    }
    markdown
}

fn markdown_escape(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

fn build_provider_compat_report(path: &Path) -> Result<QaProviderCompatReport> {
    let pack_paths = collect_yaml_paths(path, "provider compatibility fixture")?;
    let mut packs = Vec::with_capacity(pack_paths.len());
    for pack_path in pack_paths {
        packs.push(load_provider_compat_pack_report(pack_path.as_path())?);
    }
    let fixture_count = packs.iter().map(|pack| pack.fixture_count).sum();
    let categories = packs
        .iter()
        .flat_map(|pack| pack.fixtures.iter().map(|fixture| fixture.category.clone()))
        .collect::<BTreeSet<_>>();
    let missing_categories = packs
        .iter()
        .flat_map(|pack| pack.missing_categories.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    Ok(QaProviderCompatReport {
        schema_version: 1,
        format: "palyra-qa-provider-compat-report",
        path: display_path_slash(path),
        pack_count: packs.len(),
        fixture_count,
        category_count: categories.len(),
        missing_categories,
        packs,
    })
}

fn load_provider_compat_pack_report(path: &Path) -> Result<ProviderCompatPackReport> {
    let text = fs::read_to_string(path).with_context(|| {
        format!("failed to read provider compatibility fixture {}", path.display())
    })?;
    let pack = parse_provider_compat_fixture_pack_yaml(text.as_str())
        .map_err(|error| render_provider_compat_error(path, error))?;
    Ok(provider_compat_fixture_pack_report(&pack))
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

fn matches_excluded_tags(manifest: &QaScenarioManifest, excluded_tags: &[String]) -> bool {
    excluded_tags.iter().any(|tag| manifest.maturity.labels.iter().any(|label| label == tag))
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
            && display_path_slash(path).starts_with("qa/fixtures/")
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

fn collect_yaml_paths(path: &Path, label: &str) -> Result<Vec<PathBuf>> {
    if path.is_file() {
        if is_yaml_path(path) {
            return Ok(vec![path.to_path_buf()]);
        }
        anyhow::bail!("{} path {} is not a YAML file", label, path.display());
    }
    if !path.is_dir() {
        anyhow::bail!("{} path {} is not a file or directory", label, path.display());
    }
    let mut paths = Vec::new();
    for entry in fs::read_dir(path)
        .with_context(|| format!("failed to read {} directory {}", label, path.display()))?
    {
        let entry = entry.with_context(|| {
            format!("failed to read {} directory entry in {}", label, path.display())
        })?;
        let entry_path = entry.path();
        if entry_path.is_file() && is_yaml_path(entry_path.as_path()) {
            paths.push(entry_path);
        }
    }
    paths.sort();
    if paths.is_empty() {
        anyhow::bail!(
            "{} directory {} does not contain .yaml or .yml files",
            label,
            path.display()
        );
    }
    Ok(paths)
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

fn render_provider_compat_error(path: &Path, error: ProviderCompatFixtureError) -> anyhow::Error {
    match error.issues() {
        Some(issues) => anyhow::anyhow!(
            "provider compatibility fixture validation failed for {}: {}",
            path.display(),
            render_provider_compat_issues(issues)
        ),
        None => anyhow::anyhow!(
            "failed to parse provider compatibility fixture {}: {}",
            path.display(),
            error
        ),
    }
}

fn render_provider_compat_issues(issues: &[ProviderCompatFixtureIssue]) -> String {
    issues
        .iter()
        .map(|issue| format!("{} at {}: {}", issue.code, issue.path, issue.message))
        .collect::<Vec<_>>()
        .join("; ")
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
