//! `palyra qa`: schema previews and runtime-backed QA Lab qualification gates.

use super::{
    qa_checkpoint::{
        QaAttemptCompletion, QaCampaignCheckpointReport, QaCampaignIdentity, QaCheckpointStore,
        QaResumeDecision, QaResumePolicy,
    },
    qa_runner::{
        execute_prepared_scenario, load_execution_report, prepare_scenario_execution,
        qa_runner_version, recover_execution_report, scenario_result_artifact_path,
        QaPreparedScenarioExecution, QaScenarioExecutionReport, QaScenarioExecutionRequest,
    },
};
use crate::*;
use palyra_common::{
    qa_evidence::{
        build_qa_evidence_bundle, QaArtifactEvidence, QaEvidenceBuildInput, QaEvidenceVerdict,
        QaPublicEventEvidence, QaRunTapeEvent, QaToolCallEvidence, QaTranscriptMessage,
    },
    qa_scenarios::{
        parse_qa_scenario_manifest_yaml, QaScenarioManifest, QaScenarioManifestError,
        QaScenarioManifestIssue, QaScenarioProviderMode, QaScenarioRunnerMode,
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
use ulid::Ulid;

/// Runs a `palyra qa` subcommand.
///
/// # Errors
/// Returns an error when scenario files cannot be discovered, read, parsed, or
/// validated, or when a selected runtime-backed gate cannot be executed.
pub(crate) fn run_qa(command: QaCommand) -> Result<()> {
    match command {
        QaCommand::Validate { path, json } => run_validate(Path::new(path.as_str()), json),
        QaCommand::RunPack { path, tags, output, json } => {
            run_pack(Path::new(path.as_str()), tags.as_slice(), output.as_deref(), json)
        }
        QaCommand::ProviderCompat { path, output, json } => {
            run_provider_compat(Path::new(path.as_str()), output.as_deref(), json)
        }
        QaCommand::Gate {
            suite,
            output_json,
            output_markdown,
            allow_live,
            resume,
            force_rerun,
            json,
        } => run_gate(
            Path::new(suite.as_str()),
            output_json.as_deref(),
            output_markdown.as_deref(),
            allow_live,
            resume,
            force_rerun,
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
    runner_mode: &'static str,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    runner_mode: Option<String>,
    evidence_verdict: String,
    issue_codes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_class: Option<String>,
    artifact_count: usize,
    sandbox_fixture: bool,
    sandbox_cleanup_verified: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution: Option<QaScenarioExecutionReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    resume_reason_code: Option<String>,
    #[serde(skip_serializing_if = "is_false")]
    reused: bool,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
struct QaSuiteConfig {
    schema_version: u32,
    id: String,
    mode: String,
    scenario_roots: Vec<String>,
    #[serde(default)]
    expected_scenario_ids: Vec<String>,
    #[serde(default)]
    include_tags: Vec<String>,
    #[serde(default)]
    exclude_tags: Vec<String>,
    #[serde(default)]
    allow_provider_modes: Vec<String>,
    #[serde(default)]
    allow_runner_modes: Vec<String>,
    #[serde(default)]
    allow_live_providers: bool,
    #[serde(default)]
    resume: Option<QaResumePolicyConfig>,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QaResumePolicyConfig {
    enabled: bool,
    reuse_passed: bool,
    rerun_failed: bool,
    rerun_partial: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QaCapabilitySkipConfig {
    capability: String,
    reason: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QaScorecardConfig {
    #[serde(default)]
    baseline_score_bps: Option<u32>,
    #[serde(default)]
    fail_on_required_blockers: bool,
    categories: Vec<QaScorecardCategoryConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
    artifact_reference_base: String,
    suite_path: String,
    scenario_roots: Vec<String>,
    resume_requested: bool,
    force_rerun: bool,
    campaign_checkpoint: QaCampaignCheckpointReport,
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

fn is_false(value: &bool) -> bool {
    !*value
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
        path: report_path(path),
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
            "qa.scenario.valid id={} area={} provider_mode={} runner_mode={} steps={} artifacts={} path={}",
            scenario.id,
            scenario.area,
            scenario.provider_mode,
            scenario.runner_mode,
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
    resume: bool,
    force_rerun: bool,
    json: bool,
) -> Result<()> {
    validate_gate_output_directories(output_json, output_markdown)?;
    let artifact_root = gate_artifact_root(output_json, output_markdown);
    let artifact_reference_base = gate_artifact_reference_base(output_json, output_markdown);
    let runtime = build_runtime()?;
    let (report, _checkpoint_store) = runtime.block_on(build_gate_report(
        suite_path,
        allow_live,
        resume,
        force_rerun,
        artifact_root.as_path(),
        artifact_reference_base,
    ))?;
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

fn gate_artifact_root(output_json: Option<&str>, output_markdown: Option<&str>) -> PathBuf {
    [output_json, output_markdown]
        .into_iter()
        .flatten()
        .map(gate_output_parent)
        .next()
        .unwrap_or_else(|| PathBuf::from("target/qa-lab"))
}

fn gate_output_parent(output: &str) -> PathBuf {
    Path::new(output)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf)
}

fn validate_gate_output_directories(
    output_json: Option<&str>,
    output_markdown: Option<&str>,
) -> Result<()> {
    let (Some(output_json), Some(output_markdown)) = (output_json, output_markdown) else {
        return Ok(());
    };
    if gate_output_parent(output_json) != gate_output_parent(output_markdown) {
        anyhow::bail!(
            "qa.runner.output_directory_mismatch: JSON and Markdown gate reports must share one output directory"
        );
    }
    Ok(())
}

fn gate_artifact_reference_base(
    output_json: Option<&str>,
    output_markdown: Option<&str>,
) -> &'static str {
    if output_json.is_some() || output_markdown.is_some() {
        "."
    } else {
        "target/qa-lab"
    }
}

async fn build_gate_report(
    suite_path: &Path,
    allow_live: bool,
    resume: bool,
    force_rerun: bool,
    artifact_root: &Path,
    artifact_reference_base: &str,
) -> Result<(QaGateReport, QaCheckpointStore)> {
    let suite = load_suite_config(suite_path)?;
    let normalized_suite =
        serde_json::to_vec(&suite).context("qa.resume.suite_normalization_failed")?;
    let campaign_identity = QaCampaignIdentity::new(
        suite.id.as_str(),
        normalized_suite.as_slice(),
        qa_runner_version().as_str(),
    )?;
    let mut checkpoint_store = QaCheckpointStore::load(artifact_root, campaign_identity)?;
    let resume_policy = suite.resume.as_ref().map_or(
        QaResumePolicy { enabled: false, reuse_passed: false },
        |policy| QaResumePolicy {
            enabled: resume && policy.enabled,
            reuse_passed: policy.reuse_passed,
        },
    );
    let scenario_paths = collect_suite_scenario_paths(&suite)?;
    let available_capabilities =
        suite.available_capabilities.iter().cloned().collect::<BTreeSet<_>>();
    let capability_skip_reasons = suite
        .capability_skips
        .iter()
        .map(|skip| (skip.capability.clone(), skip.reason.clone()))
        .collect::<BTreeMap<_, _>>();
    let scenario_context = QaGateScenarioContext {
        suite: &suite,
        available_capabilities: &available_capabilities,
        capability_skip_reasons: &capability_skip_reasons,
        allow_live,
        resume_policy,
        force_rerun,
        artifact_root,
    };
    let mut scenarios = Vec::with_capacity(scenario_paths.len());
    for scenario_path in scenario_paths {
        scenarios.push(
            build_gate_scenario_report(
                scenario_path.as_path(),
                &scenario_context,
                &mut checkpoint_store,
            )
            .await,
        );
    }
    scenarios.sort_by(|left, right| left.id.cmp(&right.id).then(left.path.cmp(&right.path)));
    let summary = build_gate_summary(&scenarios);
    let mut policy_violations = skip_reason_policy_violations(&scenarios);
    policy_violations.extend(schema_preview_policy_violations(&scenarios));
    if summary.selected_count == 0 {
        policy_violations.push(QaGatePolicyViolation {
            code: "qa.runner.no_scenarios_selected".to_owned(),
            scenario_id: None,
            detail: "a runtime qualification gate must select at least one scenario".to_owned(),
        });
    }
    if let Some(violation) = expected_scenario_set_policy_violation(&suite, &scenarios) {
        policy_violations.push(violation);
    }
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
    let report = QaGateReport {
        schema_version: 3,
        format: "palyra-qa-gate-report",
        suite_id: suite.id,
        suite_mode: suite.mode,
        artifact_reference_base: artifact_reference_base.to_owned(),
        suite_path: report_path(suite_path),
        scenario_roots: suite
            .scenario_roots
            .iter()
            .map(|root| report_path(Path::new(root)))
            .collect(),
        resume_requested: resume,
        force_rerun,
        campaign_checkpoint: checkpoint_store.report(),
        decision,
        summary,
        flaky_policy: suite.flaky_policy,
        policy_violations,
        maturity_scorecard,
        scenarios,
    };
    Ok((report, checkpoint_store))
}

struct QaGateScenarioContext<'a> {
    suite: &'a QaSuiteConfig,
    available_capabilities: &'a BTreeSet<String>,
    capability_skip_reasons: &'a BTreeMap<String, String>,
    allow_live: bool,
    resume_policy: QaResumePolicy,
    force_rerun: bool,
    artifact_root: &'a Path,
}

fn load_suite_config(path: &Path) -> Result<QaSuiteConfig> {
    let text = fs::read_to_string(path)
        .with_context(|| format!("failed to read QA suite {}", path.display()))?;
    let raw = yaml_serde::from_str::<serde_json::Value>(text.as_str())
        .with_context(|| format!("failed to parse QA suite {}", path.display()))?;
    validate_suite_versioned_fields(&raw, path)?;
    let suite = yaml_serde::from_str::<QaSuiteConfig>(text.as_str())
        .with_context(|| format!("failed to parse QA suite {}", path.display()))?;
    validate_suite_config(&suite, path)?;
    Ok(suite)
}

fn validate_suite_versioned_fields(value: &serde_json::Value, path: &Path) -> Result<()> {
    let schema_version = value.get("schema_version").and_then(serde_json::Value::as_u64);
    if !matches!(schema_version, Some(2 | 3)) {
        return Ok(());
    }
    const FIELDS: &[&str] = &[
        "schema_version",
        "id",
        "mode",
        "scenario_roots",
        "expected_scenario_ids",
        "include_tags",
        "exclude_tags",
        "allow_provider_modes",
        "allow_runner_modes",
        "allow_live_providers",
        "resume",
        "require_p0_green",
        "available_capabilities",
        "capability_skips",
        "flaky_policy",
        "scorecard",
    ];
    let object = value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("QA suite {} must be a mapping", path.display()))?;
    if let Some(field) = object.keys().find(|field| !FIELDS.contains(&field.as_str())) {
        anyhow::bail!("QA suite {} contains unsupported field {}", path.display(), field);
    }
    if schema_version == Some(2) && object.contains_key("expected_scenario_ids") {
        anyhow::bail!(
            "QA suite {} must use schema_version 3 before pinning expected_scenario_ids",
            path.display()
        );
    }
    Ok(())
}

fn validate_suite_config(suite: &QaSuiteConfig, path: &Path) -> Result<()> {
    if !matches!(suite.schema_version, 1..=3) {
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
    if suite.schema_version < 3 && !suite.expected_scenario_ids.is_empty() {
        anyhow::bail!(
            "QA suite {} must use schema_version 3 before pinning expected scenario ids",
            path.display()
        );
    }
    if suite.schema_version == 3 {
        let expected_ids =
            suite.expected_scenario_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
        if expected_ids.is_empty()
            || expected_ids.len() != suite.expected_scenario_ids.len()
            || expected_ids.iter().any(|id| id.trim().is_empty())
        {
            anyhow::bail!(
                "QA suite {} schema_version 3 requires unique non-empty expected_scenario_ids",
                path.display()
            );
        }
    }
    if suite.scorecard.categories.is_empty() {
        anyhow::bail!("QA suite {} must define scorecard categories", path.display());
    }
    if suite.schema_version == 1 && suite.resume.is_some() {
        anyhow::bail!(
            "QA suite {} must use schema_version 2 before configuring resume",
            path.display()
        );
    }
    if let Some(resume) = &suite.resume {
        if !resume.rerun_failed || !resume.rerun_partial {
            anyhow::bail!(
                "QA suite {} resume policy must rerun failed and partial attempts",
                path.display()
            );
        }
        if resume.enabled && !resume.reuse_passed {
            anyhow::bail!(
                "QA suite {} cannot enable resume while disabling passed-result reuse",
                path.display()
            );
        }
    }
    for mode in &suite.allow_runner_modes {
        if !matches!(mode.as_str(), "fixture" | "record_replay" | "live") {
            anyhow::bail!("QA suite {} includes unsupported runner mode {}", path.display(), mode);
        }
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

async fn build_gate_scenario_report(
    path: &Path,
    context: &QaGateScenarioContext<'_>,
    checkpoint_store: &mut QaCheckpointStore,
) -> QaPackScenarioReport {
    let QaGateScenarioContext {
        suite,
        available_capabilities,
        capability_skip_reasons,
        allow_live,
        resume_policy,
        force_rerun,
        artifact_root,
    } = *context;
    let text = match fs::read_to_string(path) {
        Ok(text) => text,
        Err(_) => {
            return gate_failure(path, "<unreadable>", "qa.runner.scenario_read_failed");
        }
    };
    let manifest = match parse_qa_scenario_manifest_yaml(text.as_str()) {
        Ok(manifest) => manifest,
        Err(_) => {
            return gate_failure(path, "<invalid>", "qa.runner.manifest_invalid");
        }
    };
    if !matches_requested_tags(&manifest, suite.include_tags.as_slice())
        || matches_excluded_tags(&manifest, suite.exclude_tags.as_slice())
    {
        return gate_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Skipped,
            "skipped",
            Vec::new(),
            Some("qa.runner.tag_filtered".to_owned()),
        );
    }
    if manifest.schema_version < 2 && manifest.runner.is_none() {
        return gate_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            vec!["qa.runner.schema_preview_only".to_owned()],
            Some(
                "qa.runner.schema_preview_only: use `palyra qa run-pack` for legacy schema previews"
                    .to_owned(),
            ),
        );
    }
    if !suite_allows_runner(suite, &manifest) {
        return gate_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            vec!["qa.runner.mode_not_enabled".to_owned()],
            Some("qa.runner.mode_not_enabled".to_owned()),
        );
    }
    if manifest.mode.runner == QaScenarioRunnerMode::Live
        && !(suite.allow_live_providers && allow_live)
    {
        return gate_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            vec!["qa.runner.live_not_enabled".to_owned()],
            capability_skip_reasons.get("live_provider").cloned(),
        );
    }
    if let Some(missing_capability) = manifest
        .requires
        .capabilities
        .iter()
        .find(|capability| !available_capabilities.contains(capability.as_str()))
    {
        return gate_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Unsupported,
            "unsupported",
            vec!["qa.runner.capability_unavailable".to_owned()],
            capability_skip_reasons.get(missing_capability).cloned(),
        );
    }
    let fixture_issue_codes = validate_pack_fixtures(&manifest);
    if !fixture_issue_codes.is_empty() {
        return gate_manifest_report(
            path,
            &manifest,
            QaPackScenarioStatus::Fail,
            "failed",
            fixture_issue_codes,
            Some("required QA fixture validation failed".to_owned()),
        );
    }

    let prepared = match prepare_scenario_execution(&manifest) {
        Ok(prepared) => prepared,
        Err(error) => {
            let reason_code = runner_error_reason_code(&error);
            return manifest_gate_failure(path, &manifest, reason_code.as_str());
        }
    };
    if let Err(error) = recover_partial_execution(checkpoint_store, artifact_root, &prepared) {
        let reason_code = runner_error_reason_code(&error);
        return manifest_gate_failure(path, &manifest, reason_code.as_str());
    }
    let live_force_rerun = manifest.mode.runner == QaScenarioRunnerMode::Live;
    let decision = checkpoint_store.resume_decision(
        &prepared.execution_key,
        resume_policy,
        force_rerun || live_force_rerun,
    );
    match decision {
        QaResumeDecision::Reuse {
            reason_code,
            attempt_generation,
            execution_id,
            result_artifact,
        } => match load_execution_report(
            artifact_root,
            result_artifact,
            &prepared.execution_key,
            attempt_generation,
            execution_id.as_str(),
        ) {
            Ok(execution) => {
                let mut report = execution_scenario_report(path, &manifest, execution);
                report.resume_reason_code = Some(reason_code);
                report.reused = true;
                report
            }
            Err(_) => {
                execute_checkpointed_scenario(
                    path,
                    &manifest,
                    prepared,
                    artifact_root,
                    checkpoint_store,
                    "qa.resume.result_untrusted",
                )
                .await
            }
        },
        QaResumeDecision::Run { mut reason_code } => {
            if live_force_rerun && !force_rerun {
                reason_code = "qa.resume.live_rerun".to_owned();
            }
            execute_checkpointed_scenario(
                path,
                &manifest,
                prepared,
                artifact_root,
                checkpoint_store,
                reason_code.as_str(),
            )
            .await
        }
    }
}

fn recover_partial_execution(
    checkpoint_store: &mut QaCheckpointStore,
    artifact_root: &Path,
    prepared: &QaPreparedScenarioExecution,
) -> Result<()> {
    let Some(token) = checkpoint_store.current_partial_attempt_token(&prepared.execution_key)
    else {
        return Ok(());
    };
    let Ok(report) = recover_execution_report(
        artifact_root,
        token.expected_result_path.as_str(),
        &token.execution_key,
        token.attempt_generation,
        token.execution_id.as_str(),
    ) else {
        return Ok(());
    };
    let completion = attempt_completion(&report);
    checkpoint_store.recover_published_attempt(&token, completion)
}

async fn execute_checkpointed_scenario(
    path: &Path,
    manifest: &QaScenarioManifest,
    prepared: QaPreparedScenarioExecution,
    artifact_root: &Path,
    checkpoint_store: &mut QaCheckpointStore,
    resume_reason_code: &str,
) -> QaPackScenarioReport {
    let execution_id = Ulid::new().to_string();
    let result_path = scenario_result_artifact_path(execution_id.as_str(), &prepared.execution_key);
    let result_path = display_path_slash(result_path.as_path());
    let token = match checkpoint_store.begin_attempt(
        prepared.execution_key.clone(),
        manifest.id.as_str(),
        execution_id.as_str(),
        result_path.as_str(),
        resume_reason_code,
    ) {
        Ok(token) => token,
        Err(error) => {
            let reason_code = runner_error_reason_code(&error);
            return manifest_gate_failure(path, manifest, reason_code.as_str());
        }
    };
    let request = QaScenarioExecutionRequest {
        execution_key: token.execution_key.clone(),
        execution_id: token.execution_id.clone(),
        attempt_generation: token.attempt_generation,
        runner_version: token.runner_version.clone(),
        previous_result_artifact: token.previous_result_artifact.clone(),
    };
    match execute_prepared_scenario(manifest, &prepared, request, artifact_root).await {
        Ok(execution) => {
            let completion = attempt_completion(&execution);
            if let Err(error) = checkpoint_store.complete_attempt(&token, completion) {
                let reason_code = runner_error_reason_code(&error);
                return manifest_gate_failure(path, manifest, reason_code.as_str());
            }
            let mut report = execution_scenario_report(path, manifest, execution);
            report.resume_reason_code = Some(resume_reason_code.to_owned());
            report.reused = false;
            report
        }
        Err(error) => {
            let reason_code = runner_error_reason_code(&error);
            manifest_gate_failure(path, manifest, reason_code.as_str())
        }
    }
}

fn attempt_completion(execution: &QaScenarioExecutionReport) -> QaAttemptCompletion {
    QaAttemptCompletion {
        execution_key: execution.result.execution_key.clone(),
        attempt_generation: execution.result.attempt.generation,
        execution_id: execution.result.execution_id.clone(),
        runner_version: execution.result.attempt.runner_version.clone(),
        verdict: execution.result.verdict.clone(),
        result_artifact: execution.result_artifact.clone(),
        previous_result_artifact: execution.result.attempt.previous_result_artifact.clone(),
        reason_codes: execution.result.reason_codes.clone(),
    }
}

fn suite_allows_runner(suite: &QaSuiteConfig, manifest: &QaScenarioManifest) -> bool {
    if !suite.allow_runner_modes.is_empty() {
        return suite.allow_runner_modes.iter().any(|mode| mode == manifest.mode.runner.as_str());
    }
    suite.allow_provider_modes.is_empty()
        || suite.allow_provider_modes.iter().any(|mode| mode == manifest.mode.provider.as_str())
}

fn execution_scenario_report(
    path: &Path,
    manifest: &QaScenarioManifest,
    execution: QaScenarioExecutionReport,
) -> QaPackScenarioReport {
    let evidence_passed = execution.result.verdict == QaEvidenceVerdict::Passed.as_str();
    let cleanup_verified = execution.result.cleanup.verified;
    let status = if evidence_passed && cleanup_verified {
        QaPackScenarioStatus::Pass
    } else {
        QaPackScenarioStatus::Fail
    };
    let mut issue_codes = if evidence_passed {
        Vec::new()
    } else {
        execution
            .result
            .reason_codes
            .iter()
            .filter(|code| !is_success_reason_code(code))
            .cloned()
            .collect()
    };
    if !cleanup_verified {
        issue_codes.extend(
            execution
                .result
                .cleanup
                .reason_codes
                .iter()
                .filter(|code| !is_success_reason_code(code))
                .cloned(),
        );
    }
    issue_codes.sort();
    issue_codes.dedup();
    if status == QaPackScenarioStatus::Fail && issue_codes.is_empty() {
        issue_codes.push("qa.runner.verdict_failed".to_owned());
    }
    let reason = issue_codes.first().cloned();
    let artifact_count = execution.result.evidence_artifacts.len().saturating_add(1);
    let failure_class = classify_manifest_failure(manifest, status, issue_codes.as_slice());

    QaPackScenarioReport {
        id: manifest.id.clone(),
        path: report_path(path),
        status,
        area: manifest.area.as_str().to_owned(),
        labels: manifest.maturity.labels.clone(),
        provider_mode: manifest.mode.provider.as_str().to_owned(),
        runner_mode: Some(manifest.mode.runner.as_str().to_owned()),
        evidence_verdict: execution.result.verdict.clone(),
        issue_codes,
        failure_class,
        artifact_count,
        sandbox_fixture: manifest_requires_sandbox_fixture(manifest),
        sandbox_cleanup_verified: cleanup_verified,
        execution: Some(execution),
        resume_reason_code: None,
        reused: false,
        reason,
    }
}

fn is_success_reason_code(code: &str) -> bool {
    matches!(code, "qa.runner.assertions_passed" | "qa.runner.cleanup_verified")
}

fn manifest_gate_failure(
    path: &Path,
    manifest: &QaScenarioManifest,
    reason_code: &str,
) -> QaPackScenarioReport {
    let mut report = gate_manifest_report(
        path,
        manifest,
        QaPackScenarioStatus::Fail,
        "failed",
        vec![reason_code.to_owned()],
        Some(reason_code.to_owned()),
    );
    report.sandbox_cleanup_verified = false;
    report
}

fn gate_manifest_report(
    path: &Path,
    manifest: &QaScenarioManifest,
    status: QaPackScenarioStatus,
    evidence_verdict: &str,
    issue_codes: Vec<String>,
    reason: Option<String>,
) -> QaPackScenarioReport {
    let mut report =
        pack_manifest_report(path, manifest, status, evidence_verdict, issue_codes, reason);
    report.runner_mode = Some(manifest.mode.runner.as_str().to_owned());
    report
}

fn gate_failure(path: &Path, id: &str, reason_code: &str) -> QaPackScenarioReport {
    QaPackScenarioReport {
        id: id.to_owned(),
        path: report_path(path),
        status: QaPackScenarioStatus::Fail,
        area: "unknown".to_owned(),
        labels: Vec::new(),
        provider_mode: "unknown".to_owned(),
        runner_mode: None,
        evidence_verdict: "failed".to_owned(),
        issue_codes: vec![reason_code.to_owned()],
        failure_class: None,
        artifact_count: 0,
        sandbox_fixture: false,
        sandbox_cleanup_verified: false,
        execution: None,
        resume_reason_code: None,
        reused: false,
        reason: Some(reason_code.to_owned()),
    }
}

fn runner_error_reason_code(error: &anyhow::Error) -> String {
    error
        .chain()
        .filter_map(|cause| {
            cause
                .to_string()
                .split(|character: char| character == ':' || character.is_ascii_whitespace())
                .next()
                .map(str::to_owned)
        })
        .find(|code| {
            (code.starts_with("qa.runner.") || code.starts_with("qa.resume."))
                && code.len() <= 160
                && code.bytes().all(|byte| {
                    byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
                })
        })
        .unwrap_or_else(|| "qa.runner.execution_failed".to_owned())
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

fn schema_preview_policy_violations(
    scenarios: &[QaPackScenarioReport],
) -> Vec<QaGatePolicyViolation> {
    scenarios
        .iter()
        .filter(|scenario| {
            scenario
                .issue_codes
                .iter()
                .any(|code| code == "qa.runner.schema_preview_only")
        })
        .map(|scenario| QaGatePolicyViolation {
            code: "schema_preview_not_runtime_qualification".to_owned(),
            scenario_id: Some(scenario.id.clone()),
            detail: "legacy schema preview cannot count as a real runtime gate; validate it with `palyra qa run-pack`"
                .to_owned(),
        })
        .collect()
}

fn expected_scenario_set_policy_violation(
    suite: &QaSuiteConfig,
    scenarios: &[QaPackScenarioReport],
) -> Option<QaGatePolicyViolation> {
    if suite.expected_scenario_ids.is_empty() {
        return None;
    }
    let expected = suite.expected_scenario_ids.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let actual = scenarios
        .iter()
        .filter(|scenario| scenario.status != QaPackScenarioStatus::Skipped)
        .map(|scenario| scenario.id.as_str())
        .collect::<BTreeSet<_>>();
    if actual == expected {
        return None;
    }
    let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
    let extra = actual.difference(&expected).copied().collect::<Vec<_>>();
    Some(QaGatePolicyViolation {
        code: "qa.runner.expected_scenario_set_mismatch".to_owned(),
        scenario_id: None,
        detail: format!(
            "runtime qualification scenario set drifted: missing={missing:?} extra={extra:?}"
        ),
    })
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
    markdown.push_str(
        format!("- Artifact reference base: `{}`\n", report.artifact_reference_base).as_str(),
    );
    markdown
        .push_str(format!("- Selected scenarios: `{}`\n", report.summary.selected_count).as_str());
    markdown.push_str(format!("- Passed: `{}`\n", report.summary.passed).as_str());
    markdown.push_str(format!("- Failed: `{}`\n", report.summary.failed).as_str());
    markdown.push_str(format!("- Unsupported: `{}`\n", report.summary.unsupported).as_str());
    markdown.push_str(format!("- Resume requested: `{}`\n", report.resume_requested).as_str());
    markdown.push_str(format!("- Force rerun: `{}`\n", report.force_rerun).as_str());
    markdown.push_str(
        format!(
            "- Checkpoint generation: `{}`\n",
            report.campaign_checkpoint.checkpoint_generation
        )
        .as_str(),
    );
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
    markdown.push_str(
        "| Scenario | Status | Area | Labels | Reused | Resume reason | Failure class | Reason |\n",
    );
    markdown.push_str("| --- | --- | --- | --- | --- | --- | --- | --- |\n");
    for scenario in &report.scenarios {
        markdown.push_str(
            format!(
                "| `{}` | `{}` | `{}` | {} | `{}` | `{}` | `{}` | {} |\n",
                markdown_escape(scenario.id.as_str()),
                scenario.status.as_str(),
                markdown_escape(scenario.area.as_str()),
                markdown_escape(scenario.labels.join(", ").as_str()),
                scenario.reused,
                markdown_escape(scenario.resume_reason_code.as_deref().unwrap_or("-")),
                markdown_escape(scenario.failure_class.as_deref().unwrap_or("-")),
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
        path: report_path(path),
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
        path: report_path(path),
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

    let mut preview_manifest = manifest.clone();
    // The parser already validates runtime-path selectors. A schema preview must
    // not fabricate runtime provenance that only a real `qa gate` can prove.
    preview_manifest.expect.runtime_path = None;
    let evidence =
        build_qa_evidence_bundle(&preview_manifest, schema_preview_evidence(&preview_manifest));
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
        path: report_path(path),
        status: QaPackScenarioStatus::Fail,
        area: "unknown".to_owned(),
        labels: Vec::new(),
        provider_mode: "unknown".to_owned(),
        runner_mode: None,
        evidence_verdict: "failed".to_owned(),
        issue_codes: vec![issue_code.to_owned()],
        failure_class: None,
        artifact_count: 0,
        sandbox_fixture: false,
        sandbox_cleanup_verified: false,
        execution: None,
        resume_reason_code: None,
        reused: false,
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
    let failure_class = classify_manifest_failure(manifest, status, issue_codes.as_slice());
    QaPackScenarioReport {
        id: manifest.id.clone(),
        path: report_path(path),
        status,
        area: manifest.area.as_str().to_owned(),
        labels: manifest.maturity.labels.clone(),
        provider_mode: manifest.mode.provider.as_str().to_owned(),
        runner_mode: None,
        evidence_verdict: evidence_verdict.to_owned(),
        issue_codes,
        failure_class,
        artifact_count: manifest.artifacts.len(),
        sandbox_fixture,
        sandbox_cleanup_verified: !sandbox_fixture || status == QaPackScenarioStatus::Pass,
        execution: None,
        resume_reason_code: None,
        reused: false,
        reason,
    }
}

fn schema_preview_evidence(manifest: &QaScenarioManifest) -> QaEvidenceBuildInput {
    let final_answer = schema_preview_final_answer(manifest);
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
                success: Some(tool.success.unwrap_or(true)),
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
        ..QaEvidenceBuildInput::default()
    }
}

fn schema_preview_final_answer(manifest: &QaScenarioManifest) -> String {
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
        || manifest.runner.as_ref().is_some_and(|runner| runner.workspace_fixture().is_some())
}

fn classify_manifest_failure(
    manifest: &QaScenarioManifest,
    status: QaPackScenarioStatus,
    issue_codes: &[String],
) -> Option<String> {
    if !manifest_exercises_mcp(manifest) {
        return None;
    }
    match status {
        QaPackScenarioStatus::Unsupported => Some("server_unavailable".to_owned()),
        QaPackScenarioStatus::Fail
            if issue_codes.iter().any(|code| is_mcp_availability_issue(code)) =>
        {
            Some("server_unavailable".to_owned())
        }
        QaPackScenarioStatus::Fail => Some("runtime_regression".to_owned()),
        QaPackScenarioStatus::Pass | QaPackScenarioStatus::Skipped => None,
    }
}

fn manifest_exercises_mcp(manifest: &QaScenarioManifest) -> bool {
    manifest.maturity.labels.iter().any(|label| label == "mcp")
        || manifest.requires.capabilities.iter().any(|capability| capability == "mcp")
        || manifest.requires.tools.iter().any(|tool| tool.starts_with("mcp."))
        || manifest.requires.fixtures.iter().any(|fixture| fixture.contains("/mcp/"))
}

fn is_mcp_availability_issue(code: &str) -> bool {
    code.starts_with("missing_fixture:qa/fixtures/mcp/")
        || code.starts_with("invalid_mcp_fixture:")
        || code.contains("mcp.server_unavailable")
        || code.contains("mcp.transport_reconnect_failed")
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
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow::anyhow!("qa.runner.report_path_invalid"))?;
    let temporary_path = path.with_file_name(format!(".{file_name}.{}.tmp", Ulid::new()));
    let mut temporary = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temporary_path.as_path())
        .with_context(|| format!("failed to create QA report temp {}", temporary_path.display()))?;
    let publish_result = (|| -> Result<()> {
        temporary.write_all(bytes).with_context(|| {
            format!("failed to write QA report temp {}", temporary_path.display())
        })?;
        temporary.flush().with_context(|| {
            format!("failed to flush QA report temp {}", temporary_path.display())
        })?;
        temporary.sync_all().with_context(|| {
            format!("failed to sync QA report temp {}", temporary_path.display())
        })?;
        drop(temporary);
        replace_qa_report(temporary_path.as_path(), path)?;
        sync_qa_report_directory(path)?;
        let persisted = fs::read(path)
            .with_context(|| format!("failed to verify QA report {}", path.display()))?;
        if persisted != bytes {
            anyhow::bail!("qa.runner.report_verify_failed");
        }
        Ok(())
    })();
    if publish_result.is_err() {
        let _ = fs::remove_file(temporary_path.as_path());
    }
    publish_result
}

#[cfg(not(windows))]
fn replace_qa_report(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination).with_context(|| {
        format!(
            "qa.runner.report_publish_failed: {} -> {}",
            source.display(),
            destination.display()
        )
    })
}

#[cfg(windows)]
fn replace_qa_report(source: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    let source = source.as_os_str().encode_wide().chain(std::iter::once(0)).collect::<Vec<_>>();
    let destination =
        destination.as_os_str().encode_wide().chain(std::iter::once(0)).collect::<Vec<_>>();
    let moved = unsafe {
        // SAFETY: Both vectors are owned, valid, nul-terminated UTF-16 paths for
        // the duration of the Win32 call; the flags request an atomic replacement.
        MoveFileExW(
            source.as_ptr(),
            destination.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved == 0 {
        return Err(std::io::Error::last_os_error()).context("qa.runner.report_publish_failed");
    }
    Ok(())
}

fn sync_qa_report_directory(
    #[cfg_attr(not(unix), allow(unused_variables))] path: &Path,
) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::File::open(parent).and_then(|directory| directory.sync_all()).with_context(|| {
            format!("qa.runner.report_directory_sync_failed: {}", parent.display())
        })?;
    }
    Ok(())
}

fn display_path_slash(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn report_path(path: &Path) -> String {
    if !path_looks_absolute(path) {
        return display_path_slash(path);
    }
    if let Ok(current_dir) = std::env::current_dir() {
        if let (Ok(canonical_path), Ok(canonical_root)) =
            (fs::canonicalize(path), fs::canonicalize(current_dir.as_path()))
        {
            if let Ok(relative) = canonical_path.strip_prefix(canonical_root.as_path()) {
                return display_path_slash(relative);
            }
        }
        if let Ok(relative) = path.strip_prefix(current_dir.as_path()) {
            return display_path_slash(relative);
        }
    }
    "<normalized:absolute_path>".to_owned()
}

fn path_looks_absolute(path: &Path) -> bool {
    let value = path.to_string_lossy();
    let bytes = value.as_bytes();
    path.is_absolute()
        || value.starts_with('/')
        || value.starts_with('\\')
        || (bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':')
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
        path: report_path(path),
        id: manifest.id.clone(),
        area: manifest.area.as_str(),
        provider_mode: manifest.mode.provider.as_str(),
        runner_mode: manifest.mode.runner.as_str(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::qa_runner::{
        test_runtime_path_evidence, QaExecutionArtifactRef, QaScenarioAttemptProvenance,
        QaScenarioCleanupResult, QaScenarioExecutionKey, QaScenarioExecutionResult,
    };

    #[test]
    fn gate_artifacts_follow_report_parent_or_safe_default() {
        assert_eq!(
            gate_artifact_root(Some("artifacts/qa/report.json"), None),
            PathBuf::from("artifacts/qa")
        );
        assert_eq!(gate_artifact_root(None, Some("reports/gate.md")), PathBuf::from("reports"));
        assert_eq!(gate_artifact_root(Some("report.json"), None), PathBuf::from("."));
        assert_eq!(gate_artifact_root(None, None), PathBuf::from("target/qa-lab"));
        assert_eq!(gate_artifact_reference_base(Some("report.json"), None), ".");
        assert_eq!(gate_artifact_reference_base(None, None), "target/qa-lab");
    }

    #[test]
    fn gate_reports_must_share_one_artifact_reference_directory() {
        validate_gate_output_directories(Some("reports/gate.json"), Some("reports/gate.md"))
            .expect("co-located gate reports should be accepted");

        let error =
            validate_gate_output_directories(Some("json/gate.json"), Some("markdown/gate.md"))
                .expect_err("split gate report directories must be rejected");

        assert!(error.to_string().contains("qa.runner.output_directory_mismatch"));
    }

    #[test]
    fn report_paths_hide_external_absolute_roots() {
        for path in [
            Path::new("C:\\Users\\qa-user\\private\\suite.yaml"),
            Path::new("C:/Users/qa-user/private/suite.yaml"),
            Path::new("C:private\\suite.yaml"),
            Path::new("\\Users\\qa-user\\private\\suite.yaml"),
            Path::new("\\\\server\\share\\private\\suite.yaml"),
            Path::new("/tmp/private/suite.yaml"),
            Path::new("/home/qa-user/private/suite.yaml"),
            Path::new("/Users/qa-user/private/suite.yaml"),
        ] {
            assert_eq!(report_path(path), "<normalized:absolute_path>");
        }
    }

    #[test]
    fn report_paths_project_files_under_the_invocation_root_as_relative() {
        let current_dir = std::env::current_dir().expect("test current directory should resolve");
        let cargo_manifest = current_dir.join("Cargo.toml");

        assert!(cargo_manifest.is_file());
        assert_eq!(report_path(cargo_manifest.as_path()), "Cargo.toml");
    }

    #[test]
    fn runner_allowlist_takes_precedence_with_provider_fallback() {
        let manifest = mcp_manifest();
        let mut suite = qa_suite_config();

        assert!(suite_allows_runner(&suite, &manifest));

        suite.allow_runner_modes = vec!["record_replay".to_owned()];
        assert!(!suite_allows_runner(&suite, &manifest));

        suite.allow_runner_modes = vec!["fixture".to_owned()];
        suite.allow_provider_modes = vec!["live".to_owned()];
        assert!(suite_allows_runner(&suite, &manifest));
    }

    #[test]
    fn release_schema_preview_preserves_the_legacy_qualification_roots() {
        let suite_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("qa/suites/release_schema_preview.yaml");
        let suite = load_suite_config(suite_path.as_path())
            .expect("preserved release schema-preview suite should parse");
        let category_ids = suite
            .scorecard
            .categories
            .iter()
            .map(|category| category.id.as_str())
            .collect::<Vec<_>>();

        assert_eq!(suite.mode, "schema_preview");
        assert_eq!(
            suite.scenario_roots.iter().map(String::as_str).collect::<Vec<_>>(),
            [
                "qa/scenarios",
                "qa/scenarios/execution_backends",
                "qa/scenarios/provider",
                "qa/scenarios/mcp",
            ]
        );
        assert_eq!(
            category_ids,
            [
                "api",
                "mcp",
                "subagents",
                "provider_recovery",
                "execution_backends",
                "hooks",
                "observability",
                "security",
            ]
        );
    }

    #[test]
    fn baseline_replay_suite_pins_every_required_runtime_scenario() {
        let suite_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("qa/suites/baseline_replay.yaml");
        let suite =
            load_suite_config(suite_path.as_path()).expect("baseline replay suite should parse");

        assert_eq!(suite.schema_version, 3);
        assert_eq!(
            suite.expected_scenario_ids,
            [
                "real_runtime.malformed_stream_recovery",
                "real_runtime.mutation_approval_denied",
                "real_runtime.provider_attempt_recovery_executor",
                "real_runtime.read_only_tool",
                "real_runtime.record_replay_text",
                "real_runtime.text_exact",
            ]
        );
        assert!(suite.scorecard.categories.iter().all(|category| category.required));
    }

    #[test]
    fn expected_runtime_scenario_set_fails_closed_on_missing_or_extra_members() {
        let mut suite = qa_suite_config();
        suite.schema_version = 3;
        suite.expected_scenario_ids = vec!["baseline.a".to_owned(), "baseline.b".to_owned()];
        let report = |id: &str, status| {
            let mut report = pack_failure(
                Path::new("qa/scenarios/baseline.yaml"),
                id,
                "unit",
                "unit".to_owned(),
            );
            report.status = status;
            report
        };

        let exact = [
            report("baseline.a", QaPackScenarioStatus::Pass),
            report("baseline.b", QaPackScenarioStatus::Fail),
        ];
        assert!(expected_scenario_set_policy_violation(&suite, &exact).is_none());

        let missing = [
            report("baseline.a", QaPackScenarioStatus::Pass),
            report("baseline.b", QaPackScenarioStatus::Skipped),
        ];
        let missing_violation = expected_scenario_set_policy_violation(&suite, &missing)
            .expect("a tag-filtered baseline scenario must fail the exact set");
        assert_eq!(missing_violation.code, "qa.runner.expected_scenario_set_mismatch");
        assert!(missing_violation.detail.contains("baseline.b"));

        let extra = [
            report("baseline.a", QaPackScenarioStatus::Pass),
            report("baseline.b", QaPackScenarioStatus::Pass),
            report("baseline.c", QaPackScenarioStatus::Pass),
        ];
        let extra_violation = expected_scenario_set_policy_violation(&suite, &extra)
            .expect("an unpinned baseline scenario must fail the exact set");
        assert!(extra_violation.detail.contains("baseline.c"));
    }

    #[test]
    fn runtime_mismatch_fails_without_embedding_observation_payloads() {
        let manifest = mcp_manifest();
        let execution = QaScenarioExecutionReport {
            result: QaScenarioExecutionResult {
                schema_version: 3,
                format: "palyra-qa-scenario-execution-result".to_owned(),
                execution_key: test_execution_key(),
                attempt: QaScenarioAttemptProvenance {
                    generation: 1,
                    runner_version: "qa-runner.test".to_owned(),
                    runtime_version: "palyrad-test".to_owned(),
                    runtime_contract_version: "runtime-contracts.test".to_owned(),
                    palyrad_binary_sha256: "1".repeat(64),
                    palyrad_version: "0.1.0".to_owned(),
                    palyrad_git_hash: "test".to_owned(),
                    palyrad_build_profile: "debug".to_owned(),
                    previous_result_artifact: None,
                },
                execution_id: "execution-opaque".to_owned(),
                scenario_id: manifest.id.clone(),
                runner_mode: "fixture".to_owned(),
                verdict: "failed".to_owned(),
                reason_codes: vec![
                    "final_answer_mismatch".to_owned(),
                    "qa.runner.cleanup_verified".to_owned(),
                ],
                runtime_path: test_runtime_path_evidence(
                    "palyrad-test",
                    "runtime-contracts.test",
                    "qa-runner.test",
                    "fixture",
                ),
                run_id: Some("run-opaque".to_owned()),
                session_id: Some("session-opaque".to_owned()),
                terminal_state: Some("completed".to_owned()),
                evidence_artifacts: vec![execution_artifact("evidence")],
                evidence_output_bindings: Vec::new(),
                cleanup: QaScenarioCleanupResult {
                    run_terminal_observed: true,
                    session_cleaned: true,
                    daemon_terminated: true,
                    workspace_removed: true,
                    verified: true,
                    reason_codes: vec!["qa.runner.cleanup_verified".to_owned()],
                },
            },
            result_artifact: execution_artifact("execution_result"),
        };

        let report = execution_scenario_report(Path::new("scenario.yaml"), &manifest, execution);
        let encoded = serde_json::to_string(&report).expect("scenario report should serialize");

        assert_eq!(report.status, QaPackScenarioStatus::Fail);
        assert_eq!(report.issue_codes, ["final_answer_mismatch"]);
        assert_eq!(report.reason.as_deref(), Some("final_answer_mismatch"));
        assert!(encoded.contains("run-opaque"));
        assert!(!encoded.contains("transcript"));
        assert!(!encoded.contains("tape_events"));
    }

    #[test]
    fn runner_errors_expose_only_stable_reason_codes() {
        let error = anyhow::anyhow!(
            "qa.runner.stream_failed: raw daemon log with local paths and provider output"
        );

        assert_eq!(runner_error_reason_code(&error), "qa.runner.stream_failed");
        assert!(!runner_error_reason_code(&error).contains("raw daemon log"));
        assert_eq!(
            runner_error_reason_code(&anyhow::anyhow!("qa.runner.run_timeout")),
            "qa.runner.run_timeout"
        );
        assert_eq!(
            runner_error_reason_code(&anyhow::anyhow!(
                "qa.resume.attempt_completion_invalid: untrusted descriptor details"
            )),
            "qa.resume.attempt_completion_invalid"
        );
    }

    #[test]
    fn qa_report_write_atomically_replaces_existing_content() {
        let root = tempfile::tempdir().expect("report root should be available");
        let report_path = root.path().join("gate.json");

        write_qa_report(report_path.as_path(), b"first")
            .expect("initial report should be published");
        write_qa_report(report_path.as_path(), b"second")
            .expect("replacement report should be published");

        assert_eq!(
            fs::read(report_path.as_path()).expect("report should remain readable"),
            b"second"
        );
        assert!(fs::read_dir(root.path()).expect("report directory should be readable").all(
            |entry| !entry
                .expect("report entry should be readable")
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")
        ));
    }

    #[test]
    fn fixture_execution_failure_preserves_manifest_qualification_metadata() {
        let manifest = mcp_manifest();

        let report = manifest_gate_failure(
            Path::new("qa/scenarios/mcp/runtime.yaml"),
            &manifest,
            "qa.runner.run_timeout",
        );

        assert_eq!(report.id, manifest.id);
        assert_eq!(report.area, manifest.area.as_str());
        assert_eq!(report.runner_mode.as_deref(), Some(manifest.mode.runner.as_str()));
        assert_eq!(report.labels, manifest.maturity.labels);
        assert!(!report.sandbox_cleanup_verified);
    }

    #[test]
    fn mcp_fixture_failure_reports_server_unavailable() {
        let manifest = mcp_manifest();

        let report = pack_manifest_report(
            Path::new("qa/scenarios/mcp/mcp_server_import_read_tool.yaml"),
            &manifest,
            QaPackScenarioStatus::Fail,
            "failed",
            vec!["missing_fixture:qa/fixtures/mcp/stdio_server.json".to_owned()],
            Some("required QA fixture validation failed".to_owned()),
        );

        assert_eq!(report.failure_class.as_deref(), Some("server_unavailable"));
    }

    #[test]
    fn mcp_evidence_failure_reports_runtime_regression() {
        let manifest = mcp_manifest();

        let report = pack_manifest_report(
            Path::new("qa/scenarios/mcp/mcp_server_import_read_tool.yaml"),
            &manifest,
            QaPackScenarioStatus::Fail,
            "failed",
            vec!["missing_event:mcp.tool_import.snapshot".to_owned()],
            None,
        );

        assert_eq!(report.failure_class.as_deref(), Some("runtime_regression"));
    }

    fn execution_artifact(kind: &str) -> QaExecutionArtifactRef {
        QaExecutionArtifactRef {
            path: format!("target/qa-lab/{kind}.json"),
            kind: kind.to_owned(),
            sha256: "0".repeat(64),
            size_bytes: 128,
        }
    }

    fn test_execution_key() -> QaScenarioExecutionKey {
        QaScenarioExecutionKey {
            schema_version: 1,
            format: "palyra-qa-scenario-execution-key".to_owned(),
            digest: "2".repeat(64),
            normalized_manifest_sha256: "3".repeat(64),
            fixture_set_sha256: "4".repeat(64),
            runtime_version: "palyrad-test".to_owned(),
            runtime_contract_version: "runtime-contracts.test".to_owned(),
            runner_version: "qa-runner.test".to_owned(),
            provider_lane: "fixture".to_owned(),
            provider_binding_sha256: "5".repeat(64),
        }
    }

    fn qa_suite_config() -> QaSuiteConfig {
        QaSuiteConfig {
            schema_version: 1,
            id: "unit".to_owned(),
            mode: "pr".to_owned(),
            scenario_roots: vec!["qa/scenarios".to_owned()],
            expected_scenario_ids: Vec::new(),
            include_tags: Vec::new(),
            exclude_tags: Vec::new(),
            allow_provider_modes: vec!["mock".to_owned()],
            allow_runner_modes: Vec::new(),
            allow_live_providers: false,
            resume: None,
            require_p0_green: false,
            available_capabilities: Vec::new(),
            capability_skips: Vec::new(),
            flaky_policy: QaFlakyPolicyConfig::default(),
            scorecard: QaScorecardConfig {
                baseline_score_bps: None,
                fail_on_required_blockers: false,
                categories: vec![QaScorecardCategoryConfig {
                    id: "unit".to_owned(),
                    label: "Unit".to_owned(),
                    labels: Vec::new(),
                    areas: vec!["tools".to_owned()],
                    required: false,
                    baseline_score_bps: None,
                }],
            },
        }
    }

    fn mcp_manifest() -> QaScenarioManifest {
        parse_qa_scenario_manifest_yaml(
            r#"
schema_version: 1
id: mcp.test
area: tools
mode:
  provider: mock
  deterministic: true
requires:
  model: text
  capabilities:
    - agent_run
    - qa_lab
    - mcp
  tools:
    - mcp.docs.search
  fixtures:
    - qa/fixtures/mcp/stdio_server.json
steps:
  - id: prompt
    action: user_prompt
    prompt: "Import the MCP read tool."
expect:
  terminal_state: completed
  final_answer:
    contains:
      - "mcp imported"
  events:
    - event_type: mcp.tool_import.snapshot
      min_count: 1
  tool_calls:
    - name: mcp.docs.search
      min_count: 1
forbidden:
  tool_calls: []
  events: []
  artifacts: []
  claims: []
artifacts: []
maturity:
  labels:
    - p0
    - release_smoke
    - mcp
timeout:
  run_ms: 30000
"#,
        )
        .expect("fixture manifest should parse")
    }
}
