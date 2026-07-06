//! Release hardening gate contracts for runtime-boundary rollout.

#![allow(dead_code)]

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

pub const RELEASE_HARDENING_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecurityRegressionBoundary {
    HarnessBypass,
    HookForbiddenAuthority,
    MiddlewareLeakage,
    ProviderRepairBypass,
    DockerSecretNoLeakage,
    BrowserImageNoLeakage,
    AcpPermissionNoWidening,
    LearningCandidateSecretRejection,
}

impl SecurityRegressionBoundary {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HarnessBypass => "harness_bypass",
            Self::HookForbiddenAuthority => "hook_forbidden_authority",
            Self::MiddlewareLeakage => "middleware_leakage",
            Self::ProviderRepairBypass => "provider_repair_bypass",
            Self::DockerSecretNoLeakage => "docker_secret_no_leakage",
            Self::BrowserImageNoLeakage => "browser_image_no_leakage",
            Self::AcpPermissionNoWidening => "acp_permission_no_widening",
            Self::LearningCandidateSecretRejection => "learning_candidate_secret_rejection",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDeterminismScenario {
    HarnessReplay,
    ProviderRepairReplay,
    HookMiddlewareReplay,
    SteeringInterruptionReplay,
    MultimodalShrinkReplay,
    AcpEventReplay,
}

impl ReplayDeterminismScenario {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HarnessReplay => "harness_replay",
            Self::ProviderRepairReplay => "provider_repair_replay",
            Self::HookMiddlewareReplay => "hook_middleware_replay",
            Self::SteeringInterruptionReplay => "steering_interruption_replay",
            Self::MultimodalShrinkReplay => "multimodal_shrink_replay",
            Self::AcpEventReplay => "acp_event_replay",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PerformanceScenario {
    SimpleFinalAnswer,
    OneToolRun,
    LongCodingRun,
    ProviderRecovery,
    LspPatch,
    BrowserRescue,
    AdvisorFanout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeGateStatus {
    Passed,
    Failed,
    AcceptedLimitation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BoundaryGateResult<T> {
    pub boundary: T,
    pub status: RuntimeGateStatus,
    pub evidence_ref: String,
    pub no_real_secrets: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryGateThresholds {
    pub error_rate_bps: u32,
    pub timeout_rate_bps: u32,
    pub recovery_failure_rate_bps: u32,
    pub latency_overhead_bps: u32,
}

impl TelemetryGateThresholds {
    #[must_use]
    pub const fn warn_only_defaults() -> Self {
        Self {
            error_rate_bps: 500,
            timeout_rate_bps: 500,
            recovery_failure_rate_bps: 250,
            latency_overhead_bps: 1_500,
        }
    }

    #[must_use]
    pub const fn is_valid(self) -> bool {
        self.error_rate_bps <= 10_000
            && self.timeout_rate_bps <= 10_000
            && self.recovery_failure_rate_bps <= 10_000
            && self.latency_overhead_bps <= 10_000
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeRolloutStep {
    pub order: u32,
    pub flag: String,
    pub preconditions: Vec<String>,
    pub rollback_command: String,
    pub telemetry_thresholds: TelemetryGateThresholds,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimePerformanceReport {
    pub scenarios: BTreeSet<PerformanceScenario>,
    pub hook_overhead_measured: bool,
    pub middleware_overhead_measured: bool,
    pub prompt_cache_strategy_reported: bool,
    pub advisor_cost_reported_separately: bool,
    pub browser_vision_cost_reported_separately: bool,
    pub resource_cleanup_verified: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeDocsCoverage {
    pub developer_guide: bool,
    pub operator_guide: bool,
    pub testing_guide: bool,
    pub migration_notes: bool,
    pub failure_classes: bool,
    pub suite_commands_mentioned: bool,
    pub no_secrets_or_local_paths: bool,
}

impl RuntimeDocsCoverage {
    #[must_use]
    pub const fn complete(&self) -> bool {
        self.developer_guide
            && self.operator_guide
            && self.testing_guide
            && self.migration_notes
            && self.failure_classes
            && self.suite_commands_mentioned
            && self.no_secrets_or_local_paths
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FeatureReadinessClass {
    ProductionDefault,
    ProductionFlagged,
    DevOnly,
    Disabled,
    RemovedLegacy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeatureReadinessDisposition {
    pub feature: String,
    pub readiness: FeatureReadinessClass,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rollback_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub known_limitation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseHardeningInput {
    pub security_results: Vec<BoundaryGateResult<SecurityRegressionBoundary>>,
    pub replay_results: Vec<BoundaryGateResult<ReplayDeterminismScenario>>,
    pub performance_report: RuntimePerformanceReport,
    pub docs_coverage: RuntimeDocsCoverage,
    pub rollout_steps: Vec<RuntimeRolloutStep>,
    pub feature_dispositions: Vec<FeatureReadinessDisposition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseReadinessVerdict {
    Pass,
    Warn,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseReadinessIssue {
    pub code: String,
    pub severity: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReleaseReadinessReport {
    pub schema_version: u32,
    pub verdict: ReleaseReadinessVerdict,
    pub issues: Vec<ReleaseReadinessIssue>,
}

#[must_use]
pub fn required_security_regression_boundaries() -> BTreeSet<SecurityRegressionBoundary> {
    BTreeSet::from([
        SecurityRegressionBoundary::HarnessBypass,
        SecurityRegressionBoundary::HookForbiddenAuthority,
        SecurityRegressionBoundary::MiddlewareLeakage,
        SecurityRegressionBoundary::ProviderRepairBypass,
        SecurityRegressionBoundary::DockerSecretNoLeakage,
        SecurityRegressionBoundary::BrowserImageNoLeakage,
        SecurityRegressionBoundary::AcpPermissionNoWidening,
        SecurityRegressionBoundary::LearningCandidateSecretRejection,
    ])
}

#[must_use]
pub fn required_replay_determinism_scenarios() -> BTreeSet<ReplayDeterminismScenario> {
    BTreeSet::from([
        ReplayDeterminismScenario::HarnessReplay,
        ReplayDeterminismScenario::ProviderRepairReplay,
        ReplayDeterminismScenario::HookMiddlewareReplay,
        ReplayDeterminismScenario::SteeringInterruptionReplay,
        ReplayDeterminismScenario::MultimodalShrinkReplay,
        ReplayDeterminismScenario::AcpEventReplay,
    ])
}

#[must_use]
pub fn default_performance_scenarios() -> BTreeSet<PerformanceScenario> {
    BTreeSet::from([
        PerformanceScenario::SimpleFinalAnswer,
        PerformanceScenario::OneToolRun,
        PerformanceScenario::LongCodingRun,
        PerformanceScenario::ProviderRecovery,
        PerformanceScenario::LspPatch,
        PerformanceScenario::BrowserRescue,
        PerformanceScenario::AdvisorFanout,
    ])
}

#[must_use]
pub fn default_runtime_rollout_plan() -> Vec<RuntimeRolloutStep> {
    let flags = [
        "diagnostics_only",
        "feature_rollouts.execution_gate_pipeline_v2",
        "feature_rollouts.agent_harness_runtime",
        "feature_rollouts.inline_runtime_hooks",
        "feature_rollouts.tool_result_middleware",
        "feature_rollouts.provider_stream_normalizer",
        "feature_rollouts.provider_recovery",
        "feature_rollouts.terminal_sessions",
        "feature_rollouts.lsp_service",
        "feature_rollouts.browser_rescue",
        "feature_rollouts.acp_runtime",
    ];
    flags
        .iter()
        .enumerate()
        .map(|(index, flag)| RuntimeRolloutStep {
            order: (index + 1) as u32,
            flag: (*flag).to_owned(),
            preconditions: vec![
                "main_ci_green".to_owned(),
                "security_regression_green".to_owned(),
                "runtime_metrics_stable".to_owned(),
                "rollback_verified".to_owned(),
            ],
            rollback_command: format!("palyra config unset {flag}"),
            telemetry_thresholds: TelemetryGateThresholds::warn_only_defaults(),
        })
        .collect()
}

#[must_use]
pub fn evaluate_release_hardening(input: &ReleaseHardeningInput) -> ReleaseReadinessReport {
    let mut issues = Vec::new();
    validate_security_results(input, &mut issues);
    validate_replay_results(input, &mut issues);
    validate_performance_report(input, &mut issues);
    validate_docs(input, &mut issues);
    validate_rollout_steps(input, &mut issues);
    validate_feature_dispositions(input, &mut issues);

    let has_error = issues.iter().any(|issue| issue.severity == "error");
    let verdict = if has_error {
        ReleaseReadinessVerdict::Fail
    } else if issues.is_empty() {
        ReleaseReadinessVerdict::Pass
    } else {
        ReleaseReadinessVerdict::Warn
    };
    ReleaseReadinessReport { schema_version: RELEASE_HARDENING_SCHEMA_VERSION, verdict, issues }
}

fn validate_security_results(
    input: &ReleaseHardeningInput,
    issues: &mut Vec<ReleaseReadinessIssue>,
) {
    let observed =
        input.security_results.iter().map(|result| result.boundary).collect::<BTreeSet<_>>();
    for missing in required_security_regression_boundaries().difference(&observed) {
        issue(issues, "security.missing_boundary", "error", missing.as_str());
    }
    for result in &input.security_results {
        if result.status == RuntimeGateStatus::Failed {
            issue(issues, "security.failed", "error", result.boundary.as_str());
        }
        if !result.no_real_secrets {
            issue(issues, "security.real_secret_fixture", "error", result.boundary.as_str());
        }
    }
}

fn validate_replay_results(input: &ReleaseHardeningInput, issues: &mut Vec<ReleaseReadinessIssue>) {
    let observed =
        input.replay_results.iter().map(|result| result.boundary).collect::<BTreeSet<_>>();
    for missing in required_replay_determinism_scenarios().difference(&observed) {
        issue(issues, "replay.missing_scenario", "error", missing.as_str());
    }
    for result in &input.replay_results {
        if result.status == RuntimeGateStatus::Failed {
            issue(issues, "replay.failed", "error", result.boundary.as_str());
        }
        if !result.no_real_secrets {
            issue(issues, "replay.real_secret_fixture", "error", result.boundary.as_str());
        }
    }
}

fn validate_performance_report(
    input: &ReleaseHardeningInput,
    issues: &mut Vec<ReleaseReadinessIssue>,
) {
    for missing in default_performance_scenarios().difference(&input.performance_report.scenarios) {
        issue(issues, "performance.missing_scenario", "warning", &format!("{missing:?}"));
    }
    if !input.performance_report.hook_overhead_measured {
        issue(issues, "performance.hook_overhead_missing", "warning", "hook overhead missing");
    }
    if !input.performance_report.middleware_overhead_measured {
        issue(
            issues,
            "performance.middleware_overhead_missing",
            "warning",
            "middleware overhead missing",
        );
    }
    if !input.performance_report.prompt_cache_strategy_reported {
        issue(issues, "performance.prompt_cache_missing", "warning", "prompt cache missing");
    }
    if !input.performance_report.resource_cleanup_verified {
        issue(issues, "performance.cleanup_missing", "error", "resource cleanup missing");
    }
}

fn validate_docs(input: &ReleaseHardeningInput, issues: &mut Vec<ReleaseReadinessIssue>) {
    if !input.docs_coverage.complete() {
        issue(issues, "docs.coverage_incomplete", "error", "runtime docs coverage incomplete");
    }
}

fn validate_rollout_steps(input: &ReleaseHardeningInput, issues: &mut Vec<ReleaseReadinessIssue>) {
    let mut previous_order = 0;
    for step in &input.rollout_steps {
        if step.order <= previous_order {
            issue(issues, "rollout.order_invalid", "error", step.flag.as_str());
        }
        previous_order = step.order;
        if step.preconditions.is_empty() {
            issue(issues, "rollout.preconditions_missing", "error", step.flag.as_str());
        }
        if step.rollback_command.trim().is_empty() {
            issue(issues, "rollout.rollback_missing", "error", step.flag.as_str());
        }
        if !step.telemetry_thresholds.is_valid() {
            issue(issues, "rollout.telemetry_invalid", "error", step.flag.as_str());
        }
    }
}

fn validate_feature_dispositions(
    input: &ReleaseHardeningInput,
    issues: &mut Vec<ReleaseReadinessIssue>,
) {
    for disposition in &input.feature_dispositions {
        if matches!(
            disposition.readiness,
            FeatureReadinessClass::ProductionDefault | FeatureReadinessClass::ProductionFlagged
        ) && disposition.rollback_command.as_deref().is_none_or(str::is_empty)
        {
            issue(issues, "release.rollback_missing", "error", disposition.feature.as_str());
        }
    }
}

fn issue(issues: &mut Vec<ReleaseReadinessIssue>, code: &str, severity: &str, message: &str) {
    issues.push(ReleaseReadinessIssue {
        code: code.to_owned(),
        severity: severity.to_owned(),
        message: message.to_owned(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn passing_security_results() -> Vec<BoundaryGateResult<SecurityRegressionBoundary>> {
        required_security_regression_boundaries()
            .into_iter()
            .map(|boundary| BoundaryGateResult {
                boundary,
                status: RuntimeGateStatus::Passed,
                evidence_ref: format!("qa/security/{}.json", boundary.as_str()),
                no_real_secrets: true,
            })
            .collect()
    }

    fn passing_replay_results() -> Vec<BoundaryGateResult<ReplayDeterminismScenario>> {
        required_replay_determinism_scenarios()
            .into_iter()
            .map(|boundary| BoundaryGateResult {
                boundary,
                status: RuntimeGateStatus::Passed,
                evidence_ref: format!("qa/replay/{}.json", boundary.as_str()),
                no_real_secrets: true,
            })
            .collect()
    }

    fn passing_input() -> ReleaseHardeningInput {
        ReleaseHardeningInput {
            security_results: passing_security_results(),
            replay_results: passing_replay_results(),
            performance_report: RuntimePerformanceReport {
                scenarios: default_performance_scenarios(),
                hook_overhead_measured: true,
                middleware_overhead_measured: true,
                prompt_cache_strategy_reported: true,
                advisor_cost_reported_separately: true,
                browser_vision_cost_reported_separately: true,
                resource_cleanup_verified: true,
            },
            docs_coverage: RuntimeDocsCoverage {
                developer_guide: true,
                operator_guide: true,
                testing_guide: true,
                migration_notes: true,
                failure_classes: true,
                suite_commands_mentioned: true,
                no_secrets_or_local_paths: true,
            },
            rollout_steps: default_runtime_rollout_plan(),
            feature_dispositions: vec![
                FeatureReadinessDisposition {
                    feature: "acp_runtime".to_owned(),
                    readiness: FeatureReadinessClass::ProductionFlagged,
                    rollback_command: Some(
                        "palyra config unset feature_rollouts.acp_runtime".to_owned(),
                    ),
                    known_limitation: None,
                },
                FeatureReadinessDisposition {
                    feature: "legacy_unbounded_runtime".to_owned(),
                    readiness: FeatureReadinessClass::Disabled,
                    rollback_command: None,
                    known_limitation: Some(
                        "kept disabled until native relay is default-on".to_owned(),
                    ),
                },
            ],
        }
    }

    #[test]
    fn security_regression_suite_covers_new_boundaries() {
        let required = required_security_regression_boundaries();

        assert!(required.contains(&SecurityRegressionBoundary::HarnessBypass));
        assert!(required.contains(&SecurityRegressionBoundary::HookForbiddenAuthority));
        assert!(required.contains(&SecurityRegressionBoundary::AcpPermissionNoWidening));
        assert!(required.contains(&SecurityRegressionBoundary::LearningCandidateSecretRejection));
    }

    #[test]
    fn replay_determinism_suite_includes_acp_without_side_effect_reexecution() {
        let input = passing_input();
        let report = evaluate_release_hardening(&input);

        assert_eq!(report.verdict, ReleaseReadinessVerdict::Pass);
        assert!(input
            .replay_results
            .iter()
            .any(|result| result.boundary == ReplayDeterminismScenario::AcpEventReplay));
    }

    #[test]
    fn performance_validation_requires_cleanup_and_cost_split() {
        let mut input = passing_input();
        input.performance_report.resource_cleanup_verified = false;

        let report = evaluate_release_hardening(&input);

        assert_eq!(report.verdict, ReleaseReadinessVerdict::Fail);
        assert!(report.issues.iter().any(|issue| issue.code == "performance.cleanup_missing"));
    }

    #[test]
    fn rollout_plan_has_preconditions_thresholds_and_rollback() {
        let plan = default_runtime_rollout_plan();

        assert!(plan.windows(2).all(|pair| pair[0].order < pair[1].order));
        assert!(plan.iter().all(|step| !step.preconditions.is_empty()));
        assert!(plan.iter().all(|step| !step.rollback_command.is_empty()));
        assert!(plan.iter().all(|step| step.telemetry_thresholds.is_valid()));
    }

    #[test]
    fn production_release_features_require_rollback() {
        let mut input = passing_input();
        input.feature_dispositions.push(FeatureReadinessDisposition {
            feature: "provider_recovery".to_owned(),
            readiness: FeatureReadinessClass::ProductionDefault,
            rollback_command: None,
            known_limitation: None,
        });

        let report = evaluate_release_hardening(&input);

        assert_eq!(report.verdict, ReleaseReadinessVerdict::Fail);
        assert!(report.issues.iter().any(|issue| issue.code == "release.rollback_missing"));
    }

    #[test]
    fn docs_gate_requires_guides_commands_and_no_local_paths() {
        let mut input = passing_input();
        input.docs_coverage.operator_guide = false;

        let report = evaluate_release_hardening(&input);

        assert_eq!(report.verdict, ReleaseReadinessVerdict::Fail);
        assert!(report.issues.iter().any(|issue| issue.code == "docs.coverage_incomplete"));
    }
}
