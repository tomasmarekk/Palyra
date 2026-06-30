//! Operator-facing projections derived from release-eval reports.
//!
//! These builders intentionally consume the existing release-eval report and
//! generated replay bundle metadata instead of introducing another runtime
//! store. The resulting JSON artifacts are safe for CI and support bundles:
//! they carry counters, digests, reason codes, and event names, not raw tape
//! payloads or model output.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::schema::*;
use super::{RELEASE_EVAL_CONTRACT_VERSION, RELEASE_STRICT_SAFETY_SCORE_BPS};

/// Audit event emitted before a release-eval maturity scorecard is projected.
pub const RELEASE_EVAL_MATURITY_STARTED_EVENT_TYPE: &str =
    "release_eval.maturity_scorecard.started";
/// Audit event emitted after a release-eval maturity scorecard is projected.
pub const RELEASE_EVAL_MATURITY_COMPLETED_EVENT_TYPE: &str =
    "release_eval.maturity_scorecard.completed";
/// Audit event emitted when scorecard projection fails before a report exists.
pub const RELEASE_EVAL_MATURITY_FAILED_EVENT_TYPE: &str = "release_eval.maturity_scorecard.failed";

/// Audit event emitted before a digest-only trajectory export is projected.
pub const PALYRA_TRAJECTORY_EXPORT_STARTED_EVENT_TYPE: &str = "palyra.trajectory_export.started";
/// Audit event emitted after a digest-only trajectory export is projected.
pub const PALYRA_TRAJECTORY_EXPORT_COMPLETED_EVENT_TYPE: &str =
    "palyra.trajectory_export.completed";
/// Audit event emitted when trajectory export projection cannot be produced.
pub const PALYRA_TRAJECTORY_EXPORT_FAILED_EVENT_TYPE: &str = "palyra.trajectory_export.failed";

/// Audit event emitted before regression eval packs are projected.
pub const REGRESSION_EVAL_PACKS_STARTED_EVENT_TYPE: &str = "release_eval.regression_packs.started";
/// Audit event emitted after regression eval packs are projected.
pub const REGRESSION_EVAL_PACKS_COMPLETED_EVENT_TYPE: &str =
    "release_eval.regression_packs.completed";
/// Audit event emitted when regression pack projection cannot be produced.
pub const REGRESSION_EVAL_PACKS_FAILED_EVENT_TYPE: &str = "release_eval.regression_packs.failed";

const PROJECTION_SCHEMA_VERSION: u32 = 1;
const REDACTION_LEVEL_DIGEST_ONLY: &str = "digest_only_metadata";

/// Final operator decision derived from a release-eval report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalMaturityDecision {
    Promote,
    ManualReview,
    Block,
}

/// Coarse maturity level suitable for dashboards and release notes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalMaturityLevel {
    Mature,
    Candidate,
    Experimental,
    Blocked,
}

/// Stable reason code explaining a scorecard decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalMaturityReasonCode {
    AllGatesPassed,
    GateFailed,
    WarningIssuesPresent,
    ManualReviewRequired,
    CoverageIncomplete,
    SafetyScoreBelowThreshold,
    ReplayBundlesMissing,
    CompatibilityIncomplete,
}

/// Scorecard dimension derived from the release-eval report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReleaseEvalMaturityCategory {
    GateVerdict,
    SuiteCoverage,
    CaseCoverage,
    SafetyScore,
    ReplayDeterminism,
    ProtocolCompatibility,
}

/// One category score in basis points.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseEvalMaturityCategoryScore {
    pub category: ReleaseEvalMaturityCategory,
    pub score_bps: u32,
    pub verdict: ReleaseGateVerdict,
    pub evidence_refs: Vec<String>,
}

/// Release maturity scorecard projected from a release-eval report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReleaseEvalMaturityScorecard {
    pub schema_version: u32,
    pub contract_version: String,
    pub generated_at_unix_ms: i64,
    pub decision: ReleaseEvalMaturityDecision,
    pub level: ReleaseEvalMaturityLevel,
    pub overall_score_bps: u32,
    pub reason_codes: Vec<ReleaseEvalMaturityReasonCode>,
    pub categories: Vec<ReleaseEvalMaturityCategoryScore>,
    pub source_event_refs: Vec<String>,
    pub redaction_level: String,
    pub started_event_type: String,
    pub completed_event_type: String,
    pub failed_event_type: String,
}

/// Build a release maturity scorecard from a completed release-eval report.
#[must_use]
pub fn build_release_eval_maturity_scorecard(
    report: &ReleaseEvalReport,
    generated_at_unix_ms: i64,
) -> ReleaseEvalMaturityScorecard {
    let categories = release_eval_maturity_categories(report);
    let overall_score_bps = categories.iter().map(|category| category.score_bps).min().unwrap_or(0);
    let reason_codes = release_eval_maturity_reason_codes(report);
    let decision = release_eval_maturity_decision(reason_codes.as_slice());
    let level = release_eval_maturity_level(decision, overall_score_bps);

    ReleaseEvalMaturityScorecard {
        schema_version: PROJECTION_SCHEMA_VERSION,
        contract_version: RELEASE_EVAL_CONTRACT_VERSION.to_owned(),
        generated_at_unix_ms,
        decision,
        level,
        overall_score_bps,
        reason_codes,
        categories,
        source_event_refs: vec![
            "release_eval.report".to_owned(),
            format!("protocol_inventory:{}", report.protocol_inventory.inventory_version),
        ],
        redaction_level: REDACTION_LEVEL_DIGEST_ONLY.to_owned(),
        started_event_type: RELEASE_EVAL_MATURITY_STARTED_EVENT_TYPE.to_owned(),
        completed_event_type: RELEASE_EVAL_MATURITY_COMPLETED_EVENT_TYPE.to_owned(),
        failed_event_type: RELEASE_EVAL_MATURITY_FAILED_EVENT_TYPE.to_owned(),
    }
}

fn release_eval_maturity_categories(
    report: &ReleaseEvalReport,
) -> Vec<ReleaseEvalMaturityCategoryScore> {
    let compatible_protocols = report
        .compatibility_matrix
        .entries
        .iter()
        .filter(|entry| entry.status == ReleaseGateVerdict::Pass)
        .count();
    vec![
        ReleaseEvalMaturityCategoryScore {
            category: ReleaseEvalMaturityCategory::GateVerdict,
            score_bps: release_gate_verdict_score_bps(report.gate_verdict),
            verdict: report.gate_verdict,
            evidence_refs: vec!["release_eval.report.gate_verdict".to_owned()],
        },
        ReleaseEvalMaturityCategoryScore {
            category: ReleaseEvalMaturityCategory::SuiteCoverage,
            score_bps: ratio_bps(report.summary.suites_passed, report.summary.suites_total),
            verdict: pass_if(report.summary.suites_passed == report.summary.suites_total),
            evidence_refs: vec!["release_eval.report.summary.suites".to_owned()],
        },
        ReleaseEvalMaturityCategoryScore {
            category: ReleaseEvalMaturityCategory::CaseCoverage,
            score_bps: ratio_bps(report.summary.cases_passed, report.summary.cases_total),
            verdict: pass_if(report.summary.cases_passed == report.summary.cases_total),
            evidence_refs: vec!["release_eval.report.summary.cases".to_owned()],
        },
        ReleaseEvalMaturityCategoryScore {
            category: ReleaseEvalMaturityCategory::SafetyScore,
            score_bps: report.summary.lowest_safety_score_bps.min(10_000),
            verdict: pass_if(
                report.summary.lowest_safety_score_bps >= RELEASE_STRICT_SAFETY_SCORE_BPS,
            ),
            evidence_refs: vec!["release_eval.report.summary.lowest_safety_score_bps".to_owned()],
        },
        ReleaseEvalMaturityCategoryScore {
            category: ReleaseEvalMaturityCategory::ReplayDeterminism,
            score_bps: ratio_bps(
                report.summary.generated_replay_bundles,
                report.summary.cases_total,
            ),
            verdict: pass_if(report.summary.generated_replay_bundles == report.summary.cases_total),
            evidence_refs: vec!["release_eval.report.summary.generated_replay_bundles".to_owned()],
        },
        ReleaseEvalMaturityCategoryScore {
            category: ReleaseEvalMaturityCategory::ProtocolCompatibility,
            score_bps: ratio_bps(compatible_protocols, report.compatibility_matrix.entries.len()),
            verdict: pass_if(compatible_protocols == report.compatibility_matrix.entries.len()),
            evidence_refs: vec!["release_eval.report.compatibility_matrix".to_owned()],
        },
    ]
}

fn release_eval_maturity_reason_codes(
    report: &ReleaseEvalReport,
) -> Vec<ReleaseEvalMaturityReasonCode> {
    let mut reasons = BTreeSet::new();
    match report.gate_verdict {
        ReleaseGateVerdict::Pass => {}
        ReleaseGateVerdict::Warn => {
            reasons.insert(ReleaseEvalMaturityReasonCode::WarningIssuesPresent);
        }
        ReleaseGateVerdict::ManualReview => {
            reasons.insert(ReleaseEvalMaturityReasonCode::ManualReviewRequired);
        }
        ReleaseGateVerdict::Fail => {
            reasons.insert(ReleaseEvalMaturityReasonCode::GateFailed);
        }
    }
    if report.status == ReleaseEvalStatus::Failed
        || report.summary.suites_failed > 0
        || report.summary.cases_failed > 0
    {
        reasons.insert(ReleaseEvalMaturityReasonCode::CoverageIncomplete);
    }
    if report.summary.lowest_safety_score_bps < RELEASE_STRICT_SAFETY_SCORE_BPS {
        reasons.insert(ReleaseEvalMaturityReasonCode::SafetyScoreBelowThreshold);
    }
    if report.summary.generated_replay_bundles != report.summary.cases_total {
        reasons.insert(ReleaseEvalMaturityReasonCode::ReplayBundlesMissing);
    }
    if report
        .compatibility_matrix
        .entries
        .iter()
        .any(|entry| entry.status != ReleaseGateVerdict::Pass)
    {
        reasons.insert(ReleaseEvalMaturityReasonCode::CompatibilityIncomplete);
    }

    if reasons.is_empty() {
        vec![ReleaseEvalMaturityReasonCode::AllGatesPassed]
    } else {
        reasons.into_iter().collect()
    }
}

fn release_eval_maturity_decision(
    reasons: &[ReleaseEvalMaturityReasonCode],
) -> ReleaseEvalMaturityDecision {
    if reasons.iter().any(|reason| {
        matches!(
            reason,
            ReleaseEvalMaturityReasonCode::GateFailed
                | ReleaseEvalMaturityReasonCode::CoverageIncomplete
                | ReleaseEvalMaturityReasonCode::SafetyScoreBelowThreshold
                | ReleaseEvalMaturityReasonCode::ReplayBundlesMissing
                | ReleaseEvalMaturityReasonCode::CompatibilityIncomplete
        )
    }) {
        return ReleaseEvalMaturityDecision::Block;
    }
    if reasons.iter().any(|reason| {
        matches!(
            reason,
            ReleaseEvalMaturityReasonCode::ManualReviewRequired
                | ReleaseEvalMaturityReasonCode::WarningIssuesPresent
        )
    }) {
        return ReleaseEvalMaturityDecision::ManualReview;
    }
    ReleaseEvalMaturityDecision::Promote
}

fn release_eval_maturity_level(
    decision: ReleaseEvalMaturityDecision,
    overall_score_bps: u32,
) -> ReleaseEvalMaturityLevel {
    match decision {
        ReleaseEvalMaturityDecision::Block => ReleaseEvalMaturityLevel::Blocked,
        ReleaseEvalMaturityDecision::Promote if overall_score_bps == 10_000 => {
            ReleaseEvalMaturityLevel::Mature
        }
        ReleaseEvalMaturityDecision::Promote | ReleaseEvalMaturityDecision::ManualReview
            if overall_score_bps >= 8_500 =>
        {
            ReleaseEvalMaturityLevel::Candidate
        }
        ReleaseEvalMaturityDecision::Promote | ReleaseEvalMaturityDecision::ManualReview => {
            ReleaseEvalMaturityLevel::Experimental
        }
    }
}

fn release_gate_verdict_score_bps(verdict: ReleaseGateVerdict) -> u32 {
    match verdict {
        ReleaseGateVerdict::Pass => 10_000,
        ReleaseGateVerdict::ManualReview => 8_500,
        ReleaseGateVerdict::Warn => 7_000,
        ReleaseGateVerdict::Fail => 0,
    }
}

fn pass_if(condition: bool) -> ReleaseGateVerdict {
    if condition {
        ReleaseGateVerdict::Pass
    } else {
        ReleaseGateVerdict::Fail
    }
}

fn ratio_bps(numerator: usize, denominator: usize) -> u32 {
    if denominator == 0 {
        return 0;
    }
    ((numerator as u128 * 10_000) / denominator as u128) as u32
}

/// Digest-only trajectory export derived from generated replay bundles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalyraTrajectoryExport {
    pub schema_version: u32,
    pub contract_version: String,
    pub generated_at_unix_ms: i64,
    pub export_id: String,
    pub report_status: ReleaseEvalStatus,
    pub gate_verdict: ReleaseGateVerdict,
    pub summary: PalyraTrajectoryExportSummary,
    pub runs: Vec<PalyraTrajectoryRunExport>,
    pub source_event_refs: Vec<String>,
    pub redaction_level: String,
    pub started_event_type: String,
    pub completed_event_type: String,
    pub failed_event_type: String,
}

/// Aggregate counters for a digest-only trajectory export.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalyraTrajectoryExportSummary {
    pub runs_total: usize,
    pub tape_events_total: usize,
    pub tool_calls_total: usize,
    pub approvals_total: usize,
    pub http_exchanges_total: usize,
    pub artifact_refs_total: usize,
    pub redaction_warnings_total: usize,
}

/// One run trajectory entry with digests and counters only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PalyraTrajectoryRunExport {
    pub suite_kind: ReleaseEvalSuiteKind,
    pub case_id: String,
    pub bundle_id: String,
    pub bundle_sha256: Option<String>,
    pub run_id: String,
    pub session_id: Option<String>,
    pub origin_kind: String,
    pub state: String,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub tape_event_count: usize,
    pub tape_event_types: Vec<String>,
    pub tool_call_count: usize,
    pub approval_count: usize,
    pub http_exchange_count: usize,
    pub artifact_ref_count: usize,
    pub redaction_warning_count: usize,
    pub final_answer_sha256: Option<String>,
}

/// Build a digest-only trajectory export from release-eval output.
#[must_use]
pub fn build_palyra_trajectory_export(
    output: &ReleaseEvalOutput,
    generated_at_unix_ms: i64,
) -> PalyraTrajectoryExport {
    let runs = output
        .replay_bundles
        .iter()
        .map(|generated| {
            let bundle = &generated.bundle;
            PalyraTrajectoryRunExport {
                suite_kind: generated.suite_kind,
                case_id: generated.case_id.clone(),
                bundle_id: bundle.bundle_id.clone(),
                bundle_sha256: bundle.integrity.canonical_sha256.clone(),
                run_id: bundle.source.run_id.clone(),
                session_id: bundle.source.session_id.clone(),
                origin_kind: bundle.source.origin_kind.clone(),
                state: bundle.run.state.clone(),
                prompt_tokens: bundle.run.prompt_tokens,
                completion_tokens: bundle.run.completion_tokens,
                total_tokens: bundle.run.total_tokens,
                tape_event_count: bundle.expected.tape_event_count,
                tape_event_types: bundle.expected.tape_event_types.clone(),
                tool_call_count: bundle.tool_exchanges.len(),
                approval_count: bundle.approvals.len(),
                http_exchange_count: bundle.expected.http_exchange_count,
                artifact_ref_count: bundle.expected.artifact_ref_count,
                redaction_warning_count: bundle.redaction.warnings.len(),
                final_answer_sha256: bundle.expected.final_answer_sha256.clone(),
            }
        })
        .collect::<Vec<_>>();
    let summary = PalyraTrajectoryExportSummary {
        runs_total: runs.len(),
        tape_events_total: runs.iter().map(|run| run.tape_event_count).sum(),
        tool_calls_total: runs.iter().map(|run| run.tool_call_count).sum(),
        approvals_total: runs.iter().map(|run| run.approval_count).sum(),
        http_exchanges_total: runs.iter().map(|run| run.http_exchange_count).sum(),
        artifact_refs_total: runs.iter().map(|run| run.artifact_ref_count).sum(),
        redaction_warnings_total: runs.iter().map(|run| run.redaction_warning_count).sum(),
    };

    PalyraTrajectoryExport {
        schema_version: PROJECTION_SCHEMA_VERSION,
        contract_version: RELEASE_EVAL_CONTRACT_VERSION.to_owned(),
        generated_at_unix_ms,
        export_id: format!(
            "release-eval-trajectory:{}:{}",
            output.report.protocol_inventory.inventory_version, output.report.summary.cases_total
        ),
        report_status: output.report.status,
        gate_verdict: output.report.gate_verdict,
        summary,
        runs,
        source_event_refs: vec![
            "release_eval.generated_replay_bundles".to_owned(),
            format!("protocol_inventory:{}", output.report.protocol_inventory.inventory_version),
        ],
        redaction_level: REDACTION_LEVEL_DIGEST_ONLY.to_owned(),
        started_event_type: PALYRA_TRAJECTORY_EXPORT_STARTED_EVENT_TYPE.to_owned(),
        completed_event_type: PALYRA_TRAJECTORY_EXPORT_COMPLETED_EVENT_TYPE.to_owned(),
        failed_event_type: PALYRA_TRAJECTORY_EXPORT_FAILED_EVENT_TYPE.to_owned(),
    }
}

/// Stable reason code for one regression eval pack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RegressionEvalPackReasonCode {
    Ready,
    FailedCasesPresent,
    MissingReplayBundle,
    NonReleaseGate,
    MissingDimensions,
}

/// Index of regression packs derived from release-eval suites.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegressionEvalPackIndex {
    pub schema_version: u32,
    pub contract_version: String,
    pub generated_at_unix_ms: i64,
    pub summary: RegressionEvalPackSummary,
    pub packs: Vec<RegressionEvalPack>,
    pub source_event_refs: Vec<String>,
    pub redaction_level: String,
    pub started_event_type: String,
    pub completed_event_type: String,
    pub failed_event_type: String,
}

/// Aggregate counters for regression packs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegressionEvalPackSummary {
    pub packs_total: usize,
    pub packs_ready: usize,
    pub packs_blocked: usize,
    pub release_gate_packs: usize,
    pub replay_bundle_refs: usize,
}

/// One suite-backed regression pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegressionEvalPack {
    pub pack_id: String,
    pub suite_kind: ReleaseEvalSuiteKind,
    pub gate_verdict: ReleaseGateVerdict,
    pub release_gate: bool,
    pub case_ids: Vec<String>,
    pub missing_dimensions: Vec<ReleaseEvalDimension>,
    pub replay_bundles: Vec<RegressionEvalPackReplayBundleRef>,
    pub reason_codes: Vec<RegressionEvalPackReasonCode>,
}

/// Replay bundle reference used by a regression pack.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegressionEvalPackReplayBundleRef {
    pub case_id: String,
    pub bundle_id: String,
    pub bundle_sha256: Option<String>,
}

/// Build a suite-backed regression pack index from release-eval output.
#[must_use]
pub fn build_regression_eval_pack_index(
    output: &ReleaseEvalOutput,
    generated_at_unix_ms: i64,
) -> RegressionEvalPackIndex {
    let bundle_by_case = output
        .replay_bundles
        .iter()
        .map(|generated| (generated.case_id.as_str(), generated))
        .collect::<BTreeMap<_, _>>();
    let packs = output
        .report
        .suites
        .iter()
        .map(|suite| regression_pack_from_suite(suite, &bundle_by_case))
        .collect::<Vec<_>>();
    let packs_ready = packs
        .iter()
        .filter(|pack| pack.reason_codes == vec![RegressionEvalPackReasonCode::Ready])
        .count();
    let replay_bundle_refs = packs.iter().map(|pack| pack.replay_bundles.len()).sum();
    let release_gate_packs = packs.iter().filter(|pack| pack.release_gate).count();

    RegressionEvalPackIndex {
        schema_version: PROJECTION_SCHEMA_VERSION,
        contract_version: RELEASE_EVAL_CONTRACT_VERSION.to_owned(),
        generated_at_unix_ms,
        summary: RegressionEvalPackSummary {
            packs_total: packs.len(),
            packs_ready,
            packs_blocked: packs.len().saturating_sub(packs_ready),
            release_gate_packs,
            replay_bundle_refs,
        },
        packs,
        source_event_refs: vec![
            "release_eval.report.suites".to_owned(),
            "release_eval.generated_replay_bundles".to_owned(),
        ],
        redaction_level: REDACTION_LEVEL_DIGEST_ONLY.to_owned(),
        started_event_type: REGRESSION_EVAL_PACKS_STARTED_EVENT_TYPE.to_owned(),
        completed_event_type: REGRESSION_EVAL_PACKS_COMPLETED_EVENT_TYPE.to_owned(),
        failed_event_type: REGRESSION_EVAL_PACKS_FAILED_EVENT_TYPE.to_owned(),
    }
}

fn regression_pack_from_suite(
    suite: &ReleaseEvalSuiteReport,
    bundle_by_case: &BTreeMap<&str, &ReleaseGeneratedReplayBundle>,
) -> RegressionEvalPack {
    let case_ids = suite.cases.iter().map(|case| case.case_id.clone()).collect::<Vec<_>>();
    let replay_bundles = suite
        .cases
        .iter()
        .filter_map(|case| {
            let generated = bundle_by_case.get(case.case_id.as_str())?;
            Some(RegressionEvalPackReplayBundleRef {
                case_id: case.case_id.clone(),
                bundle_id: generated.bundle.bundle_id.clone(),
                bundle_sha256: generated.bundle.integrity.canonical_sha256.clone(),
            })
        })
        .collect::<Vec<_>>();
    let mut reason_codes = BTreeSet::new();
    if suite.status != ReleaseEvalStatus::Passed {
        reason_codes.insert(RegressionEvalPackReasonCode::FailedCasesPresent);
    }
    if !suite.release_gate {
        reason_codes.insert(RegressionEvalPackReasonCode::NonReleaseGate);
    }
    if !suite.missing_dimensions.is_empty() {
        reason_codes.insert(RegressionEvalPackReasonCode::MissingDimensions);
    }
    if replay_bundles.len() != suite.cases.len() {
        reason_codes.insert(RegressionEvalPackReasonCode::MissingReplayBundle);
    }
    let reason_codes = if reason_codes.is_empty() {
        vec![RegressionEvalPackReasonCode::Ready]
    } else {
        reason_codes.into_iter().collect()
    };

    RegressionEvalPack {
        pack_id: format!("release-eval:{}", suite.kind.as_str()),
        suite_kind: suite.kind,
        gate_verdict: if reason_codes == vec![RegressionEvalPackReasonCode::Ready] {
            ReleaseGateVerdict::Pass
        } else {
            ReleaseGateVerdict::Fail
        },
        release_gate: suite.release_gate,
        case_ids,
        missing_dimensions: suite.missing_dimensions.clone(),
        replay_bundles,
        reason_codes,
    }
}
