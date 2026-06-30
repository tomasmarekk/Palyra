//! Replay/eval contract used by release gates.
//!
//! Re-exports the manifest schema, coverage catalog, and evaluator that
//! power `just release-eval-gate`; the version constants below gate
//! compatibility with the golden manifest fixture.

mod catalog;
mod evaluator;
mod projections;
mod schema;

pub use catalog::{
    required_release_eval_dimensions, required_release_eval_protocol_inventory,
    RELEASE_STRICT_SAFETY_SCORE_BPS, REQUIRED_RELEASE_SUITES,
};
pub use evaluator::{
    ensure_release_eval_report_passed, evaluate_release_eval_manifest, parse_release_eval_manifest,
    release_eval_issue_counts_by_code, release_eval_replay_bundle_filename,
};
pub use projections::{
    build_palyra_trajectory_export, build_regression_eval_pack_index,
    build_release_eval_maturity_scorecard, PalyraTrajectoryExport, PalyraTrajectoryExportSummary,
    PalyraTrajectoryRunExport, RegressionEvalPack, RegressionEvalPackIndex,
    RegressionEvalPackReasonCode, RegressionEvalPackReplayBundleRef, RegressionEvalPackSummary,
    ReleaseEvalMaturityCategory, ReleaseEvalMaturityCategoryScore, ReleaseEvalMaturityDecision,
    ReleaseEvalMaturityLevel, ReleaseEvalMaturityReasonCode, ReleaseEvalMaturityScorecard,
    PALYRA_TRAJECTORY_EXPORT_COMPLETED_EVENT_TYPE, PALYRA_TRAJECTORY_EXPORT_FAILED_EVENT_TYPE,
    PALYRA_TRAJECTORY_EXPORT_STARTED_EVENT_TYPE, REGRESSION_EVAL_PACKS_COMPLETED_EVENT_TYPE,
    REGRESSION_EVAL_PACKS_FAILED_EVENT_TYPE, REGRESSION_EVAL_PACKS_STARTED_EVENT_TYPE,
    RELEASE_EVAL_MATURITY_COMPLETED_EVENT_TYPE, RELEASE_EVAL_MATURITY_FAILED_EVENT_TYPE,
    RELEASE_EVAL_MATURITY_STARTED_EVENT_TYPE,
};
pub use schema::*;

/// Schema version for the release eval manifest and reports.
pub const RELEASE_EVAL_SCHEMA_VERSION: u32 = 1;

/// Contract version for the release-gate eval surface.
pub const RELEASE_EVAL_CONTRACT_VERSION: &str = "release-eval-v1";
