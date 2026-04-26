//! Replay/eval contract used by release gates.

mod catalog;
mod evaluator;
mod schema;

pub use catalog::{
    required_release_eval_dimensions, required_release_eval_protocol_inventory,
    RELEASE_STRICT_SAFETY_SCORE_BPS, REQUIRED_RELEASE_SUITES,
};
pub use evaluator::{
    ensure_release_eval_report_passed, evaluate_release_eval_manifest, parse_release_eval_manifest,
    release_eval_issue_counts_by_code,
};
pub use schema::*;

/// Schema version for the release eval manifest and reports.
pub const RELEASE_EVAL_SCHEMA_VERSION: u32 = 1;

/// Contract version for the release-gate eval surface.
pub const RELEASE_EVAL_CONTRACT_VERSION: &str = "release-eval-v1";
