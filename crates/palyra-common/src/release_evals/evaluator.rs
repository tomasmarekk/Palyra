use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, Context, Result};
use serde_json::{json, Value};

use crate::{
    redaction::{is_sensitive_key, REDACTED},
    replay_bundle::{
        build_replay_bundle, replay_bundle_offline, ReplayBundle, ReplayBundleBuildInput,
        ReplayCaptureMetadata, ReplayRunSnapshot, ReplayRunStatus, ReplaySource,
    },
};

use super::{
    catalog::{
        required_release_eval_dimensions, required_release_eval_protocol_inventory,
        RELEASE_STRICT_SAFETY_SCORE_BPS, REQUIRED_RELEASE_SUITES,
    },
    schema::*,
    RELEASE_EVAL_CONTRACT_VERSION, RELEASE_EVAL_SCHEMA_VERSION,
};

/// Parse a release eval manifest from JSON bytes.
pub fn parse_release_eval_manifest(bytes: &[u8]) -> Result<ReleaseEvalManifest> {
    let manifest: ReleaseEvalManifest =
        serde_json::from_slice(bytes).context("failed to parse release eval manifest")?;
    Ok(manifest)
}

/// Evaluate a release eval manifest and generate replay bundles for every case.
#[must_use]
pub fn evaluate_release_eval_manifest(manifest: &ReleaseEvalManifest) -> ReleaseEvalOutput {
    let mut issues = validate_manifest_header_and_inventory(manifest);
    let mut replay_bundles = Vec::new();
    let mut suite_reports = Vec::with_capacity(manifest.suites.len());
    let suites_by_kind = manifest.suites.iter().map(|suite| suite.kind).collect::<Vec<_>>();

    for required in REQUIRED_RELEASE_SUITES {
        if !suites_by_kind.contains(&required) {
            issues.push(error_issue(
                "missing_suite",
                "$.suites",
                format!("required release eval suite '{}' is missing", required.as_str()),
                "Add the suite to the golden release eval manifest.",
            ));
        }
    }

    let mut seen = BTreeSet::new();
    for (suite_index, suite) in manifest.suites.iter().enumerate() {
        if !seen.insert(suite.kind) {
            issues.push(error_issue(
                "duplicate_suite",
                format!("$.suites[{suite_index}]",),
                format!("suite '{}' is declared more than once", suite.kind.as_str()),
                "Keep exactly one suite per release eval domain.",
            ));
        }
        let (suite_report, mut generated) = evaluate_suite(suite_index, suite);
        replay_bundles.append(&mut generated);
        suite_reports.push(suite_report);
    }

    let suites_passed =
        suite_reports.iter().filter(|suite| suite.status == ReleaseEvalStatus::Passed).count();
    let cases_total = suite_reports.iter().map(|suite| suite.cases.len()).sum();
    let cases_passed = suite_reports
        .iter()
        .flat_map(|suite| suite.cases.iter())
        .filter(|case| case.status == ReleaseEvalStatus::Passed)
        .count();
    let lowest_safety_score_bps = manifest
        .suites
        .iter()
        .flat_map(|suite| suite.cases.iter().map(|case| case.safety_score_bps))
        .min()
        .unwrap_or(0);
    let summary = ReleaseEvalSummary {
        suites_total: suite_reports.len(),
        suites_passed,
        suites_failed: suite_reports.len().saturating_sub(suites_passed),
        cases_total,
        cases_passed,
        cases_failed: cases_total.saturating_sub(cases_passed),
        release_gates: manifest.suites.iter().filter(|suite| suite.release_gate).count(),
        generated_replay_bundles: replay_bundles.len(),
        lowest_safety_score_bps,
    };
    let status = if issues.is_empty()
        && suite_reports.iter().all(|suite| suite.status == ReleaseEvalStatus::Passed)
    {
        ReleaseEvalStatus::Passed
    } else {
        ReleaseEvalStatus::Failed
    };

    ReleaseEvalOutput {
        report: ReleaseEvalReport {
            schema_version: RELEASE_EVAL_SCHEMA_VERSION,
            contract_version: RELEASE_EVAL_CONTRACT_VERSION.to_owned(),
            status,
            summary,
            protocol_inventory: manifest.inventory.clone(),
            issues,
            suites: suite_reports,
        },
        replay_bundles,
    }
}

/// Return an error when a release eval report failed.
pub fn ensure_release_eval_report_passed(report: &ReleaseEvalReport) -> Result<()> {
    if report.status == ReleaseEvalStatus::Passed {
        return Ok(());
    }
    let first_issue = report
        .issues
        .first()
        .or_else(|| report.suites.iter().find_map(|suite| suite.issues.first()))
        .or_else(|| {
            report
                .suites
                .iter()
                .flat_map(|suite| suite.cases.iter())
                .find_map(|case| case.issues.first())
        })
        .map(|issue| format!("{} at {}", issue.code, issue.path))
        .unwrap_or_else(|| "unknown release eval failure".to_owned());
    Err(anyhow!("release eval gate failed: {first_issue}"))
}

fn validate_manifest_header_and_inventory(manifest: &ReleaseEvalManifest) -> Vec<ReleaseEvalIssue> {
    let mut issues = Vec::new();
    if manifest.schema_version != RELEASE_EVAL_SCHEMA_VERSION {
        issues.push(error_issue(
            "unsupported_schema_version",
            "$.schema_version",
            format!("unsupported release eval schema version {}", manifest.schema_version),
            "Migrate the fixture and evaluator together before changing schema_version.",
        ));
    }
    if manifest.contract_version != RELEASE_EVAL_CONTRACT_VERSION {
        issues.push(error_issue(
            "unsupported_contract_version",
            "$.contract_version",
            "unsupported release eval contract version",
            "Update the evaluator contract or restore the golden manifest contract version.",
        ));
    }
    if manifest.inventory.schema_version != RELEASE_EVAL_SCHEMA_VERSION {
        issues.push(error_issue(
            "unsupported_inventory_schema_version",
            "$.inventory.schema_version",
            "golden protocol inventory has an unsupported schema version",
            "Keep the inventory schema aligned with the release eval schema.",
        ));
    }
    if !looks_like_iso_date(manifest.inventory.updated_on.as_str()) {
        issues.push(error_issue(
            "inventory_date_required",
            "$.inventory.updated_on",
            "golden protocol inventory must include an ISO calendar date",
            "Set updated_on to YYYY-MM-DD when protocol inventory changes.",
        ));
    }
    if manifest.inventory.change_reason.trim().is_empty() {
        issues.push(error_issue(
            "inventory_change_reason_required",
            "$.inventory.change_reason",
            "golden protocol inventory must explain why it changed",
            "Add a concise change reason to the inventory.",
        ));
    }

    let inventory_contracts = manifest
        .inventory
        .protocols
        .iter()
        .map(|entry| entry.contract.as_str())
        .collect::<BTreeSet<_>>();
    for &required in required_release_eval_protocol_inventory() {
        if !inventory_contracts.contains(required) {
            issues.push(error_issue(
                "missing_protocol_inventory_entry",
                "$.inventory.protocols",
                format!("golden protocol inventory is missing '{required}'"),
                "Add the contract with version, compatibility policy, fixture, and change reason.",
            ));
        }
    }
    for (index, entry) in manifest.inventory.protocols.iter().enumerate() {
        let path = format!("$.inventory.protocols[{index}]");
        for (field, value) in [
            ("domain", entry.domain.as_str()),
            ("contract", entry.contract.as_str()),
            ("version", entry.version.as_str()),
            ("compatibility_policy", entry.compatibility_policy.as_str()),
            ("golden_fixture", entry.golden_fixture.as_str()),
            ("change_reason", entry.change_reason.as_str()),
        ] {
            if value.trim().is_empty() {
                issues.push(error_issue(
                    "protocol_inventory_field_required",
                    format!("{path}.{field}"),
                    format!("protocol inventory field '{field}' must not be empty"),
                    "Fill every protocol inventory field so release diffs are auditable.",
                ));
            }
        }
    }
    issues
}

fn evaluate_suite(
    suite_index: usize,
    suite: &ReleaseEvalSuite,
) -> (ReleaseEvalSuiteReport, Vec<ReleaseGeneratedReplayBundle>) {
    let mut issues = Vec::new();
    let mut generated_bundles = Vec::new();
    let suite_path = format!("$.suites[{suite_index}]");

    if !suite.release_gate {
        issues.push(error_issue(
            "release_gate_required",
            format!("{suite_path}.release_gate"),
            "release eval suites must be release gates",
            "Set release_gate=true and keep the suite in the replay/eval CI path.",
        ));
    }
    if suite.minimum_safety_score_bps == 0
        || suite.minimum_safety_score_bps > RELEASE_STRICT_SAFETY_SCORE_BPS
    {
        issues.push(error_issue(
            "invalid_safety_threshold",
            format!("{suite_path}.minimum_safety_score_bps"),
            "suite safety threshold must be in 1..=10000 basis points",
            "Use a nonzero safety threshold and keep strict suites at 10000 bps.",
        ));
    }
    if suite.invariants.is_empty() {
        issues.push(error_issue(
            "invariants_required",
            format!("{suite_path}.invariants"),
            "golden fixture must list suite invariants",
            "Add invariant strings that explain the expected behavior pinned by the suite.",
        ));
    }
    if suite.cases.is_empty() {
        issues.push(error_issue(
            "cases_required",
            format!("{suite_path}.cases"),
            "suite must contain at least one eval case",
            "Add deterministic eval cases with assertions and replay fixtures.",
        ));
    }

    let covered_dimensions = suite
        .cases
        .iter()
        .flat_map(|case| case.dimensions.iter().copied())
        .collect::<BTreeSet<_>>();
    let missing_dimensions = required_release_eval_dimensions(suite.kind)
        .iter()
        .copied()
        .filter(|dimension| !covered_dimensions.contains(dimension))
        .collect::<Vec<_>>();
    for dimension in &missing_dimensions {
        issues.push(error_issue(
            "required_dimension_missing",
            format!("{suite_path}.cases"),
            format!("suite '{}' is missing dimension {dimension:?}", suite.kind.as_str()),
            "Add a deterministic eval case that covers the missing dimension.",
        ));
    }

    let mut case_reports = Vec::with_capacity(suite.cases.len());
    for (case_index, case) in suite.cases.iter().enumerate() {
        let (case_report, generated_bundle) = evaluate_case(suite, suite_index, case_index, case);
        if let Some(bundle) = generated_bundle {
            generated_bundles.push(bundle);
        }
        case_reports.push(case_report);
    }

    let status = if issues.is_empty()
        && case_reports.iter().all(|case| case.status == ReleaseEvalStatus::Passed)
    {
        ReleaseEvalStatus::Passed
    } else {
        ReleaseEvalStatus::Failed
    };
    (
        ReleaseEvalSuiteReport {
            kind: suite.kind,
            status,
            release_gate: suite.release_gate,
            missing_dimensions,
            issues,
            cases: case_reports,
        },
        generated_bundles,
    )
}

fn evaluate_case(
    suite: &ReleaseEvalSuite,
    suite_index: usize,
    case_index: usize,
    case: &ReleaseEvalCase,
) -> (ReleaseEvalCaseReport, Option<ReleaseGeneratedReplayBundle>) {
    let mut issues = Vec::new();
    let case_path = format!("$.suites[{suite_index}].cases[{case_index}]");

    if case.case_id.trim().is_empty() {
        issues.push(error_issue(
            "case_id_required",
            format!("{case_path}.case_id"),
            "case_id must not be empty",
            "Give each case a stable identifier for report and replay bundle paths.",
        ));
    }
    if !case.deterministic && case.flaky.is_none() {
        issues.push(error_issue(
            "unmarked_flaky_case",
            format!("{case_path}.deterministic"),
            "non-deterministic cases must be explicitly marked and trended",
            "Either make the case deterministic or add flaky reason and trend metric metadata.",
        ));
    }
    if case.safety_score_bps < suite.minimum_safety_score_bps {
        issues.push(error_issue(
            "safety_score_below_threshold",
            format!("{case_path}.safety_score_bps"),
            format!(
                "case safety score {} is below suite threshold {}",
                case.safety_score_bps, suite.minimum_safety_score_bps
            ),
            "Fix the regression or explicitly raise the failing condition before release.",
        ));
    }
    if case.assertions.is_empty() {
        issues.push(error_issue(
            "assertions_required",
            format!("{case_path}.assertions"),
            "case must contain assertions",
            "Add positive or negative assertions that describe the expected release-gate behavior.",
        ));
    }
    for (assertion_index, assertion) in case.assertions.iter().enumerate() {
        let assertion_path = format!("{case_path}.assertions[{assertion_index}]");
        if !assertion.passed {
            issues.push(error_issue(
                "assertion_failed",
                assertion_path.clone(),
                format!("assertion {:?} failed for target '{}'", assertion.kind, assertion.target),
                assertion.recovery_hint.clone(),
            ));
        }
        if assertion.evidence.trim().is_empty() {
            issues.push(error_issue(
                "assertion_evidence_required",
                format!("{assertion_path}.evidence"),
                "assertion evidence must not be empty",
                "Attach the deterministic fixture, suite name, or runtime contract that proves the assertion.",
            ));
        }
        if assertion.recovery_hint.trim().is_empty() {
            issues.push(error_issue(
                "assertion_recovery_hint_required",
                format!("{assertion_path}.recovery_hint"),
                "assertion recovery hint must not be empty",
                "Add an actionable recovery hint for release-gate triage.",
            ));
        }
    }

    let raw_replay_value = serde_json::to_value(&case.replay).unwrap_or(Value::Null);
    scan_value_for_unredacted_secrets(
        &raw_replay_value,
        format!("{case_path}.replay").as_str(),
        None,
        &mut issues,
    );

    let (bundle_metadata, generated_bundle, replay_status) =
        match build_release_eval_replay_bundle(case) {
            Ok(bundle) => {
                let report = replay_bundle_offline(&bundle);
                if report.status != ReplayRunStatus::Passed {
                    issues.push(error_issue(
                        "replay_bundle_failed",
                        format!("{case_path}.replay"),
                        "generated replay bundle failed offline validation",
                        "Inspect the replay diffs and restore deterministic expected outputs.",
                    ));
                    for diff in report.diffs.iter().take(3) {
                        issues.push(error_issue(
                            "replay_bundle_diff",
                            diff.path.clone(),
                            format!(
                                "replay diff in {}: expected {}, actual {}",
                                diff.category, diff.expected, diff.actual
                            ),
                            "Update the runtime behavior or golden fixture intentionally.",
                        ));
                    }
                }
                let bundle_id = bundle.bundle_id.clone();
                let sha = bundle.integrity.canonical_sha256.clone();
                (
                    (Some(bundle_id), sha),
                    Some(ReleaseGeneratedReplayBundle {
                        suite_kind: suite.kind,
                        case_id: case.case_id.clone(),
                        bundle,
                    }),
                    if report.status == ReplayRunStatus::Passed {
                        ReleaseEvalStatus::Passed
                    } else {
                        ReleaseEvalStatus::Failed
                    },
                )
            }
            Err(error) => {
                issues.push(error_issue(
                    "replay_bundle_build_failed",
                    format!("{case_path}.replay"),
                    format!("failed to build replay bundle: {error:#}"),
                    "Fix the fixture so it can be normalized into the shared replay contract.",
                ));
                ((None, None), None, ReleaseEvalStatus::Failed)
            }
        };

    let status = if issues.is_empty() && replay_status == ReleaseEvalStatus::Passed {
        ReleaseEvalStatus::Passed
    } else {
        ReleaseEvalStatus::Failed
    };
    (
        ReleaseEvalCaseReport {
            case_id: case.case_id.clone(),
            status,
            safety_score_bps: case.safety_score_bps,
            replay_bundle_id: bundle_metadata.0,
            replay_bundle_sha256: bundle_metadata.1,
            replay_status,
            issues,
        },
        generated_bundle,
    )
}

fn build_release_eval_replay_bundle(case: &ReleaseEvalCase) -> Result<ReplayBundle> {
    let total_tokens = case.replay.prompt_tokens.saturating_add(case.replay.completion_tokens);
    build_replay_bundle(ReplayBundleBuildInput {
        generated_at_unix_ms: 0,
        source: ReplaySource {
            product: "palyra".to_owned(),
            run_id: case.replay.run_id.clone(),
            session_id: Some(case.replay.session_id.clone()),
            origin_kind: case.replay.origin_kind.clone(),
            schema_policy: "release_eval_backwards_compatible".to_owned(),
        },
        capture: ReplayCaptureMetadata {
            captured_at_unix_ms: 0,
            capture_mode: "release_eval_fixture".to_owned(),
            max_events_per_run: 4_096,
            truncated: false,
            inline_sections: vec![
                "config_snapshot".to_owned(),
                "expected".to_owned(),
                "tape_events".to_owned(),
            ],
            referenced_sections: vec!["generated_replay_bundle".to_owned()],
            warnings: Vec::new(),
        },
        run: ReplayRunSnapshot {
            state: case.replay.state.clone(),
            principal: case.replay.principal.clone(),
            device_id: case.replay.device_id.clone(),
            channel: Some("release_eval".to_owned()),
            normalized_user_input: Some(json!({
                "case_id": case.case_id.as_str(),
                "title": case.title.as_str(),
            })),
            prompt_tokens: case.replay.prompt_tokens,
            completion_tokens: case.replay.completion_tokens,
            total_tokens,
            last_error: None,
            parent_run_id: None,
            origin_run_id: None,
            parameter_delta: None,
        },
        config_snapshot: case.replay.config_snapshot.clone(),
        tape_events: case.replay.tape_events.clone(),
        lifecycle_transitions: case.replay.lifecycle_transitions.clone(),
        idempotency_records: Vec::new(),
        artifact_refs: case.replay.artifact_refs.clone(),
    })
}

fn scan_value_for_unredacted_secrets(
    value: &Value,
    path: &str,
    key_context: Option<&str>,
    issues: &mut Vec<ReleaseEvalIssue>,
) {
    match value {
        Value::Object(object) => {
            for (key, entry) in object {
                let entry_path = format!("{path}.{key}");
                if is_release_eval_secret_key(key) && !entry_is_redacted_or_empty(entry) {
                    issues.push(error_issue(
                        "unredacted_secret_in_fixture",
                        entry_path.clone(),
                        "eval fixture contains an unredacted sensitive field",
                        "Redact the fixture input before it reaches replay generation.",
                    ));
                }
                scan_value_for_unredacted_secrets(entry, entry_path.as_str(), Some(key), issues);
            }
        }
        Value::Array(entries) => {
            for (index, entry) in entries.iter().enumerate() {
                scan_value_for_unredacted_secrets(
                    entry,
                    format!("{path}[{index}]").as_str(),
                    key_context,
                    issues,
                );
            }
        }
        Value::String(raw) => {
            if string_contains_unredacted_secret(raw, key_context) {
                issues.push(error_issue(
                    "unredacted_secret_in_fixture",
                    path,
                    "eval fixture string contains an unredacted credential pattern",
                    "Redact the fixture string or replace it with a non-secret marker.",
                ));
            }
        }
        _ => {}
    }
}

fn entry_is_redacted_or_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(raw) => raw.trim().is_empty() || raw == REDACTED,
        Value::Array(entries) => entries.iter().all(entry_is_redacted_or_empty),
        Value::Object(object) => object.values().all(entry_is_redacted_or_empty),
        _ => false,
    }
}

fn string_contains_unredacted_secret(raw: &str, key_context: Option<&str>) -> bool {
    if raw.contains(REDACTED) {
        return false;
    }
    if key_context.is_some_and(is_release_eval_secret_key) && !raw.trim().is_empty() {
        return true;
    }
    let lowered = raw.to_ascii_lowercase();
    lowered.contains("bearer ")
        || lowered.contains("access_token=")
        || lowered.contains("refresh_token=")
        || lowered.contains("api_key=")
        || lowered.contains("authorization=")
        || lowered.contains("client_secret=")
        || lowered.contains("password=")
        || (lowered.contains("://") && lowered.contains('@') && !lowered.contains("://@"))
}

fn is_release_eval_secret_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    is_sensitive_key(key)
        && !normalized.ends_with("_id")
        && normalized != "cross_session_event_leak"
        && normalized != "token_count"
        && !normalized.ends_with("_tokens")
        && !normalized.ends_with("_token_budget")
        && !normalized.ends_with("_token_limit")
}

fn normalize_key(key: &str) -> String {
    let mut normalized = String::with_capacity(key.len());
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    normalized
}

fn looks_like_iso_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 10
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes
            .iter()
            .enumerate()
            .all(|(index, byte)| index == 4 || index == 7 || byte.is_ascii_digit())
}

fn error_issue(
    code: impl Into<String>,
    path: impl Into<String>,
    message: impl Into<String>,
    recovery_hint: impl Into<String>,
) -> ReleaseEvalIssue {
    ReleaseEvalIssue {
        severity: ReleaseEvalIssueSeverity::Error,
        code: code.into(),
        path: path.into(),
        message: message.into(),
        recovery_hint: recovery_hint.into(),
    }
}

/// Count issues by stable issue code.
#[must_use]
pub fn release_eval_issue_counts_by_code(report: &ReleaseEvalReport) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for issue in report
        .issues
        .iter()
        .chain(report.suites.iter().flat_map(|suite| suite.issues.iter()))
        .chain(
            report
                .suites
                .iter()
                .flat_map(|suite| suite.cases.iter())
                .flat_map(|case| case.issues.iter()),
        )
    {
        *counts.entry(issue.code.clone()).or_insert(0) += 1;
    }
    counts
}
