//! QA Lab run evidence bundles and assertion evaluation.
//!
//! This module is runner-facing: it evaluates a validated scenario manifest
//! against observed run evidence, normalizes volatile tape fields, and returns
//! a redacted bundle plus stable JSON/Markdown reports for CI and review.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    qa_scenarios::{QaScenarioExpectedEvent, QaScenarioManifest},
    redaction::{is_sensitive_key, redact_auth_error, redact_url_segments_in_text, REDACTED},
};

/// Current QA evidence bundle schema version.
pub const QA_EVIDENCE_BUNDLE_SCHEMA_VERSION: u32 = 1;

/// Stable format label embedded in generated evidence bundles.
pub const QA_EVIDENCE_BUNDLE_FORMAT: &str = "palyra-qa-evidence-bundle";

/// Verdict emitted by QA evidence assertion checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QaEvidenceVerdict {
    Passed,
    Failed,
}

impl QaEvidenceVerdict {
    /// Returns the canonical serialized identifier.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
        }
    }
}

/// One transcript row captured for a QA scenario run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaTranscriptMessage {
    /// Message role, for example `user`, `assistant`, `tool`, or `system`.
    pub role: String,
    /// Redacted visible message content.
    pub content: String,
}

/// One run-tape event captured from the daemon journal.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QaRunTapeEvent {
    /// Monotonic tape sequence within the run.
    pub seq: i64,
    /// Internal tape event type.
    pub event_type: String,
    /// Event payload as JSON.
    pub payload: Value,
}

/// One public runtime event observed by a QA runner.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QaPublicEventEvidence {
    /// Public runtime event name, for example `run.completed`.
    pub event_type: String,
    /// Redacted public event payload.
    pub payload: Value,
}

/// One tool call observed during a QA scenario run.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QaToolCallEvidence {
    /// Tool identifier.
    pub name: String,
    /// Optional proposal or call id.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub proposal_id: Option<String>,
    /// Whether the call produced a successful result.
    pub success: bool,
}

/// One artifact reference observed or produced by a QA runner.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QaArtifactEvidence {
    /// Repository-relative or normalized artifact path.
    pub path: String,
    /// Artifact kind, for example `report`, `evidence`, or `replay_bundle`.
    pub kind: String,
    /// Whether the artifact exists in the bundle index.
    pub present: bool,
    /// Optional content digest. Digests are normalized in redacted outputs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Optional size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Observed data handed to the QA assertion engine by a runner.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct QaEvidenceBuildInput {
    /// Optional run id. The bundle stores a normalized form.
    pub run_id: Option<String>,
    /// Optional session id. The bundle stores a normalized form.
    pub session_id: Option<String>,
    /// Observed terminal state such as `completed` or `failed`.
    pub terminal_state: Option<String>,
    /// Final assistant answer. When omitted, the engine derives it from tape.
    pub final_answer: Option<String>,
    /// Transcript rows captured by the runner.
    pub transcript: Vec<QaTranscriptMessage>,
    /// Raw tape events captured from the daemon.
    pub tape_events: Vec<QaRunTapeEvent>,
    /// Public runtime events emitted for the run.
    pub public_events: Vec<QaPublicEventEvidence>,
    /// Tool calls observed by the runner. Empty means derive from events.
    pub tool_calls: Vec<QaToolCallEvidence>,
    /// Artifact index captured or produced by the runner.
    pub artifacts: Vec<QaArtifactEvidence>,
}

/// Top-level QA evidence bundle produced per scenario run.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct QaEvidenceBundle {
    pub schema_version: u32,
    pub format: String,
    pub scenario_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scenario_title: Option<String>,
    pub run: QaEvidenceRunRef,
    pub transcript: Vec<QaTranscriptMessage>,
    pub public_events: Vec<QaPublicEventEvidence>,
    pub redacted_tape: Vec<QaRunTapeEvent>,
    pub artifacts_index: Vec<QaArtifactEvidence>,
    pub tool_calls: Vec<QaToolCallEvidence>,
    pub checks: Vec<QaEvidenceCheck>,
    pub summary: QaEvidenceSummary,
    pub redaction: QaEvidenceRedactionReport,
}

/// Normalized run identity and terminal output in an evidence bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaEvidenceRunRef {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub terminal_state: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_answer: Option<String>,
}

/// One assertion check and its detailed findings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaEvidenceCheck {
    pub name: String,
    pub verdict: QaEvidenceVerdict,
    pub issues: Vec<QaEvidenceIssue>,
}

/// One precise assertion issue.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaEvidenceIssue {
    pub code: String,
    pub path: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual: Option<String>,
}

/// Aggregate bundle summary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaEvidenceSummary {
    pub verdict: QaEvidenceVerdict,
    pub issue_count: usize,
    pub check_count: usize,
    pub observed_event_count: usize,
    pub observed_tool_call_count: usize,
    pub artifact_count: usize,
    pub fake_progress_detected: bool,
}

/// Counts recorded while normalizing volatile and sensitive tape payloads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct QaEvidenceRedactionReport {
    pub redacted_fields: usize,
    pub normalized_timestamps: usize,
    pub normalized_identifiers: usize,
    pub normalized_paths: usize,
    pub normalized_hashes: usize,
}

#[derive(Debug, Clone)]
struct ObservedEvidence {
    final_answer: Option<String>,
    terminal_state: Option<String>,
    event_sequence: Vec<String>,
    event_counts: BTreeMap<String, usize>,
    tool_calls: Vec<QaToolCallEvidence>,
    artifacts: Vec<QaArtifactEvidence>,
}

/// Builds a redacted evidence bundle and evaluates scenario assertions.
#[must_use]
pub fn build_qa_evidence_bundle(
    manifest: &QaScenarioManifest,
    input: QaEvidenceBuildInput,
) -> QaEvidenceBundle {
    let mut redaction = QaEvidenceRedactionReport::default();
    let redacted_tape = input
        .tape_events
        .iter()
        .map(|event| QaRunTapeEvent {
            seq: event.seq,
            event_type: event.event_type.clone(),
            payload: normalize_evidence_value(&event.payload, &mut redaction),
        })
        .collect::<Vec<_>>();
    let public_events = input
        .public_events
        .iter()
        .map(|event| QaPublicEventEvidence {
            event_type: event.event_type.clone(),
            payload: normalize_evidence_value(&event.payload, &mut redaction),
        })
        .collect::<Vec<_>>();
    let transcript = input
        .transcript
        .iter()
        .map(|message| QaTranscriptMessage {
            role: message.role.clone(),
            content: redact_evidence_text(message.content.as_str()),
        })
        .collect::<Vec<_>>();
    let artifacts = input
        .artifacts
        .iter()
        .map(|artifact| QaArtifactEvidence {
            path: normalize_artifact_path(artifact.path.as_str(), &mut redaction),
            kind: artifact.kind.clone(),
            present: artifact.present,
            sha256: artifact.sha256.as_ref().map(|_| {
                redaction.normalized_hashes += 1;
                "<normalized:hash>".to_owned()
            }),
            size_bytes: artifact.size_bytes,
        })
        .collect::<Vec<_>>();
    let observed = ObservedEvidence::from_input(&input);
    let checks = evaluate_checks(manifest, &observed, &input);
    let check_count = checks.len();
    let issue_count = checks.iter().map(|check| check.issues.len()).sum::<usize>();
    let fake_progress_detected = checks.iter().any(|check| {
        check.issues.iter().any(|issue| issue.code == "fake_progress_without_evidence")
    });
    let verdict =
        if issue_count == 0 { QaEvidenceVerdict::Passed } else { QaEvidenceVerdict::Failed };

    QaEvidenceBundle {
        schema_version: QA_EVIDENCE_BUNDLE_SCHEMA_VERSION,
        format: QA_EVIDENCE_BUNDLE_FORMAT.to_owned(),
        scenario_id: manifest.id.clone(),
        scenario_title: manifest.title.clone(),
        run: QaEvidenceRunRef {
            run_id: input.run_id.as_deref().map(normalized_identifier),
            session_id: input.session_id.as_deref().map(normalized_identifier),
            terminal_state: observed.terminal_state.clone(),
            final_answer: observed.final_answer.as_deref().map(redact_evidence_text),
        },
        transcript,
        public_events,
        redacted_tape,
        artifacts_index: artifacts,
        tool_calls: observed.tool_calls.clone(),
        checks,
        summary: QaEvidenceSummary {
            verdict,
            issue_count,
            check_count,
            observed_event_count: observed.event_sequence.len(),
            observed_tool_call_count: observed.tool_calls.len(),
            artifact_count: observed.artifacts.len(),
            fake_progress_detected,
        },
        redaction,
    }
}

/// Converts an evidence bundle to the stable JSON report shape used by CI.
#[must_use]
pub fn qa_evidence_json_report(bundle: &QaEvidenceBundle) -> Value {
    json!({
        "schema_version": bundle.schema_version,
        "format": bundle.format,
        "scenario_id": bundle.scenario_id,
        "verdict": bundle.summary.verdict.as_str(),
        "issue_count": bundle.summary.issue_count,
        "checks": bundle.checks,
        "artifacts": bundle.artifacts_index,
        "redaction": bundle.redaction,
    })
}

/// Renders a human-readable Markdown report for one evidence bundle.
#[must_use]
pub fn qa_evidence_markdown_report(bundle: &QaEvidenceBundle) -> String {
    let mut lines = vec![
        "# QA Evidence Report".to_owned(),
        String::new(),
        format!("- Scenario: `{}`", bundle.scenario_id),
        format!("- Verdict: `{}`", bundle.summary.verdict.as_str()),
        format!("- Checks: {}", bundle.summary.check_count),
        format!("- Issues: {}", bundle.summary.issue_count),
        format!("- Events: {}", bundle.summary.observed_event_count),
        format!("- Tool calls: {}", bundle.summary.observed_tool_call_count),
        format!("- Artifacts: {}", bundle.summary.artifact_count),
        String::new(),
        "## Checks".to_owned(),
        String::new(),
        "| Check | Verdict | Issues |".to_owned(),
        "| --- | --- | --- |".to_owned(),
    ];
    for check in &bundle.checks {
        lines.push(format!(
            "| `{}` | `{}` | {} |",
            check.name,
            check.verdict.as_str(),
            check.issues.len()
        ));
    }
    let issues = bundle.checks.iter().flat_map(|check| check.issues.iter()).collect::<Vec<_>>();
    if !issues.is_empty() {
        lines.push(String::new());
        lines.push("## Issues".to_owned());
        lines.push(String::new());
        for issue in issues {
            lines.push(format!("- `{}` at `{}`: {}", issue.code, issue.path, issue.message));
        }
    }
    lines.push(String::new());
    lines.push("## Redaction".to_owned());
    lines.push(String::new());
    lines.push(format!("- Redacted fields: {}", bundle.redaction.redacted_fields));
    lines.push(format!("- Normalized timestamps: {}", bundle.redaction.normalized_timestamps));
    lines.push(format!("- Normalized identifiers: {}", bundle.redaction.normalized_identifiers));
    lines.push(format!("- Normalized paths: {}", bundle.redaction.normalized_paths));
    lines.push(format!("- Normalized hashes: {}", bundle.redaction.normalized_hashes));
    lines.push(String::new());
    lines.join("\n")
}

impl ObservedEvidence {
    fn from_input(input: &QaEvidenceBuildInput) -> Self {
        let final_answer =
            input.final_answer.clone().or_else(|| derive_final_answer(&input.tape_events));
        let mut event_sequence = Vec::new();
        event_sequence.extend(input.public_events.iter().map(|event| event.event_type.clone()));
        let mut tape_events = input.tape_events.clone();
        tape_events.sort_by_key(|event| event.seq);
        event_sequence.extend(tape_events.iter().map(|event| event.event_type.clone()));
        let event_counts = event_sequence.iter().fold(BTreeMap::new(), |mut counts, event| {
            *counts.entry(event.clone()).or_insert(0) += 1;
            counts
        });
        let tool_calls = if input.tool_calls.is_empty() {
            derive_tool_calls(input)
        } else {
            input.tool_calls.clone()
        };
        Self {
            final_answer,
            terminal_state: input.terminal_state.clone(),
            event_sequence,
            event_counts,
            tool_calls,
            artifacts: input.artifacts.clone(),
        }
    }
}

fn evaluate_checks(
    manifest: &QaScenarioManifest,
    observed: &ObservedEvidence,
    input: &QaEvidenceBuildInput,
) -> Vec<QaEvidenceCheck> {
    vec![
        check_terminal_state(manifest, observed),
        check_final_answer(manifest, observed),
        check_required_events(manifest, observed),
        check_required_tool_calls(manifest, observed),
        check_forbidden_observations(manifest, observed),
        check_artifacts_and_fake_progress(manifest, observed),
        check_backend_attestation_manifests(input),
    ]
}

fn check_terminal_state(
    manifest: &QaScenarioManifest,
    observed: &ObservedEvidence,
) -> QaEvidenceCheck {
    let expected = manifest.expect.terminal_state.as_str();
    let actual = observed.terminal_state.as_deref().unwrap_or("<missing>");
    let issues = if actual == expected {
        Vec::new()
    } else {
        vec![QaEvidenceIssue {
            code: "terminal_state_mismatch".to_owned(),
            path: "$.run.terminal_state".to_owned(),
            message: format!("expected terminal state `{expected}`, got `{actual}`"),
            expected: Some(expected.to_owned()),
            actual: Some(actual.to_owned()),
        }]
    };
    check("terminal_state", issues)
}

fn check_final_answer(
    manifest: &QaScenarioManifest,
    observed: &ObservedEvidence,
) -> QaEvidenceCheck {
    let mut issues = Vec::new();
    if let Some(assertion) = manifest.expect.final_answer.as_ref() {
        let actual = observed.final_answer.as_deref().unwrap_or("");
        if let Some(expected) = assertion.equals.as_deref() {
            if actual != expected {
                issues.push(QaEvidenceIssue {
                    code: "final_answer_mismatch".to_owned(),
                    path: "$.run.final_answer".to_owned(),
                    message: "final answer did not match exactly".to_owned(),
                    expected: Some(expected.to_owned()),
                    actual: Some(actual.to_owned()),
                });
            }
        }
        for fragment in &assertion.contains {
            if !actual.contains(fragment) {
                issues.push(QaEvidenceIssue {
                    code: "missing_final_answer_fragment".to_owned(),
                    path: "$.run.final_answer".to_owned(),
                    message: format!("final answer is missing required fragment `{fragment}`"),
                    expected: Some(fragment.clone()),
                    actual: Some(actual.to_owned()),
                });
            }
        }
    }
    check("final_answer", issues)
}

fn check_required_events(
    manifest: &QaScenarioManifest,
    observed: &ObservedEvidence,
) -> QaEvidenceCheck {
    let mut issues = Vec::new();
    for (index, expected_event) in manifest.expect.events.iter().enumerate() {
        let count =
            observed.event_counts.get(expected_event.event_type.as_str()).copied().unwrap_or(0);
        let min_count = expected_event.min_count.unwrap_or(1) as usize;
        if count < min_count {
            issues.push(QaEvidenceIssue {
                code: "missing_event".to_owned(),
                path: format!("$.expect.events[{index}]"),
                message: format!(
                    "expected at least {min_count} `{}` event(s), observed {count}",
                    expected_event.event_type
                ),
                expected: Some(expected_event.event_type.clone()),
                actual: Some(count.to_string()),
            });
        }
    }
    issues.extend(required_event_order_issues(&manifest.expect.events, observed));
    issues.extend(lifecycle_order_issues(observed));
    check("required_events", issues)
}

fn check_required_tool_calls(
    manifest: &QaScenarioManifest,
    observed: &ObservedEvidence,
) -> QaEvidenceCheck {
    let mut issues = Vec::new();
    for (index, expected_tool) in manifest.expect.tool_calls.iter().enumerate() {
        let count = observed
            .tool_calls
            .iter()
            .filter(|tool| token_matches(expected_tool.name.as_str(), tool.name.as_str()))
            .count();
        let min_count = expected_tool.min_count.unwrap_or(1) as usize;
        if count < min_count {
            issues.push(QaEvidenceIssue {
                code: "missing_tool_call".to_owned(),
                path: format!("$.expect.tool_calls[{index}]"),
                message: format!(
                    "expected at least {min_count} `{}` tool call(s), observed {count}",
                    expected_tool.name
                ),
                expected: Some(expected_tool.name.clone()),
                actual: Some(count.to_string()),
            });
        }
    }
    check("required_tool_calls", issues)
}

fn check_forbidden_observations(
    manifest: &QaScenarioManifest,
    observed: &ObservedEvidence,
) -> QaEvidenceCheck {
    let mut issues = Vec::new();
    for forbidden in &manifest.forbidden.tool_calls {
        for tool in observed
            .tool_calls
            .iter()
            .filter(|tool| token_matches(forbidden.as_str(), tool.name.as_str()))
        {
            issues.push(QaEvidenceIssue {
                code: "unexpected_tool_call".to_owned(),
                path: "$.forbidden.tool_calls".to_owned(),
                message: format!("forbidden tool call `{}` was observed", tool.name),
                expected: Some(format!("not {forbidden}")),
                actual: Some(tool.name.clone()),
            });
        }
    }
    for forbidden in &manifest.forbidden.events {
        for event_type in observed
            .event_sequence
            .iter()
            .filter(|event_type| token_matches(forbidden.as_str(), event_type.as_str()))
        {
            issues.push(QaEvidenceIssue {
                code: "unexpected_event".to_owned(),
                path: "$.forbidden.events".to_owned(),
                message: format!("forbidden event `{event_type}` was observed"),
                expected: Some(format!("not {forbidden}")),
                actual: Some(event_type.clone()),
            });
        }
    }
    for forbidden in &manifest.forbidden.artifacts {
        for artifact in observed.artifacts.iter().filter(|artifact| {
            token_matches(forbidden.as_str(), artifact.path.as_str())
                || token_matches(forbidden.as_str(), artifact.kind.as_str())
        }) {
            issues.push(QaEvidenceIssue {
                code: "unexpected_artifact".to_owned(),
                path: "$.forbidden.artifacts".to_owned(),
                message: format!("forbidden artifact `{}` was observed", artifact.path),
                expected: Some(format!("not {forbidden}")),
                actual: Some(artifact.path.clone()),
            });
        }
    }
    if let Some(final_answer) = observed.final_answer.as_deref() {
        for forbidden in &manifest.forbidden.claims {
            if final_answer.contains(forbidden) {
                issues.push(QaEvidenceIssue {
                    code: "unexpected_claim".to_owned(),
                    path: "$.forbidden.claims".to_owned(),
                    message: format!("forbidden final-answer claim `{forbidden}` was observed"),
                    expected: Some(format!("not {forbidden}")),
                    actual: Some(final_answer.to_owned()),
                });
            }
        }
    }
    check("forbidden_observations", issues)
}

fn check_artifacts_and_fake_progress(
    manifest: &QaScenarioManifest,
    observed: &ObservedEvidence,
) -> QaEvidenceCheck {
    let mut issues = Vec::new();
    for (index, expected_artifact) in manifest.artifacts.iter().enumerate() {
        if !expected_artifact.required {
            continue;
        }
        let expected_path = expected_artifact.path.as_str();
        let expected_kind = expected_artifact.kind.as_str();
        let present = observed.artifacts.iter().any(|artifact| {
            artifact.present
                && (artifact.path == expected_path || artifact.kind.as_str() == expected_kind)
        });
        if !present {
            issues.push(QaEvidenceIssue {
                code: "missing_artifact".to_owned(),
                path: format!("$.artifacts[{index}]"),
                message: format!(
                    "required `{expected_kind}` artifact `{expected_path}` is missing"
                ),
                expected: Some(expected_path.to_owned()),
                actual: Some("missing".to_owned()),
            });
        }
    }
    if let Some(answer) = observed.final_answer.as_deref() {
        let has_tool_or_artifact_evidence = !observed.tool_calls.is_empty()
            || observed.artifacts.iter().any(|artifact| artifact.present);
        if !has_tool_or_artifact_evidence && looks_like_fake_progress_claim(answer) {
            issues.push(QaEvidenceIssue {
                code: "fake_progress_without_evidence".to_owned(),
                path: "$.run.final_answer".to_owned(),
                message: "final answer claims tool or artifact work without matching evidence"
                    .to_owned(),
                expected: Some("tool call or artifact evidence".to_owned()),
                actual: Some(answer.to_owned()),
            });
        }
    }
    check("artifacts_and_fake_progress", issues)
}

fn check_backend_attestation_manifests(input: &QaEvidenceBuildInput) -> QaEvidenceCheck {
    let mut issues = Vec::new();
    for (index, event) in input.tape_events.iter().enumerate() {
        if event.event_type != "tool_attestation" {
            continue;
        }
        let executor = event.payload.get("executor").and_then(Value::as_str).unwrap_or_default();
        if !executor_requires_backend_manifest(executor) {
            continue;
        }
        let manifest_path = format!("$.tape_events[{index}].payload.execution_manifest");
        let Some(manifest) = event.payload.get("execution_manifest") else {
            issues.push(QaEvidenceIssue {
                code: "backend_attestation_manifest_missing".to_owned(),
                path: manifest_path,
                message: format!(
                    "executor `{executor}` requires an execution attestation manifest"
                ),
                expected: Some("execution_manifest".to_owned()),
                actual: Some("missing".to_owned()),
            });
            continue;
        };
        backend_manifest_issues(manifest, manifest_path.as_str(), &mut issues);
    }
    check("backend_attestation_manifests", issues)
}

fn executor_requires_backend_manifest(executor: &str) -> bool {
    executor == "docker"
        || executor == "ssh_tunnel"
        || executor == "host_process"
        || executor == "sandbox_tier_b"
        || executor.starts_with("sandbox_tier_c")
        || executor.starts_with("networked_worker")
}

fn backend_manifest_issues(manifest: &Value, path: &str, issues: &mut Vec<QaEvidenceIssue>) {
    let Some(object) = manifest.as_object() else {
        issues.push(backend_manifest_issue(
            path,
            "manifest must be a JSON object",
            Some("object"),
            Some(manifest_type_name(manifest)),
        ));
        return;
    };
    match object.get("schema_version").and_then(Value::as_u64) {
        Some(1) => {}
        actual => issues.push(backend_manifest_issue(
            &format!("{path}.schema_version"),
            "manifest schema_version must be 1",
            Some("1"),
            actual.map(|value| value.to_string()).as_deref(),
        )),
    }
    for field in ["backend_id", "runner_id", "runner_version", "egress_posture"] {
        require_non_empty_manifest_string(object, path, field, issues);
    }
    for field in ["workspace_strategy_digest", "input_manifest_sha256", "output_manifest_sha256"] {
        require_sha256_manifest_string(object, path, field, issues);
    }
    if object.contains_key("manifest_sha256") {
        require_sha256_manifest_string(object, path, "manifest_sha256", issues);
    }
    let cleanup_path = format!("{path}.cleanup");
    let Some(cleanup) = object.get("cleanup").and_then(Value::as_object) else {
        issues.push(backend_manifest_issue(
            cleanup_path.as_str(),
            "cleanup evidence must be an object",
            Some("object"),
            object.get("cleanup").map(manifest_type_name),
        ));
        return;
    };
    require_non_empty_manifest_string(cleanup, cleanup_path.as_str(), "strategy", issues);
    require_non_empty_manifest_string(cleanup, cleanup_path.as_str(), "reason_code", issues);
    if !matches!(cleanup.get("success"), Some(Value::Bool(_))) {
        issues.push(backend_manifest_issue(
            &format!("{cleanup_path}.success"),
            "cleanup success must be a boolean",
            Some("boolean"),
            cleanup.get("success").map(manifest_type_name),
        ));
    }
    let resources_path = format!("{cleanup_path}.resources");
    let Some(resources) = cleanup.get("resources").and_then(Value::as_array) else {
        issues.push(backend_manifest_issue(
            resources_path.as_str(),
            "cleanup resources must be a non-empty array",
            Some("non-empty array"),
            cleanup.get("resources").map(manifest_type_name),
        ));
        return;
    };
    if resources.is_empty() {
        issues.push(backend_manifest_issue(
            resources_path.as_str(),
            "cleanup resources must not be empty",
            Some("non-empty array"),
            Some("empty array"),
        ));
    }
    for (resource_index, resource) in resources.iter().enumerate() {
        let resource_path = format!("{resources_path}[{resource_index}]");
        let Some(resource_object) = resource.as_object() else {
            issues.push(backend_manifest_issue(
                resource_path.as_str(),
                "cleanup resource must be an object",
                Some("object"),
                Some(manifest_type_name(resource)),
            ));
            continue;
        };
        require_non_empty_manifest_string(resource_object, resource_path.as_str(), "kind", issues);
        require_non_empty_manifest_string(
            resource_object,
            resource_path.as_str(),
            "status",
            issues,
        );
        for field in ["cleanup_required", "cleanup_verified"] {
            if !matches!(resource_object.get(field), Some(Value::Bool(_))) {
                issues.push(backend_manifest_issue(
                    &format!("{resource_path}.{field}"),
                    "cleanup resource field must be a boolean",
                    Some("boolean"),
                    resource_object.get(field).map(manifest_type_name),
                ));
            }
        }
    }
}

fn require_non_empty_manifest_string(
    object: &serde_json::Map<String, Value>,
    path: &str,
    field: &str,
    issues: &mut Vec<QaEvidenceIssue>,
) {
    let value = object.get(field).and_then(Value::as_str).unwrap_or_default();
    if value.trim().is_empty() {
        issues.push(backend_manifest_issue(
            &format!("{path}.{field}"),
            "manifest field must be a non-empty string",
            Some("non-empty string"),
            object.get(field).map(manifest_type_name),
        ));
    }
}

fn require_sha256_manifest_string(
    object: &serde_json::Map<String, Value>,
    path: &str,
    field: &str,
    issues: &mut Vec<QaEvidenceIssue>,
) {
    let value = object.get(field).and_then(Value::as_str).unwrap_or_default();
    if !is_sha256_hex(value) {
        issues.push(backend_manifest_issue(
            &format!("{path}.{field}"),
            "manifest field must be a lowercase SHA-256 hex digest",
            Some("64 lowercase hex characters"),
            object.get(field).and_then(Value::as_str),
        ));
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn backend_manifest_issue(
    path: &str,
    message: &str,
    expected: Option<&str>,
    actual: Option<&str>,
) -> QaEvidenceIssue {
    QaEvidenceIssue {
        code: "backend_attestation_manifest_invalid".to_owned(),
        path: path.to_owned(),
        message: message.to_owned(),
        expected: expected.map(ToOwned::to_owned),
        actual: actual.map(ToOwned::to_owned),
    }
}

fn manifest_type_name(value: &Value) -> &str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn check(name: &str, issues: Vec<QaEvidenceIssue>) -> QaEvidenceCheck {
    QaEvidenceCheck {
        name: name.to_owned(),
        verdict: if issues.is_empty() {
            QaEvidenceVerdict::Passed
        } else {
            QaEvidenceVerdict::Failed
        },
        issues,
    }
}

fn required_event_order_issues(
    expected_events: &[QaScenarioExpectedEvent],
    observed: &ObservedEvidence,
) -> Vec<QaEvidenceIssue> {
    let mut issues = Vec::new();
    let mut last_position = None;
    for expected_event in expected_events {
        let Some(position) =
            first_event_position(&observed.event_sequence, expected_event.event_type.as_str())
        else {
            continue;
        };
        if let Some(previous) = last_position {
            if position < previous {
                issues.push(QaEvidenceIssue {
                    code: "event_order_mismatch".to_owned(),
                    path: "$.expect.events".to_owned(),
                    message: format!(
                        "event `{}` appeared before an earlier expected event",
                        expected_event.event_type
                    ),
                    expected: Some("expected event order".to_owned()),
                    actual: Some(format!("position {position}")),
                });
            }
        }
        last_position = Some(position);
    }
    issues
}

fn lifecycle_order_issues(observed: &ObservedEvidence) -> Vec<QaEvidenceIssue> {
    let queued = first_event_position(&observed.event_sequence, "run.queued");
    let started = first_event_position(&observed.event_sequence, "run.started");
    let terminal = ["run.completed", "run.failed", "run.cancelled"]
        .iter()
        .filter_map(|event| first_event_position(&observed.event_sequence, event))
        .min();
    let mut issues = Vec::new();
    if let (Some(started), Some(queued)) = (started, queued) {
        if started < queued {
            issues.push(QaEvidenceIssue {
                code: "event_order_mismatch".to_owned(),
                path: "$.public_events".to_owned(),
                message: "`run.started` appeared before `run.queued`".to_owned(),
                expected: Some("run.queued before run.started".to_owned()),
                actual: Some(format!(
                    "run.started position {started}, run.queued position {queued}"
                )),
            });
        }
    }
    if let (Some(terminal), Some(started)) = (terminal, started) {
        if terminal < started {
            issues.push(QaEvidenceIssue {
                code: "event_order_mismatch".to_owned(),
                path: "$.public_events".to_owned(),
                message: "terminal run event appeared before `run.started`".to_owned(),
                expected: Some("run.started before terminal event".to_owned()),
                actual: Some(format!(
                    "terminal position {terminal}, run.started position {started}"
                )),
            });
        }
    }
    issues
}

fn first_event_position(events: &[String], expected: &str) -> Option<usize> {
    events.iter().position(|event| event == expected)
}

fn derive_final_answer(tape_events: &[QaRunTapeEvent]) -> Option<String> {
    let mut ordered = tape_events.to_vec();
    ordered.sort_by_key(|event| event.seq);
    ordered.iter().rev().find_map(|event| {
        event
            .payload
            .get("reply_text")
            .or_else(|| event.payload.get("full_text"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
    })
}

fn derive_tool_calls(input: &QaEvidenceBuildInput) -> Vec<QaToolCallEvidence> {
    let mut calls = Vec::new();
    for event in &input.public_events {
        if !event.event_type.contains("tool") && !event.event_type.contains("approval") {
            continue;
        }
        if let Some(tool_name) = event.payload.get("tool_name").and_then(Value::as_str) {
            calls.push(QaToolCallEvidence {
                name: tool_name.to_owned(),
                proposal_id: event
                    .payload
                    .get("proposal_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                success: event.event_type != "tool.call.failed",
            });
        }
    }
    for event in &input.tape_events {
        if !event.event_type.contains("tool") {
            continue;
        }
        if let Some(tool_name) = event.payload.get("tool_name").and_then(Value::as_str) {
            calls.push(QaToolCallEvidence {
                name: tool_name.to_owned(),
                proposal_id: event
                    .payload
                    .get("proposal_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                success: event.event_type != "tool_result_failed",
            });
        }
    }
    dedupe_tool_calls(calls)
}

fn dedupe_tool_calls(calls: Vec<QaToolCallEvidence>) -> Vec<QaToolCallEvidence> {
    let mut seen = BTreeSet::new();
    calls
        .into_iter()
        .filter(|call| {
            seen.insert((
                call.name.clone(),
                call.proposal_id.clone().unwrap_or_default(),
                call.success,
            ))
        })
        .collect()
}

fn token_matches(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    if let Some(suffix) = pattern.strip_prefix('*') {
        return value.ends_with(suffix);
    }
    pattern == value
}

fn looks_like_fake_progress_claim(answer: &str) -> bool {
    let normalized = answer.to_ascii_lowercase();
    let action_claim = [
        "created", "wrote", "updated", "modified", "deleted", "saved", "read", "fetched", "called",
    ]
    .iter()
    .any(|term| normalized.contains(term));
    let work_object = ["file", "tool", "artifact", "report", "url", "http", "workspace"]
        .iter()
        .any(|term| normalized.contains(term));
    action_claim && work_object
}

fn normalize_evidence_value(value: &Value, report: &mut QaEvidenceRedactionReport) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, child)| (key.clone(), normalize_object_child(key, child, report)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values.iter().map(|child| normalize_evidence_value(child, report)).collect(),
        ),
        Value::String(text) => Value::String(redact_evidence_text(text.as_str())),
        _ => value.clone(),
    }
}

fn normalize_object_child(
    key: &str,
    value: &Value,
    report: &mut QaEvidenceRedactionReport,
) -> Value {
    let normalized_key = key.to_ascii_lowercase();
    if is_sensitive_key(key) {
        report.redacted_fields += 1;
        return Value::String(REDACTED.to_owned());
    }
    if is_timestamp_key(normalized_key.as_str()) {
        report.normalized_timestamps += 1;
        return normalized_scalar(value, "<normalized:timestamp>", 0);
    }
    if is_identifier_key(normalized_key.as_str()) {
        report.normalized_identifiers += 1;
        return normalized_scalar(value, "<normalized:id>", 0);
    }
    if is_hash_key(normalized_key.as_str()) {
        report.normalized_hashes += 1;
        return Value::String("<normalized:hash>".to_owned());
    }
    if is_path_key(normalized_key.as_str()) {
        return normalize_path_value(value, report);
    }
    normalize_evidence_value(value, report)
}

fn normalized_scalar(value: &Value, text_value: &str, number_value: i64) -> Value {
    match value {
        Value::Number(_) => Value::Number(number_value.into()),
        Value::String(_) => Value::String(text_value.to_owned()),
        Value::Null => Value::Null,
        _ => Value::String(text_value.to_owned()),
    }
}

fn normalize_path_value(value: &Value, report: &mut QaEvidenceRedactionReport) -> Value {
    match value {
        Value::String(path) if path_is_absolute(path.as_str()) => {
            report.normalized_paths += 1;
            Value::String("<normalized:absolute_path>".to_owned())
        }
        Value::String(path) => Value::String(redact_evidence_text(path.as_str())),
        _ => normalize_evidence_value(value, report),
    }
}

fn is_timestamp_key(key: &str) -> bool {
    key.contains("timestamp")
        || key.contains("unix_ms")
        || key.ends_with("_at")
        || key.ends_with("_at_ms")
}

fn is_identifier_key(key: &str) -> bool {
    key == "id"
        || key.ends_with("_id")
        || key == "run"
        || key == "session"
        || key == "request_id"
        || key == "proposal_id"
        || key == "approval_id"
}

fn is_hash_key(key: &str) -> bool {
    key.contains("sha256") || key.contains("digest") || key.contains("hash")
}

fn is_path_key(key: &str) -> bool {
    key == "path" || key.ends_with("_path") || key.contains("file_path") || key == "reference"
}

fn normalize_artifact_path(path: &str, report: &mut QaEvidenceRedactionReport) -> String {
    if path_is_absolute(path) {
        report.normalized_paths += 1;
        "<normalized:absolute_path>".to_owned()
    } else {
        redact_evidence_text(path)
    }
}

fn path_is_absolute(path: &str) -> bool {
    Path::new(path).is_absolute()
        || path.starts_with('/')
        || path.as_bytes().get(1).is_some_and(|byte| *byte == b':')
}

fn normalized_identifier(_value: &str) -> String {
    "<normalized:id>".to_owned()
}

fn redact_evidence_text(text: &str) -> String {
    let url_redacted = redact_url_segments_in_text(text);
    redact_auth_error(url_redacted.as_str())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::qa_scenarios::parse_qa_scenario_manifest_yaml;

    const BASIC_SCENARIO: &str = include_str!("../../../qa/scenarios/text_run_basic.yaml");
    const MARKDOWN_GOLDEN: &str =
        include_str!("../../../fixtures/golden/qa_evidence_report_basic.md");

    #[test]
    fn evidence_bundle_passes_basic_manifest() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let bundle = build_qa_evidence_bundle(&manifest, passing_input());

        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Passed);
        assert_eq!(bundle.summary.issue_count, 0);
        assert_eq!(bundle.redacted_tape[0].payload["run_id"], "<normalized:id>");
        assert_eq!(qa_evidence_json_report(&bundle)["verdict"], "passed");
    }

    #[test]
    fn missing_tool_call_reports_precise_failure() {
        let manifest = parse_qa_scenario_manifest_yaml(
            r#"
schema_version: 1
id: tool.required
area: tools
mode:
  provider: mock
requires:
  capabilities: [qa_lab]
  tools: [palyra.fs.read_file]
  fixtures: []
steps:
  - id: prompt
    action: user_prompt
    prompt: "Read the fixture."
expect:
  terminal_state: completed
  events: []
  tool_calls:
    - name: palyra.fs.read_file
      min_count: 1
forbidden:
  tool_calls: []
  events: []
  artifacts: []
artifacts: []
maturity:
  labels: [p0]
timeout:
  run_ms: 30000
"#,
        )
        .expect("tool scenario should parse");
        let bundle = build_qa_evidence_bundle(
            &manifest,
            QaEvidenceBuildInput {
                terminal_state: Some("completed".to_owned()),
                final_answer: Some("Done.".to_owned()),
                ..QaEvidenceBuildInput::default()
            },
        );

        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Failed);
        assert!(bundle.checks.iter().any(|check| {
            check.issues.iter().any(|issue| {
                issue.code == "missing_tool_call" && issue.path == "$.expect.tool_calls[0]"
            })
        }));
    }

    #[test]
    fn redacts_and_normalizes_volatile_tape_payloads() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let bundle = build_qa_evidence_bundle(
            &manifest,
            QaEvidenceBuildInput {
                terminal_state: Some("completed".to_owned()),
                final_answer: Some("friendly response".to_owned()),
                public_events: vec![QaPublicEventEvidence {
                    event_type: "run.completed".to_owned(),
                    payload: json!({"occurred_at_unix_ms": 1_700_000_000_000_i64}),
                }],
                tape_events: vec![QaRunTapeEvent {
                    seq: 0,
                    event_type: "tool_result".to_owned(),
                    payload: json!({
                        "api_key": "sk-secret",
                        "run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAT",
                        "created_at_unix_ms": 1_700_000_000_000_i64,
                        "path": "C:\\Users\\Palo\\secret.txt",
                        "sha256": "abc123",
                        "url": "https://example.com/token/secret-value"
                    }),
                }],
                artifacts: vec![QaArtifactEvidence {
                    path: "qa/reports/text_run_basic.json".to_owned(),
                    kind: "report".to_owned(),
                    present: true,
                    sha256: Some("abc123".to_owned()),
                    size_bytes: Some(42),
                }],
                ..QaEvidenceBuildInput::default()
            },
        );

        let payload = &bundle.redacted_tape[0].payload;
        assert_eq!(payload["api_key"], REDACTED);
        assert_eq!(payload["run_id"], "<normalized:id>");
        assert_eq!(payload["created_at_unix_ms"], 0);
        assert_eq!(payload["path"], "<normalized:absolute_path>");
        assert_eq!(payload["sha256"], "<normalized:hash>");
        assert!(bundle.redaction.redacted_fields >= 1);
        assert!(bundle.redaction.normalized_timestamps >= 1);
        assert!(bundle.redaction.normalized_hashes >= 2);
    }

    #[test]
    fn fake_progress_claim_requires_tool_or_artifact_evidence() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let bundle = build_qa_evidence_bundle(
            &manifest,
            QaEvidenceBuildInput {
                terminal_state: Some("completed".to_owned()),
                final_answer: Some("I created the report file successfully.".to_owned()),
                public_events: vec![QaPublicEventEvidence {
                    event_type: "run.completed".to_owned(),
                    payload: json!({}),
                }],
                ..QaEvidenceBuildInput::default()
            },
        );

        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Failed);
        assert!(bundle.summary.fake_progress_detected);
    }

    #[test]
    fn backend_attestation_manifest_passes_for_execution_backend() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let mut input = passing_input();
        input.tape_events.push(QaRunTapeEvent {
            seq: 1,
            event_type: "tool_attestation".to_owned(),
            payload: json!({
                "proposal_id": "proposal-1",
                "tool_name": "palyra.process.run",
                "executor": "sandbox_tier_b",
                "execution_manifest": valid_backend_manifest(),
            }),
        });

        let bundle = build_qa_evidence_bundle(&manifest, input);

        let check = bundle
            .checks
            .iter()
            .find(|check| check.name == "backend_attestation_manifests")
            .expect("backend manifest check should be present");
        assert_eq!(check.verdict, QaEvidenceVerdict::Passed);
        assert_eq!(bundle.summary.check_count, 7);
    }

    #[test]
    fn backend_attestation_manifest_reports_missing_and_invalid_fields() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let mut missing = passing_input();
        missing.tape_events.push(QaRunTapeEvent {
            seq: 1,
            event_type: "tool_attestation".to_owned(),
            payload: json!({
                "proposal_id": "proposal-1",
                "tool_name": "palyra.process.run",
                "executor": "docker",
            }),
        });

        let missing_bundle = build_qa_evidence_bundle(&manifest, missing);

        assert!(missing_bundle.checks.iter().any(|check| {
            check.issues.iter().any(|issue| issue.code == "backend_attestation_manifest_missing")
        }));

        let mut invalid = passing_input();
        let mut invalid_manifest = valid_backend_manifest();
        invalid_manifest["output_manifest_sha256"] = json!("not-a-digest");
        invalid_manifest["cleanup"]["resources"] = json!([]);
        invalid.tape_events.push(QaRunTapeEvent {
            seq: 1,
            event_type: "tool_attestation".to_owned(),
            payload: json!({
                "proposal_id": "proposal-1",
                "tool_name": "palyra.process.run",
                "executor": "docker",
                "execution_manifest": invalid_manifest,
            }),
        });

        let invalid_bundle = build_qa_evidence_bundle(&manifest, invalid);

        assert!(invalid_bundle.checks.iter().any(|check| {
            check.issues.iter().any(|issue| {
                issue.code == "backend_attestation_manifest_invalid"
                    && issue.path.ends_with(".output_manifest_sha256")
            })
        }));
    }

    #[test]
    fn forbidden_claim_fails_final_answer_assertion() {
        let manifest = parse_qa_scenario_manifest_yaml(
            r#"
schema_version: 1
id: forbidden.claim
area: text
mode:
  provider: mock
requires:
  capabilities: [qa_lab]
  tools: []
  fixtures: []
steps:
  - id: prompt
    action: user_prompt
    prompt: "Answer briefly."
expect:
  terminal_state: completed
  final_answer:
    contains: ["done"]
  events: []
  tool_calls: []
forbidden:
  tool_calls: []
  events: []
  artifacts: []
  claims:
    - "secret copied"
artifacts: []
maturity:
  labels: [p0]
timeout:
  run_ms: 30000
"#,
        )
        .expect("forbidden claim scenario should parse");
        let bundle = build_qa_evidence_bundle(
            &manifest,
            QaEvidenceBuildInput {
                terminal_state: Some("completed".to_owned()),
                final_answer: Some("done; secret copied".to_owned()),
                ..QaEvidenceBuildInput::default()
            },
        );

        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Failed);
        assert!(bundle
            .checks
            .iter()
            .any(|check| { check.issues.iter().any(|issue| issue.code == "unexpected_claim") }));
    }

    #[test]
    fn markdown_report_matches_golden() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let bundle = build_qa_evidence_bundle(&manifest, passing_input());

        assert_eq!(qa_evidence_markdown_report(&bundle), MARKDOWN_GOLDEN.replace("\r\n", "\n"));
    }

    fn passing_input() -> QaEvidenceBuildInput {
        QaEvidenceBuildInput {
            run_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAT".to_owned()),
            session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAU".to_owned()),
            terminal_state: Some("completed".to_owned()),
            final_answer: Some("A friendly response.".to_owned()),
            transcript: vec![
                QaTranscriptMessage {
                    role: "user".to_owned(),
                    content: "Reply with exactly one short friendly sentence.".to_owned(),
                },
                QaTranscriptMessage {
                    role: "assistant".to_owned(),
                    content: "A friendly response.".to_owned(),
                },
            ],
            public_events: vec![
                QaPublicEventEvidence {
                    event_type: "run.queued".to_owned(),
                    payload: json!({"run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAT"}),
                },
                QaPublicEventEvidence {
                    event_type: "run.started".to_owned(),
                    payload: json!({"run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAT"}),
                },
                QaPublicEventEvidence {
                    event_type: "run.completed".to_owned(),
                    payload: json!({"run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAT"}),
                },
            ],
            tape_events: vec![QaRunTapeEvent {
                seq: 0,
                event_type: "message.replied".to_owned(),
                payload: json!({
                    "run_id": "01ARZ3NDEKTSV4RRFFQ69G5FAT",
                    "reply_text": "A friendly response."
                }),
            }],
            artifacts: vec![QaArtifactEvidence {
                path: "qa/reports/text_run_basic.json".to_owned(),
                kind: "report".to_owned(),
                present: true,
                sha256: None,
                size_bytes: Some(512),
            }],
            ..QaEvidenceBuildInput::default()
        }
    }

    fn valid_backend_manifest() -> Value {
        json!({
            "schema_version": 1,
            "manifest_sha256": "f".repeat(64),
            "backend_id": "local_sandbox",
            "runner_id": "local_sandbox_runner",
            "runner_version": "v1",
            "workspace_strategy_digest": "1".repeat(64),
            "input_manifest_sha256": "2".repeat(64),
            "output_manifest_sha256": "3".repeat(64),
            "cleanup": {
                "strategy": "local_sandbox_process_lifecycle",
                "success": true,
                "reason_code": "local_sandbox.cleanup.ok",
                "resources": [
                    {
                        "kind": "process_tree",
                        "status": "foreground_process_reaped",
                        "cleanup_required": true,
                        "cleanup_verified": true
                    }
                ]
            },
            "egress_posture": "process_runner_egress:preflight"
        })
    }
}
