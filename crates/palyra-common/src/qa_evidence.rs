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
    metadata_trace::{MetadataTraceEventDataV1, MetadataTraceSegmentStatusV1, MetadataTraceV1},
    qa_fault_injection::{
        qa_fault_point_descriptor, DeterministicQaFaultScheduler, QaFaultAction,
        QaFaultEvidenceSidecar, QaFaultEvidenceSidecarRecord, QaFaultInjectionPlan,
        QaFaultInjectionPlanDigestError, QaFaultRecoveryClass,
    },
    qa_runtime_path::{evaluate_no_hidden_fallback, RuntimePathEvidence},
    qa_scenarios::{QaScenarioExpectedEvent, QaScenarioManifest},
    redaction::{is_sensitive_key, redact_auth_error, redact_url_segments_in_text, REDACTED},
};

/// Current QA evidence bundle schema version.
pub const QA_EVIDENCE_BUNDLE_SCHEMA_VERSION: u32 = 4;

/// Stable format label embedded in generated evidence bundles.
pub const QA_EVIDENCE_BUNDLE_FORMAT: &str = "palyra-qa-evidence-bundle";

const NORMALIZED_ABSOLUTE_PATH: &str = "<normalized:absolute_path>";

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
    /// Result outcome, or `None` when execution started but no durable result
    /// was observed (for example, because the process crashed before ACK).
    pub success: Option<bool>,
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
    /// Optional content digest. Arbitrary digests are normalized; an exact
    /// manifest-declared digest is retained as an auditable proof.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    /// Optional size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Typed, redaction-safe evidence for one planned fault activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct QaFaultInjectionEvidence {
    /// Canonical digest of the plan that selected this activation.
    pub plan_sha256: String,
    /// Reproduction seed from the validated plan.
    pub seed: u64,
    /// Stable activation id from the plan.
    pub activation_id: String,
    /// Exact namespaced registry point.
    pub point_id: String,
    /// One-based planned and observed occurrence.
    pub occurrence: u32,
    /// Typed action applied at the checkpoint.
    pub action: QaFaultAction,
    /// One-based ordering among distinct campaign activations.
    pub activation_sequence: u32,
    /// Complete bounded actor set that participated in the activation.
    pub actors: Vec<String>,
    /// Seeded release order; equals `actors` for a non-barrier action.
    pub release_order: Vec<String>,
    /// Recovery class recorded after activation, possibly by a restarted launch.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_class: Option<QaFaultRecoveryClass>,
    /// Stable bounded reason code accompanying recovery evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_reason_code: Option<String>,
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
    /// Typed activation and recovery evidence from the validated private sidecar.
    pub fault_injections: Vec<QaFaultInjectionEvidence>,
    /// Number of daemon restarts observed during this scenario campaign.
    pub daemon_restart_count: u32,
    /// Metadata-only proof of the runtime path used by the real run.
    pub runtime_path: Option<RuntimePathEvidence>,
    /// Always-on, validated metadata trace loaded from the real run.
    pub metadata_trace: Option<MetadataTraceV1>,
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
    pub fault_injections: Vec<QaFaultInjectionEvidence>,
    pub daemon_restart_count: u32,
    /// Validated runtime-path evidence retained without removing allowed fallbacks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime_path: Option<RuntimePathEvidence>,
    /// Validated metadata-only trace retained as hot-path QA evidence.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata_trace: Option<MetadataTraceV1>,
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
    pub observed_fault_activation_count: usize,
    pub daemon_restart_count: u32,
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

// One table intentionally spans run references, event payloads, and tool
// projections so normalization preserves cross-surface identity relationships.
#[derive(Debug, Default)]
struct IdentifierAliases {
    aliases: BTreeMap<String, String>,
    next_ordinal_by_role: BTreeMap<&'static str, u32>,
}

#[derive(Debug, Clone, Copy)]
enum IdentifierRole {
    Run,
    Session,
    Proposal,
    Approval,
    Request,
    SideEffect,
    Generic,
}

impl IdentifierRole {
    const fn label(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Session => "session",
            Self::Proposal => "proposal",
            Self::Approval => "approval",
            Self::Request => "request",
            Self::SideEffect => "side_effect",
            Self::Generic => "generic",
        }
    }
}

impl IdentifierAliases {
    fn normalize_string(
        &mut self,
        role: IdentifierRole,
        value: &str,
        report: &mut QaEvidenceRedactionReport,
    ) -> String {
        report.normalized_identifiers += 1;
        let alias = self.alias_for(role, format!("string:{value}"));
        format!("<normalized:id:{alias}>")
    }

    fn normalize_value(
        &mut self,
        role: IdentifierRole,
        value: &Value,
        report: &mut QaEvidenceRedactionReport,
    ) -> Value {
        match value {
            Value::Null => Value::Null,
            Value::String(value) => Value::String(self.normalize_string(role, value, report)),
            Value::Number(value) => {
                report.normalized_identifiers += 1;
                let alias = self.alias_for(role, format!("number:{value}"));
                Value::String(format!("<normalized:id:{alias}>"))
            }
            other => {
                report.normalized_identifiers += 1;
                let alias = self.alias_for(role, format!("json:{other}"));
                Value::String(format!("<normalized:id:{alias}>"))
            }
        }
    }

    fn alias_for(&mut self, role: IdentifierRole, value: String) -> String {
        let role_label = role.label();
        let identity = format!("{role_label}:{value}");
        if let Some(alias) = self.aliases.get(identity.as_str()) {
            return alias.clone();
        }

        let ordinal = self.next_ordinal_by_role.entry(role_label).or_default();
        *ordinal = ordinal.saturating_add(1);
        let alias = format!("{role_label}:{ordinal}");
        self.aliases.insert(identity, alias.clone());
        alias
    }
}

#[derive(Debug, Clone)]
struct ObservedEvidence {
    final_answer: Option<String>,
    terminal_state: Option<String>,
    public_event_sequence: Vec<String>,
    tape_event_sequence: Vec<String>,
    event_counts: BTreeMap<String, usize>,
    tool_calls: Vec<QaToolCallEvidence>,
    tool_call_issues: Vec<QaEvidenceIssue>,
    artifacts: Vec<QaArtifactEvidence>,
}

#[derive(Debug, Clone, Default)]
struct ToolCallDerivation {
    calls: Vec<QaToolCallEvidence>,
    issues: Vec<QaEvidenceIssue>,
}

/// Projects a validated private sidecar into redaction-safe typed evidence.
///
/// # Errors
/// Returns an error only if the validated plan cannot be canonicalized.
pub fn qa_fault_injection_evidence_from_sidecar(
    sidecar: &QaFaultEvidenceSidecar,
    plan: &QaFaultInjectionPlan,
) -> Result<Vec<QaFaultInjectionEvidence>, QaFaultInjectionPlanDigestError> {
    let plan_sha256 = plan.canonical_sha256()?;
    let mut evidence_by_id = BTreeMap::<String, QaFaultInjectionEvidence>::new();
    for record in sidecar.records() {
        match record {
            QaFaultEvidenceSidecarRecord::LaunchLoaded(_) => {}
            QaFaultEvidenceSidecarRecord::CheckpointObserved(_) => {}
            QaFaultEvidenceSidecarRecord::BarrierJoined(_) => {}
            QaFaultEvidenceSidecarRecord::BarrierReleased(_) => {}
            QaFaultEvidenceSidecarRecord::RuleActivated(activation) => {
                evidence_by_id.insert(
                    activation.activation_id.clone(),
                    QaFaultInjectionEvidence {
                        plan_sha256: plan_sha256.clone(),
                        seed: plan.seed,
                        activation_id: activation.activation_id.clone(),
                        point_id: activation.point_id.clone(),
                        occurrence: activation.occurrence,
                        action: activation.action.clone(),
                        activation_sequence: activation.activation_sequence,
                        actors: activation.actors.clone(),
                        release_order: activation.release_order.clone(),
                        recovery_class: None,
                        recovery_reason_code: None,
                    },
                );
            }
            QaFaultEvidenceSidecarRecord::RecoveryRecorded(recovery) => {
                if let Some(evidence) = evidence_by_id.get_mut(recovery.activation_id.as_str()) {
                    evidence.recovery_class = Some(recovery.recovery_class);
                    evidence.recovery_reason_code = Some(recovery.reason_code.clone());
                }
            }
        }
    }
    let mut evidence = evidence_by_id.into_values().collect::<Vec<_>>();
    evidence.sort_by_key(|activation| activation.activation_sequence);
    Ok(evidence)
}

/// Builds a redacted evidence bundle and evaluates scenario assertions.
#[must_use]
pub fn build_qa_evidence_bundle(
    manifest: &QaScenarioManifest,
    input: QaEvidenceBuildInput,
) -> QaEvidenceBundle {
    let mut redaction = QaEvidenceRedactionReport::default();
    let mut identifier_aliases = IdentifierAliases::default();
    let normalized_run_id = input.run_id.as_deref().map(|run_id| {
        identifier_aliases.normalize_string(IdentifierRole::Run, run_id, &mut redaction)
    });
    let normalized_session_id = input.session_id.as_deref().map(|session_id| {
        identifier_aliases.normalize_string(IdentifierRole::Session, session_id, &mut redaction)
    });
    let mut tape_events = input.tape_events.iter().collect::<Vec<_>>();
    tape_events.sort_by_key(|event| event.seq);
    let redacted_tape = tape_events
        .into_iter()
        .map(|event| QaRunTapeEvent {
            seq: event.seq,
            event_type: event.event_type.clone(),
            payload: normalize_evidence_value(
                &event.payload,
                &mut redaction,
                &mut identifier_aliases,
            ),
        })
        .collect::<Vec<_>>();
    let public_events = input
        .public_events
        .iter()
        .map(|event| QaPublicEventEvidence {
            event_type: event.event_type.clone(),
            payload: normalize_evidence_value(
                &event.payload,
                &mut redaction,
                &mut identifier_aliases,
            ),
        })
        .collect::<Vec<_>>();
    let transcript = input
        .transcript
        .iter()
        .map(|message| QaTranscriptMessage {
            role: message.role.clone(),
            content: redact_evidence_text(message.content.as_str(), &mut redaction),
        })
        .collect::<Vec<_>>();
    let artifacts = input
        .artifacts
        .iter()
        .map(|artifact| {
            let verified_manifest_digest = artifact.present
                && manifest.artifacts.iter().any(|expected| {
                    expected.path.as_str() == artifact.path.as_str()
                        && expected.kind.as_str() == artifact.kind.as_str()
                        && expected.sha256.as_deref() == artifact.sha256.as_deref()
                        && expected.sha256.is_some()
                });
            QaArtifactEvidence {
                path: normalize_artifact_path(artifact.path.as_str(), &mut redaction),
                kind: artifact.kind.clone(),
                present: artifact.present,
                sha256: artifact.sha256.as_ref().map(|digest| {
                    if verified_manifest_digest {
                        digest.clone()
                    } else {
                        redaction.normalized_hashes += 1;
                        "<normalized:hash>".to_owned()
                    }
                }),
                size_bytes: artifact.size_bytes,
            }
        })
        .collect::<Vec<_>>();
    let observed = ObservedEvidence::from_input(&input);
    let checks =
        normalize_evidence_checks(evaluate_checks(manifest, &observed, &input), &mut redaction);
    let tool_calls =
        normalize_tool_calls(&observed.tool_calls, &mut redaction, &mut identifier_aliases);
    let fault_injections =
        normalize_fault_injections(input.fault_injections.as_slice(), &mut redaction);
    let runtime_path = input
        .runtime_path
        .as_ref()
        .filter(|runtime_path| runtime_path.validate_shape().is_ok())
        .cloned();
    let metadata_trace =
        input.metadata_trace.as_ref().filter(|trace| trace.validate_shape().is_ok()).cloned();
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
            run_id: normalized_run_id,
            session_id: normalized_session_id,
            terminal_state: observed.terminal_state.clone(),
            final_answer: observed
                .final_answer
                .as_deref()
                .map(|answer| redact_evidence_text(answer, &mut redaction)),
        },
        transcript,
        public_events,
        redacted_tape,
        artifacts_index: artifacts,
        tool_calls,
        fault_injections,
        daemon_restart_count: input.daemon_restart_count,
        runtime_path,
        metadata_trace,
        checks,
        summary: QaEvidenceSummary {
            verdict,
            issue_count,
            check_count,
            observed_event_count: observed.observed_event_count(),
            observed_tool_call_count: observed.tool_calls.len(),
            artifact_count: observed.artifacts.len(),
            observed_fault_activation_count: input.fault_injections.len(),
            daemon_restart_count: input.daemon_restart_count,
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
        "fault_injections": bundle.fault_injections,
        "daemon_restart_count": bundle.daemon_restart_count,
        "runtime_path": bundle.runtime_path,
        "metadata_trace": bundle.metadata_trace,
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
        format!("- Fault activations: {}", bundle.summary.observed_fault_activation_count),
        format!("- Daemon restarts: {}", bundle.summary.daemon_restart_count),
    ];
    lines.push(String::new());
    lines.push("## Runtime Path".to_owned());
    lines.push(String::new());
    if let Some(runtime_path) = bundle.runtime_path.as_ref() {
        lines.push(format!("- Complete: `{}`", runtime_path.complete));
        lines.push(format!("- Runtime version: `{}`", runtime_path.runtime_version));
        lines.push(format!("- Runtime contract: `{}`", runtime_path.runtime_contract_version));
        lines.push(format!("- Runner version: `{}`", runtime_path.runner_version));
        lines.push(format!("- Provider lane: `{}`", runtime_path.provider_lane));
        lines.push(format!("- Attempt owner: `{}`", runtime_path.attempt_owner));
        lines.push(format!(
            "- Harness: `{}` (source `{}`, reason `{}`)",
            runtime_path.harness.id,
            runtime_path.harness.source_event,
            runtime_path.harness.reason_code
        ));
        lines.push(format!(
            "- Context engine: `{}` (source `{}`, reason `{}`)",
            runtime_path.context_engine.id,
            runtime_path.context_engine.source_event,
            runtime_path.context_engine.reason_code
        ));
        if let Some(mcp) = runtime_path.mcp_transport_mode.as_ref() {
            lines.push(format!(
                "- MCP transport: `{}` (source `{}`, reason `{}`)",
                mcp.id, mcp.source_event, mcp.reason_code
            ));
        } else {
            lines.push("- MCP transport: `none`".to_owned());
        }
        lines.push(format!("- Fallbacks: {}", runtime_path.fallback_count));
        if !runtime_path.fallbacks.is_empty() {
            lines.extend([
                String::new(),
                "### Runtime Fallbacks".to_owned(),
                String::new(),
                "| # | Component | From | To | Reason | Source |".to_owned(),
                "| ---: | --- | --- | --- | --- | --- |".to_owned(),
            ]);
            for (index, fallback) in runtime_path.fallbacks.iter().enumerate() {
                lines.push(format!(
                    "| {} | `{}` | `{}` | `{}` | `{}` | `{}` |",
                    index + 1,
                    fallback.component,
                    fallback.from.as_deref().unwrap_or("unknown"),
                    fallback.to,
                    fallback.reason_code,
                    fallback.source_event
                ));
            }
        }
    } else {
        lines.push("- Evidence: `not captured`".to_owned());
    }
    lines.push(String::new());
    lines.push("## Metadata Trace".to_owned());
    lines.push(String::new());
    if let Some(trace) = bundle.metadata_trace.as_ref() {
        let event_count = trace.segments.iter().map(|segment| segment.events.len()).sum::<usize>();
        let final_status =
            trace.segments.last().map_or("missing", |segment| segment.status.as_str());
        lines.push(format!("- Segments: {}", trace.segments.len()));
        lines.push(format!("- Events: {event_count}"));
        lines.push(format!("- Final segment status: `{final_status}`"));
    } else {
        lines.push("- Evidence: `not captured`".to_owned());
    }
    lines.extend([
        String::new(),
        "## Checks".to_owned(),
        String::new(),
        "| Check | Verdict | Issues |".to_owned(),
        "| --- | --- | --- |".to_owned(),
    ]);
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
        let mut tape_events = input.tape_events.clone();
        tape_events.sort_by_key(|event| event.seq);
        let public_event_sequence =
            input.public_events.iter().map(|event| event.event_type.clone()).collect::<Vec<_>>();
        let tape_event_sequence =
            tape_events.iter().map(|event| event.event_type.clone()).collect::<Vec<_>>();
        let event_counts = public_event_sequence.iter().chain(&tape_event_sequence).fold(
            BTreeMap::new(),
            |mut counts, event| {
                *counts.entry(event.clone()).or_insert(0) += 1;
                counts
            },
        );
        let has_durable_execution_attempts =
            tape_events.iter().any(|event| event.event_type == "tool_effect_started");
        let ToolCallDerivation { calls: tool_calls, issues: tool_call_issues } =
            if has_durable_execution_attempts || input.tool_calls.is_empty() {
                derive_tool_calls(input)
            } else {
                ToolCallDerivation { calls: input.tool_calls.clone(), issues: Vec::new() }
            };
        Self {
            final_answer,
            terminal_state: input.terminal_state.clone(),
            public_event_sequence,
            tape_event_sequence,
            event_counts,
            tool_calls,
            tool_call_issues,
            artifacts: input.artifacts.clone(),
        }
    }

    fn observed_event_count(&self) -> usize {
        self.public_event_sequence.len() + self.tape_event_sequence.len()
    }
}

fn evaluate_checks(
    manifest: &QaScenarioManifest,
    observed: &ObservedEvidence,
    input: &QaEvidenceBuildInput,
) -> Vec<QaEvidenceCheck> {
    let mut checks = vec![
        check_terminal_state(manifest, observed),
        check_final_answer(manifest, observed),
        check_required_events(manifest, observed),
        check_required_tool_calls(manifest, observed),
        check_fault_injection(manifest, input),
        check_forbidden_observations(manifest, observed),
        check_artifacts_and_fake_progress(manifest, observed),
        check_backend_attestation_manifests(input),
    ];
    let mut supplemental_index = 1;
    if let Some(runtime_path) = check_runtime_path(manifest, input) {
        checks.insert(supplemental_index, runtime_path);
        supplemental_index += 1;
    }
    if let Some(metadata_trace) = check_metadata_trace(manifest, input) {
        checks.insert(supplemental_index, metadata_trace);
    }
    checks
}

fn check_metadata_trace(
    manifest: &QaScenarioManifest,
    input: &QaEvidenceBuildInput,
) -> Option<QaEvidenceCheck> {
    let required = manifest.expect.runtime_path.is_some();
    let Some(trace) = input.metadata_trace.as_ref() else {
        return required.then(|| {
            check(
                "metadata_trace",
                vec![QaEvidenceIssue {
                    code: "metadata_trace_evidence_missing".to_owned(),
                    path: "$.metadata_trace".to_owned(),
                    message: "scenario requires the real run's always-on metadata trace".to_owned(),
                    expected: Some("valid complete metadata trace".to_owned()),
                    actual: Some("missing".to_owned()),
                }],
            )
        });
    };

    let mut issues = Vec::new();
    if let Err(error) = trace.validate_shape() {
        issues.push(QaEvidenceIssue {
            code: error.code().to_owned(),
            path: error.path().strip_prefix('$').map_or_else(
                || "$.metadata_trace".to_owned(),
                |suffix| format!("$.metadata_trace{suffix}"),
            ),
            message: "metadata trace failed strict contract validation".to_owned(),
            expected: Some("valid bounded metadata trace".to_owned()),
            actual: Some("invalid".to_owned()),
        });
    } else if required {
        let observed_kinds = trace
            .segments
            .iter()
            .flat_map(|segment| segment.events.iter().map(|event| event.kind()))
            .collect::<BTreeSet<_>>();
        for required_kind in [
            "run_started",
            "runtime_selected",
            "context_assembled",
            "provider_attempt",
            "terminalization",
        ] {
            if !observed_kinds.contains(required_kind) {
                issues.push(QaEvidenceIssue {
                    code: "metadata_trace_hot_path_event_missing".to_owned(),
                    path: "$.metadata_trace.segments".to_owned(),
                    message: "metadata trace is missing a required hot-path event".to_owned(),
                    expected: Some(required_kind.to_owned()),
                    actual: Some("missing".to_owned()),
                });
            }
        }
        if !trace.segments.last().is_some_and(|segment| {
            segment.status == MetadataTraceSegmentStatusV1::Complete
                && segment.events.last().is_some_and(|event| {
                    matches!(event.event, MetadataTraceEventDataV1::Terminalization(_))
                })
        }) {
            issues.push(QaEvidenceIssue {
                code: "metadata_trace_terminal_evidence_missing".to_owned(),
                path: "$.metadata_trace.segments".to_owned(),
                message: "terminal QA runs require a complete final trace segment".to_owned(),
                expected: Some("complete segment ending in terminalization".to_owned()),
                actual: Some("not complete".to_owned()),
            });
        }
    }
    Some(check("metadata_trace", issues))
}

fn check_runtime_path(
    manifest: &QaScenarioManifest,
    input: &QaEvidenceBuildInput,
) -> Option<QaEvidenceCheck> {
    let expectation = manifest.expect.runtime_path.as_ref();
    let evidence = input.runtime_path.as_ref();
    if expectation.is_none() && evidence.is_none() {
        return None;
    }

    let Some(evidence) = evidence else {
        return Some(check(
            "runtime_path",
            vec![QaEvidenceIssue {
                code: "runtime_path_evidence_missing".to_owned(),
                path: "$.runtime_path".to_owned(),
                message: "scenario requires runtime-path evidence from the real run".to_owned(),
                expected: Some("complete runtime-path evidence".to_owned()),
                actual: Some("missing".to_owned()),
            }],
        ));
    };

    let evaluated = if let Some(expectation) = expectation {
        evaluate_no_hidden_fallback(expectation, evidence)
    } else {
        evidence.validate_shape().map(|()| Vec::new())
    };
    let issues = match evaluated {
        Ok(mismatches) => mismatches
            .into_iter()
            .map(|mismatch| QaEvidenceIssue {
                code: mismatch.code,
                path: runtime_path_bundle_path(mismatch.path.as_str()),
                message: "runtime-path evidence does not match the exact scenario expectation"
                    .to_owned(),
                expected: Some(mismatch.expected),
                actual: Some(mismatch.actual),
            })
            .collect(),
        Err(error) => vec![QaEvidenceIssue {
            code: error.code().to_owned(),
            path: runtime_path_bundle_path(error.path()),
            message: "runtime-path evidence failed bounded contract validation".to_owned(),
            expected: Some("valid runtime-path evidence".to_owned()),
            actual: Some("invalid".to_owned()),
        }],
    };
    Some(check("runtime_path", issues))
}

fn runtime_path_bundle_path(path: &str) -> String {
    path.strip_prefix('$')
        .map_or_else(|| "$.runtime_path".to_owned(), |suffix| format!("$.runtime_path{suffix}"))
}

fn check_fault_injection(
    manifest: &QaScenarioManifest,
    input: &QaEvidenceBuildInput,
) -> QaEvidenceCheck {
    let mut issues = Vec::new();
    let Some(plan) = manifest.fault_injection.as_ref() else {
        for (index, activation) in input.fault_injections.iter().enumerate() {
            issues.push(QaEvidenceIssue {
                code: "unplanned_fault_activation".to_owned(),
                path: format!("$.fault_injections[{index}]"),
                message: format!(
                    "fault activation `{}` was observed without a scenario fault plan",
                    activation.activation_id
                ),
                expected: Some("no fault activation".to_owned()),
                actual: Some(activation.activation_id.clone()),
            });
        }
        if input.daemon_restart_count != 0 {
            issues.push(QaEvidenceIssue {
                code: "unexpected_daemon_restart".to_owned(),
                path: "$.daemon_restart_count".to_owned(),
                message: "daemon restarted during a scenario without fault expectations".to_owned(),
                expected: Some("0".to_owned()),
                actual: Some(input.daemon_restart_count.to_string()),
            });
        }
        return check("fault_injection", issues);
    };

    let expected_plan_sha256 = match plan.canonical_sha256() {
        Ok(digest) => Some(digest),
        Err(error) => {
            issues.push(QaEvidenceIssue {
                code: "fault_plan_digest_unavailable".to_owned(),
                path: "$.fault_injection".to_owned(),
                message: error.to_string(),
                expected: Some("valid canonical fault plan".to_owned()),
                actual: None,
            });
            None
        }
    };
    let planned_by_id = plan
        .activations
        .iter()
        .map(|activation| (activation.id.as_str(), activation))
        .collect::<BTreeMap<_, _>>();
    let mut observed_by_id = BTreeMap::<&str, Vec<(usize, &QaFaultInjectionEvidence)>>::new();
    let mut observed_sequences = BTreeMap::<u32, &str>::new();

    for (index, observed) in input.fault_injections.iter().enumerate() {
        let path = format!("$.fault_injections[{index}]");
        observed_by_id.entry(observed.activation_id.as_str()).or_default().push((index, observed));
        if observed.activation_sequence == 0 {
            issues.push(QaEvidenceIssue {
                code: "invalid_fault_activation_sequence".to_owned(),
                path: format!("{path}.activation_sequence"),
                message: "fault activation_sequence must be one-based".to_owned(),
                expected: Some("positive sequence".to_owned()),
                actual: Some("0".to_owned()),
            });
        } else if let Some(existing) =
            observed_sequences.insert(observed.activation_sequence, observed.activation_id.as_str())
        {
            issues.push(QaEvidenceIssue {
                code: "duplicate_fault_activation_sequence".to_owned(),
                path: format!("{path}.activation_sequence"),
                message: format!(
                    "activation_sequence {} is shared with `{existing}`",
                    observed.activation_sequence
                ),
                expected: Some("unique sequence".to_owned()),
                actual: Some(observed.activation_sequence.to_string()),
            });
        }
        if expected_plan_sha256.as_deref().is_some_and(|digest| observed.plan_sha256 != digest) {
            issues.push(QaEvidenceIssue {
                code: "fault_plan_digest_mismatch".to_owned(),
                path: format!("{path}.plan_sha256"),
                message: "activation evidence belongs to a different fault plan".to_owned(),
                expected: expected_plan_sha256.clone(),
                actual: Some(observed.plan_sha256.clone()),
            });
        }
        if observed.seed != plan.seed {
            issues.push(QaEvidenceIssue {
                code: "fault_seed_mismatch".to_owned(),
                path: format!("{path}.seed"),
                message: "activation evidence uses a different reproduction seed".to_owned(),
                expected: Some(plan.seed.to_string()),
                actual: Some(observed.seed.to_string()),
            });
        }
        let Some(planned) = planned_by_id.get(observed.activation_id.as_str()) else {
            issues.push(QaEvidenceIssue {
                code: "unplanned_fault_activation".to_owned(),
                path: path.clone(),
                message: format!(
                    "fault activation `{}` is not declared by the scenario plan",
                    observed.activation_id
                ),
                expected: Some("planned activation id".to_owned()),
                actual: Some(observed.activation_id.clone()),
            });
            continue;
        };
        if observed.point_id != planned.point_id
            || observed.occurrence != planned.occurrence
            || observed.action != planned.action
        {
            issues.push(QaEvidenceIssue {
                code: "fault_activation_contract_mismatch".to_owned(),
                path: path.clone(),
                message: "observed point, occurrence, or action differs from the plan".to_owned(),
                expected: Some(format!(
                    "{} occurrence {} action {}",
                    planned.point_id,
                    planned.occurrence,
                    planned.action.kind().as_str()
                )),
                actual: Some(format!(
                    "{} occurrence {} action {}",
                    observed.point_id,
                    observed.occurrence,
                    observed.action.kind().as_str()
                )),
            });
        }
        let unique_actors = observed.actors.iter().collect::<BTreeSet<_>>();
        let actors_valid = !observed.actors.is_empty()
            && unique_actors.len() == observed.actors.len()
            && observed.actors.iter().all(|actor| fault_label_is_valid(actor, true));
        let schedule_valid = match planned.action {
            QaFaultAction::Barrier { participants } => {
                observed.actors.len() == usize::from(participants)
                    && DeterministicQaFaultScheduler::new(plan.seed)
                        .release_order(planned, observed.actors.as_slice())
                        .is_ok_and(|expected| expected == observed.release_order)
            }
            _ => {
                observed.actors.len() == 1
                    && observed.release_order == observed.actors
                    && planned.actor.as_deref().is_none_or(|actor| {
                        observed.actors.first().is_some_and(|actual| actual == actor)
                    })
            }
        };
        if !actors_valid || !schedule_valid {
            issues.push(QaEvidenceIssue {
                code: "fault_actor_schedule_invalid".to_owned(),
                path: format!("{path}.actors"),
                message: "activation actors or seeded release order violate the plan".to_owned(),
                expected: Some("bounded actors in deterministic release order".to_owned()),
                actual: Some(format!("{} actor(s)", observed.actors.len())),
            });
        }
        match (observed.recovery_class, observed.recovery_reason_code.as_deref()) {
            (Some(recovery), Some(reason)) => {
                if !fault_label_is_valid(reason, false)
                    || qa_fault_point_descriptor(planned.point_id.as_str())
                        .is_some_and(|descriptor| !descriptor.supports_recovery(recovery))
                {
                    issues.push(QaEvidenceIssue {
                        code: "fault_recovery_evidence_invalid".to_owned(),
                        path: format!("{path}.recovery_class"),
                        message: "recovery class or reason code is invalid for the fault point"
                            .to_owned(),
                        expected: Some(
                            "registered recovery class and bounded reason code".to_owned(),
                        ),
                        actual: Some(recovery.as_str().to_owned()),
                    });
                }
            }
            (Some(_), None) | (None, Some(_)) => issues.push(QaEvidenceIssue {
                code: "fault_recovery_evidence_incomplete".to_owned(),
                path: path.clone(),
                message: "recovery class and reason code must be recorded together".to_owned(),
                expected: Some("recovery_class plus recovery_reason_code".to_owned()),
                actual: Some("partial recovery evidence".to_owned()),
            }),
            (None, None) => {}
        }
    }

    for (activation_id, observations) in &observed_by_id {
        if observations.len() > 1 {
            issues.push(QaEvidenceIssue {
                code: "duplicate_fault_activation".to_owned(),
                path: "$.fault_injections".to_owned(),
                message: format!(
                    "fault activation `{activation_id}` was observed {} times",
                    observations.len()
                ),
                expected: Some("exactly once".to_owned()),
                actual: Some(observations.len().to_string()),
            });
        }
    }
    for planned in &plan.activations {
        if !observed_by_id.contains_key(planned.id.as_str()) {
            issues.push(QaEvidenceIssue {
                code: "fault_injection_not_activated".to_owned(),
                path: "$.fault_injections".to_owned(),
                message: format!("planned fault activation `{}` was not observed", planned.id),
                expected: Some(planned.id.clone()),
                actual: Some("missing".to_owned()),
            });
        }
    }

    if let Some(expected) = manifest.expect.fault_injection.as_ref() {
        for (index, expected_activation) in expected.activations.iter().enumerate() {
            if let Some((_, observed)) = observed_by_id
                .get(expected_activation.activation_id.as_str())
                .and_then(|observations| observations.first())
            {
                if observed.recovery_class != Some(expected_activation.recovery_class) {
                    issues.push(QaEvidenceIssue {
                        code: "fault_recovery_class_mismatch".to_owned(),
                        path: format!("$.expect.fault_injection.activations[{index}]"),
                        message: format!(
                            "fault activation `{}` did not prove the expected recovery",
                            expected_activation.activation_id
                        ),
                        expected: Some(expected_activation.recovery_class.as_str().to_owned()),
                        actual: observed
                            .recovery_class
                            .map(|recovery| recovery.as_str().to_owned())
                            .or_else(|| Some("missing".to_owned())),
                    });
                }
            }
        }
        if input.daemon_restart_count != expected.daemon_restarts {
            issues.push(QaEvidenceIssue {
                code: "fault_restart_count_mismatch".to_owned(),
                path: "$.daemon_restart_count".to_owned(),
                message: "observed daemon restart count differs from the scenario expectation"
                    .to_owned(),
                expected: Some(expected.daemon_restarts.to_string()),
                actual: Some(input.daemon_restart_count.to_string()),
            });
        }
    } else {
        issues.push(QaEvidenceIssue {
            code: "fault_expectations_missing".to_owned(),
            path: "$.expect.fault_injection".to_owned(),
            message: "scenario fault plan has no typed expectations".to_owned(),
            expected: Some("fault expectations".to_owned()),
            actual: Some("missing".to_owned()),
        });
    }

    let expected_sequences = (1..=input.fault_injections.len())
        .filter_map(|sequence| u32::try_from(sequence).ok())
        .collect::<Vec<_>>();
    let actual_sequences = observed_sequences.keys().copied().collect::<Vec<_>>();
    if actual_sequences != expected_sequences {
        issues.push(QaEvidenceIssue {
            code: "fault_activation_sequence_mismatch".to_owned(),
            path: "$.fault_injections".to_owned(),
            message: "fault activation sequences are not unique and contiguous".to_owned(),
            expected: Some(format!("{expected_sequences:?}")),
            actual: Some(format!("{actual_sequences:?}")),
        });
    }

    check("fault_injection", issues)
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
    let mut issues = observed.tool_call_issues.clone();
    for (index, expected_tool) in manifest.expect.tool_calls.iter().enumerate() {
        let matching_calls = observed
            .tool_calls
            .iter()
            .filter(|tool| token_matches(expected_tool.name.as_str(), tool.name.as_str()))
            .collect::<Vec<_>>();
        let expected_success = if manifest.schema_version >= 4 {
            expected_tool.success
        } else {
            Some(expected_tool.success.unwrap_or(true))
        };
        let matching_outcome_count = matching_calls
            .iter()
            .filter(|tool| expected_success.is_none_or(|expected| tool.success == Some(expected)))
            .count();
        let min_count = expected_tool.min_count.unwrap_or(1) as usize;
        if matching_outcome_count < min_count {
            let code = if matching_calls.len() >= min_count {
                if expected_success == Some(true) {
                    "tool_call_failed"
                } else {
                    "tool_call_unexpected_success"
                }
            } else {
                "missing_tool_call"
            };
            issues.push(QaEvidenceIssue {
                code: code.to_owned(),
                path: format!("$.expect.tool_calls[{index}]"),
                message: format!(
                    "expected at least {min_count} `{}` tool call(s) with {}, observed {} matching outcome(s) of {} named call(s)",
                    expected_tool.name,
                    expected_success.map_or_else(
                        || "any durable result state".to_owned(),
                        |success| format!("success={success}"),
                    ),
                    matching_outcome_count,
                    matching_calls.len()
                ),
                expected: Some(expected_tool.name.clone()),
                actual: Some(matching_outcome_count.to_string()),
            });
        }
        if let Some(max_count) = expected_tool.max_count {
            if matching_calls.len() > max_count as usize {
                issues.push(QaEvidenceIssue {
                    code: "too_many_tool_calls".to_owned(),
                    path: format!("$.expect.tool_calls[{index}].max_count"),
                    message: format!(
                        "expected at most {max_count} `{}` tool call(s), observed {} across all outcomes",
                        expected_tool.name,
                        matching_calls.len()
                    ),
                    expected: Some(max_count.to_string()),
                    actual: Some(matching_calls.len().to_string()),
                });
            }
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
            .public_event_sequence
            .iter()
            .chain(&observed.tape_event_sequence)
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
        let matching = observed
            .artifacts
            .iter()
            .filter(|artifact| {
                artifact.present
                    && artifact.path == expected_path
                    && artifact.kind.as_str() == expected_kind
            })
            .collect::<Vec<_>>();
        if matching.is_empty() {
            issues.push(QaEvidenceIssue {
                code: "missing_artifact".to_owned(),
                path: format!("$.artifacts[{index}]"),
                message: format!(
                    "required `{expected_kind}` artifact `{expected_path}` is missing"
                ),
                expected: Some(expected_path.to_owned()),
                actual: Some("missing".to_owned()),
            });
            continue;
        }
        if let Some(expected_sha256) = expected_artifact.sha256.as_deref() {
            if !matching.iter().any(|artifact| artifact.sha256.as_deref() == Some(expected_sha256))
            {
                issues.push(QaEvidenceIssue {
                    code: "artifact_digest_mismatch".to_owned(),
                    path: format!("$.artifacts[{index}].sha256"),
                    message: format!(
                        "required `{expected_kind}` artifact `{expected_path}` has an unexpected content digest"
                    ),
                    expected: Some(expected_sha256.to_owned()),
                    actual: matching.first().and_then(|artifact| artifact.sha256.clone()),
                });
            }
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
    let mut issues = required_event_order_issues_for_surface(
        expected_events,
        &observed.public_event_sequence,
        "$.public_events",
    );
    issues.extend(required_event_order_issues_for_surface(
        expected_events,
        &observed.tape_event_sequence,
        "$.tape_events",
    ));
    issues
}

fn required_event_order_issues_for_surface(
    expected_events: &[QaScenarioExpectedEvent],
    observed_events: &[String],
    evidence_path: &str,
) -> Vec<QaEvidenceIssue> {
    let mut issues = Vec::new();
    let mut last_position = None;
    for expected_event in expected_events {
        let Some(position) =
            first_event_position(observed_events, expected_event.event_type.as_str())
        else {
            continue;
        };
        if let Some(previous) = last_position {
            if position < previous {
                issues.push(QaEvidenceIssue {
                    code: "event_order_mismatch".to_owned(),
                    path: evidence_path.to_owned(),
                    message: format!(
                        "event `{}` appeared before an earlier expected event on the same evidence surface",
                        expected_event.event_type,
                    ),
                    expected: Some(format!("expected event order within {evidence_path}")),
                    actual: Some(format!("position {position}")),
                });
            }
            last_position = Some(previous.max(position));
        } else {
            last_position = Some(position);
        }
    }
    issues
}

fn lifecycle_order_issues(observed: &ObservedEvidence) -> Vec<QaEvidenceIssue> {
    let queued = first_event_position(&observed.public_event_sequence, "run.queued");
    let started = first_event_position(&observed.public_event_sequence, "run.started");
    let terminal = ["run.completed", "run.failed", "run.cancelled"]
        .iter()
        .filter_map(|event| first_event_position(&observed.public_event_sequence, event))
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

fn derive_tool_calls(input: &QaEvidenceBuildInput) -> ToolCallDerivation {
    let mut ordered_tape = input.tape_events.iter().collect::<Vec<_>>();
    ordered_tape.sort_by_key(|event| event.seq);
    if ordered_tape.iter().any(|event| event.event_type == "tool_effect_started") {
        return derive_durable_tool_attempts(ordered_tape.as_slice());
    }

    let legacy_tape_results = derive_legacy_tool_results(ordered_tape.as_slice());
    if !legacy_tape_results.is_empty() {
        return ToolCallDerivation { calls: legacy_tape_results, issues: Vec::new() };
    }

    let calls = input
        .public_events
        .iter()
        .filter_map(|event| {
            let is_result = matches!(
                event.event_type.as_str(),
                "tool_result" | "tool.call.completed" | "tool.call.failed"
            );
            let tool_name = is_result
                .then(|| event.payload.get("tool_name").and_then(Value::as_str))
                .flatten()?;
            let success = event
                .payload
                .get("success")
                .and_then(Value::as_bool)
                .or_else(|| {
                    matches!(event.event_type.as_str(), "tool.call.completed").then_some(true)
                })
                .or_else(|| {
                    matches!(event.event_type.as_str(), "tool.call.failed").then_some(false)
                });
            Some(QaToolCallEvidence {
                name: tool_name.to_owned(),
                proposal_id: event
                    .payload
                    .get("proposal_id")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                success,
            })
        })
        .collect();
    ToolCallDerivation { calls, issues: Vec::new() }
}

fn derive_durable_tool_attempts(tape_events: &[&QaRunTapeEvent]) -> ToolCallDerivation {
    let mut names_by_proposal = BTreeMap::<String, String>::new();
    let mut derivation = ToolCallDerivation::default();
    for event in tape_events {
        match event.event_type.as_str() {
            "tool_proposal" => {
                let proposal_id = non_empty_payload_string(event, "proposal_id");
                let tool_name = non_empty_payload_string(event, "tool_name");
                if proposal_id.is_none() || tool_name.is_none() {
                    derivation.issues.push(malformed_tool_attempt_issue(
                        event,
                        "non-empty string `proposal_id` and `tool_name`",
                    ));
                }
                if let (Some(proposal_id), Some(tool_name)) = (proposal_id, tool_name) {
                    names_by_proposal.insert(proposal_id.to_owned(), tool_name.to_owned());
                }
            }
            "tool_effect_started" => {
                let proposal_id = non_empty_payload_string(event, "proposal_id");
                let tool_name = non_empty_payload_string(event, "tool_name");
                if proposal_id.is_none() || tool_name.is_none() {
                    derivation.issues.push(malformed_tool_attempt_issue(
                        event,
                        "non-empty string `proposal_id` and `tool_name`",
                    ));
                }
                // Preserve recoverable call surfaces after reporting malformed rows; dropping
                // them would let max-count and forbidden assertions pass open.
                let name = tool_name.or_else(|| {
                    proposal_id.and_then(|id| names_by_proposal.get(id).map(String::as_str))
                });
                let Some(name) = name else {
                    continue;
                };
                derivation.calls.push(QaToolCallEvidence {
                    name: name.to_owned(),
                    proposal_id: proposal_id.map(ToOwned::to_owned),
                    success: None,
                });
            }
            "tool_result" => {
                let proposal_id = non_empty_payload_string(event, "proposal_id");
                let success = event.payload.get("success").and_then(Value::as_bool);
                let malformed = proposal_id.is_none() || success.is_none();
                if malformed {
                    derivation.issues.push(malformed_tool_attempt_issue(
                        event,
                        "non-empty string `proposal_id` and boolean `success`",
                    ));
                }
                if let Some(proposal_id) = proposal_id {
                    if let Some(attempt) = derivation.calls.iter_mut().rev().find(|attempt| {
                        attempt.proposal_id.as_deref() == Some(proposal_id)
                            && attempt.success.is_none()
                    }) {
                        if let Some(success) = success {
                            attempt.success = Some(success);
                        }
                        continue;
                    }
                }

                // Denied calls have a durable result but deliberately never cross the
                // effect boundary. Preserve them alongside authoritative effect attempts.
                let name = proposal_id
                    .and_then(|id| names_by_proposal.get(id).map(String::as_str))
                    .or_else(|| non_empty_payload_string(event, "tool_name"));
                if let Some(name) = name {
                    derivation.calls.push(QaToolCallEvidence {
                        name: name.to_owned(),
                        proposal_id: proposal_id.map(ToOwned::to_owned),
                        success,
                    });
                } else if !malformed {
                    derivation.issues.push(malformed_tool_attempt_issue(
                        event,
                        "a matching proposal or non-empty string `tool_name`",
                    ));
                }
            }
            _ => {}
        }
    }
    derivation
}

fn non_empty_payload_string<'a>(event: &'a QaRunTapeEvent, field: &str) -> Option<&'a str> {
    event.payload.get(field).and_then(Value::as_str).filter(|value| !value.trim().is_empty())
}

fn malformed_tool_attempt_issue(event: &QaRunTapeEvent, expected: &str) -> QaEvidenceIssue {
    QaEvidenceIssue {
        code: "malformed_tool_attempt_evidence".to_owned(),
        path: "$.tape_events".to_owned(),
        message: format!(
            "malformed `{}` tool attempt evidence at tape sequence {}",
            event.event_type, event.seq
        ),
        expected: Some(expected.to_owned()),
        actual: Some("invalid or missing required payload fields".to_owned()),
    }
}

fn derive_legacy_tool_results(tape_events: &[&QaRunTapeEvent]) -> Vec<QaToolCallEvidence> {
    let mut names_by_proposal = BTreeMap::<String, String>::new();
    let mut calls = Vec::new();
    for event in tape_events {
        match event.event_type.as_str() {
            "tool_proposal" => {
                if let (Some(proposal_id), Some(tool_name)) = (
                    event.payload.get("proposal_id").and_then(Value::as_str),
                    event.payload.get("tool_name").and_then(Value::as_str),
                ) {
                    names_by_proposal.insert(proposal_id.to_owned(), tool_name.to_owned());
                }
            }
            "tool_result" => {
                let Some(proposal_id) = event.payload.get("proposal_id").and_then(Value::as_str)
                else {
                    continue;
                };
                let Some(success) = event.payload.get("success").and_then(Value::as_bool) else {
                    continue;
                };
                let Some(name) = names_by_proposal.get(proposal_id) else {
                    continue;
                };
                calls.push(QaToolCallEvidence {
                    name: name.clone(),
                    proposal_id: Some(proposal_id.to_owned()),
                    success: Some(success),
                });
            }
            _ => {}
        }
    }
    calls
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

fn normalize_evidence_checks(
    mut checks: Vec<QaEvidenceCheck>,
    report: &mut QaEvidenceRedactionReport,
) -> Vec<QaEvidenceCheck> {
    for check in &mut checks {
        for issue in &mut check.issues {
            issue.message = redact_evidence_text(issue.message.as_str(), report);
            if let Some(expected) = issue.expected.as_mut() {
                *expected = redact_evidence_text(expected.as_str(), report);
            }
            if let Some(actual) = issue.actual.as_mut() {
                *actual = redact_evidence_text(actual.as_str(), report);
            }
        }
    }
    checks
}

fn normalize_tool_calls(
    tool_calls: &[QaToolCallEvidence],
    report: &mut QaEvidenceRedactionReport,
    identifier_aliases: &mut IdentifierAliases,
) -> Vec<QaToolCallEvidence> {
    tool_calls
        .iter()
        .map(|tool_call| QaToolCallEvidence {
            name: tool_call.name.clone(),
            proposal_id: tool_call.proposal_id.as_deref().map(|proposal_id| {
                identifier_aliases.normalize_string(IdentifierRole::Proposal, proposal_id, report)
            }),
            success: tool_call.success,
        })
        .collect()
}

fn normalize_fault_injections(
    evidence: &[QaFaultInjectionEvidence],
    report: &mut QaEvidenceRedactionReport,
) -> Vec<QaFaultInjectionEvidence> {
    evidence
        .iter()
        .map(|activation| QaFaultInjectionEvidence {
            plan_sha256: if is_lowercase_sha256(activation.plan_sha256.as_str()) {
                activation.plan_sha256.clone()
            } else {
                report.redacted_fields += 1;
                REDACTED.to_owned()
            },
            seed: activation.seed,
            activation_id: normalize_fault_label(activation.activation_id.as_str(), false, report),
            point_id: normalize_fault_label(activation.point_id.as_str(), false, report),
            occurrence: activation.occurrence,
            action: activation.action.clone(),
            activation_sequence: activation.activation_sequence,
            actors: activation
                .actors
                .iter()
                .map(|actor| normalize_fault_label(actor.as_str(), true, report))
                .collect(),
            release_order: activation
                .release_order
                .iter()
                .map(|actor| normalize_fault_label(actor.as_str(), true, report))
                .collect(),
            recovery_class: activation.recovery_class,
            recovery_reason_code: activation
                .recovery_reason_code
                .as_deref()
                .map(|reason| normalize_fault_label(reason, false, report)),
        })
        .collect()
}

fn normalize_fault_label(
    value: &str,
    allow_uppercase: bool,
    report: &mut QaEvidenceRedactionReport,
) -> String {
    if fault_label_is_valid(value, allow_uppercase) {
        value.to_owned()
    } else {
        report.redacted_fields += 1;
        REDACTED.to_owned()
    }
}

fn fault_label_is_valid(value: &str, allow_uppercase: bool) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value.bytes().all(|byte| {
            (allow_uppercase && byte.is_ascii_uppercase())
                || byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || matches!(byte, b'.' | b'_' | b'-' | b':')
        })
}

fn is_lowercase_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn normalize_evidence_value(
    value: &Value,
    report: &mut QaEvidenceRedactionReport,
    identifier_aliases: &mut IdentifierAliases,
) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, child)| {
                    (key.clone(), normalize_object_child(key, child, report, identifier_aliases))
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(
            values
                .iter()
                .map(|child| normalize_evidence_value(child, report, identifier_aliases))
                .collect(),
        ),
        Value::String(text) => Value::String(redact_evidence_text(text.as_str(), report)),
        _ => value.clone(),
    }
}

fn normalize_object_child(
    key: &str,
    value: &Value,
    report: &mut QaEvidenceRedactionReport,
    identifier_aliases: &mut IdentifierAliases,
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
    if let Some(role) = identifier_role(normalized_key.as_str()) {
        return identifier_aliases.normalize_value(role, value, report);
    }
    if is_hash_key(normalized_key.as_str()) {
        report.normalized_hashes += 1;
        return Value::String("<normalized:hash>".to_owned());
    }
    if is_path_key(normalized_key.as_str()) {
        return normalize_path_value(value, report, identifier_aliases);
    }
    normalize_evidence_value(value, report, identifier_aliases)
}

fn normalized_scalar(value: &Value, text_value: &str, number_value: i64) -> Value {
    match value {
        Value::Number(_) => Value::Number(number_value.into()),
        Value::String(_) => Value::String(text_value.to_owned()),
        Value::Null => Value::Null,
        _ => Value::String(text_value.to_owned()),
    }
}

fn normalize_path_value(
    value: &Value,
    report: &mut QaEvidenceRedactionReport,
    identifier_aliases: &mut IdentifierAliases,
) -> Value {
    match value {
        Value::String(path) if path_is_absolute(path.as_str()) => {
            report.normalized_paths += 1;
            Value::String(NORMALIZED_ABSOLUTE_PATH.to_owned())
        }
        Value::String(path) => Value::String(redact_evidence_text(path.as_str(), report)),
        _ => normalize_evidence_value(value, report, identifier_aliases),
    }
}

fn is_timestamp_key(key: &str) -> bool {
    key.contains("timestamp")
        || key.contains("unix_ms")
        || key.ends_with("_at")
        || key.ends_with("_at_ms")
}

fn identifier_role(key: &str) -> Option<IdentifierRole> {
    match key {
        "run" | "run_id" => Some(IdentifierRole::Run),
        "session" | "session_id" => Some(IdentifierRole::Session),
        "proposal_id" => Some(IdentifierRole::Proposal),
        "approval_id" => Some(IdentifierRole::Approval),
        "request_id" => Some(IdentifierRole::Request),
        key if is_side_effect_identity_key(key) || key.contains("effect") => {
            Some(IdentifierRole::SideEffect)
        }
        "id" => Some(IdentifierRole::Generic),
        key if key.ends_with("_id") => Some(IdentifierRole::Generic),
        _ => None,
    }
}

fn is_side_effect_identity_key(key: &str) -> bool {
    matches!(
        key,
        "idempotency_key"
            | "idempotency_digest"
            | "side_effect_key"
            | "side_effect_identity"
            | "side_effect_digest"
    )
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
        NORMALIZED_ABSOLUTE_PATH.to_owned()
    } else {
        redact_evidence_text(path, report)
    }
}

fn path_is_absolute(path: &str) -> bool {
    Path::new(path).is_absolute() || absolute_path_prefix_len(path, 0).is_some()
}

fn redact_evidence_text(text: &str, report: &mut QaEvidenceRedactionReport) -> String {
    let url_redacted = redact_url_segments_in_text(text);
    let auth_redacted = redact_auth_error(url_redacted.as_str());
    normalize_absolute_paths_in_text(auth_redacted.as_str(), report)
}

fn normalize_absolute_paths_in_text(text: &str, report: &mut QaEvidenceRedactionReport) -> String {
    let mut output = String::with_capacity(text.len());
    let mut cursor = 0;
    while cursor < text.len() {
        if let Some(prefix_len) = absolute_path_prefix_len(text, cursor) {
            let path_end = absolute_path_end(text, cursor, prefix_len);
            output.push_str(NORMALIZED_ABSOLUTE_PATH);
            report.normalized_paths += 1;
            cursor = path_end;
            continue;
        }
        if let Some(url_end) = url_span_end(text, cursor) {
            output.push_str(&text[cursor..url_end]);
            cursor = url_end;
            continue;
        }
        let Some(ch) = text[cursor..].chars().next() else {
            break;
        };
        output.push(ch);
        cursor += ch.len_utf8();
    }
    output
}

fn absolute_path_prefix_len(text: &str, start: usize) -> Option<usize> {
    if !path_has_left_boundary(text, start) {
        return None;
    }
    let remaining = text.get(start..)?;
    let bytes = remaining.as_bytes();
    if bytes.first().is_some_and(|byte| byte.is_ascii_alphabetic()) && bytes.get(1) == Some(&b':') {
        if bytes.get(2).is_some_and(|byte| matches!(*byte, b'/' | b'\\')) {
            return Some(3);
        }
        if bytes.get(2).is_some_and(is_unquoted_path_component_byte) {
            return Some(2);
        }
    }
    if (remaining.starts_with("\\\\") || remaining.starts_with("//"))
        && bytes.get(2).is_some_and(|byte| {
            !byte.is_ascii_whitespace() && !matches!(*byte, b'/' | b'\\' | b'\'' | b'"' | b'`')
        })
    {
        return Some(2);
    }
    if remaining.starts_with('/') && bytes.get(1).is_some_and(is_unquoted_path_component_byte) {
        return Some(1);
    }
    if remaining.starts_with('\\') && bytes.get(1).is_some_and(is_unquoted_path_component_byte) {
        return Some(1);
    }
    None
}

fn is_unquoted_path_component_byte(byte: &u8) -> bool {
    !byte.is_ascii_whitespace()
        && !matches!(*byte, b'/' | b'\\' | b'\'' | b'"' | b'`' | b')' | b']' | b'}')
}

fn path_has_left_boundary(text: &str, start: usize) -> bool {
    if start == 0 {
        return true;
    }
    text.get(..start).and_then(|prefix| prefix.chars().next_back()).is_some_and(|ch| {
        ch.is_whitespace() || (!ch.is_alphanumeric() && !matches!(ch, '_' | '.' | '/' | '\\'))
    })
}

fn absolute_path_end(text: &str, start: usize, prefix_len: usize) -> usize {
    let quoted_by = text
        .get(..start)
        .and_then(|prefix| prefix.chars().next_back())
        .filter(|ch| matches!(ch, '"' | '\'' | '`'));
    let scan_start = start + prefix_len;
    let mut end = text.len();
    for (offset, ch) in text[scan_start..].char_indices() {
        let is_boundary = if let Some(quote) = quoted_by {
            ch == quote
        } else {
            ch.is_whitespace()
                || matches!(
                    ch,
                    '"' | '\''
                        | '`'
                        | '('
                        | ')'
                        | '['
                        | ']'
                        | '{'
                        | '}'
                        | '<'
                        | '>'
                        | ','
                        | ';'
                        | ':'
                        | '='
                        | '!'
                        | '?'
                        | '#'
                )
        };
        if is_boundary {
            end = scan_start + offset;
            break;
        }
    }
    if quoted_by.is_none() {
        while end > scan_start {
            let Some(ch) = text[..end].chars().next_back() else {
                break;
            };
            if ch != '.' {
                break;
            }
            end -= ch.len_utf8();
        }
    }
    end
}

fn url_span_end(text: &str, start: usize) -> Option<usize> {
    if !path_has_left_boundary(text, start) {
        return None;
    }
    let remaining = text.get(start..)?;
    let url_prefix_len = if remaining.starts_with("www.") {
        4
    } else {
        let scheme_end = remaining.find("://")?;
        let scheme = remaining.get(..scheme_end)?;
        if scheme.is_empty()
            || !scheme.as_bytes().first().is_some_and(|byte| byte.is_ascii_alphabetic())
            || !scheme
                .as_bytes()
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'+' | b'-' | b'.'))
        {
            return None;
        }
        scheme_end + 3
    };
    let url_body = remaining.get(url_prefix_len..)?;
    if url_body.is_empty() {
        return None;
    }
    let relative_end = url_body
        .char_indices()
        .find_map(|(offset, ch)| {
            (ch.is_whitespace() || matches!(ch, '"' | '\'' | '`'))
                .then_some(url_prefix_len + offset)
        })
        .unwrap_or(remaining.len());
    Some(start + relative_end)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::{
        metadata_trace::{
            metadata_trace_id_sha256, ContextAssembledMetadataV1, MetadataTraceEntrypointV1,
            MetadataTraceEventDataV1, MetadataTraceEventV1, MetadataTraceIdDomainV1,
            MetadataTraceProviderAttemptOutcomeV1, MetadataTraceRouteClassV1,
            MetadataTraceSchemaHashV1, MetadataTraceSegmentStatusV1, MetadataTraceSegmentV1,
            MetadataTraceTerminalOutcomeV1, MetadataTraceV1, ProviderAttemptMetadataV1,
            RunStartedMetadataV1, RuntimeSelectedMetadataV1, TerminalizationMetadataV1,
            METADATA_TRACE_SCHEMA_VERSION,
        },
        qa_runtime_path::{
            RuntimeFallbackEvidence, RuntimePathComponentEvidence,
            QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION,
        },
        qa_scenarios::parse_qa_scenario_manifest_yaml,
    };

    const BASIC_SCENARIO: &str = include_str!("../../../qa/scenarios/text_run_basic.yaml");
    const MALFORMED_RECOVERY_SCENARIO: &str =
        include_str!("../../../qa/scenarios/real_runtime/malformed_stream_recovery.yaml");
    const MARKDOWN_GOLDEN: &str =
        include_str!("../../../fixtures/golden/qa_evidence_report_basic.md");
    const FAULT_JSON_GOLDEN: &str =
        include_str!("../../../fixtures/golden/qa_evidence_fault_report_v4.json");
    const REQUIRED_TOOL_SCENARIO: &str = r#"
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
"#;
    const V4_ASSERTION_SCENARIO: &str = r#"
schema_version: 4
id: evidence.v4.assertions
area: tools
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: qa/fixtures/provider_basic.yaml
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
      max_count: 1
forbidden:
  tool_calls: []
  events: []
  artifacts: []
  claims: []
artifacts:
  - path: qa/reports/v4-assertion.json
    kind: report
    required: true
    sha256: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
maturity:
  labels: [p0, deterministic]
timeout:
  run_ms: 30000
"#;
    const FAULT_SCENARIO: &str = r#"
schema_version: 4
id: fault.tool.crash
area: tools
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: qa/fixtures/provider_basic.yaml
fault_injection:
  schema_version: 1
  format: palyra-qa-fault-injection-plan
  seed: 4242
  activations:
    - id: tool-crash
      point_id: tool.after_effect_before_ack
      actor: daemon
      occurrence: 1
      action:
        type: terminate_process
requires:
  capabilities: [qa_lab]
  tools: []
  fixtures: []
steps:
  - id: prompt
    action: user_prompt
    prompt: "Exercise recovery."
expect:
  terminal_state: completed
  final_answer:
    contains: ["done"]
  events: []
  tool_calls: []
  fault_injection:
    activations:
      - activation_id: tool-crash
        recovery_class: duplicate_suppressed
    daemon_restarts: 1
forbidden:
  tool_calls: []
  events: []
  artifacts: []
  claims: []
artifacts: []
maturity:
  labels: [p0, deterministic]
timeout:
  run_ms: 30000
"#;
    const RUNTIME_PATH_SCENARIO: &str = r#"
schema_version: 5
id: runtime.path.external
area: text
mode:
  runner: fixture
  deterministic: true
runner:
  provider_fixture: qa/fixtures/provider_basic.yaml
requires:
  capabilities: [qa_lab]
  tools: []
  fixtures: []
steps:
  - id: prompt
    action: user_prompt
    prompt: "Exercise the qualified runtime path."
expect:
  terminal_state: completed
  events: []
  tool_calls: []
  runtime_path:
    runtime_contract_version: runtime-contracts.v2
    provider_lane: fixture
    attempt_owner: external_harness
    harness_id: external_harness
    context_engine_id: context_engine_v2
    mcp_transport_mode: persistent
    max_fallback_count: 1
    allowed_fallback_reason_codes:
      - provider.failover.allowed
forbidden:
  tool_calls: []
  events: []
  artifacts: []
  claims: []
artifacts: []
maturity:
  labels: [p0, deterministic]
timeout:
  run_ms: 30000
"#;

    #[test]
    fn evidence_bundle_passes_basic_manifest() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let bundle = build_qa_evidence_bundle(&manifest, passing_input());

        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Passed);
        assert_eq!(bundle.summary.issue_count, 0);
        let run_alias = bundle.run.run_id.as_deref().expect("run id should be normalized");
        assert!(run_alias.starts_with("<normalized:id:"));
        assert_eq!(bundle.redacted_tape[0].payload["run_id"], run_alias);
        assert_eq!(qa_evidence_json_report(&bundle)["verdict"], "passed");
        assert_eq!(bundle.schema_version, 4);
    }

    #[test]
    fn exact_runtime_path_is_visible_in_bundle_json_and_markdown() {
        let manifest = parse_qa_scenario_manifest_yaml(RUNTIME_PATH_SCENARIO)
            .expect("schema-v5 runtime-path scenario should parse");
        let bundle = build_qa_evidence_bundle(&manifest, qualified_runtime_path_input());

        assert_eq!(runtime_path_check(&bundle).verdict, QaEvidenceVerdict::Passed);
        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Passed);
        assert_eq!(
            qa_evidence_json_report(&bundle)["runtime_path"]["harness"]["id"],
            "external_harness"
        );
        let markdown = qa_evidence_markdown_report(&bundle);
        assert!(markdown.contains("- Attempt owner: `external_harness`"));
        assert!(markdown.contains("- MCP transport: `persistent`"));
    }

    #[test]
    fn missing_and_incomplete_runtime_path_fail_v5_expectation() {
        let manifest = parse_qa_scenario_manifest_yaml(RUNTIME_PATH_SCENARIO)
            .expect("schema-v5 runtime-path scenario should parse");
        let missing = build_qa_evidence_bundle(
            &manifest,
            QaEvidenceBuildInput {
                terminal_state: Some("completed".to_owned()),
                ..QaEvidenceBuildInput::default()
            },
        );
        assert!(runtime_path_check(&missing)
            .issues
            .iter()
            .any(|issue| issue.code == "runtime_path_evidence_missing"));

        let mut incomplete_input = qualified_runtime_path_input();
        incomplete_input.runtime_path.as_mut().expect("runtime path should exist").complete = false;
        let incomplete = build_qa_evidence_bundle(&manifest, incomplete_input);
        assert!(runtime_path_check(&incomplete)
            .issues
            .iter()
            .any(|issue| issue.code == "runtime_path_evidence_incomplete"));
    }

    #[test]
    fn hidden_embedded_legacy_per_call_path_fails_exact_expectation() {
        let manifest = parse_qa_scenario_manifest_yaml(RUNTIME_PATH_SCENARIO)
            .expect("schema-v5 runtime-path scenario should parse");
        let mut input = qualified_runtime_path_input();
        let runtime_path = input.runtime_path.as_mut().expect("runtime path should exist");
        runtime_path.runtime_contract_version = "runtime-contracts.v1".to_owned();
        runtime_path.provider_lane = "live".to_owned();
        runtime_path.attempt_owner = "embedded_run_stream".to_owned();
        runtime_path.harness.id = "embedded_run_stream".to_owned();
        runtime_path.context_engine.id = "legacy_provider_input".to_owned();
        runtime_path.mcp_transport_mode =
            Some(runtime_path_component("per_call", "mcp.transport.per_call"));

        let bundle = build_qa_evidence_bundle(&manifest, input);
        let codes = runtime_path_check(&bundle)
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect::<BTreeSet<_>>();

        assert!(codes.contains("runtime_path_contract_version_mismatch"));
        assert!(codes.contains("runtime_path_provider_lane_mismatch"));
        assert!(codes.contains("runtime_path_attempt_owner_mismatch"));
        assert!(codes.contains("runtime_path_harness_mismatch"));
        assert!(codes.contains("runtime_path_context_engine_mismatch"));
        assert!(codes.contains("runtime_path_mcp_transport_mismatch"));
    }

    #[test]
    fn allowed_fallback_is_retained_but_unknown_reason_fails() {
        let manifest = parse_qa_scenario_manifest_yaml(RUNTIME_PATH_SCENARIO)
            .expect("schema-v5 runtime-path scenario should parse");
        let mut allowed_input = qualified_runtime_path_input();
        let runtime_path = allowed_input.runtime_path.as_mut().expect("runtime path should exist");
        runtime_path.fallbacks = vec![runtime_fallback("provider.failover.allowed")];
        runtime_path.fallback_count = 1;
        runtime_path.source_events.push("provider.recovery.decision".to_owned());

        let allowed = build_qa_evidence_bundle(&manifest, allowed_input.clone());
        assert_eq!(runtime_path_check(&allowed).verdict, QaEvidenceVerdict::Passed);
        assert_eq!(allowed.runtime_path.as_ref().map(|path| path.fallbacks.len()), Some(1));
        assert!(qa_evidence_markdown_report(&allowed).contains("`provider.failover.allowed`"));

        allowed_input.runtime_path.as_mut().expect("runtime path should exist").fallbacks[0]
            .reason_code = "provider.failover.unknown".to_owned();
        let unknown = build_qa_evidence_bundle(&manifest, allowed_input);
        assert!(runtime_path_check(&unknown)
            .issues
            .iter()
            .any(|issue| issue.code == "runtime_path_fallback_reason_not_allowed"));
        assert_eq!(unknown.runtime_path.as_ref().map(|path| path.fallbacks.len()), Some(1));
    }

    #[test]
    fn tool_call_max_count_rejects_duplicate_durable_attempts_with_the_same_outcome() {
        let manifest = parse_qa_scenario_manifest_yaml(V4_ASSERTION_SCENARIO)
            .expect("schema-v4 assertion scenario should parse");
        let mut input = v4_assertion_input();
        input.tape_events = vec![
            tool_attempt_event(1, "proposal-1"),
            tool_result_event(2, "proposal-1", true),
            tool_attempt_event(3, "proposal-1"),
            tool_result_event(4, "proposal-1", true),
        ];
        let bundle = build_qa_evidence_bundle(&manifest, input);

        assert!(bundle
            .checks
            .iter()
            .any(|check| { check.issues.iter().any(|issue| issue.code == "too_many_tool_calls") }));
        assert_eq!(bundle.summary.observed_tool_call_count, 2);
        assert_eq!(
            bundle.tool_calls.iter().map(|call| call.success).collect::<Vec<_>>(),
            [Some(true), Some(true)]
        );
    }

    #[test]
    fn malformed_duplicate_durable_attempt_fails_closed_and_still_counts() {
        let manifest = parse_qa_scenario_manifest_yaml(V4_ASSERTION_SCENARIO)
            .expect("schema-v4 assertion scenario should parse");
        let mut input = v4_assertion_input();
        input.tape_events = vec![
            tool_proposal_event(1, "proposal-1", "palyra.fs.read_file"),
            tool_attempt_event(2, "proposal-1"),
            QaRunTapeEvent {
                seq: 3,
                event_type: "tool_effect_started".to_owned(),
                payload: json!({"proposal_id": "proposal-1"}),
            },
        ];

        let bundle = build_qa_evidence_bundle(&manifest, input);
        let required_tool_calls = bundle
            .checks
            .iter()
            .find(|check| check.name == "required_tool_calls")
            .expect("required tool-call check should exist");

        assert!(required_tool_calls
            .issues
            .iter()
            .any(|issue| issue.code == "malformed_tool_attempt_evidence"));
        assert!(required_tool_calls.issues.iter().any(|issue| issue.code == "too_many_tool_calls"));
        assert_eq!(bundle.summary.observed_tool_call_count, 2);
    }

    #[test]
    fn malformed_durable_proposal_fails_required_tool_call_check() {
        let manifest = parse_qa_scenario_manifest_yaml(V4_ASSERTION_SCENARIO)
            .expect("schema-v4 assertion scenario should parse");
        let mut input = v4_assertion_input();
        input.tape_events = vec![
            QaRunTapeEvent {
                seq: 1,
                event_type: "tool_proposal".to_owned(),
                payload: json!({"proposal_id": "proposal-1"}),
            },
            tool_attempt_event(2, "proposal-1"),
        ];

        let bundle = build_qa_evidence_bundle(&manifest, input);
        let required_tool_calls = bundle
            .checks
            .iter()
            .find(|check| check.name == "required_tool_calls")
            .expect("required tool-call check should exist");

        assert!(required_tool_calls
            .issues
            .iter()
            .any(|issue| issue.code == "malformed_tool_attempt_evidence"));
        assert_eq!(bundle.summary.observed_tool_call_count, 1);
    }

    #[test]
    fn legacy_tool_proposal_without_result_does_not_satisfy_success_expectation() {
        let manifest = parse_qa_scenario_manifest_yaml(REQUIRED_TOOL_SCENARIO)
            .expect("legacy required-tool scenario should parse");
        let bundle = build_qa_evidence_bundle(
            &manifest,
            QaEvidenceBuildInput {
                terminal_state: Some("completed".to_owned()),
                tape_events: vec![QaRunTapeEvent {
                    seq: 1,
                    event_type: "tool_proposal".to_owned(),
                    payload: json!({
                        "proposal_id": "proposal-1",
                        "tool_name": "palyra.fs.read_file",
                    }),
                }],
                ..QaEvidenceBuildInput::default()
            },
        );

        let tool_check = bundle
            .checks
            .iter()
            .find(|check| check.name == "required_tool_calls")
            .expect("required tool-call check should exist");
        assert_eq!(tool_check.verdict, QaEvidenceVerdict::Failed);
        assert!(tool_check.issues.iter().any(|issue| issue.code == "missing_tool_call"));
        assert_eq!(bundle.summary.observed_tool_call_count, 0);
    }

    #[test]
    fn schema_v4_omitted_success_accepts_one_unknown_execution_attempt() {
        let manifest = parse_qa_scenario_manifest_yaml(V4_ASSERTION_SCENARIO)
            .expect("schema-v4 assertion scenario should parse");
        let mut input = v4_assertion_input();
        input.tape_events = vec![tool_attempt_event(1, "proposal-1")];

        let bundle = build_qa_evidence_bundle(&manifest, input);

        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Passed);
        assert_eq!(bundle.tool_calls[0].success, None);
    }

    #[test]
    fn durable_attempt_derivation_preserves_denied_calls_from_the_same_run() {
        let scenario = REQUIRED_TOOL_SCENARIO
            .replace(
                "      min_count: 1",
                "      min_count: 1\n    - name: palyra.fs.write_file\n      min_count: 1\n      success: false",
            )
            .replace(
                "tools: [palyra.fs.read_file]",
                "tools: [palyra.fs.read_file, palyra.fs.write_file]",
            );
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("mixed tool scenario should parse");
        let input = QaEvidenceBuildInput {
            terminal_state: Some("completed".to_owned()),
            tape_events: vec![
                tool_proposal_event(1, "allowed-proposal", "palyra.fs.read_file"),
                tool_attempt_event(2, "allowed-proposal"),
                tool_result_event(3, "allowed-proposal", true),
                tool_proposal_event(4, "denied-proposal", "palyra.fs.write_file"),
                tool_result_event(5, "denied-proposal", false),
            ],
            ..QaEvidenceBuildInput::default()
        };

        let bundle = build_qa_evidence_bundle(&manifest, input.clone());
        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Passed);
        assert_eq!(
            bundle
                .tool_calls
                .iter()
                .map(|call| (call.name.as_str(), call.success))
                .collect::<Vec<_>>(),
            [("palyra.fs.read_file", Some(true)), ("palyra.fs.write_file", Some(false)),]
        );

        let forbidden_scenario = scenario.replace(
            "forbidden:\n  tool_calls: []",
            "forbidden:\n  tool_calls: [palyra.fs.write_file]",
        );
        let forbidden_manifest = parse_qa_scenario_manifest_yaml(forbidden_scenario.as_str())
            .expect("forbidden mixed tool scenario should parse");
        let forbidden = build_qa_evidence_bundle(&forbidden_manifest, input);
        assert!(forbidden.checks.iter().any(|check| {
            check.issues.iter().any(|issue| issue.code == "unexpected_tool_call")
        }));
    }

    #[test]
    fn malformed_denied_result_fails_required_and_forbidden_tool_surfaces() {
        let scenario = REQUIRED_TOOL_SCENARIO.replace(
            "forbidden:\n  tool_calls: []",
            "forbidden:\n  tool_calls: [palyra.fs.write_file]",
        );
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("forbidden tool scenario should parse");
        let input = QaEvidenceBuildInput {
            terminal_state: Some("completed".to_owned()),
            tape_events: vec![
                tool_proposal_event(1, "allowed-proposal", "palyra.fs.read_file"),
                tool_attempt_event(2, "allowed-proposal"),
                tool_result_event(3, "allowed-proposal", true),
                tool_proposal_event(4, "denied-proposal", "palyra.fs.write_file"),
                QaRunTapeEvent {
                    seq: 5,
                    event_type: "tool_result".to_owned(),
                    payload: json!({"proposal_id": "denied-proposal"}),
                },
            ],
            ..QaEvidenceBuildInput::default()
        };

        let bundle = build_qa_evidence_bundle(&manifest, input);
        let required_tool_calls = bundle
            .checks
            .iter()
            .find(|check| check.name == "required_tool_calls")
            .expect("required tool-call check should exist");
        let forbidden_observations = bundle
            .checks
            .iter()
            .find(|check| check.name == "forbidden_observations")
            .expect("forbidden-observations check should exist");

        assert!(required_tool_calls
            .issues
            .iter()
            .any(|issue| issue.code == "malformed_tool_attempt_evidence"));
        assert!(forbidden_observations
            .issues
            .iter()
            .any(|issue| issue.code == "unexpected_tool_call"));
        assert!(bundle
            .tool_calls
            .iter()
            .any(|call| call.name == "palyra.fs.write_file" && call.success.is_none()));
    }

    #[test]
    fn artifact_assertions_require_exact_path_kind_and_digest() {
        let expected_digest = "a".repeat(64);
        let manifest = parse_qa_scenario_manifest_yaml(V4_ASSERTION_SCENARIO)
            .expect("schema-v4 artifact digest scenario should parse");
        let passing = v4_assertion_input();
        let passing_bundle = build_qa_evidence_bundle(&manifest, passing.clone());
        assert_eq!(passing_bundle.summary.verdict, QaEvidenceVerdict::Passed);
        assert_eq!(passing_bundle.artifacts_index[0].sha256, Some(expected_digest.clone()));
        assert_eq!(passing_bundle.redaction.normalized_hashes, 0);

        for (path, kind, digest, expected_code) in [
            ("qa/reports/other.json", "report", expected_digest.as_str(), "missing_artifact"),
            (
                "qa/reports/v4-assertion.json",
                "workspace",
                expected_digest.as_str(),
                "missing_artifact",
            ),
            (
                "qa/reports/v4-assertion.json",
                "report",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "artifact_digest_mismatch",
            ),
        ] {
            let mut input = passing.clone();
            input.artifacts[0].path = path.to_owned();
            input.artifacts[0].kind = kind.to_owned();
            input.artifacts[0].sha256 = Some(digest.to_owned());
            let bundle = build_qa_evidence_bundle(&manifest, input);
            assert!(bundle
                .checks
                .iter()
                .any(|check| { check.issues.iter().any(|issue| issue.code == expected_code) }));
        }
    }

    #[test]
    fn exact_fault_activation_recovery_and_restart_evidence_passes() {
        let manifest =
            parse_qa_scenario_manifest_yaml(FAULT_SCENARIO).expect("fault scenario should parse");
        let bundle = build_qa_evidence_bundle(&manifest, passing_fault_input(&manifest));

        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Passed);
        assert_eq!(bundle.summary.observed_fault_activation_count, 1);
        assert_eq!(bundle.summary.daemon_restart_count, 1);
        assert_eq!(fault_check(&bundle).verdict, QaEvidenceVerdict::Passed);
        let actual_report = qa_evidence_json_report(&bundle);
        let expected_report: Value =
            serde_json::from_str(FAULT_JSON_GOLDEN).expect("fault report golden should parse");
        assert_eq!(actual_report, expected_report);
    }

    #[test]
    fn missing_duplicate_and_unplanned_fault_activations_fail_strictly() {
        let manifest =
            parse_qa_scenario_manifest_yaml(FAULT_SCENARIO).expect("fault scenario should parse");
        let missing = build_qa_evidence_bundle(
            &manifest,
            QaEvidenceBuildInput {
                terminal_state: Some("completed".to_owned()),
                final_answer: Some("done".to_owned()),
                daemon_restart_count: 1,
                ..QaEvidenceBuildInput::default()
            },
        );
        assert!(fault_check(&missing)
            .issues
            .iter()
            .any(|issue| issue.code == "fault_injection_not_activated"));

        let mut duplicate_input = passing_fault_input(&manifest);
        let mut duplicate = duplicate_input.fault_injections[0].clone();
        duplicate.activation_sequence = 2;
        duplicate_input.fault_injections.push(duplicate);
        let duplicate = build_qa_evidence_bundle(&manifest, duplicate_input);
        assert!(fault_check(&duplicate)
            .issues
            .iter()
            .any(|issue| issue.code == "duplicate_fault_activation"));

        let mut unplanned_input = passing_fault_input(&manifest);
        let mut unplanned = unplanned_input.fault_injections[0].clone();
        unplanned.activation_id = "unplanned".to_owned();
        unplanned.activation_sequence = 2;
        unplanned_input.fault_injections.push(unplanned);
        let unplanned = build_qa_evidence_bundle(&manifest, unplanned_input);
        assert!(fault_check(&unplanned)
            .issues
            .iter()
            .any(|issue| issue.code == "unplanned_fault_activation"));
    }

    #[test]
    fn wrong_fault_recovery_and_restart_count_fail() {
        let manifest =
            parse_qa_scenario_manifest_yaml(FAULT_SCENARIO).expect("fault scenario should parse");
        let mut input = passing_fault_input(&manifest);
        input.fault_injections[0].recovery_class = Some(QaFaultRecoveryClass::OutcomeUnknown);
        input.daemon_restart_count = 0;

        let bundle = build_qa_evidence_bundle(&manifest, input);
        let issue_codes = fault_check(&bundle)
            .issues
            .iter()
            .map(|issue| issue.code.as_str())
            .collect::<BTreeSet<_>>();

        assert!(issue_codes.contains("fault_recovery_class_mismatch"));
        assert!(issue_codes.contains("fault_restart_count_mismatch"));
    }

    #[test]
    fn missing_tool_call_reports_precise_failure() {
        let manifest = parse_qa_scenario_manifest_yaml(REQUIRED_TOOL_SCENARIO)
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
    fn failed_tool_call_does_not_satisfy_required_tool_evidence() {
        let manifest = parse_qa_scenario_manifest_yaml(REQUIRED_TOOL_SCENARIO)
            .expect("tool scenario should parse");
        let bundle = build_qa_evidence_bundle(
            &manifest,
            QaEvidenceBuildInput {
                terminal_state: Some("completed".to_owned()),
                final_answer: Some("Done.".to_owned()),
                tool_calls: vec![QaToolCallEvidence {
                    name: "palyra.fs.read_file".to_owned(),
                    proposal_id: Some("proposal-1".to_owned()),
                    success: Some(false),
                }],
                ..QaEvidenceBuildInput::default()
            },
        );

        assert_eq!(bundle.summary.verdict, QaEvidenceVerdict::Failed);
        assert!(bundle.checks.iter().any(|check| {
            check.issues.iter().any(|issue| {
                issue.code == "tool_call_failed" && issue.path == "$.expect.tool_calls[0]"
            })
        }));
    }

    #[test]
    fn explicit_denial_expectation_requires_a_failed_tool_outcome() {
        let scenario = REQUIRED_TOOL_SCENARIO
            .replace("      min_count: 1", "      min_count: 1\n      success: false");
        let manifest = parse_qa_scenario_manifest_yaml(scenario.as_str())
            .expect("denial scenario should parse");
        let mut input = QaEvidenceBuildInput {
            terminal_state: Some("completed".to_owned()),
            tool_calls: vec![QaToolCallEvidence {
                name: "palyra.fs.read_file".to_owned(),
                proposal_id: Some("proposal-1".to_owned()),
                success: Some(false),
            }],
            ..QaEvidenceBuildInput::default()
        };

        let denied = build_qa_evidence_bundle(&manifest, input.clone());
        assert_eq!(denied.summary.verdict, QaEvidenceVerdict::Passed);

        input.tool_calls[0].success = Some(true);
        let unexpectedly_allowed = build_qa_evidence_bundle(&manifest, input);
        assert!(unexpectedly_allowed.checks.iter().any(|check| {
            check.issues.iter().any(|issue| issue.code == "tool_call_unexpected_success")
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
                tool_calls: vec![QaToolCallEvidence {
                    name: "palyra.fs.read_file".to_owned(),
                    proposal_id: Some("qa-real-read".to_owned()),
                    success: Some(true),
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
        let run_alias = payload["run_id"].as_str().expect("run id should normalize to an alias");
        assert!(run_alias.starts_with("<normalized:id:"));
        assert_eq!(payload["created_at_unix_ms"], 0);
        assert_eq!(payload["path"], "<normalized:absolute_path>");
        assert_eq!(payload["sha256"], "<normalized:hash>");
        let proposal_alias = bundle.tool_calls[0]
            .proposal_id
            .as_deref()
            .expect("proposal id should normalize to an alias");
        assert!(proposal_alias.starts_with("<normalized:id:"));
        assert_ne!(proposal_alias, run_alias);
        assert!(bundle.redaction.redacted_fields >= 1);
        assert!(bundle.redaction.normalized_timestamps >= 1);
        assert!(bundle.redaction.normalized_hashes >= 2);
    }

    #[test]
    fn identifier_aliases_preserve_cross_surface_equality_and_distinctness() {
        let mut manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        manifest.forbidden.tool_calls.clear();
        let input_with_ids = |shared_id: &str,
                              session_id: &str,
                              approval_id: &str,
                              effect_id: &str,
                              related_id: &str| {
            let mut input = passing_input();
            input.run_id = Some(shared_id.to_owned());
            input.session_id = Some(session_id.to_owned());
            input.tape_events[0].payload = json!({
                "run_id": shared_id,
                "proposal_id": shared_id,
                "approval_id": approval_id,
                "side_effect_id": effect_id,
                "idempotency_key": effect_id,
            });
            input.public_events[0].payload = json!({
                "run_id": shared_id,
                "proposal_id": shared_id,
                "side_effect_id": effect_id,
                "related_effect_id": related_id,
            });
            input.tool_calls = vec![QaToolCallEvidence {
                name: "palyra.fs.read_file".to_owned(),
                proposal_id: Some(shared_id.to_owned()),
                success: Some(true),
            }];
            input
        };

        let bundle = build_qa_evidence_bundle(
            &manifest,
            input_with_ids("shared-a", "session-a", "approval-a", "effect-a", "effect-b"),
        );
        let different_raw_ids = build_qa_evidence_bundle(
            &manifest,
            input_with_ids("shared-z", "session-z", "approval-z", "effect-z", "effect-y"),
        );
        let run_alias = bundle.run.run_id.as_deref().expect("run alias should exist");
        let proposal_alias = bundle.redacted_tape[0].payload["proposal_id"]
            .as_str()
            .expect("proposal alias should be a string");
        let approval_alias = bundle.redacted_tape[0].payload["approval_id"]
            .as_str()
            .expect("approval alias should be a string");
        let effect_alias = bundle.redacted_tape[0].payload["side_effect_id"]
            .as_str()
            .expect("side-effect alias should be a string");

        assert_eq!(bundle.redacted_tape[0].payload["run_id"], run_alias);
        assert_eq!(bundle.public_events[0].payload["run_id"], run_alias);
        assert_eq!(bundle.public_events[0].payload["proposal_id"], proposal_alias);
        assert_eq!(bundle.tool_calls[0].proposal_id.as_deref(), Some(proposal_alias));
        assert_ne!(run_alias, proposal_alias);
        assert_eq!(bundle.redacted_tape[0].payload["idempotency_key"], effect_alias);
        assert_eq!(bundle.public_events[0].payload["side_effect_id"], effect_alias);
        assert_ne!(proposal_alias, approval_alias);
        assert_ne!(
            effect_alias,
            bundle.public_events[0].payload["related_effect_id"]
                .as_str()
                .expect("related side-effect alias should be a string")
        );
        assert_eq!(
            serde_json::to_vec(&bundle).expect("normalized bundle should serialize"),
            serde_json::to_vec(&different_raw_ids)
                .expect("bundle with different raw identifiers should serialize")
        );
    }

    #[test]
    fn timestamp_normalization_does_not_hide_semantic_replay_drift() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let mut baseline = passing_input();
        baseline.tape_events[0] = QaRunTapeEvent {
            seq: 7,
            event_type: "run.terminalized".to_owned(),
            payload: json!({
                "created_at_unix_ms": 1_700_000_000_000_i64,
                "generation": 3,
                "reason_code": "run.completed",
                "terminal_count": 1,
                "side_effect_id": "effect-a",
                "idempotency_key": "effect-a",
            }),
        };
        let mut timestamp_changed = baseline.clone();
        timestamp_changed.tape_events[0].payload["created_at_unix_ms"] =
            json!(1_800_000_000_000_i64);

        let baseline_bundle = build_qa_evidence_bundle(&manifest, baseline.clone());
        let timestamp_bundle = build_qa_evidence_bundle(&manifest, timestamp_changed);
        assert_eq!(baseline_bundle.redacted_tape, timestamp_bundle.redacted_tape);

        let mut semantic_drift = baseline;
        semantic_drift.tape_events[0].seq = 8;
        semantic_drift.tape_events[0].event_type = "run.cancelled".to_owned();
        semantic_drift.tape_events[0].payload["generation"] = json!(4);
        semantic_drift.tape_events[0].payload["reason_code"] = json!("run.cancelled");
        semantic_drift.tape_events[0].payload["terminal_count"] = json!(2);
        semantic_drift.tape_events[0].payload["side_effect_id"] = json!("effect-b");
        let drift_bundle = build_qa_evidence_bundle(&manifest, semantic_drift);

        let baseline_event = &baseline_bundle.redacted_tape[0];
        let drift_event = &drift_bundle.redacted_tape[0];
        assert_ne!(baseline_event.seq, drift_event.seq);
        assert_ne!(baseline_event.event_type, drift_event.event_type);
        assert_ne!(baseline_event.payload["generation"], drift_event.payload["generation"]);
        assert_ne!(baseline_event.payload["reason_code"], drift_event.payload["reason_code"]);
        assert_ne!(baseline_event.payload["terminal_count"], drift_event.payload["terminal_count"]);
        assert_ne!(baseline_event.payload["side_effect_id"], drift_event.payload["side_effect_id"]);
        assert_eq!(
            baseline_event.payload["side_effect_id"],
            baseline_event.payload["idempotency_key"]
        );
        assert_ne!(drift_event.payload["side_effect_id"], drift_event.payload["idempotency_key"]);
    }

    #[test]
    fn normalizes_embedded_host_paths_across_free_text_surfaces() {
        let manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let mut input = passing_input();
        input.final_answer = Some("A friendly result is at /tmp/result.txt.".to_owned());
        input.transcript = vec![QaTranscriptMessage {
            role: "assistant".to_owned(),
            content: r#"before C:\Users\qa-user\one.txt, C:/Users/qa-user/two.txt, C:private\suite.yaml, \Users\qa-user\root.txt, and "C:\Program Files\Palyra\config.toml" after"#
                .to_owned(),
        }];
        input.public_events[1].payload = json!({
            "message": "public /home/qa-user/config beside https://example.test/Users/public"
        });
        input.tape_events[0].payload = json!({
            "message": r"tape \\server\share\secret.txt beside /Users/qa-user/private.key"
        });

        let bundle = build_qa_evidence_bundle(&manifest, input);

        assert_eq!(
            bundle.run.final_answer.as_deref(),
            Some("A friendly result is at <normalized:absolute_path>.")
        );
        assert_eq!(
            bundle.transcript[0].content,
            "before <normalized:absolute_path>, <normalized:absolute_path>, <normalized:absolute_path>, <normalized:absolute_path>, and \"<normalized:absolute_path>\" after"
        );
        assert_eq!(
            bundle.public_events[1].payload["message"],
            "public <normalized:absolute_path> beside https://example.test/Users/public"
        );
        assert_eq!(
            bundle.redacted_tape[0].payload["message"],
            "tape <normalized:absolute_path> beside <normalized:absolute_path>"
        );
        assert_eq!(bundle.redaction.normalized_paths, 9);
    }

    #[test]
    fn redacts_host_paths_from_assertion_issues() {
        let mut manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        let final_answer = manifest
            .expect
            .final_answer
            .as_mut()
            .expect("basic scenario should require a final answer");
        final_answer.equals = Some(r"C:\Users\qa-user\expected.txt".to_owned());
        final_answer.contains.clear();
        let mut input = passing_input();
        input.final_answer = Some(r"mismatch D:\Users\qa-user\actual.txt".to_owned());

        let bundle = build_qa_evidence_bundle(&manifest, input);
        let issue = bundle
            .checks
            .iter()
            .flat_map(|check| &check.issues)
            .find(|issue| issue.code == "final_answer_mismatch")
            .expect("mismatched answer should produce an issue");

        assert_eq!(issue.expected.as_deref(), Some("<normalized:absolute_path>"));
        assert_eq!(issue.actual.as_deref(), Some("mismatch <normalized:absolute_path>"));
        assert_eq!(bundle.run.final_answer.as_deref(), Some("mismatch <normalized:absolute_path>"));
        let serialized = serde_json::to_string(&bundle).expect("evidence bundle should serialize");
        assert!(!serialized.contains("expected.txt"), "{serialized}");
        assert!(!serialized.contains("actual.txt"), "{serialized}");
        assert_eq!(bundle.redaction.normalized_paths, 3);
    }

    #[test]
    fn expected_event_order_does_not_compare_unrelated_evidence_surfaces() {
        let mut manifest = parse_qa_scenario_manifest_yaml(BASIC_SCENARIO)
            .expect("basic QA scenario should parse");
        manifest.expect.events = vec![
            expected_event("provider.retry.started"),
            expected_event("run.started"),
            expected_event("run.completed"),
        ];
        let mut input = passing_input();
        input.tape_events.push(QaRunTapeEvent {
            seq: 1,
            event_type: "provider.retry.started".to_owned(),
            payload: json!({}),
        });

        let bundle = build_qa_evidence_bundle(&manifest, input);

        assert_eq!(required_events_check(&bundle).verdict, QaEvidenceVerdict::Passed);
        assert_eq!(bundle.summary.observed_event_count, 5);
    }

    #[test]
    fn malformed_recovery_requires_surface_local_retry_and_completion_order() {
        let manifest = parse_qa_scenario_manifest_yaml(MALFORMED_RECOVERY_SCENARIO)
            .expect("malformed recovery scenario should parse");
        let input = malformed_recovery_input();

        let ordered = build_qa_evidence_bundle(&manifest, input.clone());
        assert_eq!(required_events_check(&ordered).verdict, QaEvidenceVerdict::Passed);

        let mut reversed_tape = input.clone();
        reversed_tape.tape_events[0].seq = 20;
        reversed_tape.tape_events[1].seq = 10;
        let reversed_tape = build_qa_evidence_bundle(&manifest, reversed_tape);
        assert!(required_events_check(&reversed_tape)
            .issues
            .iter()
            .any(|issue| issue.code == "event_order_mismatch" && issue.path == "$.tape_events"));

        let mut reversed_public = input;
        reversed_public.public_events.swap(1, 2);
        let reversed_public = build_qa_evidence_bundle(&manifest, reversed_public);
        assert!(required_events_check(&reversed_public)
            .issues
            .iter()
            .any(|issue| issue.code == "event_order_mismatch" && issue.path == "$.public_events"));
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
        assert_eq!(bundle.summary.check_count, 8);
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

    fn expected_event(event_type: &str) -> QaScenarioExpectedEvent {
        QaScenarioExpectedEvent { event_type: event_type.to_owned(), min_count: Some(1) }
    }

    fn required_events_check(bundle: &QaEvidenceBundle) -> &QaEvidenceCheck {
        bundle
            .checks
            .iter()
            .find(|check| check.name == "required_events")
            .expect("required event check should be present")
    }

    fn fault_check(bundle: &QaEvidenceBundle) -> &QaEvidenceCheck {
        bundle
            .checks
            .iter()
            .find(|check| check.name == "fault_injection")
            .expect("fault-injection check should be present")
    }

    fn passing_fault_input(manifest: &QaScenarioManifest) -> QaEvidenceBuildInput {
        let plan = manifest.fault_injection.as_ref().expect("fault scenario must contain a plan");
        let plan_sha256 = plan.canonical_sha256().expect("fault plan should hash");
        QaEvidenceBuildInput {
            terminal_state: Some("completed".to_owned()),
            final_answer: Some("done".to_owned()),
            fault_injections: vec![QaFaultInjectionEvidence {
                plan_sha256,
                seed: plan.seed,
                activation_id: "tool-crash".to_owned(),
                point_id: "tool.after_effect_before_ack".to_owned(),
                occurrence: 1,
                action: QaFaultAction::TerminateProcess,
                activation_sequence: 1,
                actors: vec!["daemon".to_owned()],
                release_order: vec!["daemon".to_owned()],
                recovery_class: Some(QaFaultRecoveryClass::DuplicateSuppressed),
                recovery_reason_code: Some("tool.duplicate_suppressed".to_owned()),
            }],
            daemon_restart_count: 1,
            ..QaEvidenceBuildInput::default()
        }
    }

    fn malformed_recovery_input() -> QaEvidenceBuildInput {
        QaEvidenceBuildInput {
            terminal_state: Some("completed".to_owned()),
            final_answer: Some("Recovered after a retryable malformed response.".to_owned()),
            public_events: vec![
                QaPublicEventEvidence { event_type: "run.started".to_owned(), payload: json!({}) },
                QaPublicEventEvidence { event_type: "model.delta".to_owned(), payload: json!({}) },
                QaPublicEventEvidence {
                    event_type: "run.completed".to_owned(),
                    payload: json!({}),
                },
            ],
            tape_events: vec![
                QaRunTapeEvent {
                    seq: 10,
                    event_type: "provider.retry.started".to_owned(),
                    payload: json!({}),
                },
                QaRunTapeEvent {
                    seq: 20,
                    event_type: "model_token".to_owned(),
                    payload: json!({}),
                },
            ],
            artifacts: vec![QaArtifactEvidence {
                path: "qa/reports/real_runtime/malformed_stream_recovery.evidence.json".to_owned(),
                kind: "evidence".to_owned(),
                present: true,
                sha256: None,
                size_bytes: Some(512),
            }],
            ..QaEvidenceBuildInput::default()
        }
    }

    fn v4_assertion_input() -> QaEvidenceBuildInput {
        QaEvidenceBuildInput {
            terminal_state: Some("completed".to_owned()),
            tape_events: vec![tool_attempt_event(1, "proposal-1")],
            artifacts: vec![QaArtifactEvidence {
                path: "qa/reports/v4-assertion.json".to_owned(),
                kind: "report".to_owned(),
                present: true,
                sha256: Some("a".repeat(64)),
                size_bytes: Some(128),
            }],
            ..QaEvidenceBuildInput::default()
        }
    }

    fn tool_attempt_event(seq: i64, proposal_id: &str) -> QaRunTapeEvent {
        QaRunTapeEvent {
            seq,
            event_type: "tool_effect_started".to_owned(),
            payload: json!({
                "proposal_id": proposal_id,
                "tool_name": "palyra.fs.read_file",
            }),
        }
    }

    fn tool_proposal_event(seq: i64, proposal_id: &str, tool_name: &str) -> QaRunTapeEvent {
        QaRunTapeEvent {
            seq,
            event_type: "tool_proposal".to_owned(),
            payload: json!({
                "proposal_id": proposal_id,
                "tool_name": tool_name,
            }),
        }
    }

    fn tool_result_event(seq: i64, proposal_id: &str, success: bool) -> QaRunTapeEvent {
        QaRunTapeEvent {
            seq,
            event_type: "tool_result".to_owned(),
            payload: json!({
                "proposal_id": proposal_id,
                "success": success,
            }),
        }
    }

    fn runtime_path_component(id: &str, reason_code: &str) -> RuntimePathComponentEvidence {
        RuntimePathComponentEvidence {
            id: id.to_owned(),
            source_event: "run.runtime_path_summary".to_owned(),
            reason_code: reason_code.to_owned(),
        }
    }

    fn qualified_runtime_path() -> RuntimePathEvidence {
        RuntimePathEvidence {
            schema_version: QA_RUNTIME_PATH_EVIDENCE_SCHEMA_VERSION,
            runtime_version:
                "palyrad-sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            runtime_contract_version: "runtime-contracts.v2".to_owned(),
            runner_version: "qa-runner.v4/test".to_owned(),
            provider_lane: "fixture".to_owned(),
            attempt_owner: "external_harness".to_owned(),
            harness: runtime_path_component("external_harness", "harness.selected"),
            context_engine: runtime_path_component("context_engine_v2", "context_engine.selected"),
            mcp_transport_mode: Some(runtime_path_component(
                "persistent",
                "mcp.transport.persistent",
            )),
            complete: true,
            source_events: vec!["run.runtime_path_summary".to_owned()],
            reason_codes: vec!["runtime_path.complete".to_owned()],
            fallbacks: Vec::new(),
            fallback_count: 0,
        }
    }

    fn qualified_runtime_path_input() -> QaEvidenceBuildInput {
        QaEvidenceBuildInput {
            terminal_state: Some("completed".to_owned()),
            runtime_path: Some(qualified_runtime_path()),
            metadata_trace: Some(qualified_metadata_trace()),
            ..QaEvidenceBuildInput::default()
        }
    }

    fn qualified_metadata_trace() -> MetadataTraceV1 {
        let event = |sequence, event| MetadataTraceEventV1 {
            sequence,
            generation: 1,
            recorded_at_unix_ms: 1,
            event_id_sha256: metadata_trace_id_sha256(
                MetadataTraceIdDomainV1::Event,
                format!("event-{sequence}").as_str(),
            )
            .expect("event id should hash"),
            causal_parent_event_id_sha256: sequence.checked_sub(1).map(|parent| {
                metadata_trace_id_sha256(
                    MetadataTraceIdDomainV1::Event,
                    format!("event-{parent}").as_str(),
                )
                .expect("parent event id should hash")
            }),
            stage_duration_ms: None,
            event,
        };
        MetadataTraceV1 {
            schema_version: METADATA_TRACE_SCHEMA_VERSION,
            run_id_sha256: metadata_trace_id_sha256(MetadataTraceIdDomainV1::Run, "test-run")
                .expect("run id should hash"),
            session_id_sha256: metadata_trace_id_sha256(
                MetadataTraceIdDomainV1::Session,
                "test-session",
            )
            .expect("session id should hash"),
            segments: vec![MetadataTraceSegmentV1 {
                segment_id_sha256: metadata_trace_id_sha256(
                    MetadataTraceIdDomainV1::Segment,
                    "test-segment",
                )
                .expect("segment id should hash"),
                segment_index: 0,
                generation: 1,
                status: MetadataTraceSegmentStatusV1::Complete,
                events: vec![
                    event(
                        0,
                        MetadataTraceEventDataV1::RunStarted(RunStartedMetadataV1 {
                            entrypoint: MetadataTraceEntrypointV1::NewRun,
                        }),
                    ),
                    event(
                        1,
                        MetadataTraceEventDataV1::RuntimeSelected(RuntimeSelectedMetadataV1 {
                            harness_id: "external_harness".to_owned(),
                            harness_version: "harness.v1".to_owned(),
                            runtime_id: "palyrad".to_owned(),
                            runtime_version: "runtime.v1".to_owned(),
                            route_class: MetadataTraceRouteClassV1::Fixture,
                            auth_profile_id_sha256: None,
                            schema_hashes: vec![MetadataTraceSchemaHashV1 {
                                schema_id: "runtime".to_owned(),
                                sha256: metadata_trace_id_sha256(
                                    MetadataTraceIdDomainV1::Custom,
                                    "runtime-schema",
                                )
                                .expect("schema id should hash"),
                            }],
                        }),
                    ),
                    event(
                        2,
                        MetadataTraceEventDataV1::ContextAssembled(ContextAssembledMetadataV1 {
                            context_engine_id: "context_engine_v2".to_owned(),
                            context_engine_version: "context.v2".to_owned(),
                            context_schema_sha256: metadata_trace_id_sha256(
                                MetadataTraceIdDomainV1::Custom,
                                "context-schema",
                            )
                            .expect("context schema should hash"),
                            input_item_count: 2,
                            retained_item_count: 2,
                        }),
                    ),
                    event(
                        3,
                        MetadataTraceEventDataV1::ProviderAttempt(ProviderAttemptMetadataV1 {
                            provider_id_sha256: metadata_trace_id_sha256(
                                MetadataTraceIdDomainV1::Provider,
                                "fixture-provider",
                            )
                            .expect("provider id should hash"),
                            model_id_sha256: metadata_trace_id_sha256(
                                MetadataTraceIdDomainV1::Model,
                                "fixture-model",
                            )
                            .expect("model id should hash"),
                            route_class: MetadataTraceRouteClassV1::Fixture,
                            auth_profile_id_sha256: None,
                            attempt: 1,
                            outcome: MetadataTraceProviderAttemptOutcomeV1::Succeeded,
                            reason_code: "provider.attempt.succeeded".to_owned(),
                        }),
                    ),
                    event(
                        4,
                        MetadataTraceEventDataV1::Terminalization(TerminalizationMetadataV1 {
                            outcome: MetadataTraceTerminalOutcomeV1::Done,
                            reason_code: "run.completed".to_owned(),
                            output_emitted: true,
                            side_effect_may_have_occurred: true,
                        }),
                    ),
                ],
            }],
        }
    }

    fn runtime_fallback(reason_code: &str) -> RuntimeFallbackEvidence {
        RuntimeFallbackEvidence {
            component: "provider".to_owned(),
            from: Some("primary".to_owned()),
            to: "secondary".to_owned(),
            reason_code: reason_code.to_owned(),
            source_event: "provider.recovery.decision".to_owned(),
        }
    }

    fn runtime_path_check(bundle: &QaEvidenceBundle) -> &QaEvidenceCheck {
        bundle
            .checks
            .iter()
            .find(|check| check.name == "runtime_path")
            .expect("runtime-path check should be present")
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
