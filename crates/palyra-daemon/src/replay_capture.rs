//! Incident replay-bundle export: assembles a run's journal state (status
//! snapshot, ordered tape events, lifecycle transitions, idempotency records,
//! artifact references) into an offline-replayable [`ReplayBundle`].
//!
//! Determinism matters here: the same run must always export the same bundle,
//! so tape events are sorted by sequence before truncation. Secret redaction
//! is delegated to `build_replay_bundle` in `palyra_common::replay_bundle`;
//! this module only shapes journal rows into its input.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use palyra_common::redaction::{
    is_sensitive_key, redact_diagnostic_text, redact_internal_runtime_paths,
};
use palyra_common::replay_bundle::{
    build_replay_bundle, ReplayArtifactRef, ReplayBundle, ReplayBundleBuildInput,
    ReplayCaptureMetadata, ReplayRunSnapshot, ReplaySource, ReplayTapeEvent,
};
use palyra_common::runtime_contracts::ToolResultArtifactRef;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    config::{FeatureRolloutsConfig, ReplayCaptureConfig},
    journal::{JournalStore, OrchestratorRunStatusSnapshot},
    runtime_diagnostics::RunStageTimingReport,
};

mod fixture_matrix;

pub(crate) const REPLAY_FIXTURE_MATRIX_SCHEMA_VERSION: u32 =
    fixture_matrix::REPLAY_FIXTURE_MATRIX_SCHEMA_VERSION;
pub(crate) type ReplayFixtureValidationReport = fixture_matrix::ReplayFixtureValidationReport;

pub(crate) fn validate_replay_fixture_matrix(
    value: &Value,
) -> Result<ReplayFixtureValidationReport> {
    fixture_matrix::validate_replay_fixture_matrix(value)
}

pub(crate) const RUN_TRACE_SCHEMA_VERSION: u32 = 1;
pub(crate) const TRAJECTORY_EXPORT_SCHEMA_VERSION: u32 = 1;
pub(crate) const UNIFIED_SUPPORT_BUNDLE_SCHEMA_VERSION: u32 = 1;

const RUN_TRACE_REQUIRED_SUBSYSTEMS: &[&str] = &[
    "provider",
    "tool",
    "approval",
    "policy",
    "sandbox",
    "hook",
    "lsp",
    "verification",
    "recovery",
    "compaction",
    "advisor",
    "harness",
    "browser",
];
const RUN_TRACE_DEFAULT_EVENT_CAP: usize = 512;
const RUN_TRACE_PAYLOAD_LIMIT_BYTES: usize = 2_048;

/// Inputs for one bundle export; `max_events` caps the tape so a runaway run
/// cannot produce an unbounded bundle (truncation is recorded as a warning).
pub(crate) struct IncidentReplayCaptureRequest<'a> {
    pub journal_store: &'a JournalStore,
    pub replay_capture: &'a ReplayCaptureConfig,
    pub feature_rollouts: &'a FeatureRolloutsConfig,
    pub run_id: &'a str,
    pub generated_at_unix_ms: i64,
    pub max_events: usize,
}

pub(crate) struct RunTraceBuildInput<'a> {
    pub(crate) replay_bundle: &'a ReplayBundle,
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) stage_timings: RunStageTimingReport,
    pub(crate) max_events: usize,
    pub(crate) max_payload_bytes: usize,
    pub(crate) trace_unavailable_reason: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunTraceJournalRef {
    pub(crate) run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) first_seq: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) last_seq: Option<i64>,
    pub(crate) event_count: usize,
    pub(crate) lifecycle_transition_count: usize,
    pub(crate) idempotency_record_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunTraceTerminalSummary {
    pub(crate) state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
    pub(crate) failure_debug_available: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunTraceEvent {
    pub(crate) seq: i64,
    pub(crate) event_type: String,
    pub(crate) subsystem: String,
    pub(crate) outcome: String,
    pub(crate) payload_summary: Value,
    pub(crate) redaction_level: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunTraceSubsystemCoverage {
    pub(crate) observed: bool,
    pub(crate) event_count: usize,
    pub(crate) reason_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunTraceRedactionReport {
    pub(crate) redaction_level: String,
    pub(crate) raw_secrets_allowed: bool,
    pub(crate) raw_paths_allowed: bool,
    pub(crate) model_visible_default: bool,
    pub(crate) redacted_fields: Vec<String>,
    pub(crate) path_redactions: usize,
    pub(crate) truncated_payloads: usize,
    pub(crate) omitted_events: usize,
    pub(crate) max_payload_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExportApprovalGate {
    pub(crate) approved: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) approval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) actor_ref: Option<String>,
    pub(crate) required_reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunTraceV1 {
    pub(crate) schema_version: u32,
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) available: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) unavailable_reason: Option<String>,
    pub(crate) model_visible_default: bool,
    pub(crate) journal: RunTraceJournalRef,
    pub(crate) terminal: RunTraceTerminalSummary,
    pub(crate) subsystem_coverage: BTreeMap<String, RunTraceSubsystemCoverage>,
    pub(crate) events: Vec<RunTraceEvent>,
    pub(crate) stage_timings: RunStageTimingReport,
    pub(crate) redaction: RunTraceRedactionReport,
    pub(crate) support_bundle_gate: ExportApprovalGate,
}

pub(crate) struct TrajectoryExportRequest {
    pub(crate) approval: ExportApprovalGate,
    pub(crate) max_events: usize,
    pub(crate) max_total_bytes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrajectoryExportManifest {
    pub(crate) schema_version: u32,
    pub(crate) format: String,
    pub(crate) run_id: String,
    pub(crate) offline_support_ready: bool,
    pub(crate) approval: ExportApprovalGate,
    pub(crate) included_sections: Vec<String>,
    pub(crate) skipped_sections: BTreeMap<String, String>,
    pub(crate) size_caps: BTreeMap<String, usize>,
    pub(crate) redaction: RunTraceRedactionReport,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct TrajectoryExportBundle {
    pub(crate) schema_version: u32,
    pub(crate) manifest: TrajectoryExportManifest,
    pub(crate) events_jsonl: String,
    pub(crate) sections: BTreeMap<String, Value>,
}

pub(crate) struct UnifiedSupportBundleRequest {
    pub(crate) approval: ExportApprovalGate,
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) include_trajectory: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct UnifiedSupportBundleManifest {
    pub(crate) schema_version: u32,
    pub(crate) generated_at_unix_ms: i64,
    pub(crate) run_id: String,
    pub(crate) approval: ExportApprovalGate,
    pub(crate) included_sections: Vec<String>,
    pub(crate) skipped_sections: BTreeMap<String, String>,
    pub(crate) redaction: RunTraceRedactionReport,
    pub(crate) feature_flags: Value,
    pub(crate) runtime_path: Value,
    pub(crate) trajectory_ref: Option<String>,
    pub(crate) operator_summary: Vec<String>,
}

pub(crate) fn capture_incident_replay_bundle(
    request: IncidentReplayCaptureRequest<'_>,
) -> Result<ReplayBundle> {
    let run = request
        .journal_store
        .orchestrator_run_status_snapshot(request.run_id)
        .with_context(|| format!("failed to load run snapshot for {}", request.run_id))?
        .with_context(|| format!("orchestrator run not found: {}", request.run_id))?;
    let mut tape = request
        .journal_store
        .orchestrator_tape(request.run_id)
        .with_context(|| format!("failed to load tape for {}", request.run_id))?;
    // Sort before truncating so the cap always keeps the earliest events;
    // relying on journal read order would make truncated bundles
    // non-deterministic.
    tape.sort_by_key(|event| event.seq);
    let truncated = tape.len() > request.max_events;
    tape.truncate(request.max_events);

    let tape_events = tape
        .into_iter()
        .map(|record| {
            // Malformed payload JSON is wrapped as a raw string instead of
            // failing the export: an incident bundle missing one event body
            // is far more useful than no bundle at all.
            let payload = serde_json::from_str::<Value>(record.payload_json.as_str())
                .unwrap_or_else(|_| json!({ "raw": record.payload_json }));
            ReplayTapeEvent { seq: record.seq, event_type: record.event_type, payload }
        })
        .collect::<Vec<_>>();
    let lifecycle_transitions = request
        .journal_store
        .list_run_lifecycle_events(request.run_id)
        .with_context(|| format!("failed to load lifecycle transitions for {}", request.run_id))?;
    let idempotency_records = request
        .journal_store
        .list_idempotency_records_for_run(request.run_id)
        .with_context(|| format!("failed to load idempotency records for {}", request.run_id))?;
    let mut artifact_refs = replay_artifact_refs(&run);
    artifact_refs.extend(
        request
            .journal_store
            .list_tool_result_artifacts_for_run(request.run_id)
            .with_context(|| {
                format!("failed to load tool result artifacts for {}", request.run_id)
            })?
            .iter()
            .map(replay_tool_result_artifact_ref),
    );

    build_replay_bundle(ReplayBundleBuildInput {
        generated_at_unix_ms: request.generated_at_unix_ms,
        source: ReplaySource {
            product: "palyra".to_owned(),
            run_id: run.run_id.clone(),
            session_id: Some(run.session_id.clone()),
            origin_kind: run.origin_kind.clone(),
            schema_policy: "reject_future_schema_versions_additive_backward_compat".to_owned(),
        },
        capture: ReplayCaptureMetadata {
            captured_at_unix_ms: request.generated_at_unix_ms,
            capture_mode: "daemon_journal_export".to_owned(),
            max_events_per_run: request.max_events,
            truncated,
            inline_sections: vec![
                "run".to_owned(),
                "config_snapshot".to_owned(),
                "tape_events".to_owned(),
                "tool_exchanges".to_owned(),
                "http_exchanges".to_owned(),
                "approvals".to_owned(),
                "expected".to_owned(),
            ],
            referenced_sections: vec![
                "large_binary_artifacts".to_owned(),
                "workspace_files".to_owned(),
                "journal_events_outside_run".to_owned(),
            ],
            warnings: if truncated {
                vec![format!(
                    "tape truncated at {} events for replay bundle export",
                    request.max_events
                )]
            } else {
                Vec::new()
            },
        },
        run: replay_run_snapshot(&run),
        config_snapshot: replay_config_snapshot(request.replay_capture, request.feature_rollouts),
        tape_events,
        lifecycle_transitions,
        idempotency_records,
        artifact_refs,
    })
}

#[must_use]
pub(crate) fn build_run_trace_v1(input: RunTraceBuildInput<'_>) -> RunTraceV1 {
    let max_events = input.max_events.min(RUN_TRACE_DEFAULT_EVENT_CAP);
    let max_payload_bytes = input.max_payload_bytes.min(RUN_TRACE_PAYLOAD_LIMIT_BYTES);
    let mut redaction = base_run_trace_redaction_report(max_payload_bytes);
    let omitted_events = input.replay_bundle.tape_events.len().saturating_sub(max_events);
    redaction.omitted_events = omitted_events;

    let events = input
        .replay_bundle
        .tape_events
        .iter()
        .take(max_events)
        .map(|event| run_trace_event(event, max_payload_bytes, &mut redaction))
        .collect::<Vec<_>>();
    let subsystem_coverage = build_subsystem_coverage(events.as_slice());
    let first_seq = input.replay_bundle.tape_events.first().map(|event| event.seq);
    let last_seq = input.replay_bundle.tape_events.last().map(|event| event.seq);
    let terminal_reason = input.replay_bundle.run.last_error.as_deref().map(redact_trace_string);

    RunTraceV1 {
        schema_version: RUN_TRACE_SCHEMA_VERSION,
        generated_at_unix_ms: input.generated_at_unix_ms,
        available: input.trace_unavailable_reason.is_none(),
        unavailable_reason: input.trace_unavailable_reason.map(ToOwned::to_owned),
        model_visible_default: false,
        journal: RunTraceJournalRef {
            run_id: input.replay_bundle.source.run_id.clone(),
            session_id: input.replay_bundle.source.session_id.clone(),
            first_seq,
            last_seq,
            event_count: input.replay_bundle.tape_events.len(),
            lifecycle_transition_count: input.replay_bundle.lifecycle_transitions.len(),
            idempotency_record_count: input.replay_bundle.idempotency_records.len(),
        },
        terminal: RunTraceTerminalSummary {
            state: input.replay_bundle.run.state.clone(),
            reason: terminal_reason,
            failure_debug_available: input.replay_bundle.run.state != "done"
                || input.replay_bundle.run.last_error.is_some(),
        },
        subsystem_coverage,
        events,
        stage_timings: input.stage_timings,
        redaction,
        support_bundle_gate: ExportApprovalGate {
            approved: false,
            approval_id: None,
            actor_ref: None,
            required_reason: "support_bundle.trace_export.requires_operator_approval".to_owned(),
        },
    }
}

pub(crate) fn build_trajectory_export_bundle(
    replay_bundle: &ReplayBundle,
    run_trace: &RunTraceV1,
    request: TrajectoryExportRequest,
) -> Result<TrajectoryExportBundle> {
    require_export_approval(&request.approval, "trajectory export")?;
    let mut redaction = run_trace.redaction.clone();
    let mut events_jsonl = String::new();
    for event in run_trace.events.iter().take(request.max_events) {
        let line = serde_json::to_string(event).context("failed to serialize trajectory event")?;
        if events_jsonl.len().saturating_add(line.len()).saturating_add(1) > request.max_total_bytes
        {
            redaction.omitted_events = redaction.omitted_events.saturating_add(1);
            break;
        }
        events_jsonl.push_str(line.as_str());
        events_jsonl.push('\n');
    }

    let included_sections = vec![
        "manifest".to_owned(),
        "events_jsonl".to_owned(),
        "session".to_owned(),
        "run".to_owned(),
        "tools".to_owned(),
        "provider".to_owned(),
        "approvals".to_owned(),
        "artifacts".to_owned(),
        "policy".to_owned(),
        "compaction".to_owned(),
        "recovery".to_owned(),
        "redaction_report".to_owned(),
    ];
    let skipped_sections = BTreeMap::from([
        ("prompts".to_owned(), "raw prompts are never exported".to_owned()),
        ("system_prompt".to_owned(), "system prompt is represented by hash-only refs".to_owned()),
        ("image_data".to_owned(), "binary and image payloads require artifact refs".to_owned()),
    ]);
    let manifest = TrajectoryExportManifest {
        schema_version: TRAJECTORY_EXPORT_SCHEMA_VERSION,
        format: "directory".to_owned(),
        run_id: replay_bundle.source.run_id.clone(),
        offline_support_ready: true,
        approval: request.approval,
        included_sections,
        skipped_sections,
        size_caps: BTreeMap::from([
            ("events".to_owned(), request.max_events),
            ("total_bytes".to_owned(), request.max_total_bytes),
        ]),
        redaction: redaction.clone(),
    };
    let mut sections = BTreeMap::new();
    sections.insert(
        "session".to_owned(),
        json!({
            "session_id": replay_bundle.source.session_id.clone(),
            "origin_kind": replay_bundle.source.origin_kind.clone(),
        }),
    );
    sections.insert(
        "run".to_owned(),
        json!({
            "state": replay_bundle.run.state.clone(),
            "terminal_reason": replay_bundle.run.last_error.as_deref().map(redact_trace_string),
            "token_usage": {
                "prompt": replay_bundle.run.prompt_tokens,
                "completion": replay_bundle.run.completion_tokens,
                "total": replay_bundle.run.total_tokens,
            }
        }),
    );
    sections.insert(
        "artifacts".to_owned(),
        json!({
            "count": replay_bundle.artifact_refs.len(),
            "refs": replay_bundle.artifact_refs.clone(),
        }),
    );
    sections.insert("run_trace".to_owned(), serde_json::to_value(run_trace)?);
    sections.insert("redaction_report".to_owned(), serde_json::to_value(redaction)?);

    Ok(TrajectoryExportBundle {
        schema_version: TRAJECTORY_EXPORT_SCHEMA_VERSION,
        manifest,
        events_jsonl,
        sections,
    })
}

pub(crate) fn build_unified_support_bundle_manifest(
    replay_bundle: &ReplayBundle,
    run_trace: &RunTraceV1,
    trajectory: Option<&TrajectoryExportBundle>,
    request: UnifiedSupportBundleRequest,
) -> Result<UnifiedSupportBundleManifest> {
    if request.include_trajectory {
        require_export_approval(&request.approval, "unified support bundle trajectory section")?;
    }
    let mut included_sections = vec![
        "runtime_path".to_owned(),
        "run_trace".to_owned(),
        "feature_flags".to_owned(),
        "cache_report".to_owned(),
        "mcp_trace".to_owned(),
        "execution_backend_trace".to_owned(),
        "verification_report".to_owned(),
        "redaction_report".to_owned(),
    ];
    let mut skipped_sections = BTreeMap::new();
    let trajectory_ref = if let Some(bundle) = trajectory {
        included_sections.push("trajectory".to_owned());
        Some(format!("trajectory://{}", bundle.manifest.run_id))
    } else {
        skipped_sections
            .insert("trajectory".to_owned(), "not requested or not approved".to_owned());
        None
    };

    Ok(UnifiedSupportBundleManifest {
        schema_version: UNIFIED_SUPPORT_BUNDLE_SCHEMA_VERSION,
        generated_at_unix_ms: request.generated_at_unix_ms,
        run_id: replay_bundle.source.run_id.clone(),
        approval: request.approval,
        included_sections,
        skipped_sections,
        redaction: run_trace.redaction.clone(),
        feature_flags: replay_bundle
            .config_snapshot
            .get("feature_rollouts")
            .cloned()
            .unwrap_or_else(|| json!({ "state": "not_captured" })),
        runtime_path: json!({
            "source": "run_trace.subsystem_coverage",
            "subsystems": run_trace.subsystem_coverage.clone(),
        }),
        trajectory_ref,
        operator_summary: vec![
            "run trace included as support-visible metadata".to_owned(),
            "raw prompts, raw secrets, binary payloads, and local paths are excluded".to_owned(),
            "trajectory export requires the recorded approval gate".to_owned(),
        ],
    })
}

fn run_trace_event(
    event: &ReplayTapeEvent,
    max_payload_bytes: usize,
    redaction: &mut RunTraceRedactionReport,
) -> RunTraceEvent {
    let (payload_summary, stats) = redacted_trace_payload(&event.payload, max_payload_bytes);
    redaction.path_redactions = redaction.path_redactions.saturating_add(stats.path_redactions);
    redaction.truncated_payloads =
        redaction.truncated_payloads.saturating_add(usize::from(stats.truncated));
    redaction.redacted_fields.extend(stats.redacted_fields);
    redaction.redacted_fields.sort();
    redaction.redacted_fields.dedup();
    let event_type = sanitize_trace_label(event.event_type.as_str(), "event_type");
    let subsystem = classify_trace_subsystem(event_type.as_str()).to_owned();

    RunTraceEvent {
        seq: event.seq,
        event_type,
        subsystem,
        outcome: trace_event_outcome(&event.payload),
        payload_summary,
        redaction_level: "strict_bounded".to_owned(),
    }
}

fn build_subsystem_coverage(
    events: &[RunTraceEvent],
) -> BTreeMap<String, RunTraceSubsystemCoverage> {
    let mut coverage = RUN_TRACE_REQUIRED_SUBSYSTEMS
        .iter()
        .map(|subsystem| {
            (
                (*subsystem).to_owned(),
                RunTraceSubsystemCoverage {
                    observed: false,
                    event_count: 0,
                    reason_code: "run_trace.subsystem.not_observed_in_capture".to_owned(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for event in events {
        let entry = coverage.entry(event.subsystem.clone()).or_insert(RunTraceSubsystemCoverage {
            observed: false,
            event_count: 0,
            reason_code: "run_trace.subsystem.observed".to_owned(),
        });
        entry.observed = true;
        entry.event_count = entry.event_count.saturating_add(1);
        entry.reason_code = "run_trace.subsystem.observed".to_owned();
    }
    coverage
}

fn base_run_trace_redaction_report(max_payload_bytes: usize) -> RunTraceRedactionReport {
    RunTraceRedactionReport {
        redaction_level: "strict_bounded".to_owned(),
        raw_secrets_allowed: false,
        raw_paths_allowed: false,
        model_visible_default: false,
        redacted_fields: Vec::new(),
        path_redactions: 0,
        truncated_payloads: 0,
        omitted_events: 0,
        max_payload_bytes,
    }
}

#[derive(Debug, Default)]
struct TracePayloadRedactionStats {
    redacted_fields: Vec<String>,
    path_redactions: usize,
    truncated: bool,
}

fn redacted_trace_payload(
    payload: &Value,
    max_payload_bytes: usize,
) -> (Value, TracePayloadRedactionStats) {
    let mut redacted = payload.clone();
    let mut stats = TracePayloadRedactionStats::default();
    redact_trace_value(&mut redacted, None, &mut stats);
    let encoded_len = serde_json::to_vec(&redacted).map_or(usize::MAX, |bytes| bytes.len());
    if encoded_len > max_payload_bytes {
        stats.truncated = true;
        let sha256 = serde_json::to_vec(&redacted).ok().map(|bytes| sha256_hex(bytes.as_slice()));
        redacted = json!({
            "truncated": true,
            "original_bytes": encoded_len,
            "sha256": sha256,
        });
    }
    (redacted, stats)
}

fn redact_trace_value(
    value: &mut Value,
    key_context: Option<&str>,
    stats: &mut TracePayloadRedactionStats,
) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                if is_sensitive_key(key.as_str()) {
                    *child = json!("<redacted>");
                    stats.redacted_fields.push(key.clone());
                } else {
                    redact_trace_value(child, Some(key.as_str()), stats);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_trace_value(item, key_context, stats);
            }
        }
        Value::String(raw) => {
            let redacted = redact_trace_string(raw.as_str());
            if redacted != *raw {
                if key_context.is_some_and(is_sensitive_key) {
                    stats.redacted_fields.push(key_context.unwrap_or_default().to_owned());
                }
                if redacted.contains("<redacted:path>") || redacted.contains("<redacted:home>") {
                    stats.path_redactions = stats.path_redactions.saturating_add(1);
                }
                *raw = redacted;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn redact_trace_string(raw: &str) -> String {
    let redacted = redact_diagnostic_text(raw);
    redact_internal_runtime_paths(redacted.as_str())
}

fn sanitize_trace_label(raw: &str, fallback: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return fallback.to_owned();
    }
    let mut label = trimmed
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.') {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect::<String>();
    label.truncate(96);
    label
}

fn classify_trace_subsystem(event_type: &str) -> &'static str {
    if event_type.contains("recovery") {
        "recovery"
    } else if event_type.contains("provider") || event_type.contains("model") {
        "provider"
    } else if event_type.contains("approval") {
        "approval"
    } else if event_type.contains("policy") {
        "policy"
    } else if event_type.contains("sandbox") {
        "sandbox"
    } else if event_type.contains("hook") {
        "hook"
    } else if event_type.contains("lsp") || event_type.contains("code_intel") {
        "lsp"
    } else if event_type.contains("verification") {
        "verification"
    } else if event_type.contains("compact") {
        "compaction"
    } else if event_type.contains("advisor") {
        "advisor"
    } else if event_type.contains("harness") {
        "harness"
    } else if event_type.contains("browser") {
        "browser"
    } else if event_type.contains("tool") {
        "tool"
    } else {
        "runtime"
    }
}

fn trace_event_outcome(payload: &Value) -> String {
    let raw = payload
        .get("outcome")
        .or_else(|| payload.get("status"))
        .or_else(|| payload.get("state"))
        .and_then(Value::as_str)
        .unwrap_or("recorded");
    sanitize_trace_label(raw, "recorded")
}

fn require_export_approval(gate: &ExportApprovalGate, operation: &str) -> Result<()> {
    if !gate.approved {
        bail!("{operation} requires an explicit support export approval gate");
    }
    if gate.approval_id.as_deref().is_none_or(str::is_empty) {
        bail!("{operation} approval gate must include approval_id");
    }
    Ok(())
}

fn replay_run_snapshot(run: &OrchestratorRunStatusSnapshot) -> ReplayRunSnapshot {
    ReplayRunSnapshot {
        state: run.state.clone(),
        principal: run.principal.clone(),
        device_id: run.device_id.clone(),
        channel: run.channel.clone(),
        normalized_user_input: extract_normalized_user_input(run.parameter_delta_json.as_deref()),
        prompt_tokens: run.prompt_tokens,
        completion_tokens: run.completion_tokens,
        total_tokens: run.total_tokens,
        last_error: run.last_error.clone(),
        parent_run_id: run.parent_run_id.clone(),
        origin_run_id: run.origin_run_id.clone(),
        parameter_delta: run
            .parameter_delta_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Value>(raw).ok()),
    }
}

fn extract_normalized_user_input(parameter_delta_json: Option<&str>) -> Option<Value> {
    let value = parameter_delta_json.and_then(|raw| serde_json::from_str::<Value>(raw).ok())?;
    value.get("user_input").or_else(|| value.get("input")).or_else(|| value.get("prompt")).cloned()
}

fn replay_config_snapshot(
    replay_capture: &ReplayCaptureConfig,
    feature_rollouts: &FeatureRolloutsConfig,
) -> Value {
    json!({
        "replay_capture": {
            "mode": replay_capture.mode.as_str(),
            "capture_runtime_decisions": replay_capture.capture_runtime_decisions,
            "max_events_per_run": replay_capture.max_events_per_run,
        },
        "feature_rollouts": {
            "replay_capture": {
                "enabled": feature_rollouts.replay_capture.enabled,
                "source": feature_rollouts.replay_capture.source,
            },
            "auxiliary_executor": {
                "enabled": feature_rollouts.auxiliary_executor.enabled,
                "source": feature_rollouts.auxiliary_executor.source,
            },
            "flow_orchestration": {
                "enabled": feature_rollouts.flow_orchestration.enabled,
                "source": feature_rollouts.flow_orchestration.source,
            },
        },
        "network_policy": {
            "offline_replay_requires_live_network": false,
            "offline_replay_requires_live_provider": false,
        },
        "mcp": {
            "offline_replay_requires_live_server": false,
            "capture_discovery_snapshots": true,
            "capture_tool_import_snapshots": true,
        },
    })
}

fn replay_artifact_refs(run: &OrchestratorRunStatusSnapshot) -> Vec<ReplayArtifactRef> {
    let mut refs = Vec::new();
    if let Some(delegation) = run.delegation.as_ref() {
        refs.push(ReplayArtifactRef {
            artifact_id: format!("delegation:{}", run.run_id),
            kind: "delegation_snapshot".to_owned(),
            reference: format!("journal://orchestrator_runs/{}/delegation_json", run.run_id),
            sha256: serde_json::to_vec(delegation).ok().map(|bytes| sha256_hex(bytes.as_slice())),
            size_bytes: serde_json::to_vec(delegation)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok()),
        });
    }
    if let Some(merge_result) = run.merge_result.as_ref() {
        refs.push(ReplayArtifactRef {
            artifact_id: format!("merge:{}", run.run_id),
            kind: "delegation_merge_result".to_owned(),
            reference: format!("journal://orchestrator_runs/{}/merge_result_json", run.run_id),
            sha256: serde_json::to_vec(merge_result).ok().map(|bytes| sha256_hex(bytes.as_slice())),
            size_bytes: serde_json::to_vec(merge_result)
                .ok()
                .and_then(|bytes| u64::try_from(bytes.len()).ok()),
        });
    }
    refs
}

fn replay_tool_result_artifact_ref(artifact: &ToolResultArtifactRef) -> ReplayArtifactRef {
    ReplayArtifactRef {
        artifact_id: artifact.artifact_id.clone(),
        kind: "tool_result".to_owned(),
        reference: format!(
            "tool-result-artifact://{}/{}",
            artifact.storage_backend, artifact.artifact_id
        ),
        sha256: Some(artifact.digest_sha256.clone()),
        size_bytes: Some(artifact.size_bytes),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use palyra_common::replay_bundle::{replay_bundle_offline, ReplayRunStatus};
    use serde_json::json;

    use super::*;
    use crate::{
        config::{FeatureRolloutsConfig, ReplayCaptureConfig},
        journal::{
            JournalConfig, OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest,
            OrchestratorTapeAppendRequest,
        },
        runtime_diagnostics::{
            build_run_stage_timing_report, RunStageTimingInput, RunStageTimingReport,
        },
    };

    #[test]
    fn capture_incident_replay_bundle_exports_redacted_offline_replayable_run() {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let store = JournalStore::open(JournalConfig {
            db_path: temp.path().join("journal.sqlite3"),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 1_000,
        })
        .expect("journal should open");
        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1".to_owned(),
                session_key: "session:replay-test".to_owned(),
                session_label: Some("Replay test".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "device:local".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("session should be created");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FA1".to_owned(),
                origin_kind: "run_stream".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:ops".to_owned()),
                parameter_delta_json: Some(
                    json!({ "user_input": { "text": "call https://example.test?token=secret" } })
                        .to_string(),
                ),

                delegated_admission: None,
            })
            .expect("run should start");
        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned(),
                seq: 0,
                event_type: "tool_proposal".to_owned(),
                payload_json: json!({
                    "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FA3",
                    "tool_name": "palyra.http.fetch",
                    "input_json": {
                        "url": "https://example.test/callback?access_token=raw&mode=ok",
                        "headers": { "authorization": "Bearer raw" }
                    }
                })
                .to_string(),
            })
            .expect("proposal should append");
        store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2".to_owned(),
                seq: 1,
                event_type: "tool_result".to_owned(),
                payload_json: json!({
                    "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FA3",
                    "success": true,
                    "output_json": { "status": 200 },
                    "error": ""
                })
                .to_string(),
            })
            .expect("result should append");

        let bundle = capture_incident_replay_bundle(IncidentReplayCaptureRequest {
            journal_store: &store,
            replay_capture: &ReplayCaptureConfig::default(),
            feature_rollouts: &FeatureRolloutsConfig::default(),
            run_id: "01ARZ3NDEKTSV4RRFFQ69G5FA2",
            generated_at_unix_ms: 1_730_000_000_000,
            max_events: 128,
        })
        .expect("bundle should capture");
        let encoded = serde_json::to_string(&bundle).expect("bundle should serialize");
        assert!(!encoded.contains("access_token=raw"));
        assert!(!encoded.contains("Bearer raw"));
        assert_eq!(
            bundle.config_snapshot.pointer("/mcp/offline_replay_requires_live_server"),
            Some(&json!(false))
        );
        assert_eq!(
            bundle.config_snapshot.pointer("/mcp/capture_tool_import_snapshots"),
            Some(&json!(true))
        );
        assert_eq!(replay_bundle_offline(&bundle).status, ReplayRunStatus::Passed);
    }

    #[test]
    fn run_trace_v1_redacts_payloads_and_covers_p0_subsystems() {
        let bundle = sample_replay_bundle_with_events(vec![
            ("provider.request", json!({"status":"started","authorization":"Bearer raw"})),
            (
                "tool_proposal",
                json!({"tool_name":"palyra.fs.read_file","path":"C:\\Users\\Palo\\secret.txt"}),
            ),
            ("approval.requested", json!({"status":"pending"})),
            ("policy.decision", json!({"outcome":"allow"})),
            ("sandbox.plan", json!({"state":"planned"})),
            ("hook.completed", json!({"status":"ok"})),
            ("lsp.crash", json!({"status":"recovered"})),
            ("verification.result", json!({"status":"passed"})),
            ("provider.recovery.decision", json!({"outcome":"retry"})),
            ("compaction.completed", json!({"status":"ok"})),
            ("advisor.completed", json!({"status":"ok"})),
            ("harness.completed", json!({"status":"completed"})),
            ("browser.rescue.attempt", json!({"status":"skipped"})),
        ]);
        let trace = build_run_trace_v1(RunTraceBuildInput {
            replay_bundle: &bundle,
            generated_at_unix_ms: 1_730_000_001_000,
            stage_timings: sample_stage_timing_report(),
            max_events: 64,
            max_payload_bytes: 256,
            trace_unavailable_reason: None,
        });
        let encoded = serde_json::to_string(&trace).expect("trace should serialize");

        assert!(trace.available);
        assert!(!trace.model_visible_default);
        assert_eq!(trace.journal.first_seq, Some(0));
        assert_eq!(trace.journal.last_seq, Some(12));
        assert_eq!(trace.stage_timings.records.len(), 2);
        for subsystem in RUN_TRACE_REQUIRED_SUBSYSTEMS {
            assert_eq!(
                trace.subsystem_coverage.get(*subsystem).map(|coverage| coverage.observed),
                Some(true),
                "subsystem {subsystem} should be observed"
            );
        }
        assert!(!encoded.contains("Bearer raw"));
        assert!(!encoded.contains("C:\\Users\\Palo"));
    }

    #[test]
    fn trajectory_export_requires_approval_and_caps_events() {
        let bundle = sample_replay_bundle_with_events(vec![
            ("provider.request", json!({"status":"started"})),
            ("tool_result", json!({"status":"completed","output":"x".repeat(4_000)})),
        ]);
        let trace = build_run_trace_v1(RunTraceBuildInput {
            replay_bundle: &bundle,
            generated_at_unix_ms: 1_730_000_001_000,
            stage_timings: sample_stage_timing_report(),
            max_events: 64,
            max_payload_bytes: 256,
            trace_unavailable_reason: None,
        });
        let denied = build_trajectory_export_bundle(
            &bundle,
            &trace,
            TrajectoryExportRequest {
                approval: ExportApprovalGate {
                    approved: false,
                    approval_id: None,
                    actor_ref: None,
                    required_reason: "support export".to_owned(),
                },
                max_events: 16,
                max_total_bytes: 1_024,
            },
        )
        .expect_err("trajectory export must require approval");
        assert!(denied.to_string().contains("approval gate"));

        let bundle = build_trajectory_export_bundle(
            &bundle,
            &trace,
            TrajectoryExportRequest {
                approval: approved_gate(),
                max_events: 1,
                max_total_bytes: 1_024,
            },
        )
        .expect("approved trajectory export should build");

        assert_eq!(bundle.schema_version, TRAJECTORY_EXPORT_SCHEMA_VERSION);
        assert!(bundle.manifest.offline_support_ready);
        assert_eq!(
            bundle.manifest.skipped_sections.get("prompts").map(String::as_str),
            Some("raw prompts are never exported")
        );
        assert_eq!(bundle.events_jsonl.lines().count(), 1);
        assert!(bundle.sections.contains_key("redaction_report"));
    }

    #[test]
    fn unified_support_bundle_manifest_lists_included_and_skipped_sections() {
        let bundle = sample_replay_bundle_with_events(vec![
            ("provider.request", json!({"status":"started"})),
            ("verification.result", json!({"status":"passed"})),
        ]);
        let trace = build_run_trace_v1(RunTraceBuildInput {
            replay_bundle: &bundle,
            generated_at_unix_ms: 1_730_000_001_000,
            stage_timings: sample_stage_timing_report(),
            max_events: 64,
            max_payload_bytes: 256,
            trace_unavailable_reason: None,
        });
        let trajectory = build_trajectory_export_bundle(
            &bundle,
            &trace,
            TrajectoryExportRequest {
                approval: approved_gate(),
                max_events: 16,
                max_total_bytes: 8 * 1024,
            },
        )
        .expect("trajectory should build");
        let manifest = build_unified_support_bundle_manifest(
            &bundle,
            &trace,
            Some(&trajectory),
            UnifiedSupportBundleRequest {
                approval: approved_gate(),
                generated_at_unix_ms: 1_730_000_002_000,
                include_trajectory: true,
            },
        )
        .expect("support manifest should build");

        assert_eq!(manifest.schema_version, UNIFIED_SUPPORT_BUNDLE_SCHEMA_VERSION);
        assert!(manifest.included_sections.contains(&"run_trace".to_owned()));
        assert!(manifest.included_sections.contains(&"trajectory".to_owned()));
        assert_eq!(
            manifest.trajectory_ref,
            Some(format!("trajectory://{}", trajectory.manifest.run_id))
        );
        assert!(manifest.operator_summary.iter().any(|line| line.contains("raw prompts")));
    }

    #[test]
    fn replay_gate_artifacts_use_a_fresh_leaf_and_mountpoint_guards() {
        let script = include_str!("../../../scripts/test/run-replay-gate.sh");

        assert!(!script.contains("rm -rf"));
        assert!(script.contains("is_mount_point"));
        assert!(script.contains("mountpoint -q"));
        assert!(script.contains("stat -c '%m'"));
        assert!(script.contains("stat -f '%d'"));
        assert!(script.contains("mktemp -d"));
        assert!(script.contains("ARTIFACT_RELATIVE=\"${ARTIFACT_ABSOLUTE#"));
    }

    fn sample_replay_bundle_with_events(events: Vec<(&str, Value)>) -> ReplayBundle {
        let temp = tempfile::tempdir().expect("tempdir should be created");
        let store = JournalStore::open(JournalConfig {
            db_path: temp.path().join("journal.sqlite3"),
            hash_chain_enabled: false,
            max_payload_bytes: 256 * 1024,
            max_events: 1_000,
        })
        .expect("journal should open");
        store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "run-trace-session".to_owned(),
                session_key: "session:run-trace-test".to_owned(),
                session_label: Some("Run trace test".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "device:local".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("session should be created");
        store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "run-trace-run".to_owned(),
                session_id: "run-trace-session".to_owned(),
                origin_kind: "run_stream".to_owned(),
                origin_run_id: None,
                triggered_by_principal: Some("user:ops".to_owned()),
                parameter_delta_json: Some(
                    json!({ "user_input": { "text": "diagnose failed tool run" } }).to_string(),
                ),

                delegated_admission: None,
            })
            .expect("run should start");
        for (seq, (event_type, payload)) in events.into_iter().enumerate() {
            store
                .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                    run_id: "run-trace-run".to_owned(),
                    seq: i64::try_from(seq).expect("seq should fit i64"),
                    event_type: event_type.to_owned(),
                    payload_json: payload.to_string(),
                })
                .expect("event should append");
        }

        capture_incident_replay_bundle(IncidentReplayCaptureRequest {
            journal_store: &store,
            replay_capture: &ReplayCaptureConfig::default(),
            feature_rollouts: &FeatureRolloutsConfig::default(),
            run_id: "run-trace-run",
            generated_at_unix_ms: 1_730_000_000_000,
            max_events: 128,
        })
        .expect("bundle should capture")
    }

    fn sample_stage_timing_report() -> RunStageTimingReport {
        build_run_stage_timing_report(&[
            RunStageTimingInput {
                stage: "prepare".to_owned(),
                started_at_unix_ms: 1_000,
                completed_at_unix_ms: Some(1_010),
                first_signal_at_unix_ms: None,
                timeout_ms: None,
                timeout_kind: None,
                outcome: "ok".to_owned(),
            },
            RunStageTimingInput {
                stage: "provider_request".to_owned(),
                started_at_unix_ms: 1_020,
                completed_at_unix_ms: Some(1_200),
                first_signal_at_unix_ms: Some(1_050),
                timeout_ms: Some(1_000),
                timeout_kind: None,
                outcome: "ok".to_owned(),
            },
        ])
    }

    fn approved_gate() -> ExportApprovalGate {
        ExportApprovalGate {
            approved: true,
            approval_id: Some("approval-run-trace".to_owned()),
            actor_ref: Some("operator:test".to_owned()),
            required_reason: "support export".to_owned(),
        }
    }
}
