//! Deterministic incident replay bundles: capture, redaction, validation, offline replay.
//!
//! Builds byte-stable, secret-free bundles from orchestrator tape events (wall-clock
//! timestamps zeroed, identifiers pseudonymized) and re-derives expected outputs to detect
//! drift offline. The canonical serialization and contract version are pinned by the
//! replay gate (`scripts/test/run-replay-gate.sh`); shape or formatting changes invalidate
//! stored bundle hashes and golden fixtures.

use std::collections::{BTreeMap, HashMap};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};

use crate::{
    redaction::{
        is_sensitive_key, redact_auth_error, redact_header, redact_url,
        redact_url_segments_in_text, REDACTED,
    },
    runtime_contracts::{IdempotencyRecordSnapshot, RunLifecycleTransitionRecord},
    versioned_json::{parse_versioned_json, VersionedJsonFormat},
};

/// Human-readable format name embedded in versioned-JSON parse errors.
pub const REPLAY_BUNDLE_FORMAT_NAME: &str = "palyra incident replay bundle";
/// Current replay bundle schema version; newer versions are rejected until migrated.
pub const REPLAY_BUNDLE_SCHEMA_VERSION: u32 = 1;
/// Contract identifier validated against captured bundles.
pub const REPLAY_BUNDLE_CONTRACT_VERSION: &str = "incident-replay-v1";

// Hard caps keeping bundles reviewable and safe to load in CI; oversized captures must be
// truncated upstream, never inflated here.
const MAX_REPLAY_TAPE_EVENTS: usize = 4_096;
const MAX_REPLAY_PAYLOAD_BYTES: usize = 512 * 1024;

/// Top-level incident replay bundle: redacted capture, expected outputs, integrity hash.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayBundle {
    pub schema_version: u32,
    pub contract_version: String,
    pub bundle_id: String,
    pub generated_at_unix_ms: i64,
    pub source: ReplaySource,
    pub capture: ReplayCaptureMetadata,
    pub run: ReplayRunSnapshot,
    pub config_snapshot: Value,
    pub model_exchanges: Vec<ReplayModelExchange>,
    pub tape_events: Vec<ReplayTapeEvent>,
    pub tool_exchanges: Vec<ReplayToolExchange>,
    pub http_exchanges: Vec<ReplayHttpExchange>,
    pub approvals: Vec<ReplayApprovalExchange>,
    pub queue_decisions: Vec<ReplayDecisionRecord>,
    pub auxiliary_tasks: Vec<ReplayDecisionRecord>,
    pub flow_events: Vec<ReplayDecisionRecord>,
    pub lifecycle_transitions: Vec<RunLifecycleTransitionRecord>,
    pub idempotency_records: Vec<IdempotencyRecordSnapshot>,
    pub artifact_refs: Vec<ReplayArtifactRef>,
    pub expected: ReplayExpectedOutputs,
    pub redaction: ReplayRedactionReport,
    pub integrity: ReplayIntegrity,
}

/// Identifies the product, run, and schema policy a bundle was captured from.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplaySource {
    pub product: String,
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    pub origin_kind: String,
    pub schema_policy: String,
}

/// Capture-time settings and warnings recorded alongside the tape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayCaptureMetadata {
    pub captured_at_unix_ms: i64,
    pub capture_mode: String,
    pub max_events_per_run: usize,
    pub truncated: bool,
    pub inline_sections: Vec<String>,
    pub referenced_sections: Vec<String>,
    pub warnings: Vec<String>,
}

/// Redacted snapshot of the captured run's state and token usage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayRunSnapshot {
    pub state: String,
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalized_user_input: Option<Value>,
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin_run_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameter_delta: Option<Value>,
}

/// Single normalized orchestrator tape event, ordered by `seq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayTapeEvent {
    pub seq: i64,
    pub event_type: String,
    pub payload: Value,
}

/// Hash-level summary of one model exchange reconstructed from token events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayModelExchange {
    pub exchange_id: String,
    pub provider: String,
    pub model: String,
    pub request_metadata: Value,
    pub response: Value,
}

/// Tool proposal, decision, result, and attestation grouped by proposal id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayToolExchange {
    pub proposal_id: String,
    pub tool_name: String,
    pub input: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attestation: Option<Value>,
}

/// HTTP fetch exchange derived from `palyra.http.fetch` tool calls.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayHttpExchange {
    pub exchange_id: String,
    pub request: Value,
    pub response: Value,
    pub fixture_ref: ReplayArtifactRef,
}

/// Approval request/response pair grouped by approval id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayApprovalExchange {
    pub approval_id: String,
    pub proposal_id: String,
    pub request: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<Value>,
}

/// Generic decision record (queue, auxiliary, flow) extracted from the tape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayDecisionRecord {
    pub record_id: String,
    pub kind: String,
    pub payload: Value,
}

/// Reference to an out-of-band artifact; the reference string itself is pseudonymized.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayArtifactRef {
    pub artifact_id: String,
    pub kind: String,
    pub reference: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
}

/// Outputs re-derived during offline replay and compared against the capture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReplayExpectedOutputs {
    pub tape_event_count: usize,
    pub tape_event_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_answer_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub final_answer_summary: Option<String>,
    pub tool_outcomes: Vec<ReplayExpectedToolOutcome>,
    pub approval_decisions: Vec<ReplayExpectedApprovalDecision>,
    pub http_exchange_count: usize,
    pub auxiliary_task_count: usize,
    pub flow_event_count: usize,
    pub lifecycle_transition_count: usize,
    pub idempotency_record_count: usize,
    pub artifact_ref_count: usize,
}

/// Expected outcome of one tool exchange, carrying hashes rather than raw output.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayExpectedToolOutcome {
    pub proposal_id: String,
    pub tool_name: String,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Expected approval verdict for one approval exchange.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplayExpectedApprovalDecision {
    pub approval_id: String,
    pub proposal_id: String,
    pub approved: bool,
    pub decision_scope: String,
}

/// Counters describing how much the normalizer redacted or rewrote during capture.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReplayRedactionReport {
    pub redacted_fields: usize,
    pub normalized_timestamps: usize,
    pub pseudonymized_identifiers: usize,
    pub warnings: Vec<String>,
}

/// Canonical-bytes digest and the canonicalization scheme that produced it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ReplayIntegrity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub canonical_sha256: Option<String>,
    pub canonicalization: String,
}

/// Raw capture handed to [`build_replay_bundle`] before normalization and redaction.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayBundleBuildInput {
    /// Capture-time wall clock; deliberately ignored by the builder, which pins the
    /// bundle's `generated_at_unix_ms` to 0 for determinism.
    pub generated_at_unix_ms: i64,
    pub source: ReplaySource,
    pub capture: ReplayCaptureMetadata,
    pub run: ReplayRunSnapshot,
    pub config_snapshot: Value,
    pub tape_events: Vec<ReplayTapeEvent>,
    pub lifecycle_transitions: Vec<RunLifecycleTransitionRecord>,
    pub idempotency_records: Vec<IdempotencyRecordSnapshot>,
    pub artifact_refs: Vec<ReplayArtifactRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReplayCaptureCounts {
    artifact_refs: usize,
    auxiliary_tasks: usize,
    flow_events: usize,
    lifecycle_transitions: usize,
    idempotency_records: usize,
}

/// Result of statically validating a bundle: schema, limits, and secret scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayValidationReport {
    pub valid: bool,
    pub checked_values: usize,
    pub issues: Vec<ReplayValidationIssue>,
}

/// Single validation finding with a JSONPath-style location.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayValidationIssue {
    pub path: String,
    pub reason: String,
}

/// Outcome of an offline replay: validation result plus expected-vs-actual diffs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayRunReport {
    pub status: ReplayRunStatus,
    pub checked_categories: Vec<String>,
    pub diff_categories: BTreeMap<String, usize>,
    pub diffs: Vec<ReplayDiff>,
    pub validation: ReplayValidationReport,
}

/// Overall offline replay verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayRunStatus {
    Passed,
    Failed,
}

/// One expected-vs-actual divergence found during offline replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReplayDiff {
    pub category: String,
    pub path: String,
    pub expected: String,
    pub actual: String,
}

// Walks captured JSON, redacting secrets, zeroing wall-clock fields, and replacing
// identifiers with stable per-bundle aliases so output is deterministic and PII-free.
#[derive(Debug, Default)]
struct ReplayNormalizer {
    redaction: ReplayRedactionReport,
    id_aliases: HashMap<String, String>,
    next_id: usize,
}

#[derive(Debug, Default)]
struct ToolAccumulator {
    tool_name: Option<String>,
    input: Option<Value>,
    decision: Option<Value>,
    result: Option<Value>,
    attestation: Option<Value>,
}

#[derive(Debug, Default)]
struct ApprovalAccumulator {
    proposal_id: Option<String>,
    request: Option<Value>,
    response: Option<Value>,
}

/// Returns the machine-readable replay contract descriptor served by diagnostics surfaces.
///
/// INTENTIONAL: consumed verbatim by CLI support bundles and console diagnostics; treat
/// the contents as golden-snapshot material and change them only with the gates that pin
/// them.
#[must_use]
pub fn replay_contract_snapshot() -> Value {
    json!({
        "schema_version": REPLAY_BUNDLE_SCHEMA_VERSION,
        "contract_version": REPLAY_BUNDLE_CONTRACT_VERSION,
        "format": REPLAY_BUNDLE_FORMAT_NAME,
        "schema_policy": "backward-compatible additive fields; newer schema versions must be rejected until migrated",
        "inline_sections": [
            "source",
            "capture",
            "run",
            "config_snapshot",
            "model_exchanges",
            "tape_events",
            "tool_exchanges",
            "http_exchanges",
            "approvals",
            "queue_decisions",
            "auxiliary_tasks",
            "flow_events",
            "expected",
            "redaction",
            "integrity"
        ],
        "referenced_sections": [
            "large artifacts",
            "binary HTTP bodies",
            "browser screenshots",
            "workspace files",
            "journal rows outside the captured run"
        ],
        "offline_runtime": "deterministic stubs only; no live network, provider, browser, vault, or filesystem mutation"
    })
}

/// Parses a replay bundle from JSON bytes, rejecting newer schema versions.
///
/// # Errors
///
/// Fails when the payload is not valid JSON, is not an object, declares a schema version
/// newer than [`REPLAY_BUNDLE_SCHEMA_VERSION`], or does not deserialize into
/// [`ReplayBundle`].
pub fn parse_replay_bundle(bytes: &[u8]) -> Result<ReplayBundle> {
    parse_versioned_json(
        bytes,
        VersionedJsonFormat::new(REPLAY_BUNDLE_FORMAT_NAME, REPLAY_BUNDLE_SCHEMA_VERSION),
        &[],
    )
}

/// Builds a redacted, deterministic replay bundle from a raw capture.
///
/// Normalization zeroes wall-clock timestamps, pseudonymizes identifiers, redacts
/// secret-bearing strings, sorts capture metadata, and finalizes the integrity hash, so
/// identical captures always produce identical canonical bytes.
///
/// # Errors
///
/// Fails when the capture exceeds the tape event limit or when lifecycle/idempotency
/// records cannot be round-tripped through JSON for normalization.
pub fn build_replay_bundle(input: ReplayBundleBuildInput) -> Result<ReplayBundle> {
    if input.tape_events.len() > MAX_REPLAY_TAPE_EVENTS {
        bail!(
            "replay bundle contains too many tape events ({} > {})",
            input.tape_events.len(),
            MAX_REPLAY_TAPE_EVENTS
        );
    }

    let mut normalizer = ReplayNormalizer::default();
    let mut source = input.source;
    normalizer.normalize_string_field(&mut source.run_id, "run_id");
    if let Some(session_id) = source.session_id.as_mut() {
        normalizer.normalize_string_field(session_id, "session_id");
    }

    let mut run = input.run;
    normalizer.normalize_string_field(&mut run.principal, "principal");
    normalizer.normalize_string_field(&mut run.device_id, "device_id");
    if let Some(parent_run_id) = run.parent_run_id.as_mut() {
        normalizer.normalize_string_field(parent_run_id, "parent_run_id");
    }
    if let Some(origin_run_id) = run.origin_run_id.as_mut() {
        normalizer.normalize_string_field(origin_run_id, "origin_run_id");
    }
    if let Some(error) = run.last_error.as_mut() {
        *error = redact_auth_error(redact_url_segments_in_text(error).as_str());
    }
    if let Some(input) = run.normalized_user_input.as_mut() {
        normalizer.normalize_value(input, "$.run.normalized_user_input", None);
    }
    if let Some(delta) = run.parameter_delta.as_mut() {
        normalizer.normalize_value(delta, "$.run.parameter_delta", None);
    }

    let mut config_snapshot = input.config_snapshot;
    normalizer.normalize_value(&mut config_snapshot, "$.config_snapshot", None);

    let mut tape_events = Vec::with_capacity(input.tape_events.len());
    for mut event in input.tape_events {
        let path = format!("$.tape_events[{}].payload", event.seq);
        normalizer.normalize_value(
            &mut event.payload,
            path.as_str(),
            Some(event.event_type.as_str()),
        );
        tape_events.push(event);
    }
    tape_events.sort_by_key(|event| event.seq);

    let model_exchanges = extract_model_exchanges(&tape_events);
    let tool_exchanges = extract_tool_exchanges(&tape_events);
    let http_exchanges = extract_http_exchanges(&tool_exchanges);
    let approvals = extract_approvals(&tape_events);
    let queue_decisions = extract_decision_records(&tape_events, "queue");
    let auxiliary_tasks = extract_decision_records(&tape_events, "auxiliary");
    let flow_events = extract_decision_records(&tape_events, "flow");
    let lifecycle_transitions =
        normalize_lifecycle_transitions(input.lifecycle_transitions, &mut normalizer)?;
    let idempotency_records =
        normalize_idempotency_records(input.idempotency_records, &mut normalizer)?;
    let artifact_refs = normalize_artifact_refs(input.artifact_refs, &mut normalizer);
    let expected = expected_outputs_from_capture(
        &tape_events,
        &tool_exchanges,
        &approvals,
        &http_exchanges,
        ReplayCaptureCounts {
            artifact_refs: artifact_refs.len(),
            auxiliary_tasks: auxiliary_tasks.len(),
            flow_events: flow_events.len(),
            lifecycle_transitions: lifecycle_transitions.len(),
            idempotency_records: idempotency_records.len(),
        },
    );

    let mut capture = input.capture;
    // Wall-clock capture time is zeroed (like generated_at below) so rebuilding the same
    // run always yields identical canonical bytes.
    capture.captured_at_unix_ms = 0;
    // Hitting the per-run cap means the capture may have dropped events upstream.
    capture.truncated |= tape_events.len() >= capture.max_events_per_run;
    capture.inline_sections.sort();
    capture.inline_sections.dedup();
    capture.referenced_sections.sort();
    capture.referenced_sections.dedup();
    capture.warnings.sort();
    capture.warnings.dedup();

    let mut bundle = ReplayBundle {
        schema_version: REPLAY_BUNDLE_SCHEMA_VERSION,
        contract_version: REPLAY_BUNDLE_CONTRACT_VERSION.to_owned(),
        bundle_id: stable_bundle_id(source.run_id.as_str()),
        generated_at_unix_ms: 0,
        source,
        capture,
        run,
        config_snapshot,
        model_exchanges,
        tape_events,
        tool_exchanges,
        http_exchanges,
        approvals,
        queue_decisions,
        auxiliary_tasks,
        flow_events,
        lifecycle_transitions,
        idempotency_records,
        artifact_refs,
        expected,
        redaction: normalizer.redaction,
        integrity: ReplayIntegrity {
            canonical_sha256: None,
            canonicalization: "serde_json_pretty_sorted_maps_without_integrity_hash".to_owned(),
        },
    };

    finalize_replay_bundle(&mut bundle)?;
    Ok(bundle)
}

fn normalize_artifact_refs(
    refs: Vec<ReplayArtifactRef>,
    normalizer: &mut ReplayNormalizer,
) -> Vec<ReplayArtifactRef> {
    refs.into_iter()
        .map(|mut artifact| {
            if !artifact.reference.trim().is_empty() {
                artifact.reference = normalizer.alias_for_identifier(artifact.reference.as_str());
            }
            artifact
        })
        .collect()
}

/// Recomputes the canonical integrity hash over the bundle.
///
/// # Errors
///
/// Fails only when the bundle cannot be serialized to canonical JSON.
pub fn finalize_replay_bundle(bundle: &mut ReplayBundle) -> Result<()> {
    // Hash with the digest field cleared so the digest covers everything except itself.
    bundle.integrity.canonical_sha256 = None;
    let hash = sha256_hex(canonical_replay_bundle_bytes(bundle)?.as_slice());
    bundle.integrity.canonical_sha256 = Some(hash);
    Ok(())
}

/// Serializes the bundle to its canonical byte representation.
///
/// INTENTIONAL: pretty-printed JSON with serde_json's default sorted (BTreeMap) object
/// order is the canonicalization named in [`ReplayIntegrity`]; changing the formatting
/// invalidates every stored bundle hash.
///
/// # Errors
///
/// Fails only when JSON serialization fails.
pub fn canonical_replay_bundle_bytes(bundle: &ReplayBundle) -> Result<Vec<u8>> {
    serde_json::to_vec_pretty(bundle).context("failed to encode replay bundle")
}

/// Validates schema/contract versions, size limits, and absence of unredacted secrets.
pub fn validate_replay_bundle(bundle: &ReplayBundle) -> ReplayValidationReport {
    let mut issues = Vec::new();
    if bundle.schema_version != REPLAY_BUNDLE_SCHEMA_VERSION {
        issues.push(ReplayValidationIssue {
            path: "$.schema_version".to_owned(),
            reason: format!(
                "unsupported schema version {} (expected {})",
                bundle.schema_version, REPLAY_BUNDLE_SCHEMA_VERSION
            ),
        });
    }
    if bundle.contract_version != REPLAY_BUNDLE_CONTRACT_VERSION {
        issues.push(ReplayValidationIssue {
            path: "$.contract_version".to_owned(),
            reason: "unsupported replay contract version".to_owned(),
        });
    }
    if bundle.tape_events.len() > MAX_REPLAY_TAPE_EVENTS {
        issues.push(ReplayValidationIssue {
            path: "$.tape_events".to_owned(),
            reason: "too many tape events".to_owned(),
        });
    }
    for event in &bundle.tape_events {
        // Fail closed: a payload that cannot be serialized counts as oversized.
        let payload_bytes =
            serde_json::to_vec(&event.payload).map_or(usize::MAX, |bytes| bytes.len());
        if payload_bytes > MAX_REPLAY_PAYLOAD_BYTES {
            issues.push(ReplayValidationIssue {
                path: format!("$.tape_events[{}].payload", event.seq),
                reason: "tape event payload exceeds replay bundle limit".to_owned(),
            });
        }
    }

    let mut checked_values = 0_usize;
    match serde_json::to_value(bundle) {
        Ok(value) => {
            scan_for_unredacted_secrets(&value, "$", None, &mut checked_values, &mut issues)
        }
        Err(error) => issues.push(ReplayValidationIssue {
            path: "$".to_owned(),
            reason: format!("replay bundle cannot be serialized for secret scan: {error}"),
        }),
    }

    ReplayValidationReport { valid: issues.is_empty(), checked_values, issues }
}

/// Replays a bundle offline by re-deriving expected outputs and diffing the capture.
///
/// Purely deterministic: no network, provider, or filesystem access is involved.
pub fn replay_bundle_offline(bundle: &ReplayBundle) -> ReplayRunReport {
    let validation = validate_replay_bundle(bundle);
    let actual_expected = expected_outputs_from_capture(
        &bundle.tape_events,
        &bundle.tool_exchanges,
        &bundle.approvals,
        &bundle.http_exchanges,
        ReplayCaptureCounts {
            artifact_refs: bundle.artifact_refs.len(),
            auxiliary_tasks: bundle.auxiliary_tasks.len(),
            flow_events: bundle.flow_events.len(),
            lifecycle_transitions: bundle.lifecycle_transitions.len(),
            idempotency_records: bundle.idempotency_records.len(),
        },
    );
    let mut diffs = Vec::new();
    compare_usize(
        "tape",
        "$.expected.tape_event_count",
        bundle.expected.tape_event_count,
        actual_expected.tape_event_count,
        &mut diffs,
    );
    compare_string_vec(
        "tape",
        "$.expected.tape_event_types",
        &bundle.expected.tape_event_types,
        &actual_expected.tape_event_types,
        &mut diffs,
    );
    compare_option_string(
        "model",
        "$.expected.final_answer_sha256",
        bundle.expected.final_answer_sha256.as_ref(),
        actual_expected.final_answer_sha256.as_ref(),
        &mut diffs,
    );
    compare_tool_outcomes(
        &bundle.expected.tool_outcomes,
        &actual_expected.tool_outcomes,
        &mut diffs,
    );
    compare_approval_decisions(
        &bundle.expected.approval_decisions,
        &actual_expected.approval_decisions,
        &mut diffs,
    );
    compare_usize(
        "http",
        "$.expected.http_exchange_count",
        bundle.expected.http_exchange_count,
        actual_expected.http_exchange_count,
        &mut diffs,
    );
    compare_usize(
        "auxiliary",
        "$.expected.auxiliary_task_count",
        bundle.expected.auxiliary_task_count,
        actual_expected.auxiliary_task_count,
        &mut diffs,
    );
    compare_usize(
        "flow",
        "$.expected.flow_event_count",
        bundle.expected.flow_event_count,
        actual_expected.flow_event_count,
        &mut diffs,
    );
    compare_usize(
        "artifact",
        "$.expected.artifact_ref_count",
        bundle.expected.artifact_ref_count,
        actual_expected.artifact_ref_count,
        &mut diffs,
    );
    compare_usize(
        "lifecycle",
        "$.expected.lifecycle_transition_count",
        bundle.expected.lifecycle_transition_count,
        actual_expected.lifecycle_transition_count,
        &mut diffs,
    );
    compare_usize(
        "idempotency",
        "$.expected.idempotency_record_count",
        bundle.expected.idempotency_record_count,
        actual_expected.idempotency_record_count,
        &mut diffs,
    );

    let mut diff_categories = BTreeMap::<String, usize>::new();
    for diff in &diffs {
        *diff_categories.entry(diff.category.clone()).or_default() += 1;
    }
    for issue in &validation.issues {
        *diff_categories.entry("validation".to_owned()).or_default() += 1;
        diffs.push(ReplayDiff {
            category: "validation".to_owned(),
            path: issue.path.clone(),
            expected: "redacted valid replay bundle".to_owned(),
            actual: issue.reason.clone(),
        });
    }

    let status = if diffs.is_empty() { ReplayRunStatus::Passed } else { ReplayRunStatus::Failed };
    ReplayRunReport {
        status,
        checked_categories: vec![
            "validation".to_owned(),
            "model".to_owned(),
            "tape".to_owned(),
            "tool".to_owned(),
            "approval".to_owned(),
            "http".to_owned(),
            "auxiliary".to_owned(),
            "flow".to_owned(),
            "artifact".to_owned(),
        ],
        diff_categories,
        diffs,
        validation,
    }
}

impl ReplayNormalizer {
    fn normalize_value(&mut self, value: &mut Value, path: &str, key_context: Option<&str>) {
        match value {
            Value::Object(object) => self.normalize_object(object, path, key_context),
            Value::Array(entries) => {
                for (index, entry) in entries.iter_mut().enumerate() {
                    self.normalize_value(entry, format!("{path}[{index}]").as_str(), key_context);
                }
            }
            Value::String(raw) => self.normalize_string_value(raw, key_context),
            Value::Number(_) if key_context.is_some_and(is_nondeterministic_time_key) => {
                *value = Value::from(0);
                self.redaction.normalized_timestamps += 1;
            }
            _ => {}
        }
    }

    fn normalize_object(
        &mut self,
        object: &mut Map<String, Value>,
        path: &str,
        key_context: Option<&str>,
    ) {
        let is_headers = key_context.is_some_and(|key| normalize_key(key) == "headers");
        for (key, entry) in object.iter_mut() {
            let entry_path = format!("{path}.{key}");
            if is_headers {
                if let Some(raw) = entry.as_str() {
                    *entry = Value::String(redact_header(key, raw));
                    self.redaction.redacted_fields += 1;
                    continue;
                }
            }
            if is_replay_secret_key(key) {
                if !entry.is_null() {
                    *entry = Value::String(REDACTED.to_owned());
                    self.redaction.redacted_fields += 1;
                }
                continue;
            }
            if is_identifier_key(key) {
                match entry {
                    Value::String(raw) => self.normalize_string_field(raw, key),
                    Value::Array(entries) => {
                        for item in entries.iter_mut() {
                            if let Some(raw) = item.as_str() {
                                let normalized = self.alias_for_identifier(raw);
                                *item = Value::String(normalized);
                            } else {
                                self.normalize_value(item, entry_path.as_str(), Some(key));
                            }
                        }
                    }
                    _ => self.normalize_value(entry, entry_path.as_str(), Some(key)),
                }
                continue;
            }
            if is_nondeterministic_time_key(key) && entry.is_number() {
                *entry = Value::from(0);
                self.redaction.normalized_timestamps += 1;
                continue;
            }
            self.normalize_value(entry, entry_path.as_str(), Some(key));
        }
    }

    fn normalize_string_value(&mut self, raw: &mut String, key_context: Option<&str>) {
        if key_context.is_some_and(is_identifier_key) || looks_like_ulid(raw) {
            self.normalize_string_field(raw, key_context.unwrap_or("id"));
            return;
        }
        if key_context
            .is_some_and(|key| key_contains_any(key, &["url", "uri", "endpoint", "location"]))
        {
            *raw = redact_replay_url(raw);
            self.redaction.redacted_fields += 1;
            return;
        }
        if key_context
            .map(|key| key_contains_any(key, &["error", "reason", "message", "detail"]))
            .unwrap_or(false)
        {
            *raw = redact_replay_text(
                redact_auth_error(redact_url_segments_in_text(raw).as_str()).as_str(),
            );
            self.redaction.redacted_fields += 1;
            return;
        }
        let redacted = redact_replay_text(
            redact_auth_error(redact_url_segments_in_text(raw).as_str()).as_str(),
        );
        if redacted != *raw {
            *raw = redacted;
            self.redaction.redacted_fields += 1;
        }
    }

    fn normalize_string_field(&mut self, raw: &mut String, key: &str) {
        if !is_identifier_key(key) && !looks_like_ulid(raw) {
            self.normalize_string_value(raw, Some(key));
            return;
        }
        *raw = self.alias_for_identifier(raw);
    }

    fn alias_for_identifier(&mut self, raw: &str) -> String {
        if raw.trim().is_empty() {
            return String::new();
        }
        // Aliases are assigned in first-seen order, so identical captures always map the
        // same identifiers to the same aliases.
        if let Some(existing) = self.id_aliases.get(raw) {
            return existing.clone();
        }
        self.next_id += 1;
        let alias = format!("id:{:04}", self.next_id);
        self.id_aliases.insert(raw.to_owned(), alias.clone());
        self.redaction.pseudonymized_identifiers += 1;
        alias
    }
}

fn redact_replay_url(raw: &str) -> String {
    let redacted = redact_url(raw);
    let (base_and_query, fragment) = split_replay_once(redacted.trim(), '#');
    let (base, query) = split_replay_once(base_and_query, '?');
    let mut output = base.to_owned();

    if let Some(query) = query {
        let query = redact_replay_query_pairs(query);
        if !query.is_empty() {
            output.push('?');
            output.push_str(query.as_str());
        }
    }

    if let Some(fragment) = fragment {
        output.push('#');
        output.push_str(redact_replay_query_pairs(fragment).as_str());
    }

    output
}

fn redact_replay_text(raw: &str) -> String {
    let mut output = String::with_capacity(raw.len());
    let mut token = String::new();
    for ch in raw.chars() {
        if ch.is_whitespace() {
            flush_replay_text_token(token.as_str(), &mut output);
            token.clear();
            output.push(ch);
        } else {
            token.push(ch);
        }
    }
    flush_replay_text_token(token.as_str(), &mut output);
    output
}

fn flush_replay_text_token(token: &str, output: &mut String) {
    if token.is_empty() {
        return;
    }
    let (core, suffix) = split_replay_trailing_punctuation(token);
    if core.contains("://") || core.starts_with("www.") {
        output.push_str(redact_replay_url(core).as_str());
        output.push_str(suffix);
        return;
    }
    if let Some(redacted) = redact_replay_assignment_token(core) {
        output.push_str(redacted.as_str());
        output.push_str(suffix);
    } else {
        output.push_str(token);
    }
}

fn redact_replay_assignment_token(token: &str) -> Option<String> {
    let (key, separator, value) = split_replay_assignment(token)?;
    if value.is_empty() || !is_sensitive_key(key) {
        return None;
    }
    Some(format!("{key}{separator}{REDACTED}"))
}

fn split_replay_assignment(token: &str) -> Option<(&str, char, &str)> {
    for separator in ['=', ':'] {
        if let Some(index) = token.find(separator) {
            let key = token[..index].trim_matches('"').trim_matches('\'');
            let value = token[index + 1..].trim_matches('"').trim_matches('\'');
            return Some((key, separator, value));
        }
    }
    None
}

fn split_replay_trailing_punctuation(token: &str) -> (&str, &str) {
    let bytes = token.as_bytes();
    let mut index = bytes.len();
    while index > 0 {
        if matches!(bytes[index - 1], b',' | b';' | b'.' | b')' | b']' | b'}') {
            index -= 1;
        } else {
            break;
        }
    }
    token.split_at(index)
}

fn split_replay_once(value: &str, delimiter: char) -> (&str, Option<&str>) {
    if let Some((left, right)) = value.split_once(delimiter) {
        (left, Some(right))
    } else {
        (value, None)
    }
}

fn redact_replay_query_pairs(raw: &str) -> String {
    let mut redacted_pairs = Vec::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = split_replay_query_pair(pair);
        if is_sensitive_key(key) {
            redacted_pairs.push(format!("{key}={REDACTED}"));
        } else if value.is_empty() {
            redacted_pairs.push(key.to_owned());
        } else {
            redacted_pairs.push(format!("{key}={value}"));
        }
    }
    redacted_pairs.join("&")
}

fn split_replay_query_pair(pair: &str) -> (&str, &str) {
    if let Some(index) = pair.find('=') {
        (&pair[..index], &pair[index + 1..])
    } else {
        (pair, "")
    }
}

// Only a digest and token count are retained; raw model text never lands in the bundle.
fn extract_model_exchanges(tape_events: &[ReplayTapeEvent]) -> Vec<ReplayModelExchange> {
    let tokens = tape_events
        .iter()
        .filter(|event| event.event_type == "model_token")
        .filter_map(|event| event.payload.get("token").and_then(Value::as_str))
        .collect::<Vec<_>>();
    if tokens.is_empty() {
        return Vec::new();
    }
    let response_text = tokens.join("");
    vec![ReplayModelExchange {
        exchange_id: "model-exchange:0001".to_owned(),
        provider: "captured".to_owned(),
        model: "captured".to_owned(),
        request_metadata: json!({ "source": "orchestrator_tape" }),
        response: json!({
            "text_sha256": sha256_hex(response_text.as_bytes()),
            "token_count": tokens.len(),
        }),
    }]
}

fn extract_tool_exchanges(tape_events: &[ReplayTapeEvent]) -> Vec<ReplayToolExchange> {
    let mut by_proposal = BTreeMap::<String, ToolAccumulator>::new();
    for event in tape_events {
        let Some(proposal_id) = event.payload.get("proposal_id").and_then(Value::as_str) else {
            continue;
        };
        let entry = by_proposal.entry(proposal_id.to_owned()).or_default();
        match event.event_type.as_str() {
            "tool_proposal" => {
                entry.tool_name =
                    event.payload.get("tool_name").and_then(Value::as_str).map(ToOwned::to_owned);
                entry.input = event.payload.get("input_json").cloned();
            }
            "tool_decision" | "runtime.decision.tool_decision" => {
                entry.decision = Some(event.payload.clone());
                if entry.tool_name.is_none() {
                    entry.tool_name = event
                        .payload
                        .get("tool_name")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned);
                }
            }
            "tool_result" => entry.result = Some(event.payload.clone()),
            "tool_attestation" => entry.attestation = Some(event.payload.clone()),
            _ => {}
        }
    }

    by_proposal
        .into_iter()
        .map(|(proposal_id, entry)| ReplayToolExchange {
            proposal_id,
            tool_name: entry.tool_name.unwrap_or_else(|| "unknown".to_owned()),
            input: entry.input.unwrap_or(Value::Null),
            decision: entry.decision,
            result: entry.result,
            attestation: entry.attestation,
        })
        .collect()
}

fn extract_http_exchanges(tool_exchanges: &[ReplayToolExchange]) -> Vec<ReplayHttpExchange> {
    tool_exchanges
        .iter()
        .filter(|exchange| exchange.tool_name == "palyra.http.fetch")
        .enumerate()
        .map(|(index, exchange)| ReplayHttpExchange {
            exchange_id: format!("http-exchange:{:04}", index + 1),
            request: exchange.input.clone(),
            response: exchange.result.clone().unwrap_or(Value::Null),
            fixture_ref: ReplayArtifactRef {
                artifact_id: format!("http-fixture:{:04}", index + 1),
                kind: "http_exchange_fixture".to_owned(),
                reference: format!("inline://tool/{}", exchange.proposal_id),
                sha256: exchange
                    .result
                    .as_ref()
                    .and_then(|result| serde_json::to_vec(result).ok())
                    .map(|bytes| sha256_hex(bytes.as_slice())),
                size_bytes: exchange
                    .result
                    .as_ref()
                    .and_then(|result| serde_json::to_vec(result).ok())
                    .and_then(|bytes| u64::try_from(bytes.len()).ok()),
            },
        })
        .collect()
}

fn extract_approvals(tape_events: &[ReplayTapeEvent]) -> Vec<ReplayApprovalExchange> {
    let mut by_approval = BTreeMap::<String, ApprovalAccumulator>::new();
    for event in tape_events {
        let Some(approval_id) = event.payload.get("approval_id").and_then(Value::as_str) else {
            continue;
        };
        let entry = by_approval.entry(approval_id.to_owned()).or_default();
        if entry.proposal_id.is_none() {
            entry.proposal_id =
                event.payload.get("proposal_id").and_then(Value::as_str).map(ToOwned::to_owned);
        }
        match event.event_type.as_str() {
            "tool_approval_request" => entry.request = Some(event.payload.clone()),
            "tool_approval_response" => entry.response = Some(event.payload.clone()),
            _ => {}
        }
    }
    by_approval
        .into_iter()
        .map(|(approval_id, entry)| ReplayApprovalExchange {
            approval_id,
            proposal_id: entry.proposal_id.unwrap_or_else(|| "unknown".to_owned()),
            request: entry.request.unwrap_or(Value::Null),
            response: entry.response,
        })
        .collect()
}

fn extract_decision_records(
    tape_events: &[ReplayTapeEvent],
    kind: &str,
) -> Vec<ReplayDecisionRecord> {
    tape_events
        .iter()
        .filter(|event| event.event_type.contains(kind))
        .enumerate()
        .map(|(index, event)| ReplayDecisionRecord {
            record_id: format!("{kind}:{:04}", index + 1),
            kind: event.event_type.clone(),
            payload: event.payload.clone(),
        })
        .collect()
}

// Typed records round-trip through JSON so the generic normalizer (redaction, aliasing,
// timestamp zeroing) applies to them exactly as it does to raw payloads; the same
// rationale applies to normalize_idempotency_records below.
fn normalize_lifecycle_transitions(
    transitions: Vec<RunLifecycleTransitionRecord>,
    normalizer: &mut ReplayNormalizer,
) -> Result<Vec<RunLifecycleTransitionRecord>> {
    let mut normalized: Vec<RunLifecycleTransitionRecord> = Vec::with_capacity(transitions.len());
    for (index, transition) in transitions.into_iter().enumerate() {
        let mut value = serde_json::to_value(&transition)
            .context("failed to encode lifecycle transition for replay normalization")?;
        normalizer.normalize_value(
            &mut value,
            format!("$.lifecycle_transitions[{index}]").as_str(),
            Some("run_lifecycle_transition"),
        );
        normalized.push(
            serde_json::from_value(value)
                .context("failed to decode normalized lifecycle transition")?,
        );
    }
    normalized.sort_by(|left, right| {
        left.occurred_at_unix_ms
            .cmp(&right.occurred_at_unix_ms)
            .then_with(|| left.event_id.cmp(&right.event_id))
    });
    Ok(normalized)
}

fn normalize_idempotency_records(
    records: Vec<IdempotencyRecordSnapshot>,
    normalizer: &mut ReplayNormalizer,
) -> Result<Vec<IdempotencyRecordSnapshot>> {
    let mut normalized: Vec<IdempotencyRecordSnapshot> = Vec::with_capacity(records.len());
    for (index, record) in records.into_iter().enumerate() {
        let mut value = serde_json::to_value(&record)
            .context("failed to encode idempotency record for replay normalization")?;
        normalizer.normalize_value(
            &mut value,
            format!("$.idempotency_records[{index}]").as_str(),
            Some("idempotency_record"),
        );
        normalized.push(
            serde_json::from_value(value)
                .context("failed to decode normalized idempotency record")?,
        );
    }
    normalized.sort_by(|left, right| {
        left.first_seen_at_unix_ms
            .cmp(&right.first_seen_at_unix_ms)
            .then_with(|| left.key.cmp(&right.key))
    });
    Ok(normalized)
}

fn expected_outputs_from_capture(
    tape_events: &[ReplayTapeEvent],
    tool_exchanges: &[ReplayToolExchange],
    approvals: &[ReplayApprovalExchange],
    http_exchanges: &[ReplayHttpExchange],
    counts: ReplayCaptureCounts,
) -> ReplayExpectedOutputs {
    let final_answer = tape_events
        .iter()
        .filter(|event| event.event_type == "model_token")
        .filter_map(|event| event.payload.get("token").and_then(Value::as_str))
        .collect::<String>();
    let final_answer_sha256 =
        (!final_answer.is_empty()).then(|| sha256_hex(final_answer.as_bytes()));
    let final_answer_summary =
        (!final_answer.is_empty()).then(|| summarize_text(final_answer.as_str(), 160));
    ReplayExpectedOutputs {
        tape_event_count: tape_events.len(),
        tape_event_types: tape_events.iter().map(|event| event.event_type.clone()).collect(),
        final_answer_sha256,
        final_answer_summary,
        tool_outcomes: tool_exchanges.iter().filter_map(expected_tool_outcome).collect::<Vec<_>>(),
        approval_decisions: approvals
            .iter()
            .filter_map(expected_approval_decision)
            .collect::<Vec<_>>(),
        http_exchange_count: http_exchanges.len(),
        auxiliary_task_count: counts.auxiliary_tasks,
        flow_event_count: counts.flow_events,
        lifecycle_transition_count: counts.lifecycle_transitions,
        idempotency_record_count: counts.idempotency_records,
        artifact_ref_count: counts.artifact_refs,
    }
}

fn expected_tool_outcome(exchange: &ReplayToolExchange) -> Option<ReplayExpectedToolOutcome> {
    let result = exchange.result.as_ref()?;
    let success = result.get("success").and_then(Value::as_bool).unwrap_or(false);
    let output_sha256 = result
        .get("output_json")
        .and_then(|value| serde_json::to_vec(value).ok())
        .map(|bytes| sha256_hex(bytes.as_slice()));
    let error = result
        .get("error")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned);
    Some(ReplayExpectedToolOutcome {
        proposal_id: exchange.proposal_id.clone(),
        tool_name: exchange.tool_name.clone(),
        success,
        output_sha256,
        error,
    })
}

fn expected_approval_decision(
    exchange: &ReplayApprovalExchange,
) -> Option<ReplayExpectedApprovalDecision> {
    let response = exchange.response.as_ref()?;
    Some(ReplayExpectedApprovalDecision {
        approval_id: exchange.approval_id.clone(),
        proposal_id: exchange.proposal_id.clone(),
        approved: response.get("approved").and_then(Value::as_bool).unwrap_or(false),
        decision_scope: response
            .get("decision_scope")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
    })
}

fn scan_for_unredacted_secrets(
    value: &Value,
    path: &str,
    key_context: Option<&str>,
    checked_values: &mut usize,
    issues: &mut Vec<ReplayValidationIssue>,
) {
    *checked_values += 1;
    match value {
        Value::Object(object) => {
            for (key, entry) in object {
                let entry_path = format!("{path}.{key}");
                if is_replay_secret_key(key) && !entry_is_redacted_or_empty(entry) {
                    issues.push(ReplayValidationIssue {
                        path: entry_path.clone(),
                        reason: "sensitive field is not redacted".to_owned(),
                    });
                }
                scan_for_unredacted_secrets(
                    entry,
                    entry_path.as_str(),
                    Some(key),
                    checked_values,
                    issues,
                );
            }
        }
        Value::Array(entries) => {
            for (index, entry) in entries.iter().enumerate() {
                scan_for_unredacted_secrets(
                    entry,
                    format!("{path}[{index}]").as_str(),
                    key_context,
                    checked_values,
                    issues,
                );
            }
        }
        Value::String(raw) if string_contains_unredacted_secret(raw, key_context) => {
            issues.push(ReplayValidationIssue {
                path: path.to_owned(),
                reason: "string contains an unredacted credential pattern".to_owned(),
            });
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
    // A redaction marker means the normalizer already processed this string; skipping it
    // avoids re-flagging partially redacted text (e.g. "token=<redacted>").
    if raw.contains(REDACTED) {
        return false;
    }
    let lowered = raw.to_ascii_lowercase();
    if key_context.is_some_and(is_replay_secret_key) && !raw.trim().is_empty() {
        return true;
    }
    if lowered.contains("bearer ") {
        return true;
    }
    for marker in [
        "access_token=",
        "refresh_token=",
        "api_key=",
        "authorization=",
        "client_secret=",
        "credential=",
        "password=",
        "sig=",
        "signature=",
        "token=",
        "x-amz-credential=",
        "x-amz-signature=",
        "x-goog-signature=",
    ] {
        if assignment_marker_has_value(lowered.as_str(), marker) {
            return true;
        }
    }
    // URLs with a userinfo component embed credentials in the authority part; a bare
    // "://@" means the userinfo was already emptied and is safe.
    lowered.contains("://") && lowered.contains('@') && !lowered.contains("://@")
}

fn assignment_marker_has_value(raw: &str, marker: &str) -> bool {
    let mut offset = 0usize;
    while let Some(relative_index) = raw[offset..].find(marker) {
        let value_start = offset.saturating_add(relative_index).saturating_add(marker.len());
        let value = raw[value_start..].split(['\r', '\n']).next().unwrap_or_default().trim();
        if !matches!(value, "" | "\"\"" | "''") {
            return true;
        }
        offset = value_start;
        if offset >= raw.len() {
            break;
        }
    }
    false
}

fn compare_usize(
    category: &str,
    path: &str,
    expected: usize,
    actual: usize,
    diffs: &mut Vec<ReplayDiff>,
) {
    if expected != actual {
        diffs.push(ReplayDiff {
            category: category.to_owned(),
            path: path.to_owned(),
            expected: expected.to_string(),
            actual: actual.to_string(),
        });
    }
}

fn compare_option_string(
    category: &str,
    path: &str,
    expected: Option<&String>,
    actual: Option<&String>,
    diffs: &mut Vec<ReplayDiff>,
) {
    if expected != actual {
        diffs.push(ReplayDiff {
            category: category.to_owned(),
            path: path.to_owned(),
            expected: expected.cloned().unwrap_or_else(|| "<none>".to_owned()),
            actual: actual.cloned().unwrap_or_else(|| "<none>".to_owned()),
        });
    }
}

fn compare_string_vec(
    category: &str,
    path: &str,
    expected: &[String],
    actual: &[String],
    diffs: &mut Vec<ReplayDiff>,
) {
    if expected != actual {
        diffs.push(ReplayDiff {
            category: category.to_owned(),
            path: path.to_owned(),
            expected: format!("{expected:?}"),
            actual: format!("{actual:?}"),
        });
    }
}

fn compare_tool_outcomes(
    expected: &[ReplayExpectedToolOutcome],
    actual: &[ReplayExpectedToolOutcome],
    diffs: &mut Vec<ReplayDiff>,
) {
    if expected == actual {
        return;
    }
    diffs.push(ReplayDiff {
        category: "tool".to_owned(),
        path: "$.expected.tool_outcomes".to_owned(),
        expected: format!("{expected:?}"),
        actual: format!("{actual:?}"),
    });
}

fn compare_approval_decisions(
    expected: &[ReplayExpectedApprovalDecision],
    actual: &[ReplayExpectedApprovalDecision],
    diffs: &mut Vec<ReplayDiff>,
) {
    if expected == actual {
        return;
    }
    diffs.push(ReplayDiff {
        category: "approval".to_owned(),
        path: "$.expected.approval_decisions".to_owned(),
        expected: format!("{expected:?}"),
        actual: format!("{actual:?}"),
    });
}

// Derived from the run id so rebuilding the same run always yields the same bundle id.
fn stable_bundle_id(run_id: &str) -> String {
    format!("replay:{}", &sha256_hex(run_id.as_bytes())[..16])
}

fn summarize_text(raw: &str, max_chars: usize) -> String {
    if raw.chars().count() <= max_chars {
        return raw.to_owned();
    }
    let mut output = raw.chars().take(max_chars).collect::<String>();
    output.push_str("...");
    output
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex::encode(digest)
}

fn key_contains_any(key: &str, needles: &[&str]) -> bool {
    let lowered = key.to_ascii_lowercase();
    needles.iter().any(|needle| lowered.contains(needle))
}

fn is_identifier_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    normalized == "id"
        || normalized.ends_with("_id")
        || normalized.ends_with("_ulid")
        || normalized.ends_with("_uuid")
        || normalized.contains("request_id")
        || normalized.contains("trace_id")
        || normalized.contains("span_id")
        || normalized.contains("connection_id")
        || normalized.contains("correlation_id")
}

// Identifier-style keys are pseudonymized rather than redacted, and token *count* metrics
// are plain numbers despite containing "token", so both are carved out of secret handling.
fn is_replay_secret_key(key: &str) -> bool {
    is_sensitive_key(key) && !is_identifier_key(key) && !is_token_metric_key(key)
}

fn is_token_metric_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    normalized == "token_count"
        || normalized.ends_with("_tokens")
        || normalized.ends_with("_token_budget")
        || normalized.ends_with("_token_limit")
        || normalized.contains("tokens_")
}

// Durations, budgets, and TTLs are deterministic configuration, not wall-clock readings;
// only point-in-time fields are zeroed for reproducibility.
fn is_nondeterministic_time_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    if key_contains_any(
        normalized.as_str(),
        &["timeout", "ttl", "duration", "latency", "elapsed", "budget", "retention"],
    ) {
        return false;
    }
    normalized.contains("timestamp")
        || normalized.ends_with("_unix_ms")
        || normalized.ends_with("_at")
        || normalized.ends_with("_at_unix_ms")
}

// Cheap shape check (26 alphanumeric chars); false positives are harmless because they
// merely get pseudonymized like any other identifier.
fn looks_like_ulid(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.len() == 26 && trimmed.chars().all(|ch| ch.is_ascii_digit() || ch.is_ascii_alphabetic())
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

/// Converts a failed replay report into an error suitable for gate scripts.
///
/// # Errors
///
/// Fails when the report status is not [`ReplayRunStatus::Passed`], citing the first diff.
pub fn ensure_replay_report_passed(report: &ReplayRunReport) -> Result<()> {
    if report.status == ReplayRunStatus::Passed {
        return Ok(());
    }
    let first = report
        .diffs
        .first()
        .map(|diff| format!("{} at {}", diff.category, diff.path))
        .unwrap_or_else(|| "unknown replay diff".to_owned());
    Err(anyhow!("offline replay failed: {first}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> ReplayBundleBuildInput {
        ReplayBundleBuildInput {
            generated_at_unix_ms: 1_730_000_000_000,
            source: ReplaySource {
                product: "palyra".to_owned(),
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                session_id: Some("01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned()),
                origin_kind: "run_stream".to_owned(),
                schema_policy: "backward_compatible".to_owned(),
            },
            capture: ReplayCaptureMetadata {
                captured_at_unix_ms: 1_730_000_000_001,
                capture_mode: "offline_export".to_owned(),
                max_events_per_run: 128,
                truncated: false,
                inline_sections: vec!["tape_events".to_owned()],
                referenced_sections: vec!["binary_artifacts".to_owned()],
                warnings: Vec::new(),
            },
            run: ReplayRunSnapshot {
                state: "completed".to_owned(),
                principal: "user:alice".to_owned(),
                device_id: "device-local".to_owned(),
                channel: Some("cli".to_owned()),
                normalized_user_input: Some(json!({
                    "text": "fetch https://example.test/callback?access_token=secret&mode=ok"
                })),
                prompt_tokens: 12,
                completion_tokens: 4,
                total_tokens: 16,
                last_error: Some("Bearer secret-token".to_owned()),
                parent_run_id: None,
                origin_run_id: None,
                parameter_delta: Some(json!({ "created_at_unix_ms": 1_730_000_000_002_i64 })),
            },
            config_snapshot: json!({
                "model_provider": {
                    "openai_api_key": "sk-secret",
                    "endpoint": "https://api.example.test/v1?token=abc"
                }
            }),
            tape_events: vec![
                ReplayTapeEvent {
                    seq: 0,
                    event_type: "tool_proposal".to_owned(),
                    payload: json!({
                        "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                        "tool_name": "palyra.http.fetch",
                        "input_json": {
                            "url": "https://signed.example.test/file?X-Amz-Signature=abc&mode=ok",
                            "headers": { "authorization": "Bearer secret" }
                        }
                    }),
                },
                ReplayTapeEvent {
                    seq: 1,
                    event_type: "tool_result".to_owned(),
                    payload: json!({
                        "proposal_id": "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                        "success": true,
                        "output_json": { "status": 200 },
                        "error": ""
                    }),
                },
                ReplayTapeEvent {
                    seq: 2,
                    event_type: "model_token".to_owned(),
                    payload: json!({ "token": "done", "is_final": true }),
                },
            ],
            lifecycle_transitions: Vec::new(),
            idempotency_records: Vec::new(),
            artifact_refs: Vec::new(),
        }
    }

    #[test]
    fn bundle_build_redacts_and_normalizes_deterministically() {
        let first = build_replay_bundle(sample_input()).expect("bundle should build");
        let second = build_replay_bundle(sample_input()).expect("bundle should build");
        let first_bytes = canonical_replay_bundle_bytes(&first).expect("bundle should encode");
        let second_bytes = canonical_replay_bundle_bytes(&second).expect("bundle should encode");
        assert_eq!(first_bytes, second_bytes);
        let encoded = String::from_utf8(first_bytes).expect("json should be utf8");
        assert!(!encoded.contains("sk-secret"));
        assert!(!encoded.contains("secret-token"));
        assert!(!encoded.contains("X-Amz-Signature=abc"));
        assert!(encoded.contains("X-Amz-Signature=<redacted>"));
        assert!(encoded.contains("\"generated_at_unix_ms\": 0"));
        assert!(first.redaction.pseudonymized_identifiers > 0);
    }

    #[test]
    fn bundle_build_redacts_artifact_references() {
        let mut input = sample_input();
        input.artifact_refs = vec![ReplayArtifactRef {
            artifact_id: "artifact-1".to_owned(),
            kind: "http_fixture".to_owned(),
            reference: "https://bucket.example.test/object?X-Amz-Credential=AKIA&X-Amz-Signature=deadbeef&mode=ok".to_owned(),
            sha256: Some("abc123".to_owned()),
            size_bytes: Some(42),
        }];

        let bundle = build_replay_bundle(input).expect("bundle should build");
        let encoded = String::from_utf8(
            canonical_replay_bundle_bytes(&bundle).expect("bundle should encode"),
        )
        .expect("json should be utf8");

        assert_eq!(bundle.artifact_refs.len(), 1);
        assert!(bundle.artifact_refs[0].reference.starts_with("id:"));
        assert_eq!(bundle.artifact_refs[0].sha256.as_deref(), Some("abc123"));
        assert_eq!(bundle.artifact_refs[0].size_bytes, Some(42));
        assert!(!encoded.contains("X-Amz-Credential=AKIA"));
        assert!(!encoded.contains("X-Amz-Signature=deadbeef"));
        assert!(validate_replay_bundle(&bundle).valid);
    }

    #[test]
    fn validator_rejects_raw_sensitive_fields() {
        let mut bundle = build_replay_bundle(sample_input()).expect("bundle should build");
        bundle.config_snapshot = json!({ "token": "raw-secret" });
        let report = validate_replay_bundle(&bundle);
        assert!(!report.valid);
        assert!(report.issues.iter().any(|issue| issue.path.contains("token")));
    }

    #[test]
    fn validator_accepts_empty_secret_assignments_in_example_text() {
        assert!(!string_contains_unredacted_secret(
            "# Populate this locally.\nAPP_API_TOKEN=\nPASSWORD=\"\"\nCLIENT_SECRET=''\n",
            None
        ));
    }

    #[test]
    fn validator_rejects_non_empty_secret_assignments_in_multiline_text() {
        assert!(string_contains_unredacted_secret(
            "APP_API_TOKEN=\nSAFE=true\nCLIENT_SECRET=must-not-leak\n",
            None
        ));
        assert!(string_contains_unredacted_secret(r#"APP_API_TOKEN=""must-not-leak"#, None));
        assert!(string_contains_unredacted_secret(r#"PASSWORD=''must-not-leak"#, None));
    }

    #[test]
    fn validator_rejects_signed_artifact_url_markers() {
        let mut bundle = build_replay_bundle(sample_input()).expect("bundle should build");
        bundle.artifact_refs = vec![ReplayArtifactRef {
            artifact_id: "artifact-1".to_owned(),
            kind: "http_fixture".to_owned(),
            reference: "https://bucket.example.test/object?X-Amz-Signature=deadbeef&mode=ok"
                .to_owned(),
            sha256: None,
            size_bytes: None,
        }];

        let report = validate_replay_bundle(&bundle);

        assert!(!report.valid);
        assert!(
            report.issues.iter().any(|issue| issue.path == "$.artifact_refs[0].reference"),
            "artifact reference signed URL should be flagged: {report:#?}"
        );
    }

    #[test]
    fn offline_replay_passes_for_unchanged_capture_and_fails_on_drift() {
        let mut bundle = build_replay_bundle(sample_input()).expect("bundle should build");
        let report = replay_bundle_offline(&bundle);
        assert_eq!(report.status, ReplayRunStatus::Passed, "{report:#?}");

        bundle.expected.tape_event_count += 1;
        let report = replay_bundle_offline(&bundle);
        assert_eq!(report.status, ReplayRunStatus::Failed);
        assert!(report.diff_categories.contains_key("tape"));
    }

    #[test]
    fn parser_rejects_future_schema_versions() {
        let error = parse_replay_bundle(br#"{"schema_version":999}"#)
            .expect_err("future schema should be rejected");
        assert!(
            error.to_string().contains("unsupported palyra incident replay bundle schema version"),
            "unexpected error: {error:#}"
        );
    }
}
