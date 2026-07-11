//! Versioned replay-fixture contracts and semantic golden validation.
//!
//! Production-output projection stays independent from the checked-in golden
//! payload so the replay gate can detect protected semantic drift.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use palyra_common::redaction::is_sensitive_key;
use palyra_common::replay_bundle::ReplayTapeEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{redact_trace_string, sha256_hex};

pub(crate) const REPLAY_FIXTURE_MATRIX_SCHEMA_VERSION: u32 = 3;
const REPLAY_FIXTURE_CAPTURED_OUTPUTS_SCHEMA_VERSION: u32 = 1;

const REPLAY_FIXTURE_CONTRACT: &str = "replay-capture-stable-fixtures-v3";
const REPLAY_FIXTURE_CAPTURED_OUTPUTS_CONTRACT: &str = "replay-capture-production-outputs-v1";
const REPLAY_FIXTURE_CATEGORIES: &[&str] = &[
    "text_run",
    "tool_run",
    "approval",
    "compaction",
    "queue",
    "cancellation",
    "delivery",
    "restart_terminalization",
];
const REPLAY_TIMESTAMP_ALLOWANCE_PATHS: &[&str] = &[
    "$.generated_at_unix_ms",
    "$.capture.captured_at_unix_ms",
    "$.tape_events[*].payload.timestamp",
    "$.tape_events[*].payload.*_at_unix_ms",
];
const REPLAY_PROVIDER_USAGE_ALLOWANCE_PATHS: &[&str] =
    &["$.run.prompt_tokens", "$.run.completion_tokens", "$.run.total_tokens"];
const FORBIDDEN_REPLAY_ALLOWANCE_PATH_PARTS: &[&str] = &[
    "seq",
    "sequence",
    "generation",
    "reason",
    "terminal",
    "idempotency",
    "side_effect",
    "side-effect",
    "sideeffect",
    "runtime",
    "fallback",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplayFixtureValidationReport {
    pub(crate) schema_version: u32,
    pub(crate) case_count: usize,
    pub(crate) failure_case_count: usize,
    pub(crate) categories: Vec<String>,
    pub(crate) compatibility_allowance_count: usize,
    pub(crate) semantic_payload_count: usize,
    pub(crate) semantic_hash_count: usize,
    pub(crate) tape_event_count: usize,
    pub(crate) protected_invariant_count: usize,
    pub(crate) expected_terminal_states: Vec<String>,
    pub(crate) artifact_digest_count: usize,
    pub(crate) redaction_snapshot_present: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureMatrix {
    schema_version: u32,
    fixture_contract: String,
    compatibility_allowances: Vec<ReplayCompatibilityAllowance>,
    cases: Vec<ReplayFixtureCase>,
    redaction_snapshot: ReplayFixtureRedactionSnapshot,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayCompatibilityAllowance {
    id: String,
    kind: String,
    paths: Vec<String>,
    #[serde(default)]
    tolerance: Option<u64>,
    reason_code: String,
    justification: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureCase {
    id: String,
    category: String,
    scenario_id: String,
    source: String,
    capture_kind: String,
    golden_payload: ReplayFixtureSemanticPayload,
    semantic_sha256: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureCapturedOutputs {
    schema_version: u32,
    captured_output_contract: String,
    cases: Vec<ReplayFixtureCapturedCase>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureCapturedCase {
    id: String,
    output: ReplayFixtureCapturedOutput,
}

/// Production-shaped capture input kept separate from the checked-in semantic
/// golden so the gate exercises the projection instead of comparing a value
/// with itself.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureCapturedOutput {
    generated_at_unix_ms: i64,
    captured_at_unix_ms: i64,
    run: ReplayFixtureCapturedRun,
    tape_events: Vec<ReplayTapeEvent>,
    tool_exchanges: Vec<ReplayFixtureToolExchange>,
    approvals: Vec<ReplayFixtureApprovalExchange>,
    queue_decisions: Vec<ReplayFixtureDecisionRecord>,
    lifecycle_transitions: Vec<ReplayFixtureLifecycleTransition>,
    idempotency_records: Vec<ReplayFixtureIdempotencyRecord>,
    artifact_refs: Vec<ReplayFixtureArtifactRef>,
    redacted_fields: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureCapturedRun {
    state: String,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    #[serde(default)]
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureSemanticPayload {
    generated_at_unix_ms: i64,
    capture: ReplayFixtureCaptureProjection,
    run: ReplayFixtureRunProjection,
    tape_events: Vec<ReplayTapeEvent>,
    tool_exchanges: Vec<ReplayFixtureToolExchange>,
    approvals: Vec<ReplayFixtureApprovalExchange>,
    queue_decisions: Vec<ReplayFixtureDecisionRecord>,
    lifecycle_transitions: Vec<ReplayFixtureLifecycleTransition>,
    idempotency_records: Vec<ReplayFixtureIdempotencyRecord>,
    artifact_refs: Vec<ReplayFixtureArtifactRef>,
    expected: ReplayFixtureExpectedProjection,
    redaction: ReplayFixturePayloadRedaction,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureCaptureProjection {
    captured_at_unix_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureRunProjection {
    state: String,
    generation: u64,
    reason_code: String,
    prompt_tokens: u64,
    completion_tokens: u64,
    total_tokens: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureToolExchange {
    proposal_id: String,
    tool_name: String,
    result: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureApprovalExchange {
    approval_id: String,
    proposal_id: String,
    response: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureDecisionRecord {
    record_id: String,
    kind: String,
    payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureLifecycleTransition {
    #[serde(skip_serializing_if = "Option::is_none")]
    from_state: Option<String>,
    to_state: String,
    generation: u64,
    reason_code: String,
    terminal_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureIdempotencyRecord {
    key: String,
    scope: String,
    operation_kind: String,
    payload_sha256: String,
    state: String,
    side_effect_identity: String,
    reason_code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureArtifactRef {
    artifact_id: String,
    kind: String,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureExpectedProjection {
    terminal_state: String,
    terminal_count: u32,
    tape_event_count: usize,
    tape_event_types: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_answer_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ReplayFixturePayloadRedaction {
    raw_secrets_allowed: bool,
    raw_paths_allowed: bool,
    raw_prompts_allowed: bool,
    redacted_fields: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayFixtureRedactionSnapshot {
    raw_secrets_allowed: bool,
    raw_paths_allowed: bool,
    raw_prompts_allowed: bool,
    model_visible_default: bool,
    stable_fields: Vec<String>,
}

/// Validates the versioned replay fixture matrix against captured production output.
///
/// # Errors
/// Returns an error when either contract is malformed, incomplete, unredacted,
/// or differs outside its explicit compatibility allowances.
pub(crate) fn validate_replay_fixture_matrix(
    value: &Value,
) -> Result<ReplayFixtureValidationReport> {
    let captured_outputs = serde_json::from_str::<Value>(include_str!(
        "../../../../fixtures/golden/replay_capture_production_outputs.json"
    ))
    .context("replay fixture production output pack is invalid JSON")?;
    validate_replay_fixture_matrix_with_captured_outputs(value, &captured_outputs)
}

fn validate_replay_fixture_matrix_with_captured_outputs(
    value: &Value,
    captured_outputs: &Value,
) -> Result<ReplayFixtureValidationReport> {
    let matrix = serde_json::from_value::<ReplayFixtureMatrix>(value.clone())
        .context("replay fixture matrix shape is invalid")?;
    let captured_outputs =
        serde_json::from_value::<ReplayFixtureCapturedOutputs>(captured_outputs.clone())
            .context("replay fixture production output pack shape is invalid")?;
    if matrix.schema_version != REPLAY_FIXTURE_MATRIX_SCHEMA_VERSION {
        bail!("replay fixture matrix schema version mismatch");
    }
    if matrix.fixture_contract != REPLAY_FIXTURE_CONTRACT {
        bail!("replay fixture matrix contract mismatch");
    }
    if captured_outputs.schema_version != REPLAY_FIXTURE_CAPTURED_OUTPUTS_SCHEMA_VERSION {
        bail!("replay fixture production output schema version mismatch");
    }
    if captured_outputs.captured_output_contract != REPLAY_FIXTURE_CAPTURED_OUTPUTS_CONTRACT {
        bail!("replay fixture production output contract mismatch");
    }
    validate_replay_compatibility_allowances(matrix.compatibility_allowances.as_slice())?;
    validate_replay_redaction_snapshot(&matrix.redaction_snapshot)?;

    let mut captured_outputs_by_id = BTreeMap::new();
    for captured in &captured_outputs.cases {
        if captured.id.trim().is_empty() {
            bail!("replay fixture production output case id must not be empty");
        }
        if captured_outputs_by_id.insert(captured.id.as_str(), &captured.output).is_some() {
            bail!("replay fixture production output pack contains duplicate case id");
        }
    }

    let expected_categories = REPLAY_FIXTURE_CATEGORIES.iter().copied().collect::<BTreeSet<_>>();
    let mut observed_categories = BTreeSet::new();
    let mut case_ids = BTreeSet::new();
    let mut semantic_hashes = BTreeSet::new();
    let mut expected_terminal_states = Vec::new();
    let mut failure_case_count = 0_usize;
    let mut artifact_digest_count = 0_usize;
    let mut protected_invariant_count = 0_usize;
    let mut tape_event_count = 0_usize;
    for case in &matrix.cases {
        let captured_output =
            captured_outputs_by_id.remove(case.id.as_str()).with_context(|| {
                format!("replay fixture case `{}` has no production output", case.id)
            })?;
        let candidate = project_replay_fixture_semantic_payload(captured_output)
            .with_context(|| format!("failed to project replay fixture case `{}`", case.id))?;
        let checked_invariants = validate_replay_fixture_case(
            case,
            &candidate,
            matrix.compatibility_allowances.as_slice(),
        )?;
        if !case_ids.insert(case.id.as_str()) {
            bail!("replay fixture matrix contains duplicate case id `{}`", case.id);
        }
        if !observed_categories.insert(case.category.as_str()) {
            bail!("replay fixture matrix contains duplicate category `{}`", case.category);
        }
        if !semantic_hashes.insert(case.semantic_sha256.as_str()) {
            bail!("replay fixture matrix contains duplicate semantic payload digest");
        }
        expected_terminal_states.push(case.golden_payload.expected.terminal_state.clone());
        if case.golden_payload.expected.terminal_state != "done" {
            failure_case_count = failure_case_count.saturating_add(1);
        }
        protected_invariant_count = protected_invariant_count
            .checked_add(checked_invariants)
            .context("replay fixture protected invariant count overflow")?;
        tape_event_count = tape_event_count
            .checked_add(case.golden_payload.tape_events.len())
            .context("replay fixture tape event count overflow")?;
        artifact_digest_count = artifact_digest_count
            .checked_add(case.golden_payload.artifact_refs.len())
            .context("replay fixture artifact digest count overflow")?;
    }
    if !captured_outputs_by_id.is_empty() {
        let extra = captured_outputs_by_id.keys().copied().collect::<Vec<_>>();
        bail!("replay fixture production output pack contains extra cases: {extra:?}");
    }
    if observed_categories != expected_categories {
        let missing =
            expected_categories.difference(&observed_categories).copied().collect::<Vec<_>>();
        let extra =
            observed_categories.difference(&expected_categories).copied().collect::<Vec<_>>();
        bail!("replay fixture matrix category mismatch: missing={missing:?} extra={extra:?}");
    }
    expected_terminal_states.sort();
    expected_terminal_states.dedup();

    Ok(ReplayFixtureValidationReport {
        schema_version: REPLAY_FIXTURE_MATRIX_SCHEMA_VERSION,
        case_count: matrix.cases.len(),
        failure_case_count,
        categories: observed_categories.into_iter().map(ToOwned::to_owned).collect(),
        compatibility_allowance_count: matrix.compatibility_allowances.len(),
        semantic_payload_count: matrix.cases.len(),
        semantic_hash_count: semantic_hashes.len(),
        tape_event_count,
        protected_invariant_count,
        expected_terminal_states,
        artifact_digest_count,
        redaction_snapshot_present: true,
    })
}

fn validate_replay_compatibility_allowances(
    allowances: &[ReplayCompatibilityAllowance],
) -> Result<()> {
    let mut ids = BTreeSet::new();
    for allowance in allowances {
        if !ids.insert(allowance.id.as_str()) {
            bail!("replay fixture matrix contains duplicate compatibility allowance id");
        }
        if allowance.reason_code.trim().is_empty() || allowance.justification.trim().is_empty() {
            bail!("replay compatibility allowance must include a reason code and justification");
        }
        let paths = allowance.paths.iter().map(String::as_str).collect::<BTreeSet<_>>();
        if paths.len() != allowance.paths.len() || paths.is_empty() {
            bail!("replay compatibility allowance paths must be unique and non-empty");
        }
        for path in &paths {
            validate_replay_allowance_path(path)?;
        }
        match allowance.id.as_str() {
            "wall_clock_timestamps"
                if allowance.kind == "zero_timestamp"
                    && allowance.tolerance.is_none()
                    && allowance.reason_code == "replay.compatibility.wall_clock_not_invariant"
                    && paths
                        == REPLAY_TIMESTAMP_ALLOWANCE_PATHS
                            .iter()
                            .copied()
                            .collect::<BTreeSet<_>>() => {}
            "provider_usage_rounding"
                if allowance.kind == "numeric_tolerance"
                    && allowance.tolerance == Some(1)
                    && allowance.reason_code == "replay.compatibility.provider_usage_rounding"
                    && paths
                        == REPLAY_PROVIDER_USAGE_ALLOWANCE_PATHS
                            .iter()
                            .copied()
                            .collect::<BTreeSet<_>>() => {}
            _ => bail!("replay compatibility allowance is not part of the versioned contract"),
        }
    }
    let expected_ids =
        ["provider_usage_rounding", "wall_clock_timestamps"].into_iter().collect::<BTreeSet<_>>();
    if ids != expected_ids {
        bail!("replay fixture matrix compatibility allowance set is incomplete");
    }
    Ok(())
}

fn validate_replay_allowance_path(path: &str) -> Result<()> {
    if !path.starts_with("$.") || path.len() > 256 {
        bail!("replay compatibility allowance path is invalid");
    }
    let normalized = path.to_ascii_lowercase();
    if FORBIDDEN_REPLAY_ALLOWANCE_PATH_PARTS.iter().any(|forbidden| normalized.contains(forbidden))
    {
        bail!("replay compatibility allowance targets a protected invariant");
    }
    Ok(())
}

fn validate_replay_redaction_snapshot(snapshot: &ReplayFixtureRedactionSnapshot) -> Result<()> {
    if snapshot.raw_secrets_allowed
        || snapshot.raw_paths_allowed
        || snapshot.raw_prompts_allowed
        || snapshot.model_visible_default
    {
        bail!("replay fixture redaction snapshot must remain fail-closed");
    }
    let stable_fields = snapshot.stable_fields.iter().map(String::as_str).collect::<BTreeSet<_>>();
    let expected_fields = [
        "redaction_level",
        "redacted_fields",
        "path_redactions",
        "truncated_payloads",
        "omitted_events",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if stable_fields.len() != snapshot.stable_fields.len() || stable_fields != expected_fields {
        bail!("replay fixture redaction stable-field contract mismatch");
    }
    Ok(())
}

fn validate_replay_fixture_case(
    case: &ReplayFixtureCase,
    candidate: &ReplayFixtureSemanticPayload,
    allowances: &[ReplayCompatibilityAllowance],
) -> Result<usize> {
    if case.id.trim().is_empty()
        || case.scenario_id.trim().is_empty()
        || case.source.trim().is_empty()
        || !matches!(case.capture_kind.as_str(), "incident_replay" | "trajectory")
    {
        bail!("replay fixture case identity is invalid");
    }
    if !REPLAY_FIXTURE_CATEGORIES.contains(&case.category.as_str()) {
        bail!("replay fixture case category is unknown");
    }
    let checked_invariants =
        validate_replay_fixture_semantic_payload(case.category.as_str(), &case.golden_payload)?;
    validate_replay_fixture_semantic_payload(case.category.as_str(), candidate).with_context(
        || format!("replay fixture case `{}` production output is invalid", case.id),
    )?;
    if !is_nonplaceholder_sha256(case.semantic_sha256.as_str()) {
        bail!("replay fixture semantic digest must be a non-placeholder lowercase sha256");
    }
    let actual_sha256 = replay_fixture_semantic_sha256(&case.golden_payload, allowances)?;
    if case.semantic_sha256 != actual_sha256 {
        bail!(
            "replay fixture case `{}` semantic sha256 mismatch: expected={} actual={actual_sha256}",
            case.id,
            case.semantic_sha256
        );
    }
    compare_replay_fixture_payloads(&case.golden_payload, candidate, allowances)
        .with_context(|| format!("replay fixture case `{}` drifted from its golden", case.id))?;
    Ok(checked_invariants)
}

fn project_replay_fixture_semantic_payload(
    captured: &ReplayFixtureCapturedOutput,
) -> Result<ReplayFixtureSemanticPayload> {
    let terminal = captured
        .tape_events
        .iter()
        .rev()
        .find(|event| event.event_type == "run.terminal")
        .context("captured replay output is missing run.terminal")?;
    let terminal_payload =
        terminal.payload.as_object().context("captured run.terminal payload must be an object")?;
    let generation = required_semantic_u64(terminal_payload, "generation", "$.run.terminal")?;
    let reason_code =
        required_semantic_string(terminal_payload, "reason_code", "$.run.terminal")?.to_owned();
    let terminal_state =
        required_semantic_string(terminal_payload, "state", "$.run.terminal")?.to_owned();
    let terminal_count =
        u32::try_from(required_semantic_u64(terminal_payload, "terminal_count", "$.run.terminal")?)
            .context("captured replay terminal count exceeds u32")?;
    let final_answer_sha256 = captured.tape_events.iter().rev().find_map(|event| {
        event.payload.get("final_answer_sha256").and_then(Value::as_str).map(ToOwned::to_owned)
    });
    let mut redacted_fields = captured.redacted_fields.clone();
    redacted_fields.sort();
    redacted_fields.dedup();

    Ok(ReplayFixtureSemanticPayload {
        generated_at_unix_ms: captured.generated_at_unix_ms,
        capture: ReplayFixtureCaptureProjection {
            captured_at_unix_ms: captured.captured_at_unix_ms,
        },
        run: ReplayFixtureRunProjection {
            state: captured.run.state.clone(),
            generation,
            reason_code,
            prompt_tokens: captured.run.prompt_tokens,
            completion_tokens: captured.run.completion_tokens,
            total_tokens: captured.run.total_tokens,
            last_error: captured.run.last_error.clone(),
        },
        tape_events: captured.tape_events.clone(),
        tool_exchanges: captured.tool_exchanges.clone(),
        approvals: captured.approvals.clone(),
        queue_decisions: captured.queue_decisions.clone(),
        lifecycle_transitions: captured.lifecycle_transitions.clone(),
        idempotency_records: captured.idempotency_records.clone(),
        artifact_refs: captured.artifact_refs.clone(),
        expected: ReplayFixtureExpectedProjection {
            terminal_state,
            terminal_count,
            tape_event_count: captured.tape_events.len(),
            tape_event_types: captured
                .tape_events
                .iter()
                .map(|event| event.event_type.clone())
                .collect(),
            final_answer_sha256,
        },
        redaction: ReplayFixturePayloadRedaction {
            raw_secrets_allowed: false,
            raw_paths_allowed: false,
            raw_prompts_allowed: false,
            redacted_fields,
        },
    })
}

fn validate_replay_fixture_semantic_payload(
    category: &str,
    payload: &ReplayFixtureSemanticPayload,
) -> Result<usize> {
    if !matches!(payload.run.state.as_str(), "done" | "failed" | "cancelled")
        || payload.run.generation == 0
        || payload.run.reason_code.trim().is_empty()
    {
        bail!("replay fixture run state, generation, or reason code is invalid");
    }
    let token_sum = payload
        .run
        .prompt_tokens
        .checked_add(payload.run.completion_tokens)
        .context("replay fixture token usage overflow")?;
    if token_sum != payload.run.total_tokens {
        bail!("replay fixture token usage total is inconsistent");
    }
    if payload.expected.terminal_state != payload.run.state
        || payload.expected.terminal_count != 1
        || payload.expected.tape_event_count != payload.tape_events.len()
    {
        bail!("replay fixture expected terminal or event counts are inconsistent");
    }
    if payload.tape_events.is_empty() {
        bail!("replay fixture semantic payload must include tape events");
    }
    let event_types =
        payload.tape_events.iter().map(|event| event.event_type.clone()).collect::<Vec<_>>();
    if payload.expected.tape_event_types != event_types {
        bail!("replay fixture expected event types must preserve exact tape order");
    }
    if payload
        .expected
        .final_answer_sha256
        .as_deref()
        .is_some_and(|digest| !is_nonplaceholder_sha256(digest))
    {
        bail!("replay fixture final-answer digest must be a non-placeholder lowercase sha256");
    }

    let mut checked_invariants = 4_usize;
    let mut last_generation = None;
    let mut terminal_events = 0_u32;
    let mut observed_side_effects = BTreeSet::new();
    let mut observed_idempotency_keys = BTreeSet::new();
    for (index, event) in payload.tape_events.iter().enumerate() {
        let expected_seq = i64::try_from(index.saturating_add(1))
            .context("replay fixture tape sequence overflow")?;
        if event.seq != expected_seq {
            bail!("replay fixture tape sequence must be contiguous and start at one");
        }
        if event.event_type.trim().is_empty() {
            bail!("replay fixture tape event type must not be empty");
        }
        let path = format!("$.tape_events[{index}].payload");
        let object = event
            .payload
            .as_object()
            .with_context(|| format!("{path} must be a semantic object"))?;
        let generation = required_semantic_u64(object, "generation", path.as_str())?;
        if generation == 0 || last_generation.is_some_and(|previous| generation < previous) {
            bail!("replay fixture tape generation must be non-zero and monotonic");
        }
        last_generation = Some(generation);
        required_semantic_string(object, "reason_code", path.as_str())?;
        if let Some(identity) =
            optional_semantic_string(object, "side_effect_identity", path.as_str())?
        {
            observed_side_effects.insert(identity.to_owned());
        }
        if let Some(key) = optional_semantic_string(object, "idempotency_key", path.as_str())? {
            observed_idempotency_keys.insert(key.to_owned());
        }
        if event.event_type == "run.terminal" {
            terminal_events = terminal_events.saturating_add(1);
            let terminal_count = required_semantic_u64(object, "terminal_count", path.as_str())?;
            let terminal_state = required_semantic_string(object, "state", path.as_str())?;
            let reason_code = required_semantic_string(object, "reason_code", path.as_str())?;
            if terminal_count != u64::from(payload.expected.terminal_count)
                || terminal_state != payload.run.state
                || reason_code != payload.run.reason_code
            {
                bail!("replay fixture terminal event must match run and expected projections");
            }
        }
        checked_invariants = checked_invariants.saturating_add(3);
    }
    if terminal_events != payload.expected.terminal_count
        || last_generation != Some(payload.run.generation)
    {
        bail!("replay fixture terminal count or final generation is inconsistent");
    }

    validate_replay_fixture_tools(payload.tool_exchanges.as_slice())?;
    validate_replay_fixture_approvals(payload.approvals.as_slice())?;
    validate_replay_fixture_decisions(payload.queue_decisions.as_slice())?;
    validate_replay_fixture_lifecycle(
        payload.lifecycle_transitions.as_slice(),
        payload.run.state.as_str(),
        payload.expected.terminal_count,
    )?;
    let (idempotency_keys, side_effect_identities) =
        validate_replay_fixture_idempotency(payload.idempotency_records.as_slice())?;
    if observed_idempotency_keys.len() != idempotency_keys.len()
        || observed_idempotency_keys.iter().any(|key| !idempotency_keys.contains(key.as_str()))
    {
        bail!("replay fixture tape and durable idempotency identities must match exactly");
    }
    if observed_side_effects.len() != side_effect_identities.len()
        || observed_side_effects
            .iter()
            .any(|identity| !side_effect_identities.contains(identity.as_str()))
    {
        bail!("replay fixture tape and durable side-effect identities must match exactly");
    }
    validate_replay_fixture_artifacts(payload.artifact_refs.as_slice())?;
    validate_replay_fixture_payload_redaction(payload)?;
    validate_replay_fixture_category_payload(category, payload)?;
    checked_invariants = checked_invariants
        .saturating_add(payload.tool_exchanges.len())
        .saturating_add(payload.approvals.len())
        .saturating_add(payload.queue_decisions.len())
        .saturating_add(payload.lifecycle_transitions.len().saturating_mul(3))
        .saturating_add(payload.idempotency_records.len().saturating_mul(3))
        .saturating_add(payload.artifact_refs.len());
    Ok(checked_invariants)
}

fn validate_replay_fixture_category_payload(
    category: &str,
    payload: &ReplayFixtureSemanticPayload,
) -> Result<()> {
    match category {
        "text_run" if payload.expected.final_answer_sha256.is_some() => Ok(()),
        "tool_run" if !payload.tool_exchanges.is_empty() && !payload.artifact_refs.is_empty() => {
            Ok(())
        }
        "approval"
            if payload.approvals.iter().any(|approval| {
                approval.response.get("approved").and_then(Value::as_bool) == Some(false)
            }) =>
        {
            Ok(())
        }
        "compaction"
            if payload
                .artifact_refs
                .iter()
                .any(|artifact| artifact.kind == "compaction_summary") =>
        {
            Ok(())
        }
        "queue" if !payload.queue_decisions.is_empty() => Ok(()),
        "cancellation" if payload.run.state == "cancelled" && payload.run.last_error.is_some() => {
            Ok(())
        }
        "delivery"
            if !payload.idempotency_records.is_empty()
                && payload
                    .tape_events
                    .iter()
                    .any(|event| event.payload.get("side_effect_identity").is_some()) =>
        {
            Ok(())
        }
        "restart_terminalization"
            if payload.lifecycle_transitions.len() >= 2
                && !payload.idempotency_records.is_empty() =>
        {
            Ok(())
        }
        _ if REPLAY_FIXTURE_CATEGORIES.contains(&category) => {
            bail!("replay fixture category payload is not representative")
        }
        _ => bail!("replay fixture case category is unknown"),
    }
}

fn required_semantic_u64(
    object: &serde_json::Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<u64> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .with_context(|| format!("{path}.{field} must be an unsigned integer"))
}

fn required_semantic_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<&'a str> {
    let value = object
        .get(field)
        .and_then(Value::as_str)
        .with_context(|| format!("{path}.{field} must be a string"))?;
    if value.trim().is_empty() {
        bail!("{path}.{field} must not be empty");
    }
    Ok(value)
}

fn optional_semantic_string<'a>(
    object: &'a serde_json::Map<String, Value>,
    field: &str,
    path: &str,
) -> Result<Option<&'a str>> {
    let Some(value) = object.get(field) else {
        return Ok(None);
    };
    let value = value.as_str().with_context(|| format!("{path}.{field} must be a string"))?;
    if value.trim().is_empty() {
        bail!("{path}.{field} must not be empty");
    }
    Ok(Some(value))
}

fn validate_replay_fixture_tools(exchanges: &[ReplayFixtureToolExchange]) -> Result<()> {
    let mut proposals = BTreeSet::new();
    for exchange in exchanges {
        if exchange.proposal_id.trim().is_empty()
            || exchange.tool_name.trim().is_empty()
            || !proposals.insert(exchange.proposal_id.as_str())
            || exchange.result.get("reason_code").and_then(Value::as_str).is_none_or(str::is_empty)
        {
            bail!("replay fixture tool exchanges must retain unique identity and reason code");
        }
    }
    Ok(())
}

fn validate_replay_fixture_approvals(approvals: &[ReplayFixtureApprovalExchange]) -> Result<()> {
    let mut approval_ids = BTreeSet::new();
    for approval in approvals {
        if approval.approval_id.trim().is_empty()
            || approval.proposal_id.trim().is_empty()
            || !approval_ids.insert(approval.approval_id.as_str())
            || approval.response.get("approved").and_then(Value::as_bool).is_none()
            || approval
                .response
                .get("reason_code")
                .and_then(Value::as_str)
                .is_none_or(str::is_empty)
        {
            bail!("replay fixture approval exchanges must retain verdict and reason code");
        }
    }
    Ok(())
}

fn validate_replay_fixture_decisions(decisions: &[ReplayFixtureDecisionRecord]) -> Result<()> {
    let mut record_ids = BTreeSet::new();
    for decision in decisions {
        if decision.record_id.trim().is_empty()
            || decision.kind.trim().is_empty()
            || !record_ids.insert(decision.record_id.as_str())
        {
            bail!("replay fixture queue decisions must retain unique identity");
        }
        let object = decision
            .payload
            .as_object()
            .context("replay fixture queue decision payload must be an object")?;
        required_semantic_u64(object, "generation", "$.queue_decisions[*].payload")?;
        required_semantic_string(object, "reason_code", "$.queue_decisions[*].payload")?;
    }
    Ok(())
}

fn validate_replay_fixture_lifecycle(
    transitions: &[ReplayFixtureLifecycleTransition],
    terminal_state: &str,
    expected_terminal_count: u32,
) -> Result<()> {
    let mut previous_generation = None;
    let mut terminal_count = 0_u32;
    for transition in transitions {
        if transition.to_state.trim().is_empty()
            || transition.generation == 0
            || transition.reason_code.trim().is_empty()
            || previous_generation.is_some_and(|previous| transition.generation < previous)
        {
            bail!("replay fixture lifecycle transition is semantically invalid");
        }
        previous_generation = Some(transition.generation);
        terminal_count = terminal_count.saturating_add(transition.terminal_count);
        if transition.terminal_count > 0 && transition.to_state != terminal_state {
            bail!("replay fixture terminal lifecycle state does not match run state");
        }
    }
    if !transitions.is_empty() && terminal_count != expected_terminal_count {
        bail!("replay fixture lifecycle terminal count is inconsistent");
    }
    Ok(())
}

fn validate_replay_fixture_idempotency(
    records: &[ReplayFixtureIdempotencyRecord],
) -> Result<(BTreeSet<&str>, BTreeSet<&str>)> {
    let mut keys = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for record in records {
        if record.key.trim().is_empty()
            || record.scope.trim().is_empty()
            || record.operation_kind.trim().is_empty()
            || record.state.trim().is_empty()
            || record.side_effect_identity.trim().is_empty()
            || record.reason_code.trim().is_empty()
            || !is_nonplaceholder_sha256(record.payload_sha256.as_str())
            || !keys.insert(record.key.as_str())
            || !identities.insert(record.side_effect_identity.as_str())
        {
            bail!("replay fixture idempotency record is invalid or duplicated");
        }
    }
    Ok((keys, identities))
}

fn validate_replay_fixture_artifacts(artifacts: &[ReplayFixtureArtifactRef]) -> Result<()> {
    let mut artifact_ids = BTreeSet::new();
    for artifact in artifacts {
        if artifact.artifact_id.trim().is_empty()
            || artifact.kind.trim().is_empty()
            || !artifact_ids.insert(artifact.artifact_id.as_str())
            || !is_nonplaceholder_sha256(artifact.sha256.as_str())
        {
            bail!("replay fixture artifact reference is invalid, duplicated, or placeholder");
        }
    }
    Ok(())
}

fn is_nonplaceholder_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        && value.as_bytes().windows(2).any(|pair| pair[0] != pair[1])
}

fn validate_replay_fixture_payload_redaction(payload: &ReplayFixtureSemanticPayload) -> Result<()> {
    if payload.redaction.raw_secrets_allowed
        || payload.redaction.raw_paths_allowed
        || payload.redaction.raw_prompts_allowed
    {
        bail!("replay fixture semantic payload must remain strictly redacted");
    }
    let redacted_fields =
        payload.redaction.redacted_fields.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if redacted_fields.len() != payload.redaction.redacted_fields.len()
        || redacted_fields.iter().any(|field| field.trim().is_empty())
    {
        bail!("replay fixture semantic redacted fields must be unique and non-empty");
    }
    let value = serde_json::to_value(payload)
        .context("failed to serialize replay fixture for redaction validation")?;
    validate_replay_fixture_redacted_value(&value, "$")
}

fn validate_replay_fixture_redacted_value(value: &Value, path: &str) -> Result<()> {
    match value {
        Value::Object(object) => {
            for (key, child) in object {
                let child_path = format!("{path}.{key}");
                if is_sensitive_key(key.as_str())
                    && !matches!(
                        key.as_str(),
                        "prompt_tokens"
                            | "completion_tokens"
                            | "total_tokens"
                            | "raw_secrets_allowed"
                            | "raw_prompts_allowed"
                    )
                    && child.as_str() != Some("<redacted>")
                    && !child.is_null()
                {
                    bail!("replay fixture contains unredacted sensitive field at {child_path}");
                }
                validate_replay_fixture_redacted_value(child, child_path.as_str())?;
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                validate_replay_fixture_redacted_value(child, format!("{path}[{index}]").as_str())?;
            }
        }
        Value::String(raw) if redact_trace_string(raw.as_str()) != *raw => {
            bail!("replay fixture contains an unredacted path or diagnostic at {path}");
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn replay_fixture_semantic_sha256(
    payload: &ReplayFixtureSemanticPayload,
    allowances: &[ReplayCompatibilityAllowance],
) -> Result<String> {
    let mut normalized = payload.clone();
    if allowances.iter().any(|allowance| allowance.id == "wall_clock_timestamps") {
        normalize_replay_fixture_timestamps(&mut normalized);
    }
    // Usage values stay exact in the golden digest. Their bounded tolerance is
    // meaningful only when a candidate payload is compared with this baseline.
    let bytes = serde_json::to_vec(&normalized)
        .context("failed to serialize normalized replay fixture semantic payload")?;
    Ok(sha256_hex(bytes.as_slice()))
}

fn compare_replay_fixture_payloads(
    expected: &ReplayFixtureSemanticPayload,
    actual: &ReplayFixtureSemanticPayload,
    allowances: &[ReplayCompatibilityAllowance],
) -> Result<()> {
    validate_replay_compatibility_allowances(allowances)?;
    let mut expected = expected.clone();
    let mut actual = actual.clone();
    if allowances.iter().any(|allowance| allowance.id == "wall_clock_timestamps") {
        normalize_replay_fixture_timestamps(&mut expected);
        normalize_replay_fixture_timestamps(&mut actual);
    }
    let usage_tolerance = allowances
        .iter()
        .find(|allowance| allowance.id == "provider_usage_rounding")
        .and_then(|allowance| allowance.tolerance)
        .context("provider usage compatibility allowance is missing")?;
    compare_usage_with_tolerance(
        "$.run.prompt_tokens",
        expected.run.prompt_tokens,
        actual.run.prompt_tokens,
        usage_tolerance,
    )?;
    compare_usage_with_tolerance(
        "$.run.completion_tokens",
        expected.run.completion_tokens,
        actual.run.completion_tokens,
        usage_tolerance,
    )?;
    compare_usage_with_tolerance(
        "$.run.total_tokens",
        expected.run.total_tokens,
        actual.run.total_tokens,
        usage_tolerance,
    )?;
    actual.run.prompt_tokens = expected.run.prompt_tokens;
    actual.run.completion_tokens = expected.run.completion_tokens;
    actual.run.total_tokens = expected.run.total_tokens;

    let expected = serde_json::to_value(expected)
        .context("failed to serialize expected replay fixture semantic payload")?;
    let actual = serde_json::to_value(actual)
        .context("failed to serialize actual replay fixture semantic payload")?;
    if let Some((path, expected, actual)) = first_replay_semantic_diff(&expected, &actual, "$") {
        bail!("semantic replay payload mismatch at {path}: expected={expected} actual={actual}");
    }
    Ok(())
}

fn compare_usage_with_tolerance(
    path: &str,
    expected: u64,
    actual: u64,
    tolerance: u64,
) -> Result<()> {
    if expected.abs_diff(actual) > tolerance {
        bail!(
            "semantic replay payload mismatch at {path}: expected={expected} actual={actual} tolerance={tolerance}"
        );
    }
    Ok(())
}

fn normalize_replay_fixture_timestamps(payload: &mut ReplayFixtureSemanticPayload) {
    payload.generated_at_unix_ms = 0;
    payload.capture.captured_at_unix_ms = 0;
    for event in &mut payload.tape_events {
        let Some(object) = event.payload.as_object_mut() else {
            continue;
        };
        for (key, value) in object {
            if key == "timestamp" || key.ends_with("_at_unix_ms") {
                *value = Value::from(0);
            }
        }
    }
}

fn first_replay_semantic_diff(
    expected: &Value,
    actual: &Value,
    path: &str,
) -> Option<(String, String, String)> {
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            let keys = expected.keys().chain(actual.keys()).collect::<BTreeSet<_>>();
            for key in keys {
                let child_path = format!("{path}.{key}");
                match (expected.get(key), actual.get(key)) {
                    (Some(expected), Some(actual)) => {
                        if let Some(diff) =
                            first_replay_semantic_diff(expected, actual, child_path.as_str())
                        {
                            return Some(diff);
                        }
                    }
                    (Some(expected), None) => {
                        return Some((
                            child_path,
                            semantic_value_display(expected),
                            "<missing>".to_owned(),
                        ));
                    }
                    (None, Some(actual)) => {
                        return Some((
                            child_path,
                            "<missing>".to_owned(),
                            semantic_value_display(actual),
                        ));
                    }
                    (None, None) => {}
                }
            }
            None
        }
        (Value::Array(expected), Value::Array(actual)) => {
            if expected.len() != actual.len() {
                return Some((
                    path.to_owned(),
                    format!("array length {}", expected.len()),
                    format!("array length {}", actual.len()),
                ));
            }
            for (index, (expected, actual)) in expected.iter().zip(actual).enumerate() {
                if let Some(diff) = first_replay_semantic_diff(
                    expected,
                    actual,
                    format!("{path}[{index}]").as_str(),
                ) {
                    return Some(diff);
                }
            }
            None
        }
        _ if expected == actual => None,
        _ => Some((
            path.to_owned(),
            semantic_value_display(expected),
            semantic_value_display(actual),
        )),
    }
}

fn semantic_value_display(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "<unserializable>".to_owned())
}

#[cfg(test)]
mod tests;
