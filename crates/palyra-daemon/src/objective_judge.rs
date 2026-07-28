//! Objective judge contracts and strict output materialization.
//!
//! The judge is intentionally side-effect free: it can interpret auxiliary
//! model output and recommend a status, but it does not mutate objectives,
//! plans, tools, or flows. Background task dispatch remains responsible for
//! executing the provider call through the auxiliary executor.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::objectives::{ObjectiveContract, ObjectiveRecord};

pub(crate) const OBJECTIVE_JUDGE_SCHEMA_VERSION: u32 = 1;
pub(crate) const OBJECTIVE_JUDGE_STARTED_EVENT: &str = "objective.judge.started";
pub(crate) const OBJECTIVE_JUDGE_COMPLETED_EVENT: &str = "objective.judge.completed";
pub(crate) const OBJECTIVE_JUDGE_FAILED_EVENT: &str = "objective.judge.failed";
const OBJECTIVE_JUDGE_PARSE_BACKOFF_MS: u64 = 30_000;
const OBJECTIVE_JUDGE_REASON_CODE: &str = "objective_judge_auxiliary_loop";

/// Strict status vocabulary accepted from objective judge output.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ObjectiveJudgeStatus {
    Done,
    #[serde(rename = "not_done", alias = "continue")]
    Continue,
    Blocked,
    NeedsUser,
    Wait,
}

impl ObjectiveJudgeStatus {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Done => "done",
            Self::Continue => "not_done",
            Self::Blocked => "blocked",
            Self::NeedsUser => "needs_user",
            Self::Wait => "wait",
        }
    }

    pub(crate) const fn all() -> [Self; 5] {
        [Self::Done, Self::Continue, Self::Blocked, Self::NeedsUser, Self::Wait]
    }
}

/// Bounded input passed to an auxiliary objective judge task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectiveJudgeInput {
    #[serde(default = "default_objective_judge_schema_version")]
    pub schema_version: u32,
    pub objective_id: String,
    pub objective_name: String,
    pub contract: ObjectiveContract,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_focus: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_final_answer: Option<String>,
    #[serde(default)]
    pub completed_evidence_refs: Vec<String>,
    #[serde(default)]
    pub source_event_refs: Vec<String>,
    #[serde(default = "default_objective_judge_reason_code")]
    pub reason_code: String,
    #[serde(default = "default_objective_judge_redaction_level")]
    pub redaction_level: String,
}

impl ObjectiveJudgeInput {
    /// Builds judge input from an objective plus caller-supplied evidence.
    pub(crate) fn from_objective(
        objective: &ObjectiveRecord,
        candidate_final_answer: Option<String>,
        completed_evidence_refs: Vec<String>,
    ) -> Self {
        Self {
            schema_version: OBJECTIVE_JUDGE_SCHEMA_VERSION,
            objective_id: objective.objective_id.clone(),
            objective_name: objective.name.clone(),
            contract: objective.contract.clone(),
            current_focus: objective.current_focus.clone(),
            candidate_final_answer,
            completed_evidence_refs,
            source_event_refs: objective
                .contract_history
                .iter()
                .map(|entry| entry.event_id.clone())
                .collect(),
            reason_code: OBJECTIVE_JUDGE_REASON_CODE.to_owned(),
            redaction_level: objective.contract.redaction_level.clone(),
        }
    }
}

/// Sanitized judge output consumed by task and finalization read models.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ObjectiveJudgeOutput {
    pub schema_version: u32,
    pub status: ObjectiveJudgeStatus,
    pub summary: String,
    pub confidence_bps: u32,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub missing_evidence: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_action: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocked_reason: Option<String>,
    pub degraded: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_ms: Option<u64>,
    pub reason_code: String,
    pub redaction_level: String,
}

/// Non-authoritative finalization review decision derived from judge output.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ObjectiveFinalizationDecision {
    Pass,
    NeedsRevision,
    NotApplicable,
}

#[allow(dead_code)]
impl ObjectiveFinalizationDecision {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::NeedsRevision => "needs_revision",
            Self::NotApplicable => "not_applicable",
        }
    }
}

/// Inputs for building the final pre-answer objective review projection.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveFinalizationReviewInput {
    pub rollout_enabled: bool,
    pub candidate_final_answer_present: bool,
    pub completed_evidence_refs: Vec<String>,
    pub advisor_summary_refs: Vec<String>,
    pub revision_budget_tokens: u64,
}

/// Advisory finalization review consumed by host finalizer surfaces.
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ObjectiveFinalizationReview {
    pub schema_version: u32,
    pub decision: ObjectiveFinalizationDecision,
    pub decision_wire: String,
    pub non_authoritative: bool,
    pub can_mutate_final_answer: bool,
    pub revision_budget_tokens: u64,
    pub explanation: String,
    pub evidence_refs: Vec<String>,
    pub advisor_summary_refs: Vec<String>,
    pub event_type: String,
    pub reason_code: String,
    pub redaction_level: String,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RawObjectiveJudgeOutput {
    status: ObjectiveJudgeStatus,
    summary: String,
    #[serde(default)]
    confidence_bps: Option<u32>,
    #[serde(default)]
    evidence_refs: Vec<String>,
    #[serde(default)]
    missing_evidence: Vec<String>,
    #[serde(default)]
    next_action: Option<String>,
    #[serde(default)]
    blocked_reason: Option<String>,
    #[serde(default)]
    reason_code: Option<String>,
}

/// Materialized background-task result after objective judge parsing.
pub(crate) struct ObjectiveJudgeMaterializedResult {
    pub(crate) result_json: Value,
    pub(crate) parse_failed: bool,
    pub(crate) last_error: Option<String>,
}

/// Builds the exact prompt sent to the auxiliary executor for judge tasks.
///
/// The prompt constrains the model to one JSON object and explicitly forbids
/// tool calls or state changes; enforcement still happens in the runtime by
/// treating this as ordinary auxiliary model output.
pub(crate) fn build_objective_judge_prompt(input: &ObjectiveJudgeInput) -> String {
    let input_json = serde_json::to_string_pretty(input)
        .expect("objective judge input schema should always serialize");
    format!(
        "You are Palyra ObjectiveJudge. Return only strict JSON with fields status, summary, confidence_bps, evidence_refs, missing_evidence, next_action, blocked_reason, and reason_code. Allowed status values are done, not_done, blocked, needs_user, wait. Do not call tools, request tools, or mutate state. Mark done only when required evidence is present.\n<input>\n{input_json}\n</input>"
    )
}

/// Decodes queued-task payload JSON and turns it into the judge prompt.
pub(crate) fn build_objective_judge_prompt_from_payload(
    payload_json: Option<&str>,
) -> Result<String, String> {
    let payload = payload_json.ok_or_else(|| "objective judge payload is required".to_owned())?;
    let input = serde_json::from_str::<ObjectiveJudgeInput>(payload)
        .map_err(|error| format!("objective judge input did not match schema: {error}"))?;
    Ok(build_objective_judge_prompt(&input))
}

/// Builds a diagnostics payload describing the judge contract and rollout posture.
pub(crate) fn objective_judge_diagnostics_payload(rollout_enabled: bool) -> Value {
    json!({
        "schema_version": OBJECTIVE_JUDGE_SCHEMA_VERSION,
        "rollout_enabled": rollout_enabled,
        "auxiliary_task_kind": "objective_judge",
        "events": [
            OBJECTIVE_JUDGE_STARTED_EVENT,
            OBJECTIVE_JUDGE_COMPLETED_EVENT,
            OBJECTIVE_JUDGE_FAILED_EVENT,
        ],
        "allowed_statuses": ObjectiveJudgeStatus::all()
            .into_iter()
            .map(ObjectiveJudgeStatus::as_str)
            .collect::<Vec<_>>(),
        "parse_failure_backoff_ms": OBJECTIVE_JUDGE_PARSE_BACKOFF_MS,
        "reason_code": OBJECTIVE_JUDGE_REASON_CODE,
        "redaction_level": "metadata_only",
    })
}

/// Converts provider output plus judge input into stable task result JSON.
pub(crate) fn materialize_objective_judge_result(
    payload_json: Option<&str>,
    provider_output: &str,
    auxiliary_result_json: Value,
) -> ObjectiveJudgeMaterializedResult {
    let input = match payload_json {
        Some(payload) => match serde_json::from_str::<ObjectiveJudgeInput>(payload) {
            Ok(input) => Some(input),
            Err(error) => {
                let degraded = degraded_output(
                    "objective_judge_invalid_input",
                    format!("objective judge input did not match schema: {error}"),
                );
                return ObjectiveJudgeMaterializedResult {
                    result_json: objective_judge_result_json(
                        degraded,
                        auxiliary_result_json,
                        OBJECTIVE_JUDGE_FAILED_EVENT,
                    ),
                    parse_failed: true,
                    last_error: Some("objective judge input did not match schema".to_owned()),
                };
            }
        },
        None => None,
    };
    match parse_objective_judge_output(provider_output, input.as_ref()) {
        Ok(output) => ObjectiveJudgeMaterializedResult {
            result_json: objective_judge_result_json(
                output,
                auxiliary_result_json,
                OBJECTIVE_JUDGE_COMPLETED_EVENT,
            ),
            parse_failed: false,
            last_error: None,
        },
        Err(error) => {
            let degraded =
                degraded_output("objective_judge_parse_failed", error.safe_message.clone());
            ObjectiveJudgeMaterializedResult {
                result_json: objective_judge_result_json(
                    degraded,
                    auxiliary_result_json,
                    OBJECTIVE_JUDGE_FAILED_EVENT,
                ),
                parse_failed: true,
                last_error: Some(error.safe_message),
            }
        }
    }
}

/// Builds a degraded task result before any provider call is attempted.
pub(crate) fn invalid_objective_judge_input_result(
    message: String,
    auxiliary_result_json: Value,
) -> ObjectiveJudgeMaterializedResult {
    let degraded = degraded_output("objective_judge_invalid_input", message.clone());
    ObjectiveJudgeMaterializedResult {
        result_json: objective_judge_result_json(
            degraded,
            auxiliary_result_json,
            OBJECTIVE_JUDGE_FAILED_EVENT,
        ),
        parse_failed: true,
        last_error: Some(message),
    }
}

/// Builds the non-authoritative finalization review from an optional judge output.
#[allow(dead_code)]
#[must_use]
pub(crate) fn build_objective_finalization_review(
    input: ObjectiveFinalizationReviewInput,
    judge_output: Option<&ObjectiveJudgeOutput>,
) -> ObjectiveFinalizationReview {
    let (decision, explanation, reason_code) = if !input.rollout_enabled {
        (
            ObjectiveFinalizationDecision::NotApplicable,
            "objective judge rollout is disabled".to_owned(),
            "objective_finalization_review.rollout_disabled".to_owned(),
        )
    } else if !input.candidate_final_answer_present {
        (
            ObjectiveFinalizationDecision::NotApplicable,
            "no candidate final answer is available for review".to_owned(),
            "objective_finalization_review.no_candidate_final".to_owned(),
        )
    } else if let Some(output) = judge_output {
        finalization_decision_from_judge(output)
    } else {
        (
            ObjectiveFinalizationDecision::NotApplicable,
            "objective judge output is unavailable".to_owned(),
            "objective_finalization_review.judge_unavailable".to_owned(),
        )
    };
    ObjectiveFinalizationReview {
        schema_version: OBJECTIVE_JUDGE_SCHEMA_VERSION,
        decision,
        decision_wire: decision.as_str().to_owned(),
        non_authoritative: true,
        can_mutate_final_answer: false,
        revision_budget_tokens: input.revision_budget_tokens.min(1_000),
        explanation,
        evidence_refs: normalize_refs(input.completed_evidence_refs),
        advisor_summary_refs: normalize_refs(input.advisor_summary_refs),
        event_type: "objective.finalization_review.completed".to_owned(),
        reason_code,
        redaction_level: default_objective_judge_redaction_level(),
    }
}

#[allow(dead_code)]
fn finalization_decision_from_judge(
    output: &ObjectiveJudgeOutput,
) -> (ObjectiveFinalizationDecision, String, String) {
    if output.degraded {
        return (
            ObjectiveFinalizationDecision::NotApplicable,
            "degraded objective judge output cannot block terminal failure".to_owned(),
            "objective_finalization_review.degraded_not_applicable".to_owned(),
        );
    }
    match output.status {
        ObjectiveJudgeStatus::Done => (
            ObjectiveFinalizationDecision::Pass,
            output.summary.clone(),
            "objective_finalization_review.pass".to_owned(),
        ),
        ObjectiveJudgeStatus::Continue
        | ObjectiveJudgeStatus::Blocked
        | ObjectiveJudgeStatus::Wait => (
            ObjectiveFinalizationDecision::NeedsRevision,
            output.next_action.clone().unwrap_or_else(|| output.summary.clone()),
            "objective_finalization_review.needs_revision".to_owned(),
        ),
        ObjectiveJudgeStatus::NeedsUser => (
            ObjectiveFinalizationDecision::NeedsRevision,
            output.blocked_reason.clone().unwrap_or_else(|| {
                "objective judge needs user input before final answer".to_owned()
            }),
            "objective_finalization_review.needs_user".to_owned(),
        ),
    }
}

fn parse_objective_judge_output(
    raw: &str,
    input: Option<&ObjectiveJudgeInput>,
) -> Result<ObjectiveJudgeOutput, ObjectiveJudgeParseError> {
    let parsed = serde_json::from_str::<RawObjectiveJudgeOutput>(raw).map_err(|error| {
        ObjectiveJudgeParseError {
            safe_message: format!("objective judge output was not strict JSON: {error}"),
        }
    })?;
    let mut output = ObjectiveJudgeOutput {
        schema_version: OBJECTIVE_JUDGE_SCHEMA_VERSION,
        status: parsed.status,
        summary: bounded_text(parsed.summary, "summary")?,
        confidence_bps: parsed.confidence_bps.unwrap_or(0).min(10_000),
        evidence_refs: normalize_refs(parsed.evidence_refs),
        missing_evidence: normalize_refs(parsed.missing_evidence),
        next_action: parsed
            .next_action
            .map(|value| bounded_text(value, "next_action"))
            .transpose()?,
        blocked_reason: parsed
            .blocked_reason
            .map(|value| bounded_text(value, "blocked_reason"))
            .transpose()?,
        degraded: false,
        backoff_ms: None,
        reason_code: parsed.reason_code.unwrap_or_else(|| OBJECTIVE_JUDGE_REASON_CODE.to_owned()),
        redaction_level: input
            .map(|entry| entry.redaction_level.clone())
            .unwrap_or_else(default_objective_judge_redaction_level),
    };
    if output.status == ObjectiveJudgeStatus::Done {
        let missing = missing_required_evidence(input, output.evidence_refs.as_slice());
        if !missing.is_empty() {
            output.status = ObjectiveJudgeStatus::Continue;
            output.degraded = true;
            output.reason_code = "objective_judge_missing_required_evidence".to_owned();
            output.missing_evidence = merge_refs(output.missing_evidence, missing);
            output.summary = "Objective judge attempted done without required evidence.".to_owned();
        }
    }
    Ok(output)
}

fn objective_judge_result_json(
    output: ObjectiveJudgeOutput,
    auxiliary_result_json: Value,
    event_type: &'static str,
) -> Value {
    json!({
        "status": if output.degraded { "degraded" } else { "succeeded" },
        "event_type": event_type,
        "objective_judge": output,
        "auxiliary": auxiliary_result_json,
    })
}

fn degraded_output(reason_code: &'static str, summary: String) -> ObjectiveJudgeOutput {
    ObjectiveJudgeOutput {
        schema_version: OBJECTIVE_JUDGE_SCHEMA_VERSION,
        status: ObjectiveJudgeStatus::Wait,
        summary,
        confidence_bps: 0,
        evidence_refs: Vec::new(),
        missing_evidence: Vec::new(),
        next_action: Some("retry_objective_judge_after_backoff".to_owned()),
        blocked_reason: None,
        degraded: true,
        backoff_ms: Some(OBJECTIVE_JUDGE_PARSE_BACKOFF_MS),
        reason_code: reason_code.to_owned(),
        redaction_level: default_objective_judge_redaction_level(),
    }
}

fn missing_required_evidence(
    input: Option<&ObjectiveJudgeInput>,
    output_evidence_refs: &[String],
) -> Vec<String> {
    let Some(input) = input else {
        return Vec::new();
    };
    let required = required_evidence_refs(input.contract.clone());
    if required.is_empty() {
        return Vec::new();
    }
    let supplied = merge_refs(input.completed_evidence_refs.clone(), output_evidence_refs.to_vec());
    required
        .into_iter()
        .filter(|required_ref| !supplied.iter().any(|supplied_ref| supplied_ref == required_ref))
        .collect()
}

fn required_evidence_refs(contract: ObjectiveContract) -> Vec<String> {
    let mut refs = contract.required_evidence;
    for criterion in contract.success_criteria.items {
        if criterion.required {
            refs = merge_refs(refs, criterion.evidence_refs);
        }
    }
    normalize_refs(refs)
}

fn merge_refs(left: Vec<String>, right: Vec<String>) -> Vec<String> {
    normalize_refs(left.into_iter().chain(right).collect())
}

fn normalize_refs(values: Vec<String>) -> Vec<String> {
    let mut refs = Vec::new();
    for value in values {
        let trimmed = value.trim();
        if trimmed.is_empty() || refs.iter().any(|existing| existing == trimmed) {
            continue;
        }
        refs.push(trimmed.chars().take(256).collect());
        if refs.len() >= 64 {
            break;
        }
    }
    refs
}

fn bounded_text(value: String, field: &'static str) -> Result<String, ObjectiveJudgeParseError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ObjectiveJudgeParseError {
            safe_message: format!("objective judge field {field} cannot be empty"),
        });
    }
    Ok(trimmed.chars().take(2_000).collect())
}

fn default_objective_judge_schema_version() -> u32 {
    OBJECTIVE_JUDGE_SCHEMA_VERSION
}

fn default_objective_judge_reason_code() -> String {
    OBJECTIVE_JUDGE_REASON_CODE.to_owned()
}

fn default_objective_judge_redaction_level() -> String {
    "metadata_only".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectiveJudgeParseError {
    safe_message: String,
}

#[cfg(test)]
mod tests {
    use super::{
        build_objective_finalization_review, build_objective_judge_prompt,
        materialize_objective_judge_result, ObjectiveFinalizationDecision,
        ObjectiveFinalizationReviewInput, ObjectiveJudgeInput, ObjectiveJudgeOutput,
        ObjectiveJudgeStatus, OBJECTIVE_JUDGE_FAILED_EVENT, OBJECTIVE_JUDGE_SCHEMA_VERSION,
    };
    use crate::objectives::{
        ObjectiveContract, ObjectiveSuccessCriteria, ObjectiveSuccessCriterion,
    };
    use serde_json::json;

    fn judge_input() -> ObjectiveJudgeInput {
        ObjectiveJudgeInput {
            schema_version: OBJECTIVE_JUDGE_SCHEMA_VERSION,
            objective_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
            objective_name: "Ship contracts".to_owned(),
            contract: ObjectiveContract {
                success_criteria: ObjectiveSuccessCriteria {
                    items: vec![ObjectiveSuccessCriterion {
                        description: "Tests passed".to_owned(),
                        required: true,
                        evidence_refs: vec!["cargo:test".to_owned()],
                    }],
                },
                required_evidence: vec!["git:diff-check".to_owned()],
                ..ObjectiveContract::default()
            },
            current_focus: Some("Finish the judge parser.".to_owned()),
            candidate_final_answer: Some("All checks pass.".to_owned()),
            completed_evidence_refs: vec!["git:diff-check".to_owned()],
            source_event_refs: vec!["event:contract".to_owned()],
            reason_code: "test".to_owned(),
            redaction_level: "metadata_only".to_owned(),
        }
    }

    #[test]
    fn objective_judge_prompt_forbids_tools_and_state_changes() {
        let prompt = build_objective_judge_prompt(&judge_input());

        assert!(prompt.contains("Return only strict JSON"));
        assert!(prompt.contains("Do not call tools"));
        assert!(prompt.contains("\"objective_id\""));
    }

    #[test]
    fn objective_judge_done_requires_required_evidence() {
        let payload = serde_json::to_string(&judge_input()).expect("input should serialize");
        let output = json!({
            "status": "done",
            "summary": "Looks done.",
            "confidence_bps": 9500,
            "evidence_refs": [],
            "missing_evidence": [],
            "reason_code": "judge"
        })
        .to_string();

        let materialized =
            materialize_objective_judge_result(Some(payload.as_str()), output.as_str(), json!({}));

        assert!(!materialized.parse_failed);
        assert_eq!(materialized.result_json["objective_judge"]["status"], "not_done");
        assert_eq!(materialized.result_json["objective_judge"]["degraded"], true);
        assert_eq!(
            materialized.result_json["objective_judge"]["missing_evidence"][0],
            "cargo:test"
        );
    }

    #[test]
    fn objective_judge_accepts_done_with_required_evidence() {
        let mut input = judge_input();
        input.completed_evidence_refs.push("cargo:test".to_owned());
        let payload = serde_json::to_string(&input).expect("input should serialize");
        let output = json!({
            "status": "done",
            "summary": "Looks done.",
            "confidence_bps": 9500,
            "evidence_refs": [],
            "missing_evidence": [],
            "reason_code": "judge"
        })
        .to_string();

        let materialized =
            materialize_objective_judge_result(Some(payload.as_str()), output.as_str(), json!({}));

        assert!(!materialized.parse_failed);
        assert_eq!(materialized.result_json["objective_judge"]["status"], "done");
    }

    #[test]
    fn malformed_judge_output_returns_degraded_backoff() {
        let materialized = materialize_objective_judge_result(None, "not json", json!({}));

        assert!(materialized.parse_failed);
        assert_eq!(materialized.result_json["event_type"], OBJECTIVE_JUDGE_FAILED_EVENT);
        assert_eq!(materialized.result_json["objective_judge"]["status"], "wait");
        assert_eq!(materialized.result_json["objective_judge"]["backoff_ms"], 30_000);
        assert!(materialized.last_error.is_some());
    }

    #[test]
    fn objective_judge_status_wire_values_are_stable() {
        assert_eq!(ObjectiveJudgeStatus::Done.as_str(), "done");
        assert_eq!(ObjectiveJudgeStatus::Continue.as_str(), "not_done");
        assert_eq!(ObjectiveJudgeStatus::Blocked.as_str(), "blocked");
        assert_eq!(ObjectiveJudgeStatus::NeedsUser.as_str(), "needs_user");
        assert_eq!(ObjectiveJudgeStatus::Wait.as_str(), "wait");
    }

    fn finalization_input() -> ObjectiveFinalizationReviewInput {
        ObjectiveFinalizationReviewInput {
            rollout_enabled: true,
            candidate_final_answer_present: true,
            completed_evidence_refs: vec!["cargo:test".to_owned()],
            advisor_summary_refs: vec!["advisor:security".to_owned()],
            revision_budget_tokens: 5_000,
        }
    }

    fn judge_output(status: ObjectiveJudgeStatus, degraded: bool) -> ObjectiveJudgeOutput {
        ObjectiveJudgeOutput {
            schema_version: OBJECTIVE_JUDGE_SCHEMA_VERSION,
            status,
            summary: "review summary".to_owned(),
            confidence_bps: 9_000,
            evidence_refs: vec!["cargo:test".to_owned()],
            missing_evidence: Vec::new(),
            next_action: Some("run missing verification".to_owned()),
            blocked_reason: Some("needs operator input".to_owned()),
            degraded,
            backoff_ms: degraded.then_some(30_000),
            reason_code: "judge".to_owned(),
            redaction_level: "metadata_only".to_owned(),
        }
    }

    #[test]
    fn finalization_review_passes_without_final_mutation_authority() {
        let review = build_objective_finalization_review(
            finalization_input(),
            Some(&judge_output(ObjectiveJudgeStatus::Done, false)),
        );

        assert_eq!(review.decision, ObjectiveFinalizationDecision::Pass);
        assert_eq!(review.decision_wire, "pass");
        assert!(!review.can_mutate_final_answer);
        assert!(review.non_authoritative);
        assert_eq!(review.revision_budget_tokens, 1_000);
        assert_eq!(review.event_type, "objective.finalization_review.completed");
    }

    #[test]
    fn finalization_review_requests_bounded_revision() {
        let review = build_objective_finalization_review(
            finalization_input(),
            Some(&judge_output(ObjectiveJudgeStatus::Continue, false)),
        );

        assert_eq!(review.decision, ObjectiveFinalizationDecision::NeedsRevision);
        assert_eq!(review.reason_code, "objective_finalization_review.needs_revision");
        assert_eq!(review.explanation, "run missing verification");
    }

    #[test]
    fn degraded_finalization_review_is_not_applicable() {
        let review = build_objective_finalization_review(
            finalization_input(),
            Some(&judge_output(ObjectiveJudgeStatus::Wait, true)),
        );

        assert_eq!(review.decision, ObjectiveFinalizationDecision::NotApplicable);
        assert_eq!(review.reason_code, "objective_finalization_review.degraded_not_applicable");
    }
}
