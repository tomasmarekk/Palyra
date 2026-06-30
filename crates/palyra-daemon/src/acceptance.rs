//! Acceptance criteria contracts shared by flows, commitments, and task views.
//!
//! The criteria live inside existing journal-backed JSON fields. This module
//! keeps their shape typed and replayable without adding a second persistence
//! path for flow or commitment state.

use palyra_common::runtime_contracts::FlowStepState;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::journal::{CommitmentRecord, FlowStepRecord};

pub(crate) const ACCEPTANCE_CRITERIA_SCHEMA_VERSION: u32 = 1;
pub(crate) const ACCEPTANCE_CRITERIA_STARTED_EVENT: &str =
    "acceptance_criteria_pro_flows_a_commitments.started";
pub(crate) const ACCEPTANCE_CRITERIA_COMPLETED_EVENT: &str =
    "acceptance_criteria_pro_flows_a_commitments.completed";
pub(crate) const ACCEPTANCE_CRITERIA_FAILED_EVENT: &str =
    "acceptance_criteria_pro_flows_a_commitments.failed";
const ACCEPTANCE_CRITERIA_KEY: &str = "acceptance_criteria";
const ACCEPTANCE_REDACTION_LEVEL: &str = "metadata_only";

/// Decision projected from a flow step or commitment against its criteria.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AcceptanceDecision {
    Pending,
    Satisfied,
    Unsatisfied,
    Blocked,
    Waived,
}

impl AcceptanceDecision {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Satisfied => "satisfied",
            Self::Unsatisfied => "unsatisfied",
            Self::Blocked => "blocked",
            Self::Waived => "waived",
        }
    }
}

/// One required or optional acceptance criterion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcceptanceCriterion {
    pub description: String,
    #[serde(default = "default_required")]
    pub required: bool,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
}

/// Criteria block embedded in flow step input JSON or commitment evidence JSON.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcceptanceCriteria {
    #[serde(default = "default_acceptance_schema_version")]
    pub schema_version: u32,
    #[serde(default)]
    pub criteria: Vec<AcceptanceCriterion>,
    #[serde(default = "default_acceptance_decision")]
    pub decision: AcceptanceDecision,
    #[serde(default = "default_acceptance_reason_code")]
    pub reason_code: String,
    #[serde(default = "default_acceptance_redaction_level")]
    pub redaction_level: String,
}

/// Journal/read-model projection for one flow step or commitment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcceptanceJournalProjection {
    pub schema_version: u32,
    pub source_kind: String,
    pub source_id: String,
    pub decision: AcceptanceDecision,
    pub criteria_count: usize,
    pub required_count: usize,
    pub reason_code: String,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    pub redaction_level: String,
    pub event_types: Vec<String>,
}

/// Adds a typed acceptance block to a JSON object, preserving existing input fields.
pub(crate) fn attach_acceptance_criteria(input: Value, criteria: AcceptanceCriteria) -> Value {
    let mut object = match input {
        Value::Object(map) => map,
        other => {
            let mut map = serde_json::Map::new();
            map.insert("payload".to_owned(), other);
            map
        }
    };
    object.insert(ACCEPTANCE_CRITERIA_KEY.to_owned(), json!(criteria));
    Value::Object(object)
}

/// Default acceptance criteria attached to new flow steps.
pub(crate) fn flow_step_acceptance_criteria(
    adapter: &str,
    step_kind: &str,
    title: &str,
) -> AcceptanceCriteria {
    AcceptanceCriteria {
        schema_version: ACCEPTANCE_CRITERIA_SCHEMA_VERSION,
        criteria: vec![AcceptanceCriterion {
            description: format!(
                "Flow step '{title}' via adapter '{adapter}' reaches a terminal accepted outcome"
            ),
            required: true,
            evidence_refs: vec![format!("flow_step:{step_kind}:{adapter}")],
        }],
        decision: AcceptanceDecision::Pending,
        reason_code: "flow_step_acceptance_required".to_owned(),
        redaction_level: ACCEPTANCE_REDACTION_LEVEL.to_owned(),
    }
}

/// Summarizes step-level criteria for the flow metadata event.
pub(crate) fn flow_acceptance_metadata(steps: &[crate::journal::FlowStepCreateRequest]) -> Value {
    json!({
        "schema_version": ACCEPTANCE_CRITERIA_SCHEMA_VERSION,
        "criteria_count": steps
            .iter()
            .filter_map(|step| acceptance_from_json_str(step.input_json.as_str()))
            .map(|criteria| criteria.criteria.len())
            .sum::<usize>(),
        "reason_code": "flow_acceptance_metadata",
        "event_types": acceptance_event_types(),
        "redaction_level": ACCEPTANCE_REDACTION_LEVEL,
    })
}

/// Builds a read-model projection for a stored flow step.
pub(crate) fn flow_step_acceptance_projection(
    step: &FlowStepRecord,
) -> AcceptanceJournalProjection {
    let criteria = acceptance_from_json_str(step.input_json.as_str()).unwrap_or_else(|| {
        flow_step_acceptance_criteria(
            step.adapter.as_str(),
            step.step_kind.as_str(),
            step.title.as_str(),
        )
    });
    acceptance_projection(
        "flow_step",
        step.step_id.as_str(),
        decision_from_flow_step(step.state.as_str()),
        criteria,
    )
}

/// Acceptance criteria attached to newly extracted commitments.
pub(crate) fn commitment_acceptance_criteria(action: &str) -> AcceptanceCriteria {
    AcceptanceCriteria {
        schema_version: ACCEPTANCE_CRITERIA_SCHEMA_VERSION,
        criteria: vec![AcceptanceCriterion {
            description: format!(
                "Commitment '{action}' is reviewed and reaches a delivery outcome"
            ),
            required: true,
            evidence_refs: vec!["commitment.review".to_owned(), "commitment.delivery".to_owned()],
        }],
        decision: AcceptanceDecision::Pending,
        reason_code: "commitment_acceptance_required".to_owned(),
        redaction_level: ACCEPTANCE_REDACTION_LEVEL.to_owned(),
    }
}

/// Builds commitment source evidence JSON with embedded acceptance criteria.
pub(crate) fn commitment_evidence_json(
    source_text_preview: String,
    session_id: Option<String>,
    run_id: Option<String>,
    criteria: AcceptanceCriteria,
) -> String {
    json!({
        "source_text_preview": source_text_preview,
        "session_id": session_id,
        "run_id": run_id,
        ACCEPTANCE_CRITERIA_KEY: criteria,
        "acceptance_events": acceptance_event_types(),
    })
    .to_string()
}

/// Builds a read-model projection for a stored commitment.
pub(crate) fn commitment_acceptance_projection(
    commitment: &CommitmentRecord,
) -> AcceptanceJournalProjection {
    let criteria = acceptance_from_json_str(commitment.scheduler_binding_json.as_str())
        .unwrap_or_else(|| commitment_acceptance_criteria(commitment.normalized_action.as_str()));
    acceptance_projection(
        "commitment",
        commitment.commitment_id.as_str(),
        decision_from_commitment_status(commitment.status.as_str()),
        criteria,
    )
}

pub(crate) fn acceptance_event_types() -> [&'static str; 3] {
    [
        ACCEPTANCE_CRITERIA_STARTED_EVENT,
        ACCEPTANCE_CRITERIA_COMPLETED_EVENT,
        ACCEPTANCE_CRITERIA_FAILED_EVENT,
    ]
}

fn acceptance_from_json_str(raw: &str) -> Option<AcceptanceCriteria> {
    let value = serde_json::from_str::<Value>(raw).ok()?;
    value
        .get(ACCEPTANCE_CRITERIA_KEY)
        .cloned()
        .and_then(|value| serde_json::from_value::<AcceptanceCriteria>(value).ok())
}

fn acceptance_projection(
    source_kind: &str,
    source_id: &str,
    decision: AcceptanceDecision,
    criteria: AcceptanceCriteria,
) -> AcceptanceJournalProjection {
    let evidence_refs = criteria
        .criteria
        .iter()
        .flat_map(|criterion| criterion.evidence_refs.iter().cloned())
        .collect::<Vec<_>>();
    AcceptanceJournalProjection {
        schema_version: ACCEPTANCE_CRITERIA_SCHEMA_VERSION,
        source_kind: source_kind.to_owned(),
        source_id: source_id.to_owned(),
        decision,
        criteria_count: criteria.criteria.len(),
        required_count: criteria.criteria.iter().filter(|criterion| criterion.required).count(),
        reason_code: criteria.reason_code,
        evidence_refs,
        redaction_level: criteria.redaction_level,
        event_types: acceptance_event_types().into_iter().map(str::to_owned).collect(),
    }
}

fn decision_from_flow_step(state: &str) -> AcceptanceDecision {
    match FlowStepState::from_str(state) {
        Some(FlowStepState::Succeeded | FlowStepState::Compensated) => {
            AcceptanceDecision::Satisfied
        }
        Some(FlowStepState::Skipped) => AcceptanceDecision::Waived,
        Some(FlowStepState::Failed | FlowStepState::TimedOut | FlowStepState::Cancelled) => {
            AcceptanceDecision::Unsatisfied
        }
        Some(FlowStepState::Blocked | FlowStepState::WaitingForApproval) => {
            AcceptanceDecision::Blocked
        }
        _ => AcceptanceDecision::Pending,
    }
}

fn decision_from_commitment_status(status: &str) -> AcceptanceDecision {
    match status {
        "delivered" => AcceptanceDecision::Satisfied,
        "dismissed" => AcceptanceDecision::Waived,
        "failed" => AcceptanceDecision::Unsatisfied,
        _ => AcceptanceDecision::Pending,
    }
}

fn default_acceptance_schema_version() -> u32 {
    ACCEPTANCE_CRITERIA_SCHEMA_VERSION
}

fn default_required() -> bool {
    true
}

fn default_acceptance_decision() -> AcceptanceDecision {
    AcceptanceDecision::Pending
}

fn default_acceptance_reason_code() -> String {
    "acceptance_criteria_pending".to_owned()
}

fn default_acceptance_redaction_level() -> String {
    ACCEPTANCE_REDACTION_LEVEL.to_owned()
}

#[cfg(test)]
mod tests {
    use super::{
        attach_acceptance_criteria, commitment_acceptance_criteria, commitment_evidence_json,
        flow_step_acceptance_criteria, flow_step_acceptance_projection, AcceptanceDecision,
    };
    use crate::journal::FlowStepRecord;
    use serde_json::json;

    #[test]
    fn flow_step_projection_derives_decision_from_state() {
        let criteria = flow_step_acceptance_criteria("auxiliary_task", "summary", "Summarize");
        let input = attach_acceptance_criteria(json!({"input_text":"hello"}), criteria);
        let step = FlowStepRecord {
            step_id: "step-1".to_owned(),
            flow_id: "flow-1".to_owned(),
            step_index: 0,
            step_kind: "summary".to_owned(),
            adapter: "auxiliary_task".to_owned(),
            state: "succeeded".to_owned(),
            title: "Summarize".to_owned(),
            input_json: input.to_string(),
            output_json: None,
            lineage_json: "{}".to_owned(),
            depends_on_step_ids_json: "[]".to_owned(),
            attempt_count: 0,
            max_attempts: 1,
            backoff_ms: 1_000,
            timeout_ms: None,
            not_before_unix_ms: None,
            waiting_reason: None,
            last_error: None,
            created_at_unix_ms: 1,
            updated_at_unix_ms: 2,
            started_at_unix_ms: Some(1),
            completed_at_unix_ms: Some(2),
        };

        let projection = flow_step_acceptance_projection(&step);

        assert_eq!(projection.decision, AcceptanceDecision::Satisfied);
        assert_eq!(projection.criteria_count, 1);
        assert_eq!(projection.required_count, 1);
    }

    #[test]
    fn commitment_evidence_embeds_acceptance_criteria() {
        let criteria = commitment_acceptance_criteria("send the report");
        let evidence = commitment_evidence_json(
            "I will send the report".to_owned(),
            Some("session".to_owned()),
            Some("run".to_owned()),
            criteria,
        );
        let parsed = serde_json::from_str::<serde_json::Value>(evidence.as_str())
            .expect("evidence should be json");

        assert_eq!(parsed["acceptance_criteria"]["reason_code"], "commitment_acceptance_required");
        assert_eq!(
            parsed["acceptance_events"][1],
            "acceptance_criteria_pro_flows_a_commitments.completed"
        );
    }
}
