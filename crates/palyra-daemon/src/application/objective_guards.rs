//! Host-owned projection of durable run evidence into objective guard inputs.
//!
//! Only bounded redacted evidence and stable digests cross into the ledger;
//! provider messages and raw tool payloads remain outside objective state.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use palyra_common::redaction::redact_diagnostic_text;
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use tonic::Status;

use crate::{
    application::{
        plan_state::{AgentPlanQuery, AgentPlanStore},
        run_stream::agent_loop::{AgentLoopFinalizationEnvelope, FinalizationVerificationStatus},
    },
    gateway::GatewayRuntimeState,
    journal::{
        objective_continuation::{
            ObjectiveContinuationAttemptRecord, ObjectiveContinuationDecision,
        },
        ObjectiveGuardEvaluationRequest, ObjectiveGuardPolicy, ObjectiveProgressObservation,
        ObjectiveVerificationStatus, OrchestratorTapeRecord,
    },
    objectives::ObjectiveRecord,
};

const DEFAULT_MAX_RUNS: u64 = 64;
const DEFAULT_MAX_TURNS: u64 = 256;
const DEFAULT_MAX_PROVIDER_CALLS: u64 = 512;
const DEFAULT_MAX_TOKENS: u64 = 1_000_000;
const DEFAULT_MAX_COST_MICROS: u64 = 100_000_000;
const DEFAULT_MAX_WALL_TIME_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const MAX_EVIDENCE_REFS: usize = 64;
const MAX_EVIDENCE_REF_CHARS: usize = 512;

/// Builds the transaction input for one objective judge settlement.
///
/// # Errors
/// Returns a runtime status when the objective, source run, tape, or active
/// plan cannot be read or projected into a bounded guard observation.
pub(crate) async fn build_objective_guard_request(
    runtime: &Arc<GatewayRuntimeState>,
    objective: &ObjectiveRecord,
    attempt: &ObjectiveContinuationAttemptRecord,
    decision: ObjectiveContinuationDecision,
    parse_failure: bool,
    judge_evidence_json: &str,
) -> Result<ObjectiveGuardEvaluationRequest, Status> {
    let source_run = runtime
        .orchestrator_run_status_snapshot(attempt.source_run_id.clone())
        .await?
        .ok_or_else(|| Status::failed_precondition("objective source run is missing"))?;
    let state = Arc::clone(runtime);
    let source_run_id = attempt.source_run_id.clone();
    let session_id = attempt.session_id.clone();
    let objective_id = attempt.objective_id.clone();
    let (tape, plan_items) = tokio::task::spawn_blocking(move || {
        let tape = state
            .journal_store
            .orchestrator_tape(source_run_id.as_str())
            .map_err(objective_guard_journal_status)?;
        let linked_plan_ids = state
            .journal_store
            .active_plan_item_ids_for_objective(objective_id.as_str())
            .map_err(objective_guard_journal_status)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut plan_items = AgentPlanStore::new(&state.journal_store)
            .list_items(&AgentPlanQuery {
                owner_principal: None,
                device_id: None,
                channel: None,
                session_id: Some(session_id),
                run_id: None,
                status: None,
                include_terminal: false,
                limit: 500,
            })
            .map_err(objective_guard_journal_status)?;
        plan_items.retain(|item| linked_plan_ids.contains(&item.plan_item_id));
        Ok::<_, Status>((tape, plan_items))
    })
    .await
    .map_err(|_| Status::internal("objective guard evidence worker panicked"))??;

    let projection = project_tape_evidence(tape.as_slice(), judge_evidence_json)?;
    let plan_sha256 = plan_fingerprint(plan_items.as_slice())?;
    let wall_time_ms_delta = source_run
        .completed_at_unix_ms
        .unwrap_or(source_run.updated_at_unix_ms)
        .saturating_sub(source_run.started_at_unix_ms)
        .max(0)
        .try_into()
        .unwrap_or(u64::MAX);
    let verification_status =
        verification_status(decision, projection.finalization.as_ref(), objective);
    let verification_reason_code =
        verification_reason(decision, projection.finalization.as_ref(), objective);
    let verification_evidence_json = serde_json::to_string(&json!({
        "status": verification_status,
        "reason_code": verification_reason_code.as_deref(),
        "evidence_refs": projection.evidence_refs,
    }))
    .map_err(|error| {
        Status::internal(format!("objective verification evidence could not serialize: {error}"))
    })?;
    let observation = ObjectiveProgressObservation {
        attempt_id: attempt.attempt_id.clone(),
        objective_id: attempt.objective_id.clone(),
        session_id: attempt.session_id.clone(),
        root_run_id: attempt.root_run_id.clone(),
        source_run_id: attempt.source_run_id.clone(),
        source_run_generation: attempt.source_run_generation,
        decision,
        runs_delta: 1,
        turns_delta: projection.turns,
        provider_calls_delta: projection.provider_calls,
        tokens_delta: source_run.total_tokens,
        cost_micros_delta: projection.cost_micros,
        wall_time_ms_delta,
        progress_detected: projection.progress_detected,
        progress_sha256: projection.progress_sha256,
        plan_sha256,
        tool_error_sha256: projection.tool_error_sha256,
        parse_failure,
        verification_status,
        verification_reason_code,
        verification_evidence_json,
        missing_artifacts_json: serde_json::to_string(&projection.missing_artifacts).map_err(
            |error| {
                Status::internal(format!(
                    "missing objective artifacts could not serialize: {error}"
                ))
            },
        )?,
    };
    Ok(ObjectiveGuardEvaluationRequest { policy: objective_guard_policy(objective), observation })
}

fn objective_guard_policy(objective: &ObjectiveRecord) -> ObjectiveGuardPolicy {
    let max_runs = objective.budget.max_runs.map(u64::from).unwrap_or(DEFAULT_MAX_RUNS);
    let max_turns = objective.contract.max_turns.map(u64::from).unwrap_or(DEFAULT_MAX_TURNS);
    ObjectiveGuardPolicy {
        max_runs: Some(max_runs),
        max_turns: Some(max_turns),
        max_provider_calls: Some(max_turns.saturating_mul(2).clamp(1, DEFAULT_MAX_PROVIDER_CALLS)),
        max_tokens: Some(objective.budget.max_tokens.unwrap_or(DEFAULT_MAX_TOKENS)),
        max_cost_micros: Some(
            objective.contract.max_cost_microusd.unwrap_or(DEFAULT_MAX_COST_MICROS),
        ),
        max_wall_time_ms: Some(DEFAULT_MAX_WALL_TIME_MS),
        ..ObjectiveGuardPolicy::default()
    }
}

#[derive(Debug, Default)]
struct TapeEvidenceProjection {
    turns: u64,
    provider_calls: u64,
    cost_micros: u64,
    progress_detected: bool,
    progress_sha256: Option<String>,
    tool_error_sha256: Option<String>,
    evidence_refs: Vec<String>,
    missing_artifacts: Vec<String>,
    finalization: Option<AgentLoopFinalizationEnvelope>,
}

fn project_tape_evidence(
    tape: &[OrchestratorTapeRecord],
    judge_evidence_json: &str,
) -> Result<TapeEvidenceProjection, Status> {
    let mut projection = TapeEvidenceProjection::default();
    let mut progress_fragments = Vec::<Value>::new();
    let mut evidence_refs = parse_bounded_string_array(judge_evidence_json);
    for record in tape {
        match record.event_type.as_str() {
            "agent_loop.turn_started" => {
                projection.turns = projection.turns.saturating_add(1);
            }
            "provider.attempt.outcome" => {
                let payload = parse_json_object(record.payload_json.as_str());
                let calls = payload
                    .get("candidate_attempt_count")
                    .and_then(Value::as_u64)
                    .unwrap_or(1)
                    .max(1);
                projection.provider_calls = projection.provider_calls.saturating_add(calls);
                projection.cost_micros = projection.cost_micros.saturating_add(
                    payload
                        .get("aggregate_usage")
                        .and_then(Value::as_object)
                        .and_then(|usage| usage.get("estimated_cost_microusd"))
                        .and_then(Value::as_u64)
                        .unwrap_or_else(|| {
                            payload
                                .get("usage")
                                .and_then(Value::as_object)
                                .and_then(|usage| usage.get("estimated_cost_microusd"))
                                .and_then(Value::as_u64)
                                .unwrap_or(0)
                        }),
                );
            }
            "tool.loop.warning" | "tool.loop.blocked" => {
                let payload = parse_json_object(record.payload_json.as_str());
                projection.tool_error_sha256 = Some(canonical_sha256(&Value::Object(payload))?);
            }
            "agent_loop.terminated" => {
                let payload = parse_json_object(record.payload_json.as_str());
                let finalization = payload.get("finalization").cloned().and_then(|value| {
                    serde_json::from_value::<AgentLoopFinalizationEnvelope>(value).ok()
                });
                if let Some(finalization) = finalization {
                    evidence_refs.extend(finalization.artifact_refs.iter().cloned());
                    evidence_refs
                        .extend(finalization.evidence_summary.evidence_refs.iter().cloned());
                    evidence_refs
                        .extend(finalization.verification_finalizer.evidence_refs.iter().cloned());
                    if let Some(checkpoint) = finalization.progress_checkpoint.as_ref() {
                        projection.missing_artifacts.extend(
                            checkpoint
                                .missing_artifacts
                                .iter()
                                .map(|artifact| artifact.path.clone()),
                        );
                        progress_fragments.push(json!({
                            "last_successful_tool": checkpoint.last_successful_tool,
                            "produced_files": checkpoint.produced_files,
                        }));
                    }
                    projection.missing_artifacts.extend(
                        (0..finalization.evidence_summary.missing_artifacts_count)
                            .map(|index| format!("unresolved_artifact_{}", index + 1)),
                    );
                    progress_fragments.push(json!({
                        "artifact_refs": finalization.artifact_refs,
                        "verification_evidence_refs": finalization
                            .verification_finalizer
                            .evidence_refs,
                        "summary_evidence_refs": finalization.evidence_summary.evidence_refs,
                        "evidence_summary": {
                            "coverage": finalization.evidence_summary.coverage,
                            "produced_files_count": finalization
                                .evidence_summary
                                .produced_files_count,
                            "last_successful_tool": finalization
                                .evidence_summary
                                .last_successful_tool,
                        },
                    }));
                    projection.finalization = Some(finalization);
                }
            }
            _ => {}
        }
    }
    projection.evidence_refs = normalize_evidence_refs(evidence_refs);
    projection.missing_artifacts =
        normalize_evidence_refs(std::mem::take(&mut projection.missing_artifacts));
    if !progress_fragments.is_empty() {
        let progress = Value::Array(progress_fragments);
        projection.progress_detected = progress_contains_success(&progress);
        if projection.progress_detected {
            projection.progress_sha256 = Some(canonical_sha256(&progress)?);
        }
    }
    Ok(projection)
}

fn verification_status(
    decision: ObjectiveContinuationDecision,
    finalization: Option<&AgentLoopFinalizationEnvelope>,
    objective: &ObjectiveRecord,
) -> ObjectiveVerificationStatus {
    if decision != ObjectiveContinuationDecision::Done {
        return ObjectiveVerificationStatus::Unknown;
    }
    let Some(finalization) = finalization else {
        return ObjectiveVerificationStatus::Failed;
    };
    if finalization.evidence_summary.missing_artifacts_count > 0
        || finalization
            .progress_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| !checkpoint.missing_artifacts.is_empty())
    {
        return ObjectiveVerificationStatus::MissingArtifacts;
    }
    let evidence_available = !finalization.artifact_refs.is_empty()
        || !finalization.evidence_summary.evidence_refs.is_empty()
        || !finalization.verification_finalizer.evidence_refs.is_empty();
    if objective.contract.finalization_policy.require_required_evidence
        && !objective.contract.required_evidence.is_empty()
        && !evidence_available
    {
        return ObjectiveVerificationStatus::MissingEvidence;
    }
    match finalization.verification_finalizer.status {
        FinalizationVerificationStatus::Verified => ObjectiveVerificationStatus::Verified,
        FinalizationVerificationStatus::NotRequired => ObjectiveVerificationStatus::NotRequired,
        FinalizationVerificationStatus::NudgeRequired
        | FinalizationVerificationStatus::UnverifiedAllowed => ObjectiveVerificationStatus::Failed,
    }
}

fn verification_reason(
    decision: ObjectiveContinuationDecision,
    finalization: Option<&AgentLoopFinalizationEnvelope>,
    objective: &ObjectiveRecord,
) -> Option<String> {
    if decision != ObjectiveContinuationDecision::Done {
        return None;
    }
    let Some(finalization) = finalization else {
        return Some("objective.guard.verification_terminal_missing".to_owned());
    };
    if finalization.evidence_summary.missing_artifacts_count > 0
        || finalization
            .progress_checkpoint
            .as_ref()
            .is_some_and(|checkpoint| !checkpoint.missing_artifacts.is_empty())
    {
        return Some("objective.guard.verification_missing_artifacts".to_owned());
    }
    let evidence_available = !finalization.artifact_refs.is_empty()
        || !finalization.evidence_summary.evidence_refs.is_empty()
        || !finalization.verification_finalizer.evidence_refs.is_empty();
    if objective.contract.finalization_policy.require_required_evidence
        && !objective.contract.required_evidence.is_empty()
        && !evidence_available
    {
        return Some("objective.guard.verification_missing_evidence".to_owned());
    }
    Some(finalization.verification_finalizer.reason_code.clone())
}

fn plan_fingerprint(
    items: &[crate::application::plan_state::AgentPlanItem],
) -> Result<Option<String>, Status> {
    if items.is_empty() {
        return Ok(None);
    }
    let mut rows = items
        .iter()
        .map(|item| {
            json!({
                "plan_item_id": item.plan_item_id,
                "run_id": item.run_id,
                "parent_run_id": item.parent_run_id,
                "status": item.status,
                "priority": item.priority,
                "blocked": item.blocked_reason.is_some(),
                "reason_code": item.reason_code,
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by_key(|left| left.to_string());
    canonical_sha256(&Value::Array(rows)).map(Some)
}

fn progress_contains_success(progress: &Value) -> bool {
    progress.as_array().into_iter().flatten().filter_map(Value::as_object).any(|fragment| {
        fragment.get("artifact_refs").and_then(Value::as_array).is_some_and(|refs| !refs.is_empty())
            || fragment
                .get("verification_evidence_refs")
                .and_then(Value::as_array)
                .is_some_and(|refs| !refs.is_empty())
            || fragment
                .get("summary_evidence_refs")
                .and_then(Value::as_array)
                .is_some_and(|refs| !refs.is_empty())
            || fragment
                .get("produced_files")
                .and_then(Value::as_array)
                .is_some_and(|files| !files.is_empty())
            || fragment
                .get("evidence_summary")
                .and_then(Value::as_object)
                .and_then(|summary| summary.get("produced_files_count"))
                .and_then(Value::as_u64)
                .is_some_and(|count| count > 0)
    })
}

fn parse_json_object(raw: &str) -> Map<String, Value> {
    serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default()
}

fn parse_bounded_string_array(raw: &str) -> Vec<String> {
    serde_json::from_str::<Vec<String>>(raw).unwrap_or_default()
}

fn normalize_evidence_refs(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .map(|value| truncate_chars(redact_diagnostic_text(value.as_str()), MAX_EVIDENCE_REF_CHARS))
        .filter(|value| !value.trim().is_empty())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(MAX_EVIDENCE_REFS)
        .collect()
}

fn truncate_chars(value: String, maximum: usize) -> String {
    if value.chars().count() <= maximum {
        return value;
    }
    value.chars().take(maximum).collect()
}

fn canonical_sha256(value: &Value) -> Result<String, Status> {
    let canonical = canonicalize_json(value);
    let encoded = serde_json::to_vec(&canonical).map_err(|error| {
        Status::internal(format!("canonical evidence could not serialize: {error}"))
    })?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Object(object) => Value::Object(
            object
                .iter()
                .map(|(key, value)| (key.clone(), canonicalize_json(value)))
                .collect::<BTreeMap<_, _>>()
                .into_iter()
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => value.clone(),
    }
}

fn objective_guard_journal_status(error: crate::journal::JournalError) -> Status {
    Status::internal(format!("objective guard journal error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        application::run_stream::agent_loop::{
            AgentLoopTerminationReason, AgentLoopUsageSnapshot, FinalAnswerContract,
            FinalAnswerDecision, FinalAnswerEvidenceCoverage, FinalAnswerEvidenceSummary,
            FinalAnswerJournalProjection, FinalizationVerificationReport,
        },
        model_provider::{TerminalOutcomeClass, TerminalOutcomeClassification},
        objectives::{
            ObjectiveAutomationBinding, ObjectiveBudget, ObjectiveContract,
            ObjectiveFinalizationPolicy, ObjectiveKind, ObjectivePriority, ObjectiveState,
            ObjectiveSuccessCriteria, ObjectiveWorkspaceBinding,
        },
        routines::{
            shadow_manual_schedule_payload_json, RoutineApprovalPolicy, RoutineDeliveryConfig,
            RoutineExecutionConfig, RoutineTriggerKind,
        },
    };

    fn objective_with_required_evidence() -> ObjectiveRecord {
        ObjectiveRecord {
            objective_id: "objective".to_owned(),
            kind: ObjectiveKind::Objective,
            state: ObjectiveState::Active,
            name: "Objective".to_owned(),
            prompt: "Complete the objective.".to_owned(),
            owner_principal: "user:test".to_owned(),
            channel: None,
            priority: ObjectivePriority::Normal,
            budget: ObjectiveBudget::default(),
            current_focus: None,
            success_criteria: Some("verified".to_owned()),
            contract: ObjectiveContract {
                success_criteria: ObjectiveSuccessCriteria::default(),
                required_evidence: vec!["test:objective".to_owned()],
                finalization_policy: ObjectiveFinalizationPolicy::default(),
                ..ObjectiveContract::default()
            },
            contract_history: Vec::new(),
            exit_condition: None,
            next_recommended_step: None,
            standing_order: None,
            workspace: ObjectiveWorkspaceBinding::default(),
            automation: ObjectiveAutomationBinding {
                routine_id: None,
                enabled: true,
                trigger_kind: RoutineTriggerKind::Manual,
                schedule_type: "at".to_owned(),
                schedule_payload_json: shadow_manual_schedule_payload_json(),
                execution: RoutineExecutionConfig::default(),
                delivery: RoutineDeliveryConfig::default(),
                quiet_hours: None,
                cooldown_ms: 0,
                approval_policy: RoutineApprovalPolicy::default(),
                template_id: None,
            },
            last_attempt: None,
            attempt_history: Vec::new(),
            approach_history: Vec::new(),
            lifecycle_history: Vec::new(),
            linked_run_ids: Vec::new(),
            linked_artifact_paths: Vec::new(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            archived_at_unix_ms: None,
        }
    }

    fn finalization(
        status: FinalizationVerificationStatus,
        evidence_refs: Vec<String>,
        missing_artifacts_count: usize,
    ) -> AgentLoopFinalizationEnvelope {
        AgentLoopFinalizationEnvelope {
            schema_version: 1,
            termination_reason: AgentLoopTerminationReason::FinalAnswer,
            status: "completed".to_owned(),
            lifecycle_state: "done".to_owned(),
            reason_code: "agent_loop.final_answer".to_owned(),
            terminal_outcome: TerminalOutcomeClassification {
                class: TerminalOutcomeClass::VisibleText,
                reason_code: "provider.completed".to_owned(),
                requires_recovery: false,
                continues_tool_execution: false,
                visible_text_bytes: 4,
                tool_call_count: 0,
                finish_reason: None,
            },
            partial: false,
            continuation_required: false,
            user_visible_message: "done".to_owned(),
            usage: AgentLoopUsageSnapshot::default(),
            tool_count: 1,
            artifact_refs: Vec::new(),
            final_answer_contract: FinalAnswerContract {
                schema_version: 1,
                decision: FinalAnswerDecision::Accepted,
                reason_code: "accepted".to_owned(),
                final_answer_required: true,
                evidence_summary_required: true,
                tool_evidence_required_for_tool_claims: true,
                enforcement_mode: "enforced".to_owned(),
                journal_projection: FinalAnswerJournalProjection {
                    schema_version: 1,
                    event_type: "final_answer.accepted".to_owned(),
                    source_event_refs: Vec::new(),
                    redaction_level: "metadata_only".to_owned(),
                },
                event_types: Vec::new(),
                redaction_level: "metadata_only".to_owned(),
            },
            evidence_summary: FinalAnswerEvidenceSummary {
                schema_version: 1,
                run_id: "run".to_owned(),
                decision: FinalAnswerDecision::Accepted,
                coverage: FinalAnswerEvidenceCoverage::Satisfied,
                reason_code: "evidence.satisfied".to_owned(),
                tool_count: 1,
                produced_files_count: 0,
                missing_artifacts_count,
                active_process_count: 0,
                known_failed_attempt_count: 0,
                last_successful_tool: Some("test".to_owned()),
                evidence_refs,
                redaction_level: "metadata_only".to_owned(),
            },
            verification_finalizer: FinalizationVerificationReport {
                schema_version: 1,
                status,
                reason_code: "verification.test".to_owned(),
                enforcement_mode: "enforced".to_owned(),
                surface_policy: "objective".to_owned(),
                code_mutation_seen: true,
                pending_requirement_count: 0,
                satisfied_requirement_count: 1,
                pending_requirements: Vec::new(),
                evidence_refs: Vec::new(),
                event_type: None,
                nudge: None,
                unverified_reason: None,
                redaction_level: "metadata_only".to_owned(),
            },
            progress_checkpoint: None,
            provider_trace_ref: None,
        }
    }

    #[test]
    fn verified_done_with_evidence_is_accepted() {
        let objective = objective_with_required_evidence();
        let finalization =
            finalization(FinalizationVerificationStatus::Verified, vec!["test:ok".to_owned()], 0);

        assert_eq!(
            verification_status(
                ObjectiveContinuationDecision::Done,
                Some(&finalization),
                &objective
            ),
            ObjectiveVerificationStatus::Verified
        );
    }

    #[test]
    fn missing_artifact_rejects_done() {
        let objective = objective_with_required_evidence();
        let finalization =
            finalization(FinalizationVerificationStatus::Verified, vec!["test:ok".to_owned()], 1);

        assert_eq!(
            verification_status(
                ObjectiveContinuationDecision::Done,
                Some(&finalization),
                &objective
            ),
            ObjectiveVerificationStatus::MissingArtifacts
        );
    }

    #[test]
    fn missing_terminal_envelope_rejects_done() {
        let objective = objective_with_required_evidence();

        assert_eq!(
            verification_status(ObjectiveContinuationDecision::Done, None, &objective),
            ObjectiveVerificationStatus::Failed
        );
    }

    #[test]
    fn canonical_hash_is_independent_of_object_key_order() {
        let left = json!({"alpha": 1, "beta": {"x": 2, "y": 3}});
        let right = json!({"beta": {"y": 3, "x": 2}, "alpha": 1});

        assert_eq!(canonical_sha256(&left).unwrap(), canonical_sha256(&right).unwrap());
    }
}
