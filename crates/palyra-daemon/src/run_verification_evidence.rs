//! Run-tape fallback evidence for public verification summaries.
//!
//! Formal verification projections remain authoritative when enabled. This
//! module supplies redacted, bounded observations of ordinary patch and
//! process tool activity so status surfaces never equate a disabled rollout
//! with a run that performed no mutations.

use std::{collections::BTreeMap, sync::Arc};

use palyra_common::process_runner_input::parse_process_runner_tool_input;
use serde_json::Value;

use crate::{
    application::verification::{
        VerificationCommandClassifier, VerificationObservedToolActivity,
        VerificationSummaryCommand, VerificationSummaryCommandClassification,
        VERIFICATION_OBSERVED_PATCH_MUTATION, VERIFICATION_OBSERVED_PROCESS_ATTEMPT,
        VERIFICATION_OBSERVED_TOOL_ACTIVITY, VERIFICATION_OBSERVED_TOOL_ACTIVITY_INCOMPLETE,
    },
    gateway::GatewayRuntimeState,
    journal::{OrchestratorRunStatusSnapshot, OrchestratorTapeRecord},
};

const RUN_VERIFICATION_TAPE_PAGE_LIMIT: usize = 256;
const RUN_VERIFICATION_TAPE_EVENT_LIMIT: usize = 4_096;

#[derive(Debug)]
pub(crate) struct RunVerificationTapeEvidence {
    pub(crate) finalizer: Option<Value>,
    pub(crate) observed_tool_activity: VerificationObservedToolActivity,
}

#[derive(Debug)]
struct ObservedToolProposal {
    tool_name: String,
    input_json: Value,
    seq: i64,
}

pub(crate) async fn collect_run_verification_tape_evidence(
    runtime: &Arc<GatewayRuntimeState>,
    snapshot: &OrchestratorRunStatusSnapshot,
) -> RunVerificationTapeEvidence {
    let mut records = Vec::new();
    let mut after_seq = None;
    let complete = loop {
        let remaining = RUN_VERIFICATION_TAPE_EVENT_LIMIT.saturating_sub(records.len());
        if remaining == 0 {
            break false;
        }
        let limit = remaining.min(RUN_VERIFICATION_TAPE_PAGE_LIMIT);
        let page = match runtime
            .orchestrator_tape_snapshot(snapshot.run_id.clone(), after_seq, Some(limit))
            .await
        {
            Ok(page) => page,
            Err(_) => break false,
        };
        let next_after_seq = page.next_after_seq;
        records.extend(page.events);
        match next_after_seq {
            None => break true,
            Some(next) if Some(next) == after_seq => break false,
            Some(next) => after_seq = Some(next),
        }
    };

    let mut evidence = run_verification_tape_evidence_from_records(records.as_slice(), complete);
    if evidence.finalizer.is_none() {
        evidence.finalizer = collect_run_verification_finalizer(runtime, snapshot).await;
    }
    evidence
}

fn run_verification_tape_evidence_from_records(
    records: &[OrchestratorTapeRecord],
    complete: bool,
) -> RunVerificationTapeEvidence {
    let mut proposals = BTreeMap::<String, ObservedToolProposal>::new();
    let mut result_payloads = Vec::<(i64, Value)>::new();
    let mut executed_at_by_proposal = BTreeMap::<String, i64>::new();
    let mut finalizer = None;

    for record in records {
        let Ok(payload) = serde_json::from_str::<Value>(record.payload_json.as_str()) else {
            continue;
        };
        if let Some(observed_finalizer) =
            payload.pointer("/finalization/verification_finalizer").cloned()
        {
            finalizer = Some(observed_finalizer);
        }
        match record.event_type.as_str() {
            "tool_proposal" => {
                let Some(proposal_id) =
                    payload.get("proposal_id").and_then(Value::as_str).map(str::to_owned)
                else {
                    continue;
                };
                let Some(tool_name) =
                    payload.get("tool_name").and_then(Value::as_str).map(str::to_owned)
                else {
                    continue;
                };
                let Some(input_json) = payload.get("input_json").cloned() else {
                    continue;
                };
                proposals.insert(
                    proposal_id,
                    ObservedToolProposal { tool_name, input_json, seq: record.seq },
                );
            }
            "tool_result" => result_payloads.push((record.seq, payload)),
            "tool_attestation" => {
                let Some(proposal_id) = payload.get("proposal_id").and_then(Value::as_str) else {
                    continue;
                };
                let Some(executed_at_unix_ms) =
                    payload.get("executed_at_unix_ms").and_then(Value::as_i64)
                else {
                    continue;
                };
                executed_at_by_proposal.insert(proposal_id.to_owned(), executed_at_unix_ms);
            }
            _ => {}
        }
    }

    let mut activity = VerificationObservedToolActivity {
        complete,
        ..VerificationObservedToolActivity::default()
    };
    for (result_seq, payload) in result_payloads {
        let Some(proposal_id) = payload.get("proposal_id").and_then(Value::as_str) else {
            continue;
        };
        let Some(proposal) = proposals.get(proposal_id) else {
            continue;
        };
        let evidence_refs =
            vec![format!("tool_proposal:{proposal_id}"), format!("tape:{result_seq}")];
        match proposal.tool_name.as_str() {
            crate::gateway::WORKSPACE_PATCH_TOOL_NAME => {
                collect_observed_patch_mutation(&payload, evidence_refs.as_slice(), &mut activity);
            }
            "palyra.process.run" => collect_observed_process_attempt(
                proposal_id,
                proposal,
                &payload,
                executed_at_by_proposal.get(proposal_id).copied().unwrap_or_default(),
                evidence_refs,
                &mut activity,
            ),
            _ => {}
        }
    }

    if !activity.changed_files.is_empty() || !activity.commands_executed.is_empty() {
        activity.reason_codes.push(VERIFICATION_OBSERVED_TOOL_ACTIVITY.to_owned());
    }
    if !activity.complete {
        activity.reason_codes.push(VERIFICATION_OBSERVED_TOOL_ACTIVITY_INCOMPLETE.to_owned());
    }
    activity.evidence_refs.sort();
    activity.evidence_refs.dedup();
    activity.reason_codes.sort();
    activity.reason_codes.dedup();

    RunVerificationTapeEvidence { finalizer, observed_tool_activity: activity }
}

fn collect_observed_patch_mutation(
    result_payload: &Value,
    evidence_refs: &[String],
    activity: &mut VerificationObservedToolActivity,
) {
    if result_payload.get("success").and_then(Value::as_bool) != Some(true) {
        return;
    }
    let Some(output) = projected_tool_result_output(result_payload) else {
        return;
    };
    if output.get("dry_run").and_then(Value::as_bool) == Some(true)
        || output.get("rollback_performed").and_then(Value::as_bool) == Some(true)
    {
        return;
    }
    let Some(files) = output.get("files_touched").and_then(Value::as_array) else {
        return;
    };
    let mut observed_mutation = false;
    for file in files {
        if file.get("operation").and_then(Value::as_str) == Some("no_op") {
            continue;
        }
        let Some(path) = file.get("path").and_then(Value::as_str) else {
            continue;
        };
        activity.changed_files.push(path.to_owned());
        observed_mutation = true;
    }
    if observed_mutation {
        activity.evidence_refs.extend(evidence_refs.iter().cloned());
        activity.reason_codes.push(VERIFICATION_OBSERVED_PATCH_MUTATION.to_owned());
    }
    if output.get("files_touched_truncated").and_then(Value::as_bool) == Some(true) {
        activity.complete = false;
    }
}

fn collect_observed_process_attempt(
    proposal_id: &str,
    proposal: &ObservedToolProposal,
    result_payload: &Value,
    created_at_unix_ms: i64,
    evidence_refs: Vec<String>,
    activity: &mut VerificationObservedToolActivity,
) {
    let Ok(input_json) = serde_json::to_vec(&proposal.input_json) else {
        return;
    };
    let Ok(input) = parse_process_runner_tool_input(input_json.as_slice()) else {
        return;
    };
    let classification = VerificationCommandClassifier::classify_process_run(&input);
    let output = projected_tool_result_output(result_payload);
    let exit_code = output
        .as_ref()
        .and_then(|value| value.get("exit_code"))
        .and_then(Value::as_i64)
        .and_then(|value| i32::try_from(value).ok());
    let timed_out =
        output.as_ref().and_then(|value| value.get("timed_out")).and_then(Value::as_bool)
            == Some(true)
            || result_payload.pointer("/diagnostic/error_kind").and_then(Value::as_str)
                == Some("timeout");
    let success = result_payload.get("success").and_then(Value::as_bool) == Some(true)
        && exit_code.unwrap_or_default() == 0;
    let status = if timed_out {
        "timed_out"
    } else if success {
        "passed"
    } else {
        "failed"
    };
    let mut reason_codes = classification.reason_codes.clone();
    reason_codes.push(VERIFICATION_OBSERVED_PROCESS_ATTEMPT.to_owned());
    reason_codes.sort();
    reason_codes.dedup();
    activity.commands_executed.push(VerificationSummaryCommand {
        command: classification.canonical_command.display.clone(),
        is_verification: classification.is_verification,
        kind: classification.kind.as_str().to_owned(),
        scope: classification.scope.as_str().to_owned(),
        status: Some(status.to_owned()),
        exit_code,
        created_at_unix_ms,
        evidence_refs: evidence_refs.clone(),
        reason_codes: reason_codes.clone(),
    });
    activity.command_classification.push(VerificationSummaryCommandClassification {
        command: classification.canonical_command.display,
        is_verification: classification.is_verification,
        kind: classification.kind.as_str().to_owned(),
        scope: classification.scope.as_str().to_owned(),
        created_at_unix_ms,
        reason_codes,
    });
    activity.evidence_refs.extend(evidence_refs);
    activity.evidence_refs.push(format!("tool_proposal:{proposal_id}"));
    activity.evidence_refs.push(format!("tape:{}", proposal.seq));
    activity.reason_codes.push(VERIFICATION_OBSERVED_PROCESS_ATTEMPT.to_owned());
}

fn projected_tool_result_output(result_payload: &Value) -> Option<Value> {
    let output = result_payload.get("output_json")?;
    let Some(summary) = output.get("summary") else {
        return Some(output.clone());
    };
    match summary {
        Value::String(encoded) => {
            serde_json::from_str::<Value>(encoded).ok().or_else(|| Some(output.clone()))
        }
        Value::Object(_) => Some(summary.clone()),
        _ => Some(output.clone()),
    }
}

async fn collect_run_verification_finalizer(
    runtime: &Arc<GatewayRuntimeState>,
    snapshot: &OrchestratorRunStatusSnapshot,
) -> Option<Value> {
    let after_seq = tail_tape_after_seq(snapshot.tape_events, RUN_VERIFICATION_TAPE_PAGE_LIMIT);
    let tape = runtime
        .orchestrator_tape_snapshot(
            snapshot.run_id.clone(),
            after_seq,
            Some(RUN_VERIFICATION_TAPE_PAGE_LIMIT),
        )
        .await
        .ok()?;
    tape.events.iter().rev().find_map(|event| {
        let payload = serde_json::from_str::<Value>(event.payload_json.as_str()).ok()?;
        payload.pointer("/finalization/verification_finalizer").cloned()
    })
}

fn tail_tape_after_seq(tape_events: u64, limit: usize) -> Option<i64> {
    let limit = u64::try_from(limit).unwrap_or(u64::MAX);
    (tape_events > limit).then(|| {
        i64::try_from(tape_events.saturating_sub(limit).saturating_sub(1)).unwrap_or(i64::MAX)
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn tape_fallback_reports_patch_and_process_activity() {
        let records = vec![
            OrchestratorTapeRecord {
                seq: 1,
                event_type: "tool_proposal".to_owned(),
                payload_json: json!({
                    "proposal_id": "patch-1",
                    "tool_name": "palyra.fs.apply_patch",
                    "input_json": {"patch": "<redacted>"},
                    "approval_required": false,
                })
                .to_string(),
            },
            OrchestratorTapeRecord {
                seq: 2,
                event_type: "tool_result".to_owned(),
                payload_json: json!({
                    "proposal_id": "patch-1",
                    "success": true,
                    "output_json": {
                        "summary": json!({
                            "dry_run": false,
                            "rollback_performed": false,
                            "files_touched": [
                                {
                                    "workspace_root_index": 0,
                                    "path": "src/lib.rs",
                                    "operation": "update"
                                },
                                {
                                    "workspace_root_index": 0,
                                    "path": "src/no-op.rs",
                                    "operation": "no_op"
                                }
                            ]
                        })
                        .to_string()
                    },
                    "error": "",
                })
                .to_string(),
            },
            OrchestratorTapeRecord {
                seq: 3,
                event_type: "tool_proposal".to_owned(),
                payload_json: json!({
                    "proposal_id": "process-1",
                    "tool_name": "palyra.process.run",
                    "input_json": {
                        "command": "npm",
                        "args": ["test", "--token", "must-not-leak"]
                    },
                    "approval_required": false,
                })
                .to_string(),
            },
            OrchestratorTapeRecord {
                seq: 4,
                event_type: "tool_result".to_owned(),
                payload_json: json!({
                    "proposal_id": "process-1",
                    "success": false,
                    "output_json": {
                        "summary": json!({
                            "success": false,
                            "exit_code": 1
                        })
                        .to_string()
                    },
                    "error": "sandbox denied execution",
                    "diagnostic": {"error_kind": "policy_denial"},
                })
                .to_string(),
            },
            OrchestratorTapeRecord {
                seq: 5,
                event_type: "tool_attestation".to_owned(),
                payload_json: json!({
                    "proposal_id": "process-1",
                    "executed_at_unix_ms": 1234,
                })
                .to_string(),
            },
            OrchestratorTapeRecord {
                seq: 6,
                event_type: "status".to_owned(),
                payload_json: json!({
                    "finalization": {
                        "verification_finalizer": {
                            "status": "not_required",
                            "reason_code": "verification.finalizer.no_code_mutation",
                            "pending_requirement_count": 0,
                            "satisfied_requirement_count": 0,
                            "evidence_refs": []
                        }
                    }
                })
                .to_string(),
            },
        ];

        let evidence = run_verification_tape_evidence_from_records(records.as_slice(), true);
        assert_eq!(evidence.observed_tool_activity.changed_files, vec!["src/lib.rs"]);
        assert_eq!(evidence.observed_tool_activity.commands_executed.len(), 1);
        assert_eq!(
            evidence.observed_tool_activity.commands_executed[0].status.as_deref(),
            Some("failed")
        );
        assert!(evidence.observed_tool_activity.commands_executed[0]
            .command
            .contains("<redacted>"));
        assert!(!evidence.observed_tool_activity.commands_executed[0]
            .command
            .contains("must-not-leak"));

        let summary = crate::application::verification::verification_summary_for_public_artifact(
            crate::application::verification::VerificationSummaryRequest {
                rollout_enabled: false,
                journal_total_events: 0,
                journal_window_events: 0,
                projections: &[],
                diagnostics: &[],
                finalizer: evidence.finalizer.as_ref(),
                observed_tool_activity: Some(&evidence.observed_tool_activity),
            },
        );
        assert_eq!(summary.state, "disabled");
        assert_eq!(summary.changed_files, vec!["src/lib.rs"]);
        assert_eq!(summary.commands_executed.len(), 1);
        assert_eq!(summary.final_answer.status.as_deref(), Some("unverified_allowed"));
        assert_eq!(
            summary.final_answer.reason_code.as_deref(),
            Some("verification.finalizer.rollout_disabled_with_observed_mutation")
        );
        assert!(summary.final_answer_allowed);
    }
}
