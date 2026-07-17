use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{json, Value};
use ulid::Ulid;

use crate::{
    application::{
        plan_state::{
            AgentPlanCreateCommand, AgentPlanQuery, AgentPlanStatus, AgentPlanStore,
            AgentPlanUpdateCommand, AGENT_PLAN_BLOCKED_EVENT, AGENT_PLAN_COMPLETED_EVENT,
            AGENT_PLAN_CREATED_EVENT,
        },
        run_stream::agent_loop::{
            AgentLoopTerminationReason, AgentRunLoopState, FinalAnswerDecision,
            FinalAnswerEvidenceCoverage, FINAL_ANSWER_CONTRACT_COMPLETED_EVENT,
            FINAL_ANSWER_CONTRACT_FAILED_EVENT,
        },
    },
    journal::{
        JournalConfig, JournalStore, OrchestratorRunStartRequest, OrchestratorSessionUpsertRequest,
    },
    model_provider::{
        ProviderFinishReason, ProviderMessage, ProviderOutputContentPart, ProviderRawProviderRefs,
        ProviderTurnOutput, ProviderUsage,
    },
    objective_judge::{materialize_objective_judge_result, ObjectiveJudgeInput},
    objectives::{
        ObjectiveAutomationBinding, ObjectiveBudget, ObjectiveContract, ObjectiveFinalizationMode,
        ObjectiveFinalizationPolicy, ObjectiveKind, ObjectivePriority, ObjectiveRecord,
        ObjectiveRegistry, ObjectiveState, ObjectiveSuccessCriteria, ObjectiveSuccessCriterion,
        ObjectiveUpsert, ObjectiveWorkspaceBinding, OBJECTIVE_CONTRACT_CREATED_EVENT,
    },
    routines::{
        shadow_manual_schedule_payload_json, RoutineApprovalPolicy, RoutineDeliveryConfig,
        RoutineExecutionConfig, RoutineTriggerKind,
    },
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

#[test]
fn completed_plan_objective_judge_and_final_answer_contract_align_on_evidence() {
    let journal = test_journal_store();
    let plan_store = AgentPlanStore::new(&journal);
    let objective_id = Ulid::new().to_string();
    let run_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    start_test_run(&journal, &session_id, &run_id);
    let plan = create_plan_item(&plan_store, &session_id, &run_id, &objective_id);
    let completed = plan_store
        .update_item(AgentPlanUpdateCommand {
            plan_item_id: plan.plan_item_id.clone(),
            expected_status: Some(AgentPlanStatus::InProgress),
            status: Some(AgentPlanStatus::Completed),
            evidence_refs: Some(json!(["cargo:test", "tape:run:agent_loop.terminated"])),
            reason_code: "planning_objective_regression_completed".to_owned(),
            actor_principal: "user:ops".to_owned(),
            summary: "Plan item completed with test and final-answer evidence.".to_owned(),
            payload: json!({"regression": "objective_loop_completed"}),
            ..AgentPlanUpdateCommand::default()
        })
        .expect("plan item should complete");

    let plan_events = plan_store
        .list_events(completed.plan_item_id.as_str(), 16)
        .expect("plan events should list");
    assert_eq!(completed.status, AgentPlanStatus::Completed);
    assert!(plan_events.iter().any(|event| event.event_type == AGENT_PLAN_CREATED_EVENT));
    assert!(plan_events.iter().any(|event| event.event_type == AGENT_PLAN_COMPLETED_EVENT));
    assert_eq!(
        plan_store
            .list_items(&AgentPlanQuery {
                owner_principal: Some("user:ops".to_owned()),
                device_id: Some("device".to_owned()),
                channel: Some("cli".to_owned()),
                session_id: Some(session_id.clone()),
                run_id: Some(run_id.clone()),
                status: Some(AgentPlanStatus::Completed),
                include_terminal: true,
                limit: 10,
            })
            .expect("completed plan query should succeed")
            .len(),
        1
    );

    let registry = ObjectiveRegistry::open(test_state_root().as_path())
        .expect("objective registry should open");
    let objective = registry
        .upsert_objective(ObjectiveUpsert {
            record: objective_record(
                &objective_id,
                "Run the planning/objective regression tests.",
                vec!["cargo:test", "tape:run:agent_loop.terminated"],
            ),
        })
        .expect("objective should persist");
    assert_eq!(objective.contract_history.len(), 1);
    assert_eq!(objective.contract_history[0].event_type, OBJECTIVE_CONTRACT_CREATED_EVENT);

    let completed_evidence_refs = string_array(&completed.evidence_refs);
    let judge_input = ObjectiveJudgeInput::from_objective(
        &objective,
        Some("All regression checks passed.".to_owned()),
        completed_evidence_refs,
    );
    let judge_payload = serde_json::to_string(&judge_input).expect("judge input should serialize");
    let judge_output = json!({
        "status": "done",
        "summary": "Required evidence is present.",
        "confidence_bps": 9800,
        "evidence_refs": [],
        "missing_evidence": [],
        "reason_code": "planning_objective_regression_done"
    })
    .to_string();
    let materialized =
        materialize_objective_judge_result(Some(judge_payload.as_str()), &judge_output, json!({}));
    assert!(!materialized.parse_failed);
    assert_eq!(materialized.result_json["event_type"], "objective.judge.completed");
    assert_eq!(materialized.result_json["objective_judge"]["status"], "done");

    let mut loop_state = AgentRunLoopState::new(
        vec![ProviderMessage::user_text("Create final_report.txt.".to_owned())],
        2,
        4,
        10_000,
    );
    append_workspace_patch_evidence(&mut loop_state, "call-final-report", "final_report.txt");
    let finalization = loop_state.finalization_envelope(
        &run_id,
        AgentLoopTerminationReason::FinalAnswer,
        "Created final_report.txt and validated the objective evidence.",
        None,
    );

    assert_eq!(finalization.final_answer_contract.decision, FinalAnswerDecision::Accepted);
    assert_eq!(
        finalization.final_answer_contract.journal_projection.event_type,
        FINAL_ANSWER_CONTRACT_COMPLETED_EVENT
    );
    assert_eq!(finalization.evidence_summary.coverage, FinalAnswerEvidenceCoverage::Satisfied);
    assert_eq!(finalization.evidence_summary.tool_count, 1);
    assert!(finalization
        .evidence_summary
        .evidence_refs
        .iter()
        .any(|reference| reference == "file:final_report.txt"));
}

#[test]
fn blocked_plan_and_missing_evidence_keep_objective_loop_open() {
    let journal = test_journal_store();
    let plan_store = AgentPlanStore::new(&journal);
    let objective_id = Ulid::new().to_string();
    let run_id = Ulid::new().to_string();
    let session_id = Ulid::new().to_string();
    start_test_run(&journal, &session_id, &run_id);
    let plan = create_plan_item(&plan_store, &session_id, &run_id, &objective_id);
    let blocked = plan_store
        .update_item(AgentPlanUpdateCommand {
            plan_item_id: plan.plan_item_id.clone(),
            expected_status: Some(AgentPlanStatus::InProgress),
            status: Some(AgentPlanStatus::Blocked),
            blocked_reason: Some(Some("waiting for cargo test evidence".to_owned())),
            evidence_refs: Some(json!(["blocker:missing_cargo_test"])),
            reason_code: "planning_objective_regression_blocked".to_owned(),
            actor_principal: "user:ops".to_owned(),
            summary: "Plan item blocked on missing test evidence.".to_owned(),
            payload: json!({"regression": "objective_loop_blocked"}),
            ..AgentPlanUpdateCommand::default()
        })
        .expect("plan item should become blocked");
    let plan_events = plan_store
        .list_events(blocked.plan_item_id.as_str(), 16)
        .expect("blocked plan events should list");
    assert_eq!(blocked.status, AgentPlanStatus::Blocked);
    assert!(plan_events.iter().any(|event| event.event_type == AGENT_PLAN_BLOCKED_EVENT));

    let registry = ObjectiveRegistry::open(test_state_root().as_path())
        .expect("objective registry should open");
    let objective = registry
        .upsert_objective(ObjectiveUpsert {
            record: objective_record(
                &objective_id,
                "Keep the objective open until tests pass.",
                vec!["cargo:test"],
            ),
        })
        .expect("objective should persist");
    let judge_input = ObjectiveJudgeInput::from_objective(
        &objective,
        Some("Looks done.".to_owned()),
        string_array(&blocked.evidence_refs),
    );
    let judge_payload = serde_json::to_string(&judge_input).expect("judge input should serialize");
    let premature_done = json!({
        "status": "done",
        "summary": "Looks done.",
        "confidence_bps": 9100,
        "evidence_refs": [],
        "missing_evidence": [],
        "reason_code": "planning_objective_regression_premature_done"
    })
    .to_string();
    let materialized = materialize_objective_judge_result(
        Some(judge_payload.as_str()),
        &premature_done,
        json!({}),
    );
    assert!(!materialized.parse_failed);
    assert_eq!(materialized.result_json["objective_judge"]["status"], "not_done");
    assert_eq!(
        materialized.result_json["objective_judge"]["reason_code"],
        "objective_judge_missing_required_evidence"
    );
    assert_eq!(materialized.result_json["objective_judge"]["missing_evidence"][0], "cargo:test");

    let loop_state = AgentRunLoopState::new(
        vec![ProviderMessage::user_text("Finish it.".to_owned())],
        2,
        4,
        10_000,
    );
    let finalization = loop_state.finalization_envelope(
        &run_id,
        AgentLoopTerminationReason::IncompleteFinalAnswer,
        "model returned no usable answer before any tool evidence",
        None,
    );

    assert_eq!(finalization.final_answer_contract.decision, FinalAnswerDecision::Rejected);
    assert_eq!(
        finalization.final_answer_contract.journal_projection.event_type,
        FINAL_ANSWER_CONTRACT_FAILED_EVENT
    );
    assert_eq!(finalization.evidence_summary.coverage, FinalAnswerEvidenceCoverage::NoToolEvidence);
}

fn create_plan_item(
    store: &AgentPlanStore<'_>,
    session_id: &str,
    run_id: &str,
    objective_id: &str,
) -> crate::application::plan_state::AgentPlanItem {
    store
        .create_item(AgentPlanCreateCommand {
            plan_item_id: Some(Ulid::new().to_string()),
            session_id: session_id.to_owned(),
            run_id: Some(run_id.to_owned()),
            parent_run_id: None,
            owner_principal: "user:ops".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("cli".to_owned()),
            title: "Run objective regression checks".to_owned(),
            details: json!({"objective_id": objective_id}),
            status: AgentPlanStatus::InProgress,
            priority: 10,
            blocked_reason: None,
            evidence_refs: json!([]),
            reason_code: "planning_objective_regression_created".to_owned(),
            actor_principal: "user:ops".to_owned(),
            payload: json!({"regression": "objective_loop"}),
        })
        .expect("plan item should be created")
}

fn start_test_run(journal: &JournalStore, session_id: &str, run_id: &str) {
    journal
        .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
            session_id: session_id.to_owned(),
            session_key: session_id.to_owned(),
            session_label: Some("Planning Objective Regression".to_owned()),
            principal: "user:ops".to_owned(),
            device_id: "device".to_owned(),
            channel: Some("cli".to_owned()),
        })
        .expect("orchestrator session should be created");
    journal
        .start_orchestrator_run(&OrchestratorRunStartRequest {
            run_id: run_id.to_owned(),
            session_id: session_id.to_owned(),
            origin_kind: "foreground".to_owned(),
            origin_run_id: None,
            triggered_by_principal: Some("user:ops".to_owned()),
            parameter_delta_json: None,

            delegated_admission: None,
        })
        .expect("orchestrator run should be created");
}

fn objective_record(
    objective_id: &str,
    prompt: &str,
    required_evidence: Vec<&str>,
) -> ObjectiveRecord {
    let required_evidence = required_evidence.into_iter().map(str::to_owned).collect::<Vec<_>>();
    ObjectiveRecord {
        objective_id: objective_id.to_owned(),
        kind: ObjectiveKind::Objective,
        state: ObjectiveState::Active,
        name: "Planning objective regression".to_owned(),
        prompt: prompt.to_owned(),
        owner_principal: "user:ops".to_owned(),
        channel: Some("cli".to_owned()),
        priority: ObjectivePriority::High,
        budget: ObjectiveBudget::default(),
        current_focus: Some(
            "Verify plan state, objective judge, and final answer contract.".to_owned(),
        ),
        success_criteria: Some("Required evidence is present before finalization.".to_owned()),
        contract: ObjectiveContract {
            success_criteria: ObjectiveSuccessCriteria {
                items: vec![ObjectiveSuccessCriterion {
                    description: "Required regression evidence is present.".to_owned(),
                    required: true,
                    evidence_refs: required_evidence.clone(),
                }],
            },
            required_evidence,
            finalization_policy: ObjectiveFinalizationPolicy {
                mode: ObjectiveFinalizationMode::ManualReview,
                ..ObjectiveFinalizationPolicy::default()
            },
            reason_code: "planning_objective_regression_contract".to_owned(),
            ..ObjectiveContract::default()
        },
        contract_history: Vec::new(),
        exit_condition: None,
        next_recommended_step: None,
        standing_order: None,
        workspace: ObjectiveWorkspaceBinding {
            workspace_document_path: "objectives/regression.md".to_owned(),
            session_key: Some("session:regression".to_owned()),
            session_label: Some("Regression".to_owned()),
            related_document_paths: Vec::new(),
            related_memory_ids: Vec::new(),
            related_session_ids: Vec::new(),
        },
        automation: ObjectiveAutomationBinding {
            routine_id: None,
            enabled: false,
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

fn append_workspace_patch_evidence(state: &mut AgentRunLoopState, proposal_id: &str, path: &str) {
    state.append_assistant_turn(&ProviderTurnOutput {
        full_text: String::new(),
        content_parts: vec![ProviderOutputContentPart::ToolCall {
            proposal_id: proposal_id.to_owned(),
            tool_name: "palyra.fs.apply_patch".to_owned(),
            input_json: json!({
                "patch": format!(
                    "*** Begin Patch\n*** Add File: {path}\n+regression evidence\n*** End Patch"
                )
            }),
        }],
        finish_reason: ProviderFinishReason::ToolCalls,
        usage: ProviderUsage::new(0, 0, "test"),
        raw_provider_refs: ProviderRawProviderRefs::default(),
        redaction_state: Default::default(),
    });
    state.append_tool_result_messages(vec![ProviderMessage::tool_result(
        proposal_id,
        json!({
            "patch_sha256": "abc",
            "dry_run": false,
            "files_touched": [{
                "path": path,
                "workspace_root_index": 0,
                "operation": "create",
                "after_sha256": "sha",
                "after_size_bytes": 42
            }],
            "rollback_performed": false,
            "redacted_preview": ""
        })
        .to_string(),
    )]);
}

fn test_journal_store() -> JournalStore {
    JournalStore::open(JournalConfig {
        db_path: unique_temp_path("palyra-planning-objective-regression", "sqlite3"),
        hash_chain_enabled: false,
        max_payload_bytes: 256 * 1024,
        max_events: 10_000,
    })
    .expect("journal store should open")
}

fn test_state_root() -> PathBuf {
    let path = unique_temp_path("palyra-planning-objective-regression-state", "dir");
    fs::create_dir_all(&path).expect("state root should be created");
    path
}

fn unique_temp_path(prefix: &str, extension: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("{prefix}-{nonce}-{}-{counter}.{extension}", std::process::id()))
}

fn string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("evidence refs should be an array")
        .iter()
        .map(|entry| entry.as_str().expect("evidence ref should be a string").to_owned())
        .collect()
}
