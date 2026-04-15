use std::{sync::Arc, time::Instant};

use serde_json::json;
use tonic::Status;

use crate::{
    application::{
        route_message::approval::resolve_route_tool_approval_outcome,
        run_stream::tape::{append_tool_decision_tape_event, append_tool_proposal_tape_event},
        tool_security::{
            evaluate_tool_proposal_security, record_tool_proposal_decision_audit_trail,
            resolve_tool_proposal_decision_for_context, ToolProposalSecurityEvaluation,
        },
    },
    gateway::{
        build_and_ingest_tool_result_memory_summary, execute_tool_with_runtime_dispatch,
        record_tool_execution_outcome_metrics, GatewayRuntimeState, ToolExecutionTraceContext,
        ToolRuntimeExecutionContext,
    },
    journal::OrchestratorTapeAppendRequest,
    tool_protocol::denied_execution_outcome,
    transport::grpc::auth::RequestContext,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_route_tool_proposal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    route_request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<String, Status> {
    let ToolProposalSecurityEvaluation {
        skill_context,
        skill_gate_decision,
        approval_subject_id: _,
        proposal_approval_required,
        effective_posture,
    } = evaluate_tool_proposal_security(
        runtime_state,
        route_request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
    )
    .await;
    runtime_state.record_tool_proposal();
    append_tool_proposal_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        input_json,
        proposal_approval_required,
    )
    .await?;
    let pending_approval_id = resolve_route_tool_approval_outcome(
        runtime_state,
        route_request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
        skill_context.as_ref(),
        proposal_approval_required,
        tape_seq,
    )
    .await?;

    let mut decision = resolve_tool_proposal_decision_for_context(
        runtime_state,
        route_request_context,
        route_request_context.channel.as_deref(),
        session_id,
        run_id,
        tool_name,
        skill_context.as_ref(),
        remaining_tool_budget,
        skill_gate_decision,
        &effective_posture,
        None,
    );
    if let Some(approval_id) = pending_approval_id {
        if !decision.allowed && decision.approval_required {
            decision.reason = format!("approval required (pending approval_id={approval_id})");
        }
    }
    append_tool_decision_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "tool.decision",
        proposal_id,
        tool_name,
        decision.allowed,
        decision.reason.as_str(),
        decision.approval_required,
        decision.policy_enforced,
    )
    .await?;
    record_tool_proposal_decision_audit_trail(
        runtime_state,
        route_request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        skill_context.as_ref(),
        &decision,
    )
    .await?;

    let execution_outcome = if decision.allowed {
        runtime_state.record_tool_execution_attempt();
        let started_at = Instant::now();
        let outcome = execute_tool_with_runtime_dispatch(
            runtime_state,
            ToolRuntimeExecutionContext {
                principal: route_request_context.principal.as_str(),
                device_id: route_request_context.device_id.as_str(),
                channel: route_request_context.channel.as_deref(),
                session_id,
                run_id,
            },
            proposal_id,
            tool_name,
            input_json,
        )
        .await;
        record_tool_execution_outcome_metrics(
            runtime_state,
            ToolExecutionTraceContext {
                run_id,
                proposal_id,
                tool_name,
                execution_surface: "route_message",
            },
            decision.allowed,
            started_at,
            &outcome,
        );
        outcome
    } else {
        denied_execution_outcome(proposal_id, tool_name, input_json, decision.reason.as_str())
    };

    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool.executed".to_owned(),
            payload_json: json!({
                "proposal_id": proposal_id,
                "tool_name": tool_name,
                "success": execution_outcome.success,
                "error": execution_outcome.error.clone(),
                "attestation": {
                    "attestation_id": execution_outcome.attestation.attestation_id.clone(),
                    "execution_sha256": execution_outcome.attestation.execution_sha256.clone(),
                    "executed_at_unix_ms": execution_outcome.attestation.executed_at_unix_ms,
                    "timed_out": execution_outcome.attestation.timed_out,
                    "executor": execution_outcome.attestation.executor.clone(),
                    "sandbox_enforcement": execution_outcome.attestation.sandbox_enforcement.clone(),
                }
            })
            .to_string(),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);

    Ok(build_and_ingest_tool_result_memory_summary(
        runtime_state,
        ToolRuntimeExecutionContext {
            principal: route_request_context.principal.as_str(),
            device_id: route_request_context.device_id.as_str(),
            channel: route_request_context.channel.as_deref(),
            session_id,
            run_id,
        },
        tool_name,
        decision.allowed,
        &execution_outcome,
        "route_message_tool_result",
    )
    .await)
}
