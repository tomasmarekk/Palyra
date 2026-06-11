//! Inline tool execution for the route-message surface.
//!
//! Mirrors the run-stream tool flow (catalog validation, security
//! evaluation, approval gate, policy decision, runtime dispatch) but runs
//! synchronously inside the single routed exchange and returns a one-line
//! summary string that is folded back into the model conversation instead
//! of streamed wire events. Every stage still lands on the orchestrator
//! tape so routed runs replay like streamed ones.

use std::{sync::Arc, time::Instant};

use serde_json::json;
use tonic::Status;

use crate::{
    application::{
        execution_gate::ToolProposalApprovalState,
        route_message::approval::resolve_route_tool_approval_outcome,
        run_stream::tape::{append_tool_decision_tape_event, append_tool_proposal_tape_event},
        tool_registry::{
            normalization_audit_tape_payload, rejection_tape_payload, tool_call_rejection_outcome,
            validate_tool_call_against_catalog_snapshot, ModelVisibleToolCatalogSnapshot,
            NormalizedToolCall, ToolArgumentNormalizationAudit, ToolCallRejection,
        },
        tool_security::{
            evaluate_tool_proposal_security, record_tool_proposal_decision_audit_trail,
            resolve_tool_proposal_decision_for_context, ResolvedToolProposalDecision,
            ToolProposalSecurityEvaluation,
        },
    },
    gateway::{
        build_and_ingest_tool_result_memory_summary, execute_tool_with_runtime_dispatch,
        record_tool_execution_outcome_metrics, shared_tool_budget, shared_tool_budget_remaining,
        GatewayRuntimeState, ToolExecutionTraceContext, ToolRuntimeExecutionContext,
    },
    journal::OrchestratorTapeAppendRequest,
    tool_protocol::{denied_execution_outcome, ToolExecutionOutcome},
    transport::grpc::auth::RequestContext,
};

/// Validates, gates, and (when allowed) executes one tool proposal from the
/// routed provider turn, returning the textual tool-result summary fed back
/// to the model.
///
/// Approval-required proposals are recorded as pending and denied for this
/// run (there is no interactive client to answer the prompt); denied
/// proposals still produce a denial outcome on the tape so replays stay
/// complete.
///
/// # Errors
/// Returns a status when a tape append, approval recording, or the decision
/// audit trail fails; tool execution failures are reported inside the
/// returned summary, not as errors.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_route_tool_proposal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    route_request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<String, Status> {
    let NormalizedToolCall { input_json: normalized_input_json, audit } =
        match validate_tool_call_against_catalog_snapshot(
            tool_catalog_snapshot,
            tool_name,
            input_json,
        ) {
            Ok(normalized) => normalized,
            Err(rejection) => {
                // Rejected calls still consume budget so a model emitting
                // invalid calls cannot loop on free attempts forever.
                *remaining_tool_budget = (*remaining_tool_budget).saturating_sub(1);
                return reject_route_tool_call(
                    runtime_state,
                    run_id,
                    proposal_id,
                    tool_name,
                    input_json,
                    rejection,
                    tape_seq,
                )
                .await;
            }
        };
    if !audit.steps.is_empty() {
        append_route_tool_argument_normalization_tape_event(
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            tool_name,
            &audit,
        )
        .await?;
    }
    let input_json = normalized_input_json.as_slice();

    let ToolProposalSecurityEvaluation {
        skill_context,
        skill_gate_decision,
        approval_subject_id: _,
        proposal_approval_required,
        effective_posture,
        backend_selection,
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
        &backend_selection,
        tape_seq,
    )
    .await?;

    let ResolvedToolProposalDecision { decision, gate_report } =
        resolve_tool_proposal_decision_for_context(
            runtime_state,
            route_request_context,
            route_request_context.channel.as_deref(),
            session_id,
            run_id,
            tool_name,
            skill_context.as_ref(),
            remaining_tool_budget,
            skill_gate_decision,
            proposal_approval_required,
            &effective_posture,
            &backend_selection,
            ToolProposalApprovalState {
                outcome: None,
                pending_approval_id: pending_approval_id.as_deref(),
            },
        );
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
        gate_report.as_ref(),
    )
    .await?;

    let execution_outcome = if decision.allowed {
        runtime_state.record_tool_execution_attempt();
        let started_at = Instant::now();
        // Dispatch may spawn nested tool calls (e.g. delegation), so the
        // budget travels as a shared counter and is read back afterwards to
        // charge everything the dispatch consumed.
        let nested_tool_budget = shared_tool_budget(*remaining_tool_budget);
        let outcome = execute_tool_with_runtime_dispatch(
            runtime_state,
            ToolRuntimeExecutionContext {
                principal: route_request_context.principal.as_str(),
                device_id: route_request_context.device_id.as_str(),
                channel: route_request_context.channel.as_deref(),
                session_id,
                run_id,
                execution_backend: backend_selection.resolution.resolved,
                backend_reason_code: backend_selection.resolution.reason_code.as_str(),
            },
            proposal_id,
            tool_name,
            input_json,
            Some(nested_tool_budget.clone()),
        )
        .await;
        *remaining_tool_budget = shared_tool_budget_remaining(&nested_tool_budget);
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
            execution_backend: backend_selection.resolution.resolved,
            backend_reason_code: backend_selection.resolution.reason_code.as_str(),
        },
        tool_name,
        decision.allowed,
        &execution_outcome,
        "route_message_tool_result",
    )
    .await)
}

/// Records the argument-normalization audit on the tape so replays can see
/// how the model's raw arguments were rewritten before validation.
#[allow(clippy::result_large_err)]
async fn append_route_tool_argument_normalization_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    audit: &ToolArgumentNormalizationAudit,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool.arguments.normalized".to_owned(),
            payload_json: normalization_audit_tape_payload(proposal_id, tool_name, audit),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);
    Ok(())
}

/// Records the full proposal/decision/rejection/executed tape sequence for a
/// call that failed catalog validation, so a rejected call replays with the
/// same event shape as an executed one, and returns its summary line.
#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn reject_route_tool_call(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    rejection: ToolCallRejection,
    tape_seq: &mut i64,
) -> Result<String, Status> {
    runtime_state.record_tool_proposal();
    append_tool_proposal_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        input_json,
        false,
    )
    .await?;
    let reason = format!("{}: {}", rejection.kind.as_str(), rejection.message);
    append_tool_decision_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "tool.decision",
        proposal_id,
        tool_name,
        false,
        reason.as_str(),
        false,
        true,
    )
    .await?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool.intake_rejected".to_owned(),
            payload_json: rejection_tape_payload(proposal_id, &rejection),
        })
        .await?;
    *tape_seq = (*tape_seq).saturating_add(1);

    let execution_outcome = tool_call_rejection_outcome(proposal_id, input_json, &rejection);
    append_route_tool_execution_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        &execution_outcome,
    )
    .await?;
    Ok(format!("tool={tool_name} success=false error={reason}"))
}

/// Appends a `tool.executed` tape event carrying the outcome's attestation
/// for an intake-rejected call.
#[allow(clippy::result_large_err)]
async fn append_route_tool_execution_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    execution_outcome: &ToolExecutionOutcome,
) -> Result<(), Status> {
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
    Ok(())
}
