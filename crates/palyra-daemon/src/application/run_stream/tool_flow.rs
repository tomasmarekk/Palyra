use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{
    sync::mpsc,
    time::{interval, timeout, MissedTickBehavior},
};
use tonic::{Status, Streaming};
use tracing::info;

use crate::{
    application::approvals::{
        approval_subject_type_for_tool, build_pending_tool_approval,
        record_approval_requested_journal_event, record_approval_resolved_journal_event,
        resolve_cached_tool_approval_for_proposal,
    },
    application::tool_security::{
        evaluate_tool_proposal_security, record_tool_proposal_decision_audit_trail,
        resolve_tool_proposal_decision_for_context, ToolProposalSecurityEvaluation,
    },
    gateway::{
        await_tool_approval_response, best_effort_mark_approval_error,
        build_and_ingest_tool_result_memory_summary, execute_tool_with_runtime_dispatch,
        record_tool_execution_outcome_metrics, tool_cancellation_requires_execution_drain,
        GatewayRuntimeState, RunStreamToolExecutionOutcome, ToolApprovalOutcome,
        ToolRuntimeExecutionContext, TOOL_APPROVAL_RESPONSE_TIMEOUT,
    },
    journal::{ApprovalCreateRequest, ApprovalResolveRequest},
    orchestrator::RunStateMachine,
    tool_protocol::denied_execution_outcome,
    transport::grpc::auth::RequestContext,
};

use super::{
    cancellation::transition_run_stream_to_cancelled,
    tape::{
        send_tool_approval_request_with_tape, send_tool_approval_response_with_tape,
        send_tool_attestation_with_tape, send_tool_decision_with_tape,
        send_tool_proposal_with_tape, send_tool_result_with_tape,
    },
};

#[derive(Debug, Clone)]
pub(crate) struct RunStreamToolProposalPreparation {
    decision: crate::tool_protocol::ToolDecision,
    resolved_session_id: String,
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn process_run_stream_tool_proposal_event(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    stream: &mut Streaming<crate::transport::grpc::proto::palyra::common::v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    run_state: &mut RunStateMachine,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    let RunStreamToolProposalPreparation { decision, resolved_session_id } =
        prepare_run_stream_tool_proposal_execution(
            sender,
            stream,
            runtime_state,
            request_context,
            active_session_id,
            session_id,
            run_id,
            proposal_id,
            tool_name,
            input_json,
            remaining_tool_budget,
            tape_seq,
        )
        .await?;

    execute_run_stream_tool_proposal(
        sender,
        runtime_state,
        request_context,
        run_state,
        run_id,
        proposal_id,
        tool_name,
        input_json,
        &decision,
        resolved_session_id.as_str(),
        tape_seq,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn prepare_run_stream_tool_proposal_execution(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    stream: &mut Streaming<crate::transport::grpc::proto::palyra::common::v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    active_session_id: Option<&str>,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<RunStreamToolProposalPreparation, Status> {
    let resolved_session_id = active_session_id.ok_or_else(|| {
        Status::internal(
            "run stream internal invariant violated: missing session_id while preparing tool proposal",
        )
    })?;
    let ToolProposalSecurityEvaluation {
        skill_context,
        skill_gate_decision,
        approval_subject_id,
        proposal_approval_required,
        effective_posture,
    } = evaluate_tool_proposal_security(
        runtime_state,
        request_context,
        resolved_session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
    )
    .await;
    runtime_state.record_tool_proposal();
    send_tool_proposal_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        input_json,
        proposal_approval_required,
    )
    .await?;
    let approval_outcome = resolve_run_stream_tool_approval_outcome(
        sender,
        stream,
        runtime_state,
        request_context,
        session_id,
        run_id,
        proposal_id,
        tool_name,
        input_json,
        skill_context.as_ref(),
        approval_subject_id.as_str(),
        proposal_approval_required,
        tape_seq,
    )
    .await?;
    let decision = resolve_tool_proposal_decision_for_context(
        runtime_state,
        request_context,
        request_context.channel.as_deref(),
        session_id,
        run_id,
        tool_name,
        skill_context.as_ref(),
        remaining_tool_budget,
        skill_gate_decision,
        &effective_posture,
        approval_outcome.as_ref(),
    );
    send_tool_decision_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
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
        request_context,
        resolved_session_id,
        run_id,
        proposal_id,
        tool_name,
        skill_context.as_ref(),
        &decision,
    )
    .await?;
    Ok(RunStreamToolProposalPreparation {
        decision,
        resolved_session_id: resolved_session_id.to_owned(),
    })
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn resolve_run_stream_tool_approval_outcome(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    stream: &mut Streaming<crate::transport::grpc::proto::palyra::common::v1::RunStreamRequest>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    skill_context: Option<&crate::gateway::ToolSkillContext>,
    approval_subject_id: &str,
    proposal_approval_required: bool,
    tape_seq: &mut i64,
) -> Result<Option<ToolApprovalOutcome>, Status> {
    let cached_approval_outcome = resolve_cached_tool_approval_for_proposal(
        runtime_state,
        request_context,
        session_id,
        approval_subject_id,
        proposal_approval_required,
        run_id,
        proposal_id,
        "run stream",
    );
    if let Some(cached_outcome) = cached_approval_outcome {
        send_tool_approval_response_with_tape(
            sender,
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            cached_outcome.approval_id.as_str(),
            cached_outcome.approved,
            cached_outcome.reason.as_str(),
            cached_outcome.decision_scope,
            cached_outcome.decision_scope_ttl_ms,
        )
        .await?;
        return Ok(Some(cached_outcome));
    }
    if !proposal_approval_required {
        return Ok(None);
    }

    let pending_approval = build_pending_tool_approval(
        tool_name,
        skill_context,
        input_json,
        &runtime_state.config.tool_call,
    );
    runtime_state
        .create_approval_record(ApprovalCreateRequest {
            approval_id: pending_approval.approval_id.clone(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            principal: request_context.principal.clone(),
            device_id: request_context.device_id.clone(),
            channel: request_context.channel.clone(),
            subject_type: approval_subject_type_for_tool(tool_name),
            subject_id: pending_approval.prompt.subject_id.clone(),
            request_summary: pending_approval.request_summary.clone(),
            policy_snapshot: pending_approval.policy_snapshot.clone(),
            prompt: pending_approval.prompt.clone(),
        })
        .await?;
    info!(
        run_id = run_id,
        proposal_id = proposal_id,
        approval_id = %pending_approval.approval_id,
        subject_id = %pending_approval.prompt.subject_id,
        "approval requested"
    );

    if let Err(error) = send_tool_approval_request_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        pending_approval.approval_id.as_str(),
        tool_name,
        input_json,
        true,
        pending_approval.request_summary.as_str(),
        &pending_approval.prompt,
    )
    .await
    {
        best_effort_mark_approval_error(
            runtime_state,
            pending_approval.approval_id.as_str(),
            format!("approval_request_dispatch_error: {}", error.message()),
        )
        .await;
        return Err(error);
    }
    if let Err(error) = record_approval_requested_journal_event(
        runtime_state,
        request_context,
        session_id,
        run_id,
        proposal_id,
        pending_approval.approval_id.as_str(),
        tool_name,
        pending_approval.prompt.subject_id.as_str(),
        pending_approval.request_summary.as_str(),
        &pending_approval.policy_snapshot,
        &pending_approval.prompt,
    )
    .await
    {
        best_effort_mark_approval_error(
            runtime_state,
            pending_approval.approval_id.as_str(),
            format!("approval_request_journal_error: {}", error.message()),
        )
        .await;
        return Err(error);
    }

    let response = match timeout(
        TOOL_APPROVAL_RESPONSE_TIMEOUT,
        await_tool_approval_response(
            stream,
            session_id,
            run_id,
            proposal_id,
            pending_approval.approval_id.as_str(),
        ),
    )
    .await
    {
        Ok(Ok(value)) => value,
        Ok(Err(error)) => ToolApprovalOutcome {
            approval_id: pending_approval.approval_id.clone(),
            approved: false,
            reason: format!("approval_response_error: {}", error.message()),
            decision: crate::journal::ApprovalDecision::Error,
            decision_scope: crate::journal::ApprovalDecisionScope::Once,
            decision_scope_ttl_ms: None,
        },
        Err(_) => ToolApprovalOutcome {
            approval_id: pending_approval.approval_id.clone(),
            approved: false,
            reason: "approval_response_timeout".to_owned(),
            decision: crate::journal::ApprovalDecision::Timeout,
            decision_scope: crate::journal::ApprovalDecisionScope::Once,
            decision_scope_ttl_ms: None,
        },
    };

    let resolved = runtime_state
        .resolve_approval_record(ApprovalResolveRequest {
            approval_id: pending_approval.approval_id.clone(),
            decision: response.decision,
            decision_scope: response.decision_scope,
            decision_reason: response.reason.clone(),
            decision_scope_ttl_ms: response.decision_scope_ttl_ms,
        })
        .await?;
    info!(
        run_id = run_id,
        proposal_id = proposal_id,
        approval_id = %resolved.approval_id,
        decision = %response.decision.as_str(),
        decision_scope = %response.decision_scope.as_str(),
        "approval resolved"
    );

    record_approval_resolved_journal_event(
        runtime_state,
        request_context,
        session_id,
        run_id,
        Some(proposal_id),
        response.approval_id.as_str(),
        response.decision,
        response.decision_scope,
        response.decision_scope_ttl_ms,
        response.reason.as_str(),
    )
    .await?;

    send_tool_approval_response_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        response.approval_id.as_str(),
        response.approved,
        response.reason.as_str(),
        response.decision_scope,
        response.decision_scope_ttl_ms,
    )
    .await?;

    runtime_state.remember_tool_approval(
        request_context,
        session_id,
        approval_subject_id,
        &response,
    );
    Ok(Some(response))
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn execute_run_stream_tool_proposal(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    run_state: &mut RunStateMachine,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    decision: &crate::tool_protocol::ToolDecision,
    resolved_session_id: &str,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    let execution_outcome = if decision.allowed {
        runtime_state.record_tool_execution_attempt();
        let started_at = Instant::now();
        let mut cancel_poll = interval(Duration::from_millis(100));
        cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let must_drain_execution_after_cancel =
            tool_cancellation_requires_execution_drain(tool_name);
        let mut execution_future = Box::pin(execute_tool_with_runtime_dispatch(
            runtime_state,
            ToolRuntimeExecutionContext {
                principal: request_context.principal.as_str(),
                device_id: request_context.device_id.as_str(),
                channel: request_context.channel.as_deref(),
                session_id: resolved_session_id,
                run_id,
            },
            proposal_id,
            tool_name,
            input_json,
        ));
        let mut cancel_requested_during_execution = false;
        let outcome = loop {
            tokio::select! {
                result = &mut execution_future => {
                    break result;
                }
                _ = cancel_poll.tick() => {
                    match runtime_state.is_orchestrator_cancel_requested(run_id.to_owned()).await {
                        Ok(true) => {
                            if must_drain_execution_after_cancel {
                                cancel_requested_during_execution = true;
                                break execution_future.await;
                            }
                            transition_run_stream_to_cancelled(
                                sender,
                                runtime_state,
                                run_state,
                                run_id,
                                tape_seq,
                            )
                            .await?;
                            return Ok(RunStreamToolExecutionOutcome::Cancelled);
                        }
                        Ok(false) => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        };
        record_tool_execution_outcome_metrics(
            runtime_state,
            crate::gateway::ToolExecutionTraceContext {
                run_id,
                proposal_id,
                tool_name,
                execution_surface: "run_stream",
            },
            decision.allowed,
            started_at,
            &outcome,
        );
        if cancel_requested_during_execution {
            transition_run_stream_to_cancelled(sender, runtime_state, run_state, run_id, tape_seq)
                .await?;
            return Ok(RunStreamToolExecutionOutcome::Cancelled);
        }
        outcome
    } else {
        denied_execution_outcome(proposal_id, tool_name, input_json, decision.reason.as_str())
    };

    send_tool_result_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        execution_outcome.success,
        execution_outcome.output_json.as_slice(),
        execution_outcome.error.as_str(),
    )
    .await?;

    send_tool_attestation_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        execution_outcome.attestation.attestation_id.as_str(),
        execution_outcome.attestation.execution_sha256.as_str(),
        execution_outcome.attestation.executed_at_unix_ms,
        execution_outcome.attestation.timed_out,
        execution_outcome.attestation.executor.as_str(),
        execution_outcome.attestation.sandbox_enforcement.as_str(),
    )
    .await?;
    runtime_state.record_tool_attestation_emitted();

    let _ = build_and_ingest_tool_result_memory_summary(
        runtime_state,
        ToolRuntimeExecutionContext {
            principal: request_context.principal.as_str(),
            device_id: request_context.device_id.as_str(),
            channel: request_context.channel.as_deref(),
            session_id: resolved_session_id,
            run_id,
        },
        tool_name,
        decision.allowed,
        &execution_outcome,
        "run_stream_tool_result",
    )
    .await;
    Ok(RunStreamToolExecutionOutcome::Completed)
}
