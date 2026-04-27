use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use palyra_common::{
    redaction::{is_sensitive_key, redact_auth_error, redact_url_segments_in_text, REDACTED},
    runtime_contracts::{
        ArtifactRetentionPolicy, ToolResultSensitivity, ToolResultVisibility, ToolTurnBudget,
    },
};
use serde_json::{json, Map, Value};
use tokio::{
    sync::mpsc,
    time::{interval, timeout, MissedTickBehavior},
};
use tonic::{Status, Streaming};
use tracing::info;
use ulid::Ulid;

use crate::{
    application::approvals::{
        approval_subject_type_for_tool, build_pending_tool_approval,
        record_approval_requested_journal_event, record_approval_resolved_journal_event,
        resolve_cached_tool_approval_for_proposal,
    },
    application::execution_gate::ToolProposalApprovalState,
    application::tool_registry::{
        normalization_audit_tape_payload, projection_policy_for_tool, rejection_tape_payload,
        tool_call_rejection_outcome, validate_tool_call_against_catalog_snapshot,
        ModelVisibleToolCatalogSnapshot, NormalizedToolCall, ToolArgumentNormalizationAudit,
        ToolCallRejection, ToolResultProjectionPolicy,
    },
    application::tool_security::{
        approval_execution_context_for_backend_selection, evaluate_tool_proposal_security,
        record_tool_proposal_decision_audit_trail, resolve_tool_proposal_decision_for_context,
        ResolvedToolProposalDecision, ToolProposalBackendSelection, ToolProposalSecurityEvaluation,
    },
    gateway::{
        await_tool_approval_response, best_effort_mark_approval_error,
        build_and_ingest_tool_result_memory_summary, execute_tool_with_runtime_dispatch,
        record_tool_execution_outcome_metrics, tool_cancellation_requires_execution_drain,
        GatewayRuntimeState, RunStreamToolExecutionOutcome, ToolApprovalOutcome,
        ToolRuntimeExecutionContext, TOOL_APPROVAL_RESPONSE_TIMEOUT,
    },
    journal::{
        ApprovalCreateRequest, ApprovalResolveRequest, OrchestratorTapeAppendRequest,
        ToolResultArtifactCreateRequest,
    },
    orchestrator::RunStateMachine,
    tool_protocol::{denied_execution_outcome, ToolExecutionOutcome},
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
    backend_selection: ToolProposalBackendSelection,
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
    tool_catalog_snapshot: &ModelVisibleToolCatalogSnapshot,
    remaining_tool_budget: &mut u32,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    let NormalizedToolCall { input_json: normalized_input_json, audit } =
        match validate_tool_call_against_catalog_snapshot(
            tool_catalog_snapshot,
            tool_name,
            input_json,
        ) {
            Ok(normalized) => normalized,
            Err(rejection) => {
                *remaining_tool_budget = (*remaining_tool_budget).saturating_sub(1);
                return reject_run_stream_tool_call(
                    sender,
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
        append_tool_argument_normalization_tape_event(
            runtime_state,
            run_id,
            tape_seq,
            proposal_id,
            tool_name,
            &audit,
        )
        .await?;
    }

    let RunStreamToolProposalPreparation { decision, resolved_session_id, backend_selection } =
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
            normalized_input_json.as_slice(),
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
        normalized_input_json.as_slice(),
        &decision,
        &backend_selection,
        resolved_session_id.as_str(),
        tape_seq,
    )
    .await
}

#[allow(clippy::result_large_err)]
async fn append_tool_argument_normalization_tape_event(
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

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn reject_run_stream_tool_call(
    sender: &mpsc::Sender<
        Result<crate::transport::grpc::proto::palyra::common::v1::RunStreamEvent, Status>,
    >,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    rejection: ToolCallRejection,
    tape_seq: &mut i64,
) -> Result<RunStreamToolExecutionOutcome, Status> {
    runtime_state.record_tool_proposal();
    send_tool_proposal_with_tape(
        sender,
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
    send_tool_decision_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
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
    send_tool_result_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        false,
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
    Ok(RunStreamToolExecutionOutcome::Completed {
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        outcome: execution_outcome,
    })
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
        backend_selection,
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
        &backend_selection,
        tape_seq,
    )
    .await?;
    let ResolvedToolProposalDecision { decision, gate_report } =
        resolve_tool_proposal_decision_for_context(
            runtime_state,
            request_context,
            request_context.channel.as_deref(),
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
                outcome: approval_outcome.as_ref(),
                pending_approval_id: None,
            },
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
        gate_report.as_ref(),
    )
    .await?;
    Ok(RunStreamToolProposalPreparation {
        decision,
        resolved_session_id: resolved_session_id.to_owned(),
        backend_selection,
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
    backend_selection: &ToolProposalBackendSelection,
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
        approval_execution_context_for_backend_selection(backend_selection).as_ref(),
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
    backend_selection: &ToolProposalBackendSelection,
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
                execution_backend: backend_selection.resolution.resolved,
                backend_reason_code: backend_selection.resolution.reason_code.as_str(),
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
    let execution_outcome = project_tool_result_for_model(
        runtime_state,
        ToolRuntimeExecutionContext {
            principal: request_context.principal.as_str(),
            device_id: request_context.device_id.as_str(),
            channel: request_context.channel.as_deref(),
            session_id: resolved_session_id,
            run_id,
            execution_backend: backend_selection.resolution.resolved,
            backend_reason_code: backend_selection.resolution.reason_code.as_str(),
        },
        proposal_id,
        tool_name,
        execution_outcome,
    )
    .await?;

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
            execution_backend: backend_selection.resolution.resolved,
            backend_reason_code: backend_selection.resolution.reason_code.as_str(),
        },
        tool_name,
        decision.allowed,
        &execution_outcome,
        "run_stream_tool_result",
    )
    .await;
    Ok(RunStreamToolExecutionOutcome::Completed {
        proposal_id: proposal_id.to_owned(),
        tool_name: tool_name.to_owned(),
        outcome: execution_outcome,
    })
}

async fn project_tool_result_for_model(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: ToolRuntimeExecutionContext<'_>,
    proposal_id: &str,
    tool_name: &str,
    outcome: ToolExecutionOutcome,
) -> Result<ToolExecutionOutcome, Status> {
    if !outcome.success {
        return Ok(outcome);
    }

    let budget = ToolTurnBudget::default();
    let projection_policy = projection_policy_for_tool(tool_name);
    let default_sensitive =
        matches!(projection_policy, ToolResultProjectionPolicy::RedactedPreviewAndArtifact);
    let should_spill = match projection_policy {
        ToolResultProjectionPolicy::InlineUnlessLarge => {
            outcome.output_json.len() > budget.max_model_inline_bytes
        }
        ToolResultProjectionPolicy::SummarizeAndArtifact => true,
        ToolResultProjectionPolicy::RedactedPreviewAndArtifact => true,
    };
    if !should_spill {
        return Ok(outcome);
    }

    let sensitivity = tool_result_sensitivity(tool_name, default_sensitive);
    let preview = redacted_tool_result_preview(
        outcome.output_json.as_slice(),
        budget.max_artifact_preview_bytes,
    );
    let artifact = runtime_state
        .create_tool_result_artifact(ToolResultArtifactCreateRequest {
            artifact_id: Ulid::new().to_string(),
            session_id: context.session_id.to_owned(),
            run_id: context.run_id.to_owned(),
            proposal_id: proposal_id.to_owned(),
            tool_name: tool_name.to_owned(),
            mime_type: "application/json".to_owned(),
            sensitivity,
            retention: ArtifactRetentionPolicy::keep(),
            redacted_preview: preview.clone(),
            content: outcome.output_json.clone(),
        })
        .await?;

    let summary = summarize_tool_result_for_model(
        outcome.output_json.as_slice(),
        budget.max_model_summary_bytes,
    );
    let visibility = if default_sensitive {
        ToolResultVisibility::RedactedPreview
    } else {
        ToolResultVisibility::ModelSummary
    };
    let saved_model_visible_bytes =
        outcome.output_json.len().saturating_sub(summary.len()).try_into().unwrap_or(u64::MAX);
    let projected = json!({
        "schema_version": 1,
        "visibility": visibility.as_str(),
        "projection_policy": projection_policy.as_str(),
        "summary": summary,
        "redacted_preview": preview,
        "artifact": artifact,
        "budget": {
            "max_model_inline_bytes": budget.max_model_inline_bytes,
            "max_model_summary_bytes": budget.max_model_summary_bytes,
            "max_artifact_preview_bytes": budget.max_artifact_preview_bytes,
        },
        "metrics": {
            "spilled_artifacts": 1,
            "saved_model_visible_bytes": saved_model_visible_bytes,
        }
    });
    let mut projected_outcome = outcome;
    projected_outcome.output_json = serde_json::to_vec(&projected).map_err(|error| {
        Status::internal(format!("failed to serialize projected tool result: {error}"))
    })?;
    Ok(projected_outcome)
}

fn tool_result_sensitivity(tool_name: &str, default_sensitive: bool) -> ToolResultSensitivity {
    if tool_name == crate::gateway::PROCESS_RUNNER_TOOL_NAME {
        ToolResultSensitivity::StdoutStderr
    } else if tool_name == crate::gateway::HTTP_FETCH_TOOL_NAME
        || tool_name.starts_with("palyra.browser.")
        || tool_name == "palyra.plugin.run"
    {
        ToolResultSensitivity::ProviderRawPayload
    } else if tool_name == crate::gateway::WORKSPACE_PATCH_TOOL_NAME {
        ToolResultSensitivity::InternalPath
    } else if default_sensitive {
        ToolResultSensitivity::ApprovalRiskData
    } else {
        ToolResultSensitivity::Public
    }
}

fn summarize_tool_result_for_model(output_json: &[u8], max_bytes: usize) -> String {
    let preview = redacted_tool_result_preview(output_json, max_bytes);
    if preview.len() <= max_bytes {
        preview
    } else {
        truncate_utf8(preview.as_str(), max_bytes)
    }
}

fn redacted_tool_result_preview(output_json: &[u8], max_bytes: usize) -> String {
    let redacted = serde_json::from_slice::<Value>(output_json)
        .map(|mut value| {
            redact_sensitive_json_value(&mut value);
            serde_json::to_string(&value).unwrap_or_else(|_| REDACTED.to_owned())
        })
        .unwrap_or_else(|_| String::from_utf8_lossy(output_json).to_string());
    let redacted = redact_auth_error(redact_url_segments_in_text(redacted.as_str()).as_str());
    truncate_utf8(redacted.as_str(), max_bytes)
}

fn redact_sensitive_json_value(value: &mut Value) {
    match value {
        Value::Object(map) => redact_sensitive_json_object(map),
        Value::Array(values) => {
            for value in values {
                redact_sensitive_json_value(value);
            }
        }
        Value::String(text) => {
            *text = redact_auth_error(redact_url_segments_in_text(text).as_str());
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn redact_sensitive_json_object(map: &mut Map<String, Value>) {
    for (key, value) in map.iter_mut() {
        if is_sensitive_key(key.as_str()) {
            *value = Value::String(REDACTED.to_owned());
        } else {
            redact_sensitive_json_value(value);
        }
    }
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_owned();
    }
    value
        .char_indices()
        .take_while(|(index, ch)| index.saturating_add(ch.len_utf8()) <= max_bytes)
        .map(|(_, ch)| ch)
        .collect()
}
