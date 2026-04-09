use std::sync::Arc;

use palyra_common::CANONICAL_PROTOCOL_MAJOR;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tonic::Status;

use crate::{
    application::session_compaction::{
        apply_session_compaction, preview_session_compaction, SessionCompactionApplyRequest,
    },
    gateway::{
        approval_prompt_message, approval_scope_to_proto, status_kind_name, GatewayRuntimeState,
        MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN,
    },
    journal::{ApprovalDecisionScope, ApprovalPromptRecord, OrchestratorTapeAppendRequest},
    transport::grpc::{auth::RequestContext, proto::palyra::common::v1 as common_v1},
};
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_tool_decision_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    event_type: &str,
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: event_type.to_owned(),
            payload_json: tool_decision_tape_payload(
                proposal_id,
                tool_name,
                allowed,
                reason,
                approval_required,
                policy_enforced,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

fn status_event(
    run_id: String,
    kind: common_v1::stream_status::StatusKind,
    message: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::Status(common_v1::StreamStatus {
            kind: kind as i32,
            message: message.into(),
        })),
    }
}

fn model_token_event(
    run_id: String,
    token: impl Into<String>,
    is_final: bool,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
            token: token.into(),
            is_final,
        })),
    }
}

fn tool_proposal_event(
    run_id: String,
    proposal_id: impl Into<String>,
    tool_name: impl Into<String>,
    input_json: Vec<u8>,
    approval_required: bool,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolProposal(common_v1::ToolProposal {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            tool_name: tool_name.into(),
            input_json,
            approval_required,
        })),
    }
}

#[allow(clippy::too_many_arguments)]
fn tool_approval_request_event(
    run_id: String,
    proposal_id: impl Into<String>,
    approval_id: impl Into<String>,
    tool_name: impl Into<String>,
    input_json: Vec<u8>,
    approval_required: bool,
    request_summary: impl Into<String>,
    prompt: &ApprovalPromptRecord,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolApprovalRequest(
            common_v1::ToolApprovalRequest {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                tool_name: tool_name.into(),
                input_json,
                approval_required,
                approval_id: Some(common_v1::CanonicalId { ulid: approval_id.into() }),
                prompt: Some(approval_prompt_message(prompt)),
                request_summary: request_summary.into(),
            },
        )),
    }
}

fn tool_approval_response_event(
    run_id: String,
    proposal_id: impl Into<String>,
    approval_id: impl Into<String>,
    approved: bool,
    reason: impl Into<String>,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolApprovalResponse(
            common_v1::ToolApprovalResponse {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                approved,
                reason: reason.into(),
                approval_id: Some(common_v1::CanonicalId { ulid: approval_id.into() }),
                decision_scope: approval_scope_to_proto(decision_scope),
                decision_scope_ttl_ms: decision_scope_ttl_ms.unwrap_or_default(),
            },
        )),
    }
}

fn tool_decision_event(
    run_id: String,
    proposal_id: impl Into<String>,
    allowed: bool,
    reason: impl Into<String>,
    approval_required: bool,
    policy_enforced: bool,
) -> common_v1::RunStreamEvent {
    let kind = if allowed {
        common_v1::tool_decision::DecisionKind::Allow
    } else {
        common_v1::tool_decision::DecisionKind::Deny
    };
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolDecision(common_v1::ToolDecision {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            kind: kind as i32,
            reason: reason.into(),
            approval_required,
            policy_enforced,
        })),
    }
}

fn tool_result_event(
    run_id: String,
    proposal_id: impl Into<String>,
    success: bool,
    output_json: Vec<u8>,
    error: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolResult(common_v1::ToolResult {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            success,
            output_json,
            error: error.into(),
        })),
    }
}

fn tool_attestation_event(
    run_id: String,
    proposal_id: impl Into<String>,
    attestation_id: impl Into<String>,
    execution_sha256: impl Into<String>,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolAttestation(
            common_v1::ToolAttestation {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                attestation_id: Some(common_v1::CanonicalId { ulid: attestation_id.into() }),
                execution_sha256: execution_sha256.into(),
                executed_at_unix_ms,
                timed_out,
                executor: executor.into(),
            },
        )),
    }
}

#[allow(clippy::result_large_err)]
pub(crate) async fn send_status_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    kind: common_v1::stream_status::StatusKind,
    message: &str,
) -> Result<(), Status> {
    let event = status_event(run_id.to_owned(), kind, message.to_owned());
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "status".to_owned(),
            payload_json: status_tape_payload(kind, message),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_model_token_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    tape_seq: &mut i64,
    token_tape_events: &mut usize,
    compaction_emitted: &mut bool,
    token: &str,
    is_final: bool,
) -> Result<(), Status> {
    let event = model_token_event(run_id.to_owned(), token.to_owned(), is_final);
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    if !is_final && *token_tape_events >= MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN {
        if !*compaction_emitted {
            compact_model_token_tape(runtime_state, request_context, session_id, run_id, tape_seq)
                .await?;
            send_status_with_tape(
                sender,
                runtime_state,
                run_id,
                tape_seq,
                common_v1::stream_status::StatusKind::InProgress,
                "Automatic compaction lifecycle executed after the model token tape cap was reached.",
            )
            .await?;
            *compaction_emitted = true;
        }
        return Ok(());
    }
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "model_token".to_owned(),
            payload_json: model_token_tape_payload(token, is_final),
        })
        .await?;
    *tape_seq += 1;
    *token_tape_events += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
pub(crate) async fn compact_model_token_tape(
    runtime_state: &Arc<GatewayRuntimeState>,
    request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    tape_seq: &mut i64,
) -> Result<(), Status> {
    let session = runtime_state
        .resolve_orchestrator_session(crate::journal::OrchestratorSessionResolveRequest {
            session_id: Some(session_id.to_owned()),
            session_key: None,
            session_label: None,
            principal: request_context.principal.clone(),
            device_id: request_context.device_id.clone(),
            channel: request_context.channel.clone(),
            require_existing: true,
            reset_session: false,
        })
        .await?
        .session;
    let preview = preview_session_compaction(
        runtime_state,
        &session,
        Some("token_tape_cap_reached"),
        Some("token_tape_cap_v1"),
    )
    .await?;
    let payload_json = if preview.eligible {
        let execution = apply_session_compaction(SessionCompactionApplyRequest {
            runtime_state,
            session: &session,
            actor_principal: session.principal.as_str(),
            run_id: Some(run_id),
            mode: "automatic",
            trigger_reason: Some("token_tape_cap_reached"),
            trigger_policy: Some("token_tape_cap_v1"),
            accept_candidate_ids: &[],
            reject_candidate_ids: &[],
        })
        .await?;
        json!({
            "event": "session.compaction.applied",
            "artifact_id": execution.artifact.artifact_id,
            "checkpoint_id": execution.checkpoint.checkpoint_id,
            "policy": "token_tape_cap_v1",
            "estimated_input_tokens": execution.plan.estimated_input_tokens,
            "estimated_output_tokens": execution.plan.estimated_output_tokens,
            "token_delta": execution.plan.estimated_input_tokens.saturating_sub(execution.plan.estimated_output_tokens),
            "write_count": execution.writes.len(),
        })
        .to_string()
    } else {
        json!({
            "event": "session.compaction.blocked",
            "policy": "token_tape_cap_v1",
            "blocked_reason": preview.blocked_reason,
            "estimated_input_tokens": preview.estimated_input_tokens,
            "estimated_output_tokens": preview.estimated_output_tokens,
        })
        .to_string()
    };
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "session.compaction".to_owned(),
            payload_json,
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_tool_proposal_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_proposal".to_owned(),
            payload_json: tool_proposal_tape_payload(
                proposal_id,
                tool_name,
                input_json,
                approval_required,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_tool_approval_request_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
    request_summary: &str,
    prompt: &ApprovalPromptRecord,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_approval_request".to_owned(),
            payload_json: tool_approval_request_tape_payload(
                proposal_id,
                approval_id,
                tool_name,
                input_json,
                approval_required,
                request_summary,
                prompt,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn append_tool_approval_response_tape_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    approval_id: &str,
    approved: bool,
    reason: &str,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_approval_response".to_owned(),
            payload_json: tool_approval_response_tape_payload(
                proposal_id,
                approval_id,
                approved,
                reason,
                decision_scope,
                decision_scope_ttl_ms,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_tool_proposal_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> Result<(), Status> {
    let event = tool_proposal_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        tool_name.to_owned(),
        input_json.to_vec(),
        approval_required,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    append_tool_proposal_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        tool_name,
        input_json,
        approval_required,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_tool_approval_request_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
    request_summary: &str,
    prompt: &ApprovalPromptRecord,
) -> Result<(), Status> {
    let event = tool_approval_request_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        approval_id.to_owned(),
        tool_name.to_owned(),
        input_json.to_vec(),
        approval_required,
        request_summary.to_owned(),
        prompt,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    append_tool_approval_request_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        approval_id,
        tool_name,
        input_json,
        approval_required,
        request_summary,
        prompt,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_tool_approval_response_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    approval_id: &str,
    approved: bool,
    reason: &str,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
) -> Result<(), Status> {
    let event = tool_approval_response_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        approval_id.to_owned(),
        approved,
        reason.to_owned(),
        decision_scope,
        decision_scope_ttl_ms,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    append_tool_approval_response_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        proposal_id,
        approval_id,
        approved,
        reason,
        decision_scope,
        decision_scope_ttl_ms,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_tool_decision_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Result<(), Status> {
    let event = tool_decision_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        allowed,
        reason.to_owned(),
        approval_required,
        policy_enforced,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    append_tool_decision_tape_event(
        runtime_state,
        run_id,
        tape_seq,
        "tool_decision",
        proposal_id,
        tool_name,
        allowed,
        reason,
        approval_required,
        policy_enforced,
    )
    .await
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_tool_result_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    success: bool,
    output_json: &[u8],
    error: &str,
) -> Result<(), Status> {
    let event = tool_result_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        success,
        output_json.to_vec(),
        error.to_owned(),
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_result".to_owned(),
            payload_json: tool_result_tape_payload(proposal_id, success, output_json, error),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_tool_attestation_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    attestation_id: &str,
    execution_sha256: &str,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: &str,
    sandbox_enforcement: &str,
) -> Result<(), Status> {
    let event = tool_attestation_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        attestation_id.to_owned(),
        execution_sha256.to_owned(),
        executed_at_unix_ms,
        timed_out,
        executor.to_owned(),
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_attestation".to_owned(),
            payload_json: tool_attestation_tape_payload(
                proposal_id,
                attestation_id,
                execution_sha256,
                executed_at_unix_ms,
                timed_out,
                executor,
                sandbox_enforcement,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

fn status_tape_payload(kind: common_v1::stream_status::StatusKind, message: &str) -> String {
    json!({
        "kind": status_kind_name(kind),
        "message": message,
    })
    .to_string()
}

fn model_token_tape_payload(token: &str, is_final: bool) -> String {
    json!({
        "is_final": is_final,
        "token": token,
    })
    .to_string()
}

fn tool_proposal_tape_payload(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    json!({
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "input_json": normalized_input,
        "approval_required": approval_required,
    })
    .to_string()
}

fn tool_approval_request_tape_payload(
    proposal_id: &str,
    approval_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
    request_summary: &str,
    prompt: &ApprovalPromptRecord,
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    let prompt_details_json = serde_json::from_str::<Value>(prompt.details_json.as_str())
        .unwrap_or_else(|_| json!({ "raw": prompt.details_json }));
    json!({
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "tool_name": tool_name,
        "input_json": normalized_input,
        "approval_required": approval_required,
        "request_summary": request_summary,
        "prompt": {
            "title": prompt.title,
            "risk_level": prompt.risk_level.as_str(),
            "subject_id": prompt.subject_id,
            "summary": prompt.summary,
            "timeout_seconds": prompt.timeout_seconds,
            "policy_explanation": prompt.policy_explanation,
            "options": prompt.options.iter().map(|option| json!({
                "option_id": option.option_id,
                "label": option.label,
                "description": option.description,
                "default_selected": option.default_selected,
                "decision_scope": option.decision_scope.as_str(),
                "timebox_ttl_ms": option.timebox_ttl_ms,
            })).collect::<Vec<_>>(),
            "details_json": prompt_details_json,
        },
    })
    .to_string()
}

fn tool_approval_response_tape_payload(
    proposal_id: &str,
    approval_id: &str,
    approved: bool,
    reason: &str,
    decision_scope: ApprovalDecisionScope,
    decision_scope_ttl_ms: Option<i64>,
) -> String {
    json!({
        "proposal_id": proposal_id,
        "approval_id": approval_id,
        "approved": approved,
        "reason": reason,
        "decision_scope": decision_scope.as_str(),
        "decision_scope_ttl_ms": decision_scope_ttl_ms,
    })
    .to_string()
}

fn tool_decision_tape_payload(
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> String {
    json!({
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "kind": if allowed { "allow" } else { "deny" },
        "reason": reason,
        "approval_required": approval_required,
        "policy_enforced": policy_enforced,
    })
    .to_string()
}

fn tool_result_tape_payload(
    proposal_id: &str,
    success: bool,
    output_json: &[u8],
    error: &str,
) -> String {
    let normalized_output = serde_json::from_slice::<Value>(output_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(output_json).to_string() }));
    json!({
        "proposal_id": proposal_id,
        "success": success,
        "output_json": normalized_output,
        "error": error,
    })
    .to_string()
}

fn tool_attestation_tape_payload(
    proposal_id: &str,
    attestation_id: &str,
    execution_sha256: &str,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: &str,
    sandbox_enforcement: &str,
) -> String {
    json!({
        "proposal_id": proposal_id,
        "attestation_id": attestation_id,
        "execution_sha256": execution_sha256,
        "executed_at_unix_ms": executed_at_unix_ms,
        "timed_out": timed_out,
        "executor": executor,
        "sandbox_enforcement": sandbox_enforcement,
    })
    .to_string()
}
