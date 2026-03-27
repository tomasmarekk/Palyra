use std::sync::Arc;

use tonic::Status;
use tracing::info;

use crate::{
    application::{
        approvals::{
            approval_subject_type_for_tool, build_pending_tool_approval,
            record_approval_requested_journal_event,
        },
        run_stream::tape::append_tool_approval_request_tape_event,
    },
    gateway::{best_effort_mark_approval_error, GatewayRuntimeState, ToolSkillContext},
    journal::ApprovalCreateRequest,
    transport::grpc::auth::RequestContext,
};

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_route_tool_approval_outcome(
    runtime_state: &Arc<GatewayRuntimeState>,
    route_request_context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    skill_context: Option<&ToolSkillContext>,
    proposal_approval_required: bool,
    tape_seq: &mut i64,
) -> Result<Option<String>, Status> {
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
            principal: route_request_context.principal.clone(),
            device_id: route_request_context.device_id.clone(),
            channel: route_request_context.channel.clone(),
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
        "route message approval requested"
    );
    if let Err(error) = append_tool_approval_request_tape_event(
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
            format!("route_approval_request_tape_error: {}", error.message()),
        )
        .await;
        return Err(error);
    }
    if let Err(error) = record_approval_requested_journal_event(
        runtime_state,
        route_request_context,
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
            format!("route_approval_request_journal_error: {}", error.message()),
        )
        .await;
        return Err(error);
    }
    Ok(Some(pending_approval.approval_id))
}
