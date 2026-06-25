//! Approval-request recording for tool proposals on the route-message surface.
//!
//! Route message has no interactive client that could answer an approval
//! prompt mid-request, so a required approval is persisted as a pending
//! record (plus tape and journal events) and handed to the decision layer,
//! which denies the proposal for this run while the record stays available
//! for later operator action.

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
        tool_runtime::workspace_patch::normalized_workspace_patch_approval_input_json,
        tool_security::{
            approval_execution_context_for_backend_selection, ToolProposalBackendSelection,
        },
    },
    gateway::{best_effort_mark_approval_error, GatewayRuntimeState, ToolSkillContext},
    journal::ApprovalCreateRequest,
    transport::grpc::auth::RequestContext,
};

/// Creates the pending approval record for a proposal that requires one and
/// mirrors the request to the tape and journal.
///
/// Returns `Ok(None)` when no approval is required, otherwise
/// `Ok(Some(approval_id))` for the decision layer to attach as the pending
/// approval (which denies the proposal for this run).
///
/// # Errors
/// Returns the status from approval-record creation, tape append, or journal
/// append failures. Tape/journal failures first mark the freshly created
/// approval as errored (best effort) so no permanently pending record is
/// left behind for a request that was never durably recorded.
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
    backend_selection: &ToolProposalBackendSelection,
    tape_seq: &mut i64,
) -> Result<Option<String>, Status> {
    if !proposal_approval_required {
        return Ok(None);
    }

    let approval_input_json_override = if tool_name == "palyra.fs.apply_patch" {
        normalized_workspace_patch_approval_input_json(
            runtime_state,
            route_request_context.principal.as_str(),
            route_request_context.channel.as_deref(),
            session_id,
            run_id,
            input_json,
        )
        .await
    } else {
        None
    };
    let approval_input_json = approval_input_json_override.as_deref().unwrap_or(input_json);
    let pending_approval = build_pending_tool_approval(
        tool_name,
        skill_context,
        approval_input_json,
        &runtime_state.config.tool_call,
        approval_execution_context_for_backend_selection(backend_selection).as_ref(),
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
        approval_input_json,
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
