use crate::*;

pub(crate) fn emit_list(
    response: &gateway_v1::ListApprovalsResponse,
    json_output: bool,
) -> Result<()> {
    if json_output {
        let payload = json!({
            "approvals": response
                .approvals
                .iter()
                .map(approval_record_to_json)
                .collect::<Vec<_>>(),
            "next_after_approval_ulid": response.next_after_approval_ulid,
        });
        return super::print_json_pretty(
            &payload,
            "failed to encode approvals list payload as JSON",
        );
    }

    println!(
        "approvals.list approvals={} next_after={}",
        response.approvals.len(),
        if response.next_after_approval_ulid.is_empty() {
            "none"
        } else {
            response.next_after_approval_ulid.as_str()
        }
    );
    for approval in &response.approvals {
        println!(
            "approval id={} subject_type={} subject={} decision={} principal={} requested_at_ms={} resolved_at_ms={}",
            approval
                .approval_id
                .as_ref()
                .map(|value| value.ulid.as_str())
                .unwrap_or("unknown"),
            approval_subject_type_to_text(approval.subject_type),
            approval.subject_id,
            approval_status_text(approval),
            approval.principal,
            approval.requested_at_unix_ms,
            approval.resolved_at_unix_ms
        );
    }
    Ok(())
}

pub(crate) fn emit_show(approval: &gateway_v1::ApprovalRecord, json_output: bool) -> Result<()> {
    if json_output {
        return super::print_json_pretty(
            &approval_record_to_json(approval),
            "failed to encode approval payload as JSON",
        );
    }

    println!(
        "approvals.show id={} subject_type={} subject={} decision={} scope={} reason={}",
        approval.approval_id.as_ref().map(|value| value.ulid.as_str()).unwrap_or("unknown"),
        approval_subject_type_to_text(approval.subject_type),
        approval.subject_id,
        approval_status_text(approval),
        approval_scope_to_text(approval.decision_scope),
        approval.decision_reason
    );
    Ok(())
}

fn approval_status_text(approval: &gateway_v1::ApprovalRecord) -> &'static str {
    if approval.resolved_at_unix_ms == 0
        && approval.decision == gateway_v1::ApprovalDecision::Unspecified as i32
    {
        "pending"
    } else {
        approval_decision_to_text(approval.decision)
    }
}
