use std::borrow::Cow;

use crate::args::{ApprovalDecisionScopeArg, ApprovalResolveDecisionArg, ApprovalSubjectTypeArg};
use crate::{output::approvals as approvals_output, *};
use palyra_control_plane as control_plane;

pub(crate) fn run_approvals(command: ApprovalsCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_approvals_async(command))
}

pub(crate) async fn run_approvals_async(command: ApprovalsCommand) -> Result<()> {
    match command {
        ApprovalsCommand::Decide { approval_id, decision, scope, ttl_ms, reason, json } => {
            run_approval_decide(approval_id, decision, scope, ttl_ms, reason, json).await
        }
        command => run_approvals_grpc(command).await,
    }
}

async fn run_approvals_grpc(command: ApprovalsCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for approvals command"))?;
    let connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides::default(),
        app::ConnectionDefaults::USER,
    )?;
    let mut client = gateway_v1::approvals_service_client::ApprovalsServiceClient::connect(
        connection.grpc_url.clone(),
    )
    .await
    .with_context(|| format!("failed to connect gateway gRPC endpoint {}", connection.grpc_url))?;

    match command {
        ApprovalsCommand::List {
            after,
            limit,
            since,
            until,
            subject,
            principal,
            decision,
            subject_type,
            json,
        } => {
            let json = output::preferred_json(json);
            validate_approval_time_window("approvals list", since, until)?;
            if let Some(after_value) = after.as_deref() {
                validate_canonical_id(after_value)
                    .context("approval cursor (--after) must be a canonical ULID")?;
            }
            let mut request = Request::new(gateway_v1::ListApprovalsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                after_approval_ulid: after.unwrap_or_default(),
                limit: limit.unwrap_or(100),
                since_unix_ms: since.unwrap_or_default(),
                until_unix_ms: until.unwrap_or_default(),
                subject_id: subject.unwrap_or_default(),
                principal: principal.unwrap_or_default(),
                decision: approval_decision_filter_to_proto(decision),
                subject_type: approval_subject_type_filter_to_proto(subject_type),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .list_approvals(request)
                .await
                .context("failed to call approvals ListApprovals")?
                .into_inner();
            approvals_output::emit_list(&response, json)?;
        }
        ApprovalsCommand::Show { approval_id, json } => {
            let json = output::preferred_json(json);
            validate_canonical_id(approval_id.as_str())
                .context("approval id must be a canonical ULID")?;
            let mut request = Request::new(gateway_v1::GetApprovalRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                approval_id: Some(common_v1::CanonicalId { ulid: approval_id.clone() }),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let response = client
                .get_approval(request)
                .await
                .context("failed to call approvals GetApproval")?
                .into_inner();
            let approval = response
                .approval
                .context("approvals GetApproval returned empty approval payload")?;
            approvals_output::emit_show(&approval, json)?;
        }
        ApprovalsCommand::Export {
            format,
            limit,
            since,
            until,
            subject,
            principal,
            decision,
            subject_type,
        } => {
            validate_approval_time_window("approvals export", since, until)?;
            let mut request = Request::new(gateway_v1::ExportApprovalsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                format: approval_export_format_to_proto(format),
                limit: limit.unwrap_or(1_000),
                since_unix_ms: since.unwrap_or_default(),
                until_unix_ms: until.unwrap_or_default(),
                subject_id: subject.unwrap_or_default(),
                principal: principal.unwrap_or_default(),
                decision: approval_decision_filter_to_proto(decision),
                subject_type: approval_subject_type_filter_to_proto(subject_type),
            });
            inject_run_stream_metadata(request.metadata_mut(), &connection)?;
            let mut stream = client
                .export_approvals(request)
                .await
                .context("failed to call approvals ExportApprovals")?
                .into_inner();
            while let Some(item) = stream.next().await {
                let chunk = item.context("failed to read approvals export stream chunk")?;
                if !chunk.chunk.is_empty() {
                    std::io::stdout()
                        .write_all(chunk.chunk.as_slice())
                        .context("failed to write approvals export chunk to stdout")?;
                }
                if chunk.done {
                    break;
                }
            }
        }
        ApprovalsCommand::Decide { .. } => unreachable!("handled before gRPC dispatch"),
    }

    std::io::stdout().flush().context("stdout flush failed")
}

async fn run_approval_decide(
    approval_id: String,
    decision: ApprovalResolveDecisionArg,
    scope: ApprovalDecisionScopeArg,
    ttl_ms: Option<i64>,
    reason: Option<String>,
    json: bool,
) -> Result<()> {
    validate_canonical_id(approval_id.as_str()).context("approval id must be a canonical ULID")?;
    validate_approval_decision_scope(scope, ttl_ms)?;
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let payload = context
        .client
        .decide_approval(
            approval_id.as_str(),
            &control_plane::ApprovalDecisionRequest {
                approved: matches!(decision, ApprovalResolveDecisionArg::Allow),
                reason: normalize_optional_reason(reason),
                decision_scope: Some(approval_scope_arg_to_text(scope).to_owned()),
                decision_scope_ttl_ms: ttl_ms,
            },
        )
        .await
        .with_context(|| format!("failed to resolve approval {approval_id}"))?;
    emit_approval_decision(&payload, output::preferred_json(json))
}

fn emit_approval_decision(
    payload: &control_plane::ApprovalDecisionEnvelope,
    json_output: bool,
) -> Result<()> {
    if json_output {
        return output::print_json_pretty(
            payload,
            "failed to encode approval decision payload as JSON",
        );
    }

    println!(
        "approvals.decide id={} subject_type={} subject={} decision={} scope={} ttl_ms={} reason=\"{}\" dm_pairing={}",
        approval_value_at(payload, "/approval/approval_id", "unknown"),
        approval_value_at(payload, "/approval/subject_type", "unknown"),
        approval_value_at(payload, "/approval/subject_id", "unknown"),
        approval_value_at(payload, "/approval/decision", "unknown"),
        approval_value_at(payload, "/approval/decision_scope", "unknown"),
        approval_scalar_at(payload, "/approval/decision_scope_ttl_ms", "none"),
        approval_value_at(payload, "/approval/decision_reason", "").replace('"', "'"),
        payload.dm_pairing.as_deref().unwrap_or("none"),
    );
    std::io::stdout().flush().context("stdout flush failed")
}

fn approval_value_at<'a>(
    payload: &'a control_plane::ApprovalDecisionEnvelope,
    pointer: &str,
    fallback: &'a str,
) -> &'a str {
    payload.approval.pointer(pointer).and_then(Value::as_str).unwrap_or(fallback)
}

fn approval_scalar_at<'a>(
    payload: &'a control_plane::ApprovalDecisionEnvelope,
    pointer: &str,
    fallback: &'a str,
) -> Cow<'a, str> {
    match payload.approval.pointer(pointer) {
        Some(Value::String(value)) => Cow::Borrowed(value.as_str()),
        Some(Value::Number(value)) => Cow::Owned(value.to_string()),
        _ => Cow::Borrowed(fallback),
    }
}

fn normalize_optional_reason(reason: Option<String>) -> Option<String> {
    reason.as_deref().map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned)
}

fn validate_approval_time_window(
    command_name: &str,
    since: Option<i64>,
    until: Option<i64>,
) -> Result<()> {
    if let (Some(since_ms), Some(until_ms)) = (since, until) {
        if since_ms > until_ms {
            anyhow::bail!("{command_name} requires --since <= --until when both filters are set");
        }
    }
    Ok(())
}

fn validate_approval_decision_scope(
    scope: ApprovalDecisionScopeArg,
    ttl_ms: Option<i64>,
) -> Result<()> {
    if let Some(ttl_ms) = ttl_ms {
        if ttl_ms <= 0 {
            anyhow::bail!("--ttl-ms must be greater than zero when provided");
        }
        if !matches!(scope, ApprovalDecisionScopeArg::Timeboxed) {
            anyhow::bail!("--ttl-ms is only valid with --scope timeboxed");
        }
    }
    if matches!(scope, ApprovalDecisionScopeArg::Timeboxed) && ttl_ms.is_none() {
        anyhow::bail!("--scope timeboxed requires --ttl-ms");
    }
    Ok(())
}

fn approval_subject_type_filter_to_proto(value: Option<ApprovalSubjectTypeArg>) -> i32 {
    match value {
        Some(ApprovalSubjectTypeArg::Tool) => gateway_v1::ApprovalSubjectType::Tool as i32,
        Some(ApprovalSubjectTypeArg::ChannelSend) => {
            gateway_v1::ApprovalSubjectType::ChannelSend as i32
        }
        Some(ApprovalSubjectTypeArg::SecretAccess) => {
            gateway_v1::ApprovalSubjectType::SecretAccess as i32
        }
        Some(ApprovalSubjectTypeArg::BrowserAction) => {
            gateway_v1::ApprovalSubjectType::BrowserAction as i32
        }
        Some(ApprovalSubjectTypeArg::NodeCapability) => {
            gateway_v1::ApprovalSubjectType::NodeCapability as i32
        }
        None => gateway_v1::ApprovalSubjectType::Unspecified as i32,
    }
}

fn approval_scope_arg_to_text(value: ApprovalDecisionScopeArg) -> &'static str {
    match value {
        ApprovalDecisionScopeArg::Once => "once",
        ApprovalDecisionScopeArg::Session => "session",
        ApprovalDecisionScopeArg::Timeboxed => "timeboxed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approval_decision_scope_rejects_non_timeboxed_ttl() {
        let result = validate_approval_decision_scope(ApprovalDecisionScopeArg::Session, Some(500));
        assert!(result.is_err(), "--ttl-ms should require --scope timeboxed");
    }

    #[test]
    fn approval_decision_scope_requires_positive_ttl_for_timeboxed() {
        let missing = validate_approval_decision_scope(ApprovalDecisionScopeArg::Timeboxed, None);
        assert!(missing.is_err(), "timeboxed approvals should require --ttl-ms");

        let zero = validate_approval_decision_scope(ApprovalDecisionScopeArg::Timeboxed, Some(0));
        assert!(zero.is_err(), "--ttl-ms must stay positive");
    }

    #[test]
    fn approval_decision_scope_accepts_positive_timeboxed_ttl() {
        let result =
            validate_approval_decision_scope(ApprovalDecisionScopeArg::Timeboxed, Some(60_000));
        assert!(result.is_ok(), "positive timeboxed TTL should be accepted");
    }
}
