use crate::{output::approvals as approvals_output, *};

pub(crate) fn run_approvals(command: ApprovalsCommand) -> Result<()> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for approvals command"))?;
    let connection = root_context
        .resolve_grpc_connection(app::ConnectionOverrides::default(), app::ConnectionDefaults::USER)?;
    let runtime = build_runtime()?;
    runtime.block_on(run_approvals_async(command, connection))
}

pub(crate) async fn run_approvals_async(
    command: ApprovalsCommand,
    connection: AgentConnection,
) -> Result<()> {
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
            json,
        } => {
            let json = output::preferred_json(json);
            if let (Some(since_ms), Some(until_ms)) = (since, until) {
                if since_ms > until_ms {
                    return Err(anyhow!(
                        "approvals list requires --since <= --until when both filters are set"
                    ));
                }
            }
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
                subject_type: gateway_v1::ApprovalSubjectType::Unspecified as i32,
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
        ApprovalsCommand::Export { format, limit, since, until, subject, principal, decision } => {
            if let (Some(since_ms), Some(until_ms)) = (since, until) {
                if since_ms > until_ms {
                    return Err(anyhow!(
                        "approvals export requires --since <= --until when both filters are set"
                    ));
                }
            }
            let mut request = Request::new(gateway_v1::ExportApprovalsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                format: approval_export_format_to_proto(format),
                limit: limit.unwrap_or(1_000),
                since_unix_ms: since.unwrap_or_default(),
                until_unix_ms: until.unwrap_or_default(),
                subject_id: subject.unwrap_or_default(),
                principal: principal.unwrap_or_default(),
                decision: approval_decision_filter_to_proto(decision),
                subject_type: gateway_v1::ApprovalSubjectType::Unspecified as i32,
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
    }

    std::io::stdout().flush().context("stdout flush failed")
}
