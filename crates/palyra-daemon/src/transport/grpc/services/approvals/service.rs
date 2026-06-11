//! Approvals gRPC service implementation.
//!
//! Methods authorize the caller before exposing approval records or streaming
//! export chunks, preserving the audit chain semantics from the runtime layer.

use std::sync::Arc;

use serde_json::to_vec;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{metadata::MetadataMap, Request, Response, Status};
use tracing::warn;

use crate::{
    application::service_authorization::authorize_approvals_action,
    gateway::{
        approval_decision_from_proto, approval_export_ndjson_record_line,
        approval_export_ndjson_trailer_line, approval_record_message,
        approval_subject_type_from_proto, canonical_id, non_empty, require_supported_version,
        GatewayRuntimeState, APPROVAL_EXPORT_CHAIN_SEED_HEX, MAX_APPROVAL_EXPORT_CHUNK_BYTES,
        MAX_APPROVAL_EXPORT_LIMIT, MAX_APPROVAL_PAGE_LIMIT,
    },
    transport::grpc::{
        auth::{authorize_metadata, GatewayAuthConfig, RequestContext},
        proto::palyra::gateway::v1 as gateway_v1,
    },
};
use palyra_common::CANONICAL_PROTOCOL_MAJOR;

/// Tonic service adapter for approval record operations.
#[derive(Clone)]
pub struct ApprovalsServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl ApprovalsServiceImpl {
    /// Creates an approvals service bound to shared gateway state and auth.
    #[must_use]
    pub fn new(state: Arc<GatewayRuntimeState>, auth: GatewayAuthConfig) -> Self {
        Self { state, auth }
    }

    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth, method).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "approvals rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[tonic::async_trait]
impl gateway_v1::approvals_service_server::ApprovalsService for ApprovalsServiceImpl {
    type ExportApprovalsStream =
        ReceiverStream<Result<gateway_v1::ExportApprovalsResponse, Status>>;

    async fn list_approvals(
        &self,
        request: Request<gateway_v1::ListApprovalsRequest>,
    ) -> Result<Response<gateway_v1::ListApprovalsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListApprovals")?;
        authorize_approvals_action(
            context.principal.as_str(),
            "approvals.list",
            "approvals:records",
        )?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let decision = approval_decision_from_proto(payload.decision);
        let subject_type = approval_subject_type_from_proto(payload.subject_type);
        let since_unix_ms =
            if payload.since_unix_ms > 0 { Some(payload.since_unix_ms) } else { None };
        let until_unix_ms =
            if payload.until_unix_ms > 0 { Some(payload.until_unix_ms) } else { None };
        if let (Some(since), Some(until)) = (since_unix_ms, until_unix_ms) {
            if since > until {
                return Err(Status::invalid_argument(
                    "since_unix_ms cannot be greater than until_unix_ms",
                ));
            }
        }

        let (records, next_after_approval_ulid) = self
            .state
            .list_approval_records(
                non_empty(payload.after_approval_ulid),
                Some(payload.limit as usize),
                since_unix_ms,
                until_unix_ms,
                non_empty(payload.subject_id),
                non_empty(payload.principal),
                decision,
                subject_type,
            )
            .await?;
        Ok(Response::new(gateway_v1::ListApprovalsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            approvals: records.iter().map(approval_record_message).collect(),
            next_after_approval_ulid: next_after_approval_ulid.unwrap_or_default(),
        }))
    }

    async fn get_approval(
        &self,
        request: Request<gateway_v1::GetApprovalRequest>,
    ) -> Result<Response<gateway_v1::GetApprovalResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetApproval")?;
        authorize_approvals_action(
            context.principal.as_str(),
            "approvals.get",
            "approvals:record",
        )?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let approval_id = canonical_id(payload.approval_id, "approval_id")?;
        let record = self.state.approval_record(approval_id.clone()).await?.ok_or_else(|| {
            Status::not_found(format!("approval record not found: {approval_id}"))
        })?;
        Ok(Response::new(gateway_v1::GetApprovalResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            approval: Some(approval_record_message(&record)),
        }))
    }

    async fn export_approvals(
        &self,
        request: Request<gateway_v1::ExportApprovalsRequest>,
    ) -> Result<Response<Self::ExportApprovalsStream>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ExportApprovals")?;
        authorize_approvals_action(
            context.principal.as_str(),
            "approvals.export",
            "approvals:records",
        )?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let decision = approval_decision_from_proto(payload.decision);
        let subject_type = approval_subject_type_from_proto(payload.subject_type);
        let since_unix_ms =
            if payload.since_unix_ms > 0 { Some(payload.since_unix_ms) } else { None };
        let until_unix_ms =
            if payload.until_unix_ms > 0 { Some(payload.until_unix_ms) } else { None };
        if let (Some(since), Some(until)) = (since_unix_ms, until_unix_ms) {
            if since > until {
                return Err(Status::invalid_argument(
                    "since_unix_ms cannot be greater than until_unix_ms",
                ));
            }
        }
        let export_format = match gateway_v1::ApprovalExportFormat::try_from(payload.format)
            .unwrap_or(gateway_v1::ApprovalExportFormat::Unspecified)
        {
            gateway_v1::ApprovalExportFormat::Unspecified => {
                gateway_v1::ApprovalExportFormat::Ndjson
            }
            other => other,
        };
        let export_limit = if payload.limit == 0 { 1_000_usize } else { payload.limit as usize }
            .clamp(1, MAX_APPROVAL_EXPORT_LIMIT);

        let state = Arc::clone(&self.state);
        let subject_id = non_empty(payload.subject_id);
        let principal = non_empty(payload.principal);
        let (sender, receiver) = mpsc::channel(8);
        tokio::spawn(async move {
            let mut after_approval_id: Option<String> = None;
            let mut exported = 0_usize;
            let mut chunk_seq = 0_u32;
            let mut json_array_started = false;
            let mut json_first_item = true;
            let mut ndjson_sequence = 0_u64;
            let mut ndjson_last_chain_checksum = APPROVAL_EXPORT_CHAIN_SEED_HEX.to_owned();

            loop {
                if exported >= export_limit {
                    break;
                }
                let page_limit =
                    export_limit.saturating_sub(exported).clamp(1, MAX_APPROVAL_PAGE_LIMIT);
                let (records, next_after) = match state
                    .list_approval_records(
                        after_approval_id.clone(),
                        Some(page_limit),
                        since_unix_ms,
                        until_unix_ms,
                        subject_id.clone(),
                        principal.clone(),
                        decision,
                        subject_type,
                    )
                    .await
                {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                if records.is_empty() {
                    break;
                }

                for record in records {
                    if exported >= export_limit {
                        break;
                    }
                    match export_format {
                        gateway_v1::ApprovalExportFormat::Ndjson => {
                            ndjson_sequence = ndjson_sequence.saturating_add(1);
                            let (line, chain_checksum) = match approval_export_ndjson_record_line(
                                &record,
                                ndjson_sequence,
                                ndjson_last_chain_checksum.as_str(),
                            ) {
                                Ok(value) => value,
                                Err(error) => {
                                    let _ = sender.send(Err(error)).await;
                                    return;
                                }
                            };
                            ndjson_last_chain_checksum = chain_checksum;
                            for chunk in line.chunks(MAX_APPROVAL_EXPORT_CHUNK_BYTES) {
                                chunk_seq = chunk_seq.saturating_add(1);
                                if sender
                                    .send(Ok(gateway_v1::ExportApprovalsResponse {
                                        v: CANONICAL_PROTOCOL_MAJOR,
                                        chunk: chunk.to_vec(),
                                        chunk_seq,
                                        done: false,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        gateway_v1::ApprovalExportFormat::Json => {
                            if !json_array_started {
                                json_array_started = true;
                                chunk_seq = chunk_seq.saturating_add(1);
                                if sender
                                    .send(Ok(gateway_v1::ExportApprovalsResponse {
                                        v: CANONICAL_PROTOCOL_MAJOR,
                                        chunk: b"[".to_vec(),
                                        chunk_seq,
                                        done: false,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            if !json_first_item {
                                chunk_seq = chunk_seq.saturating_add(1);
                                if sender
                                    .send(Ok(gateway_v1::ExportApprovalsResponse {
                                        v: CANONICAL_PROTOCOL_MAJOR,
                                        chunk: b",".to_vec(),
                                        chunk_seq,
                                        done: false,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            json_first_item = false;
                            let payload = match to_vec(&record) {
                                Ok(value) => value,
                                Err(error) => {
                                    let _ = sender
                                        .send(Err(Status::internal(format!(
                                            "failed to serialize approvals JSON export record: {error}"
                                        ))))
                                        .await;
                                    return;
                                }
                            };
                            for chunk in payload.chunks(MAX_APPROVAL_EXPORT_CHUNK_BYTES) {
                                chunk_seq = chunk_seq.saturating_add(1);
                                if sender
                                    .send(Ok(gateway_v1::ExportApprovalsResponse {
                                        v: CANONICAL_PROTOCOL_MAJOR,
                                        chunk: chunk.to_vec(),
                                        chunk_seq,
                                        done: false,
                                    }))
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                        gateway_v1::ApprovalExportFormat::Unspecified => {}
                    }
                    exported = exported.saturating_add(1);
                }

                let Some(next_after) = next_after else {
                    break;
                };
                after_approval_id = Some(next_after);
            }

            let export_suffix = if export_format == gateway_v1::ApprovalExportFormat::Json {
                Some(if json_array_started { b"]".to_vec() } else { b"[]".to_vec() })
            } else if export_format == gateway_v1::ApprovalExportFormat::Ndjson {
                match approval_export_ndjson_trailer_line(
                    exported,
                    ndjson_last_chain_checksum.as_str(),
                ) {
                    Ok(value) => Some(value),
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            } else {
                None
            };
            if let Some(suffix) = export_suffix {
                for chunk in suffix.chunks(MAX_APPROVAL_EXPORT_CHUNK_BYTES) {
                    chunk_seq = chunk_seq.saturating_add(1);
                    if sender
                        .send(Ok(gateway_v1::ExportApprovalsResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            chunk: chunk.to_vec(),
                            chunk_seq,
                            done: false,
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }

            chunk_seq = chunk_seq.saturating_add(1);
            let _ = sender
                .send(Ok(gateway_v1::ExportApprovalsResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    chunk: Vec::new(),
                    chunk_seq,
                    done: true,
                }))
                .await;
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}
