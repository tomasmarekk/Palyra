//! Canvas gRPC service implementation.
//!
//! This service owns signed canvas creation, state updates, and patch
//! subscriptions for runtime-generated interactive canvas bundles.

use std::sync::Arc;

use palyra_common::CANONICAL_PROTOCOL_MAJOR;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{metadata::MetadataMap, Request, Response, Status};
use tracing::warn;

use crate::{
    gateway::{
        canonical_id, canvas_message, canvas_patch_update_message, non_empty,
        optional_canonical_id, GatewayRuntimeState, CANVAS_STREAM_POLL_INTERVAL,
        MAX_CANVAS_STREAM_PATCH_BATCH,
    },
    transport::grpc::{
        auth::{authorize_metadata, GatewayAuthConfig, RequestContext},
        proto::palyra::gateway::v1 as gateway_v1,
    },
};

/// Tonic service adapter for canvas lifecycle and streaming operations.
#[derive(Clone)]
pub struct CanvasServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl CanvasServiceImpl {
    /// Creates a canvas service bound to shared gateway state and auth.
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
            warn!(method, error = %error, "canvas rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[tonic::async_trait]
impl gateway_v1::canvas_service_server::CanvasService for CanvasServiceImpl {
    type SubscribeCanvasUpdatesStream =
        ReceiverStream<Result<gateway_v1::SubscribeCanvasUpdatesResponse, Status>>;

    async fn create_canvas(
        &self,
        request: Request<gateway_v1::CreateCanvasRequest>,
    ) -> Result<Response<gateway_v1::CreateCanvasResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CreateCanvas")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let requested_canvas_id = optional_canonical_id(payload.canvas_id, "canvas_id")?;
        let session_id = canonical_id(payload.session_id, "session_id")?;
        let bundle =
            payload.bundle.ok_or_else(|| Status::invalid_argument("bundle is required"))?;
        let (record, descriptor) = self.state.create_canvas(
            &context,
            requested_canvas_id,
            session_id,
            payload.initial_state_json.as_slice(),
            payload.initial_state_version,
            if payload.state_schema_version == 0 {
                None
            } else {
                Some(payload.state_schema_version)
            },
            bundle,
            payload.allowed_parent_origins,
            if payload.auth_token_ttl_seconds == 0 {
                None
            } else {
                Some(payload.auth_token_ttl_seconds)
            },
        )?;
        Ok(Response::new(gateway_v1::CreateCanvasResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            canvas: Some(canvas_message(&record)),
            frame_url: descriptor.frame_url,
            runtime_url: descriptor.runtime_url,
            auth_token: descriptor.auth_token,
        }))
    }

    async fn update_canvas(
        &self,
        request: Request<gateway_v1::UpdateCanvasRequest>,
    ) -> Result<Response<gateway_v1::UpdateCanvasResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "UpdateCanvas")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let canvas_id = canonical_id(payload.canvas_id, "canvas_id")?;
        let record = self.state.update_canvas_state(
            &context,
            canvas_id.as_str(),
            if payload.state_json.is_empty() { None } else { Some(payload.state_json.as_slice()) },
            if payload.patch_json.is_empty() { None } else { Some(payload.patch_json.as_slice()) },
            if payload.expected_state_version == 0 {
                None
            } else {
                Some(payload.expected_state_version)
            },
            if payload.expected_state_schema_version == 0 {
                None
            } else {
                Some(payload.expected_state_schema_version)
            },
        )?;
        Ok(Response::new(gateway_v1::UpdateCanvasResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            canvas: Some(canvas_message(&record)),
        }))
    }

    async fn close_canvas(
        &self,
        request: Request<gateway_v1::CloseCanvasRequest>,
    ) -> Result<Response<gateway_v1::CloseCanvasResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CloseCanvas")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let canvas_id = canonical_id(payload.canvas_id, "canvas_id")?;
        let record =
            self.state.close_canvas(&context, canvas_id.as_str(), non_empty(payload.reason))?;
        let canvas = canvas_message(&record);
        Ok(Response::new(gateway_v1::CloseCanvasResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            canvas_id: canvas.canvas_id,
            closed: canvas.closed,
            close_reason: canvas.close_reason,
        }))
    }

    async fn get_canvas(
        &self,
        request: Request<gateway_v1::GetCanvasRequest>,
    ) -> Result<Response<gateway_v1::GetCanvasResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetCanvas")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let canvas_id = canonical_id(payload.canvas_id, "canvas_id")?;
        let record = self.state.get_canvas(&context, canvas_id.as_str())?;
        Ok(Response::new(gateway_v1::GetCanvasResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            canvas: Some(canvas_message(&record)),
        }))
    }

    async fn subscribe_canvas_updates(
        &self,
        request: Request<gateway_v1::SubscribeCanvasUpdatesRequest>,
    ) -> Result<Response<Self::SubscribeCanvasUpdatesStream>, Status> {
        let context = self.authorize_rpc(request.metadata(), "SubscribeCanvasUpdates")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let canvas_id = canonical_id(payload.canvas_id, "canvas_id")?;
        let include_snapshot_state = payload.include_snapshot_state;

        let _existing_canvas = self.state.get_canvas(&context, canvas_id.as_str())?;
        let state = Arc::clone(&self.state);
        let context_for_stream = context.clone();
        let canvas_id_for_stream = canvas_id.clone();
        let mut after_state_version = payload.after_state_version;
        let (tx, rx) = mpsc::channel::<Result<gateway_v1::SubscribeCanvasUpdatesResponse, Status>>(
            MAX_CANVAS_STREAM_PATCH_BATCH,
        );

        tokio::spawn(async move {
            loop {
                if tx.is_closed() {
                    return;
                }
                let patches = match state.list_canvas_state_patches(
                    &context_for_stream,
                    canvas_id_for_stream.as_str(),
                    after_state_version,
                    MAX_CANVAS_STREAM_PATCH_BATCH,
                ) {
                    Ok(records) => records,
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                };
                if patches.is_empty() {
                    match state.get_canvas(&context_for_stream, canvas_id_for_stream.as_str()) {
                        Ok(record)
                            if {
                                let canvas = canvas_message(&record);
                                canvas.closed && after_state_version >= canvas.state_version
                            } =>
                        {
                            return;
                        }
                        Ok(_) => {}
                        Err(error) => {
                            let _ = tx.send(Err(error)).await;
                            return;
                        }
                    }
                    tokio::time::sleep(CANVAS_STREAM_POLL_INTERVAL).await;
                    continue;
                }

                for patch in patches {
                    after_state_version = patch.state_version;
                    if tx
                        .send(Ok(canvas_patch_update_message(&patch, include_snapshot_state)))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if patch.closed {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
