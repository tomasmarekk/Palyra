//! Memory gRPC service implementation.
//!
//! The service validates memory scope at the protobuf boundary and delegates
//! storage/search/purge operations to the runtime journal-backed memory layer.

use std::sync::Arc;

use palyra_common::{validate_canonical_id, CANONICAL_PROTOCOL_MAJOR};
use tonic::{metadata::MetadataMap, Request, Response, Status};
use tracing::warn;
use ulid::Ulid;

use crate::{
    application::{
        memory::{
            enforce_memory_item_delete_scope, enforce_memory_item_scope, memory_item_message,
            memory_search_hit_message, memory_source_from_proto, resolve_memory_channel_scope,
        },
        service_authorization::{authorize_memory_action, authorize_memory_purge_action},
    },
    gateway::{
        canonical_id, non_empty, optional_canonical_id, require_supported_version,
        GatewayRuntimeState, MAX_MEMORY_SEARCH_TOP_K,
    },
    journal::{MemoryItemCreateRequest, MemoryPurgeRequest, MemorySearchRequest},
    transport::grpc::{
        auth::{authorize_metadata, GatewayAuthConfig, RequestContext},
        proto::palyra::memory::v1 as memory_v1,
    },
};

/// Tonic service adapter for memory ingest, search, get, and purge operations.
#[derive(Clone)]
pub struct MemoryServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl MemoryServiceImpl {
    /// Creates a memory service bound to shared gateway state and auth.
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
            warn!(method, error = %error, "memory rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[tonic::async_trait]
impl memory_v1::memory_service_server::MemoryService for MemoryServiceImpl {
    async fn ingest_memory(
        &self,
        request: Request<memory_v1::IngestMemoryRequest>,
    ) -> Result<Response<memory_v1::IngestMemoryResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "IngestMemory")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_memory_action(context.principal.as_str(), "memory.ingest", "memory:item")?;

        let source = memory_source_from_proto(payload.source)?;
        let channel =
            resolve_memory_channel_scope(context.channel.as_deref(), non_empty(payload.channel))?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let confidence = if payload.confidence == 0.0 {
            None
        } else if payload.confidence.is_finite() && (0.0..=1.0).contains(&payload.confidence) {
            Some(payload.confidence)
        } else {
            return Err(Status::invalid_argument(
                "memory confidence must be a finite value in range 0.0..=1.0",
            ));
        };
        let ttl_unix_ms = if payload.ttl_unix_ms > 0 { Some(payload.ttl_unix_ms) } else { None };

        let created = self
            .state
            .ingest_memory_item(MemoryItemCreateRequest {
                memory_id: Ulid::generate().to_string(),
                principal: context.principal,
                channel,
                session_id,
                source,
                content_text: payload.content_text,
                tags: payload.tags,
                confidence,
                ttl_unix_ms,
            })
            .await?;
        Ok(Response::new(memory_v1::IngestMemoryResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            item: Some(memory_item_message(&created)),
        }))
    }

    async fn search_memory(
        &self,
        request: Request<memory_v1::SearchMemoryRequest>,
    ) -> Result<Response<memory_v1::SearchMemoryResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "SearchMemory")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;

        let channel =
            resolve_memory_channel_scope(context.channel.as_deref(), non_empty(payload.channel))?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let resource = if let Some(session_id) = session_id.as_deref() {
            format!("memory:session:{session_id}")
        } else if let Some(channel) = channel.as_deref() {
            format!("memory:channel:{channel}")
        } else {
            "memory:principal".to_owned()
        };
        authorize_memory_action(context.principal.as_str(), "memory.search", resource.as_str())?;

        if !payload.min_score.is_finite() || payload.min_score < 0.0 || payload.min_score > 1.0 {
            return Err(Status::invalid_argument(
                "memory min_score must be a finite value in range 0.0..=1.0",
            ));
        }
        let sources = payload
            .sources
            .into_iter()
            .map(memory_source_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let top_k = if payload.top_k == 0 {
            None
        } else {
            Some((payload.top_k as usize).clamp(1, MAX_MEMORY_SEARCH_TOP_K))
        };

        let hits = self
            .state
            .search_memory(MemorySearchRequest {
                principal: context.principal,
                channel,
                session_id,
                query: payload.query,
                top_k: top_k.unwrap_or(8),
                min_score: payload.min_score,
                tags: payload.tags,
                sources,
            })
            .await?;
        Ok(Response::new(memory_v1::SearchMemoryResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            hits: hits
                .iter()
                .map(|hit| memory_search_hit_message(hit, payload.include_score_breakdown))
                .collect(),
        }))
    }

    async fn get_memory_item(
        &self,
        request: Request<memory_v1::GetMemoryItemRequest>,
    ) -> Result<Response<memory_v1::GetMemoryItemResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetMemoryItem")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let memory_id = canonical_id(payload.memory_id, "memory_id")?;
        authorize_memory_action(
            context.principal.as_str(),
            "memory.get",
            format!("memory:{memory_id}").as_str(),
        )?;
        let item = self
            .state
            .memory_item(memory_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("memory item not found: {memory_id}")))?;
        enforce_memory_item_scope(&item, context.principal.as_str(), context.channel.as_deref())?;
        Ok(Response::new(memory_v1::GetMemoryItemResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            item: Some(memory_item_message(&item)),
        }))
    }

    async fn delete_memory_item(
        &self,
        request: Request<memory_v1::DeleteMemoryItemRequest>,
    ) -> Result<Response<memory_v1::DeleteMemoryItemResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteMemoryItem")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let memory_id = canonical_id(payload.memory_id, "memory_id")?;
        authorize_memory_action(
            context.principal.as_str(),
            "memory.delete",
            format!("memory:{memory_id}").as_str(),
        )?;
        let delete_channel = context.channel;
        if let Some(item) = self.state.memory_item(memory_id.clone()).await? {
            enforce_memory_item_delete_scope(
                &item,
                context.principal.as_str(),
                delete_channel.as_deref(),
            )?;
        }
        let deleted =
            self.state.delete_memory_item(memory_id, context.principal, delete_channel).await?;
        Ok(Response::new(memory_v1::DeleteMemoryItemResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted,
        }))
    }

    async fn list_memory_items(
        &self,
        request: Request<memory_v1::ListMemoryItemsRequest>,
    ) -> Result<Response<memory_v1::ListMemoryItemsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListMemoryItems")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_memory_action(context.principal.as_str(), "memory.list", "memory:items")?;
        let after_memory_id = non_empty(payload.after_memory_ulid);
        if let Some(after) = after_memory_id.as_deref() {
            validate_canonical_id(after).map_err(|_| {
                Status::invalid_argument("after_memory_ulid must be a canonical ULID")
            })?;
        }
        let channel =
            resolve_memory_channel_scope(context.channel.as_deref(), non_empty(payload.channel))?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let sources = payload
            .sources
            .into_iter()
            .map(memory_source_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let (items, next_after_memory_id) = self
            .state
            .list_memory_items(
                after_memory_id,
                if payload.limit == 0 { None } else { Some(payload.limit as usize) },
                context.principal,
                channel,
                session_id,
                payload.tags,
                sources,
            )
            .await?;
        Ok(Response::new(memory_v1::ListMemoryItemsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            items: items.iter().map(memory_item_message).collect(),
            next_after_memory_ulid: next_after_memory_id.unwrap_or_default(),
        }))
    }

    async fn purge_memory(
        &self,
        request: Request<memory_v1::PurgeMemoryRequest>,
    ) -> Result<Response<memory_v1::PurgeMemoryResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "PurgeMemory")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_memory_purge_action(
            context.principal.as_str(),
            "memory.purge",
            "memory:items",
            payload.user_confirmed,
        )?;
        let channel =
            resolve_memory_channel_scope(context.channel.as_deref(), non_empty(payload.channel))?;
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        if !payload.purge_all_principal && channel.is_none() && session_id.is_none() {
            return Err(Status::invalid_argument(
                "purge request requires purge_all_principal=true or a channel/session scope",
            ));
        }

        let deleted_count = self
            .state
            .purge_memory(MemoryPurgeRequest {
                principal: context.principal,
                channel,
                session_id,
                purge_all_principal: payload.purge_all_principal,
            })
            .await?;
        Ok(Response::new(memory_v1::PurgeMemoryResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted_count,
        }))
    }
}
