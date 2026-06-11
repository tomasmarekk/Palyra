//! Vault gRPC service implementation.
//!
//! Vault calls enforce per-principal rate limits and scope access before any
//! secret value is read, written, listed, or deleted.

use std::sync::Arc;

use palyra_common::CANONICAL_PROTOCOL_MAJOR;
use tonic::{metadata::MetadataMap, Request, Response, Status};
use tracing::warn;

use crate::{
    application::service_authorization::authorize_vault_action,
    gateway::{
        enforce_vault_scope_access, parse_vault_scope, read_vault_secret_for_context,
        record_vault_journal_event, require_supported_version, vault_secret_metadata_message,
        GatewayRuntimeState, MAX_VAULT_LIST_RESULTS, MAX_VAULT_SECRET_BYTES,
    },
    transport::grpc::{
        auth::{authorize_metadata, GatewayAuthConfig, RequestContext},
        proto::palyra::gateway::v1 as gateway_v1,
    },
};

/// Tonic service adapter for scoped secret operations.
#[derive(Clone)]
pub struct VaultServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl VaultServiceImpl {
    /// Creates a vault service bound to shared gateway state and auth.
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
            warn!(method, error = %error, "vault rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[tonic::async_trait]
impl gateway_v1::vault_service_server::VaultService for VaultServiceImpl {
    async fn put_secret(
        &self,
        request: Request<gateway_v1::PutSecretRequest>,
    ) -> Result<Response<gateway_v1::PutSecretResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "PutSecret")?;
        if !self.state.consume_vault_rate_limit(context.principal.as_str()) {
            self.state.record_vault_rate_limited_request();
            return Err(Status::resource_exhausted("vault rate limit exceeded"));
        }
        self.state.record_vault_put_request();

        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        if payload.value.len() > MAX_VAULT_SECRET_BYTES {
            return Err(Status::invalid_argument(format!(
                "secret value exceeds maximum bytes ({} > {MAX_VAULT_SECRET_BYTES})",
                payload.value.len()
            )));
        }
        let scope = parse_vault_scope(payload.scope.as_str())?;
        enforce_vault_scope_access(&scope, &context)?;
        let key = payload.key.trim().to_owned();
        authorize_vault_action(
            context.principal.as_str(),
            "vault.put",
            format!("secrets:{scope}:{key}").as_str(),
        )?;
        let metadata =
            self.state.vault_put_secret(scope.clone(), key.clone(), payload.value).await?;
        record_vault_journal_event(
            &self.state,
            &context,
            "secret.updated",
            "vault.put",
            &scope,
            Some(key.as_str()),
            Some(metadata.value_bytes),
        )
        .await?;
        Ok(Response::new(gateway_v1::PutSecretResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            secret: Some(vault_secret_metadata_message(&metadata)),
        }))
    }

    async fn get_secret(
        &self,
        request: Request<gateway_v1::GetSecretRequest>,
    ) -> Result<Response<gateway_v1::GetSecretResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetSecret")?;
        if !self.state.consume_vault_rate_limit(context.principal.as_str()) {
            self.state.record_vault_rate_limited_request();
            return Err(Status::resource_exhausted("vault rate limit exceeded"));
        }
        self.state.record_vault_get_request();
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let scope = parse_vault_scope(payload.scope.as_str())?;
        let key = payload.key.trim().to_owned();
        let value = read_vault_secret_for_context(&self.state, &context, scope, key, false).await?;
        Ok(Response::new(gateway_v1::GetSecretResponse { v: CANONICAL_PROTOCOL_MAJOR, value }))
    }

    async fn delete_secret(
        &self,
        request: Request<gateway_v1::DeleteSecretRequest>,
    ) -> Result<Response<gateway_v1::DeleteSecretResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteSecret")?;
        if !self.state.consume_vault_rate_limit(context.principal.as_str()) {
            self.state.record_vault_rate_limited_request();
            return Err(Status::resource_exhausted("vault rate limit exceeded"));
        }
        self.state.record_vault_delete_request();

        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let scope = parse_vault_scope(payload.scope.as_str())?;
        enforce_vault_scope_access(&scope, &context)?;
        let key = payload.key.trim().to_owned();
        authorize_vault_action(
            context.principal.as_str(),
            "vault.delete",
            format!("secrets:{scope}:{key}").as_str(),
        )?;
        let deleted = self.state.vault_delete_secret(scope.clone(), key.clone()).await?;
        if deleted {
            record_vault_journal_event(
                &self.state,
                &context,
                "secret.deleted",
                "vault.delete",
                &scope,
                Some(key.as_str()),
                None,
            )
            .await?;
        }
        Ok(Response::new(gateway_v1::DeleteSecretResponse { v: CANONICAL_PROTOCOL_MAJOR, deleted }))
    }

    async fn list_secrets(
        &self,
        request: Request<gateway_v1::ListSecretsRequest>,
    ) -> Result<Response<gateway_v1::ListSecretsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListSecrets")?;
        if !self.state.consume_vault_rate_limit(context.principal.as_str()) {
            self.state.record_vault_rate_limited_request();
            return Err(Status::resource_exhausted("vault rate limit exceeded"));
        }
        self.state.record_vault_list_request();

        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let scope = parse_vault_scope(payload.scope.as_str())?;
        enforce_vault_scope_access(&scope, &context)?;
        authorize_vault_action(
            context.principal.as_str(),
            "vault.list",
            format!("secrets:{scope}").as_str(),
        )?;
        let mut secrets = self.state.vault_list_secrets(scope.clone()).await?;
        if secrets.len() > MAX_VAULT_LIST_RESULTS {
            secrets.truncate(MAX_VAULT_LIST_RESULTS);
        }
        record_vault_journal_event(
            &self.state,
            &context,
            "secret.listed",
            "vault.list",
            &scope,
            None,
            Some(secrets.len()),
        )
        .await?;
        Ok(Response::new(gateway_v1::ListSecretsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            secrets: secrets.iter().map(vault_secret_metadata_message).collect(),
        }))
    }
}
