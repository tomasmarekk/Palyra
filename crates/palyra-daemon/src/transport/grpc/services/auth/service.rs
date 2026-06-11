//! Auth-profile gRPC service implementation.
//!
//! Registry calls are run through blocking workers where needed so the tonic
//! async runtime stays responsive while preserving profile authorization gates.

use std::sync::Arc;

use palyra_common::CANONICAL_PROTOCOL_MAJOR;
use tonic::{metadata::MetadataMap, Request, Response, Status};
use tracing::warn;

use crate::{
    application::{
        auth::{
            auth_expiry_distribution_to_proto, auth_health_profile_to_proto,
            auth_health_summary_to_proto, auth_list_filter_from_proto, auth_profile_to_proto,
            auth_refresh_metrics_to_proto, auth_set_request_from_proto, map_auth_profile_error,
            record_auth_profile_deleted_journal_event, record_auth_profile_saved_journal_event,
            record_auth_refresh_journal_event,
        },
        service_authorization::authorize_auth_profile_action,
    },
    gateway::{require_supported_version, AuthRuntimeState, GatewayRuntimeState},
    transport::grpc::{
        auth::{authorize_metadata, GatewayAuthConfig, RequestContext},
        proto::palyra::auth::v1 as auth_v1,
    },
};

/// Tonic service adapter for auth profile and provider health operations.
#[derive(Clone)]
pub struct AuthServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    auth_runtime: Arc<AuthRuntimeState>,
}

impl AuthServiceImpl {
    /// Creates an auth service bound to gateway state, auth, and auth runtime.
    #[must_use]
    pub fn new(
        state: Arc<GatewayRuntimeState>,
        auth: GatewayAuthConfig,
        auth_runtime: Arc<AuthRuntimeState>,
    ) -> Self {
        Self { state, auth, auth_runtime }
    }

    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth, method).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "auth rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[tonic::async_trait]
impl auth_v1::auth_service_server::AuthService for AuthServiceImpl {
    async fn list_profiles(
        &self,
        request: Request<auth_v1::ListAuthProfilesRequest>,
    ) -> Result<Response<auth_v1::ListAuthProfilesResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListProfiles")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.list",
            "auth:profiles",
        )?;
        let filter = auth_list_filter_from_proto(payload)?;

        let auth_runtime = Arc::clone(&self.auth_runtime);
        let page = tokio::task::spawn_blocking(move || {
            auth_runtime.registry().list_profiles(filter).map_err(map_auth_profile_error)
        })
        .await
        .map_err(|_| Status::internal("auth list worker panicked"))??;

        Ok(Response::new(auth_v1::ListAuthProfilesResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profiles: page.profiles.iter().map(auth_profile_to_proto).collect(),
            next_after_profile_id: page.next_after_profile_id.unwrap_or_default(),
        }))
    }

    async fn get_profile(
        &self,
        request: Request<auth_v1::GetAuthProfileRequest>,
    ) -> Result<Response<auth_v1::GetAuthProfileResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetProfile")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let profile_id = payload.profile_id.trim().to_owned();
        if profile_id.is_empty() {
            return Err(Status::invalid_argument("profile_id is required"));
        }
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.get",
            format!("auth:profile:{profile_id}").as_str(),
        )?;
        let auth_runtime = Arc::clone(&self.auth_runtime);
        let profile = tokio::task::spawn_blocking(move || {
            auth_runtime.registry().get_profile(profile_id.as_str()).map_err(map_auth_profile_error)
        })
        .await
        .map_err(|_| Status::internal("auth get worker panicked"))??;
        let profile = profile.ok_or_else(|| Status::not_found("auth profile not found"))?;

        Ok(Response::new(auth_v1::GetAuthProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(auth_profile_to_proto(&profile)),
        }))
    }

    async fn set_profile(
        &self,
        request: Request<auth_v1::SetAuthProfileRequest>,
    ) -> Result<Response<auth_v1::SetAuthProfileResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "SetProfile")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let profile =
            payload.profile.ok_or_else(|| Status::invalid_argument("profile is required"))?;
        let set_request = auth_set_request_from_proto(profile)?;
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.set",
            format!("auth:profile:{}", set_request.profile_id).as_str(),
        )?;
        let auth_runtime = Arc::clone(&self.auth_runtime);
        let saved = tokio::task::spawn_blocking(move || {
            auth_runtime.registry().set_profile(set_request).map_err(map_auth_profile_error)
        })
        .await
        .map_err(|_| Status::internal("auth set worker panicked"))??;
        record_auth_profile_saved_journal_event(&self.state, &context, &saved).await?;

        Ok(Response::new(auth_v1::SetAuthProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(auth_profile_to_proto(&saved)),
        }))
    }

    async fn delete_profile(
        &self,
        request: Request<auth_v1::DeleteAuthProfileRequest>,
    ) -> Result<Response<auth_v1::DeleteAuthProfileResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteProfile")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let profile_id = payload.profile_id.trim().to_owned();
        if profile_id.is_empty() {
            return Err(Status::invalid_argument("profile_id is required"));
        }
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.delete",
            format!("auth:profile:{profile_id}").as_str(),
        )?;
        let auth_runtime = Arc::clone(&self.auth_runtime);
        let profile_id_for_delete = profile_id.clone();
        let (deleted_profile, deleted) = tokio::task::spawn_blocking(move || {
            let existing = auth_runtime
                .registry()
                .get_profile(profile_id_for_delete.as_str())
                .map_err(map_auth_profile_error)?;
            let deleted = auth_runtime
                .registry()
                .delete_profile(profile_id_for_delete.as_str())
                .map_err(map_auth_profile_error)?;
            Ok::<_, Status>((existing, deleted))
        })
        .await
        .map_err(|_| Status::internal("auth delete worker panicked"))??;
        if deleted {
            record_auth_profile_deleted_journal_event(
                &self.state,
                &context,
                profile_id.as_str(),
                deleted_profile.as_ref(),
            )
            .await?;
        }

        Ok(Response::new(auth_v1::DeleteAuthProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted,
        }))
    }

    async fn get_health(
        &self,
        request: Request<auth_v1::GetAuthHealthRequest>,
    ) -> Result<Response<auth_v1::GetAuthHealthResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetHealth")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_auth_profile_action(
            context.principal.as_str(),
            "auth.profile.health",
            "auth:health",
        )?;
        let include_profiles = payload.include_profiles;
        let (report, outcomes, refresh_metrics) = self
            .auth_runtime
            .refresh_health_report(Arc::clone(&self.state), payload.agent_id)
            .await?;

        for outcome in outcomes {
            record_auth_refresh_journal_event(&self.state, &context, &outcome).await?;
        }

        Ok(Response::new(auth_v1::GetAuthHealthResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            summary: Some(auth_health_summary_to_proto(&report.summary)),
            expiry_distribution: Some(auth_expiry_distribution_to_proto(
                &report.expiry_distribution,
            )),
            profiles: if include_profiles {
                report.profiles.iter().map(auth_health_profile_to_proto).collect()
            } else {
                Vec::new()
            },
            refresh_metrics: Some(auth_refresh_metrics_to_proto(&refresh_metrics)),
        }))
    }
}
