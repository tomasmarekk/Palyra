//! Cron gRPC service implementation.
//!
//! Methods validate schedule payloads at the transport boundary, enforce owner
//! and authorization rules, then wake the scheduler when job state changes.

use std::sync::Arc;

use palyra_common::CANONICAL_PROTOCOL_MAJOR;
use tokio::sync::Notify;
use tonic::{metadata::MetadataMap, Request, Response, Status};
use tracing::warn;
use ulid::Ulid;

use crate::{
    application::service_authorization::authorize_cron_action,
    cron::{
        ensure_archived_objective_allows_cron_job_enable, normalize_schedule, trigger_job_now,
        CronTimezoneMode,
    },
    gateway::{
        canonical_id, cron_concurrency_from_proto, cron_job_message, cron_misfire_from_proto,
        cron_retry_from_proto, cron_run_message, cron_run_status_to_proto, current_unix_ms_status,
        enforce_cron_job_owner, non_empty, require_supported_version,
        resolve_cron_job_channel_for_create, validate_cron_jitter_ms,
        validate_cron_job_channel_for_update, validate_cron_job_name,
        validate_cron_job_owner_principal, validate_cron_job_owner_principal_for_update,
        validate_cron_job_prompt, GatewayRuntimeState,
    },
    journal::{CronJobCreateRequest, CronJobUpdatePatch},
    transport::grpc::{
        auth::{authorize_metadata, GatewayAuthConfig, RequestContext},
        proto::palyra::{common::v1 as common_v1, cron::v1 as cron_v1},
    },
};

/// Tonic service adapter for cron job and run operations.
#[derive(Clone)]
pub struct CronServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
    grpc_url: String,
    scheduler_wake: Arc<Notify>,
    cron_timezone_mode: CronTimezoneMode,
}

impl CronServiceImpl {
    /// Creates a cron service bound to scheduler state and timezone policy.
    #[must_use]
    pub fn new(
        state: Arc<GatewayRuntimeState>,
        auth: GatewayAuthConfig,
        grpc_url: String,
        scheduler_wake: Arc<Notify>,
        cron_timezone_mode: CronTimezoneMode,
    ) -> Self {
        Self { state, auth, grpc_url, scheduler_wake, cron_timezone_mode }
    }

    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth, method).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "cron rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[tonic::async_trait]
impl cron_v1::cron_service_server::CronService for CronServiceImpl {
    async fn create_job(
        &self,
        request: Request<cron_v1::CreateJobRequest>,
    ) -> Result<Response<cron_v1::CreateJobResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "CreateJob")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_cron_action(context.principal.as_str(), "cron.create", "cron:job")?;

        let now_unix_ms = current_unix_ms_status()?;
        let schedule = normalize_schedule(payload.schedule, now_unix_ms, self.cron_timezone_mode)?;
        let name = validate_cron_job_name(payload.name)?;
        let prompt = validate_cron_job_prompt(payload.prompt)?;
        let owner_principal =
            validate_cron_job_owner_principal(context.principal.as_str(), payload.owner_principal)?;
        let channel =
            resolve_cron_job_channel_for_create(context.channel.as_deref(), payload.channel)?;
        let session_key = non_empty(payload.session_key);
        let session_label = non_empty(payload.session_label);
        let concurrency_policy = cron_concurrency_from_proto(payload.concurrency_policy)?;
        let retry_policy = cron_retry_from_proto(payload.retry_policy)?;
        let misfire_policy = cron_misfire_from_proto(payload.misfire_policy)?;
        let jitter_ms = validate_cron_jitter_ms(payload.jitter_ms)?;

        let job = self
            .state
            .create_cron_job(CronJobCreateRequest {
                job_id: Ulid::generate().to_string(),
                name,
                prompt,
                owner_principal,
                channel,
                session_key,
                session_label,
                workdir: None,
                schedule_type: schedule.schedule_type,
                schedule_payload_json: schedule.schedule_payload_json,
                enabled: payload.enabled,
                concurrency_policy,
                retry_policy,
                misfire_policy,
                jitter_ms,
                next_run_at_unix_ms: schedule.next_run_at_unix_ms,
            })
            .await?;
        self.scheduler_wake.notify_one();
        Ok(Response::new(cron_v1::CreateJobResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            job: Some(cron_job_message(&job)?),
        }))
    }

    async fn update_job(
        &self,
        request: Request<cron_v1::UpdateJobRequest>,
    ) -> Result<Response<cron_v1::UpdateJobResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "UpdateJob")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.update",
            format!("cron:{job_id}").as_str(),
        )?;
        let existing_job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), existing_job.owner_principal.as_str())?;

        let now_unix_ms = current_unix_ms_status()?;
        let mut patch = CronJobUpdatePatch::default();
        if let Some(name) = payload.name {
            patch.name = Some(validate_cron_job_name(name)?);
        }
        if let Some(prompt) = payload.prompt {
            patch.prompt = Some(validate_cron_job_prompt(prompt)?);
        }
        if let Some(owner_principal) = payload.owner_principal {
            patch.owner_principal = Some(validate_cron_job_owner_principal_for_update(
                context.principal.as_str(),
                owner_principal,
            )?);
        }
        if let Some(channel) = payload.channel {
            patch.channel =
                validate_cron_job_channel_for_update(context.channel.as_deref(), channel)?;
        }
        if let Some(session_key) = payload.session_key {
            patch.session_key = Some(non_empty(session_key));
        }
        if let Some(session_label) = payload.session_label {
            patch.session_label = Some(non_empty(session_label));
        }
        if payload.schedule.is_some() {
            let schedule =
                normalize_schedule(payload.schedule, now_unix_ms, self.cron_timezone_mode)?;
            patch.schedule_type = Some(schedule.schedule_type);
            patch.schedule_payload_json = Some(schedule.schedule_payload_json);
            patch.next_run_at_unix_ms = Some(schedule.next_run_at_unix_ms);
        }
        if let Some(enabled) = payload.enabled {
            patch.enabled = Some(enabled);
            if enabled {
                ensure_archived_objective_allows_cron_job_enable(
                    self.state.as_ref(),
                    existing_job.job_id.as_str(),
                )?;
                if patch.next_run_at_unix_ms.is_none() {
                    patch.next_run_at_unix_ms = Some(crate::cron::next_run_at_for_enabled_state(
                        &existing_job,
                        true,
                        now_unix_ms,
                    )?);
                }
            } else {
                patch.next_run_at_unix_ms = Some(None);
            }
        }
        if let Some(concurrency_policy) = payload.concurrency_policy {
            patch.concurrency_policy = Some(cron_concurrency_from_proto(concurrency_policy)?);
        }
        if let Some(retry_policy) = payload.retry_policy {
            patch.retry_policy = Some(cron_retry_from_proto(Some(retry_policy))?);
        }
        if let Some(misfire_policy) = payload.misfire_policy {
            patch.misfire_policy = Some(cron_misfire_from_proto(misfire_policy)?);
        }
        if let Some(jitter_ms) = payload.jitter_ms {
            patch.jitter_ms = Some(validate_cron_jitter_ms(jitter_ms)?);
        }

        let updated = self.state.update_cron_job(job_id, patch).await?;
        self.scheduler_wake.notify_one();
        Ok(Response::new(cron_v1::UpdateJobResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            job: Some(cron_job_message(&updated)?),
        }))
    }

    async fn delete_job(
        &self,
        request: Request<cron_v1::DeleteJobRequest>,
    ) -> Result<Response<cron_v1::DeleteJobResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "DeleteJob")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.delete",
            format!("cron:{job_id}").as_str(),
        )?;
        let job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        let deleted = self.state.delete_cron_job(job_id).await?;
        self.scheduler_wake.notify_one();
        Ok(Response::new(cron_v1::DeleteJobResponse { v: CANONICAL_PROTOCOL_MAJOR, deleted }))
    }

    async fn get_job(
        &self,
        request: Request<cron_v1::GetJobRequest>,
    ) -> Result<Response<cron_v1::GetJobResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetJob")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.get",
            format!("cron:{job_id}").as_str(),
        )?;
        let job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        Ok(Response::new(cron_v1::GetJobResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            job: Some(cron_job_message(&job)?),
        }))
    }

    async fn list_jobs(
        &self,
        request: Request<cron_v1::ListJobsRequest>,
    ) -> Result<Response<cron_v1::ListJobsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListJobs")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        authorize_cron_action(context.principal.as_str(), "cron.list", "cron:jobs")?;
        if let Some(owner_principal) = payload.owner_principal.as_deref() {
            if owner_principal != context.principal.as_str() {
                return Err(Status::permission_denied(
                    "owner_principal must match authenticated principal",
                ));
            }
        }

        let (jobs, next_after_job_ulid) = self
            .state
            .list_cron_jobs(
                non_empty(payload.after_job_ulid),
                Some(payload.limit as usize),
                payload.enabled,
                Some(context.principal.clone()),
                payload.channel,
            )
            .await?;
        let jobs = jobs.iter().map(cron_job_message).collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(cron_v1::ListJobsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            jobs,
            next_after_job_ulid: next_after_job_ulid.unwrap_or_default(),
        }))
    }

    async fn run_job_now(
        &self,
        request: Request<cron_v1::RunJobNowRequest>,
    ) -> Result<Response<cron_v1::RunJobNowResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "RunJobNow")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.run",
            format!("cron:{job_id}").as_str(),
        )?;
        let job = self
            .state
            .cron_job(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        if !job.enabled {
            return Err(Status::failed_precondition("disabled cron jobs cannot be run manually"));
        }
        let outcome = trigger_job_now(
            Arc::clone(&self.state),
            self.auth.clone(),
            self.grpc_url.clone(),
            job,
            Arc::clone(&self.scheduler_wake),
        )
        .await?;
        Ok(Response::new(cron_v1::RunJobNowResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            run_id: outcome.run_id.map(|ulid| common_v1::CanonicalId { ulid }),
            status: cron_run_status_to_proto(outcome.status),
            message: outcome.message,
        }))
    }

    async fn list_job_runs(
        &self,
        request: Request<cron_v1::ListJobRunsRequest>,
    ) -> Result<Response<cron_v1::ListJobRunsResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ListJobRuns")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let job_id = canonical_id(payload.job_id, "job_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.logs",
            format!("cron:{job_id}").as_str(),
        )?;
        let job = self
            .state
            .cron_job_for_history(job_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron job not found: {job_id}")))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        let (runs, next_after_run_ulid) = self
            .state
            .list_cron_runs(
                Some(job_id),
                non_empty(payload.after_run_ulid),
                Some(payload.limit as usize),
            )
            .await?;
        Ok(Response::new(cron_v1::ListJobRunsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            runs: runs.iter().map(cron_run_message).collect(),
            next_after_run_ulid: next_after_run_ulid.unwrap_or_default(),
        }))
    }

    async fn get_job_run(
        &self,
        request: Request<cron_v1::GetJobRunRequest>,
    ) -> Result<Response<cron_v1::GetJobRunResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "GetJobRun")?;
        let payload = request.into_inner();
        require_supported_version(payload.v)?;
        let run_id = canonical_id(payload.run_id, "run_id")?;
        authorize_cron_action(
            context.principal.as_str(),
            "cron.logs",
            format!("cron:run:{run_id}").as_str(),
        )?;
        let run = self
            .state
            .cron_run(run_id.clone())
            .await?
            .ok_or_else(|| Status::not_found(format!("cron run not found: {run_id}")))?;
        let job = self
            .state
            .cron_job_for_history(run.job_id.clone())
            .await?
            .ok_or_else(|| Status::internal("cron job for run not found"))?;
        enforce_cron_job_owner(context.principal.as_str(), job.owner_principal.as_str())?;
        Ok(Response::new(cron_v1::GetJobRunResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            run: Some(cron_run_message(&run)),
        }))
    }
}
