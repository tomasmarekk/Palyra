use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use axum::http::{header::AUTHORIZATION, HeaderMap};
use palyra_common::{build_metadata, validate_canonical_id, CANONICAL_PROTOCOL_MAJOR};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};
use tracing::{info, warn};
use ulid::Ulid;

use crate::{
    journal::{
        JournalAppendRequest, JournalError, JournalEventRecord, JournalStore,
        OrchestratorCancelRequest, OrchestratorRunStartRequest, OrchestratorRunStatusSnapshot,
        OrchestratorSessionUpsertRequest, OrchestratorTapeAppendRequest, OrchestratorTapeRecord,
        OrchestratorUsageDelta,
    },
    model_provider::{
        ModelProvider, ProviderError, ProviderEvent, ProviderRequest, ProviderStatusSnapshot,
    },
    orchestrator::{is_cancel_command, RunLifecycleState, RunStateMachine, RunTransition},
};

pub mod proto {
    pub mod palyra {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("palyra.common.v1");
            }
        }

        pub mod gateway {
            pub mod v1 {
                tonic::include_proto!("palyra.gateway.v1");
            }
        }
    }
}

use proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};

pub const HEADER_PRINCIPAL: &str = "x-palyra-principal";
pub const HEADER_DEVICE_ID: &str = "x-palyra-device-id";
pub const HEADER_CHANNEL: &str = "x-palyra-channel";
const MAX_JOURNAL_RECENT_EVENTS: usize = 100;
const JOURNAL_WRITE_LATENCY_BUDGET_MS: u128 = 25;
const SENSITIVE_TOOLS_DENY_REASON: &str =
    "allow_sensitive_tools=true is denied by default and requires explicit approvals";
const CANCELLED_REASON: &str = "cancelled by request";

#[derive(Debug, Clone)]
pub struct GatewayRuntimeConfigSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
    pub orchestrator_runloop_v1_enabled: bool,
    pub node_rpc_mtls_required: bool,
    pub admin_auth_required: bool,
}

#[derive(Debug, Clone)]
pub struct GatewayJournalConfigSnapshot {
    pub db_path: PathBuf,
    pub hash_chain_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct GatewayAuthConfig {
    pub require_auth: bool,
    pub admin_token: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct RequestContext {
    pub principal: String,
    pub device_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
}

pub struct GatewayRuntimeState {
    started_at: Instant,
    build: BuildSnapshot,
    config: GatewayRuntimeConfigSnapshot,
    journal_config: GatewayJournalConfigSnapshot,
    counters: RuntimeCounters,
    journal_store: JournalStore,
    revoked_certificate_count: usize,
    model_provider: Arc<dyn ModelProvider>,
}

#[derive(Debug)]
struct RuntimeCounters {
    run_stream_requests: AtomicU64,
    append_event_requests: AtomicU64,
    admin_status_requests: AtomicU64,
    denied_requests: AtomicU64,
    journal_events: AtomicU64,
    journal_persist_failures: AtomicU64,
    journal_redacted_events: AtomicU64,
    orchestrator_runs_started: AtomicU64,
    orchestrator_runs_completed: AtomicU64,
    orchestrator_runs_cancelled: AtomicU64,
    orchestrator_cancel_requests: AtomicU64,
    orchestrator_tape_events: AtomicU64,
    model_provider_requests: AtomicU64,
    model_provider_failures: AtomicU64,
    model_provider_retry_attempts: AtomicU64,
    model_provider_circuit_open_rejections: AtomicU64,
}

#[derive(Debug, Clone, Serialize)]
struct BuildSnapshot {
    version: String,
    git_hash: String,
    build_profile: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct GatewayStatusSnapshot {
    pub service: &'static str,
    pub status: &'static str,
    pub version: String,
    pub git_hash: String,
    pub build_profile: String,
    pub uptime_seconds: u64,
    pub transport: TransportSnapshot,
    pub security: SecuritySnapshot,
    pub storage: StorageSnapshot,
    pub model_provider: ProviderStatusSnapshot,
    pub counters: CountersSnapshot,
    pub request_context: RequestContext,
}

#[derive(Debug, Clone, Serialize)]
pub struct TransportSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SecuritySnapshot {
    pub deny_by_default: bool,
    pub admin_auth_required: bool,
    pub admin_token_configured: bool,
    pub orchestrator_runloop_v1_enabled: bool,
    pub node_rpc_mtls_required: bool,
    pub revoked_certificate_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageSnapshot {
    pub journal_db_path: String,
    pub journal_hash_chain_enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latest_event_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CountersSnapshot {
    pub run_stream_requests: u64,
    pub append_event_requests: u64,
    pub admin_status_requests: u64,
    pub denied_requests: u64,
    pub journal_events: u64,
    pub journal_persist_failures: u64,
    pub journal_redacted_events: u64,
    pub orchestrator_runs_started: u64,
    pub orchestrator_runs_completed: u64,
    pub orchestrator_runs_cancelled: u64,
    pub orchestrator_cancel_requests: u64,
    pub orchestrator_tape_events: u64,
    pub model_provider_requests: u64,
    pub model_provider_failures: u64,
    pub model_provider_retry_attempts: u64,
    pub model_provider_circuit_open_rejections: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct JournalRecentSnapshot {
    pub total_events: u64,
    pub hash_chain_enabled: bool,
    pub events: Vec<JournalEventRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunTapeSnapshot {
    pub run_id: String,
    pub events: Vec<OrchestratorTapeRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunCancelSnapshot {
    pub run_id: String,
    pub cancel_requested: bool,
    pub reason: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthError {
    #[error("admin auth token is required but no token is configured")]
    MissingConfiguredToken,
    #[error("authorization header is missing or malformed")]
    InvalidAuthorizationHeader,
    #[error("authorization token is invalid")]
    InvalidToken,
    #[error("request context field '{0}' is required")]
    MissingContext(&'static str),
    #[error("request context field '{0}' cannot be empty")]
    EmptyContext(&'static str),
    #[error("request context device_id must be a canonical ULID")]
    InvalidDeviceId,
}

impl RuntimeCounters {
    fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            run_stream_requests: self.run_stream_requests.load(Ordering::Relaxed),
            append_event_requests: self.append_event_requests.load(Ordering::Relaxed),
            admin_status_requests: self.admin_status_requests.load(Ordering::Relaxed),
            denied_requests: self.denied_requests.load(Ordering::Relaxed),
            journal_events: self.journal_events.load(Ordering::Relaxed),
            journal_persist_failures: self.journal_persist_failures.load(Ordering::Relaxed),
            journal_redacted_events: self.journal_redacted_events.load(Ordering::Relaxed),
            orchestrator_runs_started: self.orchestrator_runs_started.load(Ordering::Relaxed),
            orchestrator_runs_completed: self.orchestrator_runs_completed.load(Ordering::Relaxed),
            orchestrator_runs_cancelled: self.orchestrator_runs_cancelled.load(Ordering::Relaxed),
            orchestrator_cancel_requests: self.orchestrator_cancel_requests.load(Ordering::Relaxed),
            orchestrator_tape_events: self.orchestrator_tape_events.load(Ordering::Relaxed),
            model_provider_requests: self.model_provider_requests.load(Ordering::Relaxed),
            model_provider_failures: self.model_provider_failures.load(Ordering::Relaxed),
            model_provider_retry_attempts: self
                .model_provider_retry_attempts
                .load(Ordering::Relaxed),
            model_provider_circuit_open_rejections: self
                .model_provider_circuit_open_rejections
                .load(Ordering::Relaxed),
        }
    }
}

impl GatewayRuntimeState {
    #[cfg(test)]
    pub fn new(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
    ) -> Result<Arc<Self>, JournalError> {
        let default_provider = crate::model_provider::build_model_provider(
            &crate::model_provider::ModelProviderConfig::default(),
        )
        .expect("default deterministic model provider should initialize");
        Self::new_with_provider(
            config,
            journal_config,
            journal_store,
            revoked_certificate_count,
            default_provider,
        )
    }

    pub fn new_with_provider(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
        model_provider: Arc<dyn ModelProvider>,
    ) -> Result<Arc<Self>, JournalError> {
        let build = build_metadata();
        let existing_events = journal_store.total_events()? as u64;
        Ok(Arc::new(Self {
            started_at: Instant::now(),
            build: BuildSnapshot {
                version: build.version.to_owned(),
                git_hash: build.git_hash.to_owned(),
                build_profile: build.build_profile.to_owned(),
            },
            config,
            journal_config,
            counters: RuntimeCounters {
                run_stream_requests: AtomicU64::new(0),
                append_event_requests: AtomicU64::new(0),
                admin_status_requests: AtomicU64::new(0),
                denied_requests: AtomicU64::new(0),
                journal_events: AtomicU64::new(existing_events),
                journal_persist_failures: AtomicU64::new(0),
                journal_redacted_events: AtomicU64::new(0),
                orchestrator_runs_started: AtomicU64::new(0),
                orchestrator_runs_completed: AtomicU64::new(0),
                orchestrator_runs_cancelled: AtomicU64::new(0),
                orchestrator_cancel_requests: AtomicU64::new(0),
                orchestrator_tape_events: AtomicU64::new(0),
                model_provider_requests: AtomicU64::new(0),
                model_provider_failures: AtomicU64::new(0),
                model_provider_retry_attempts: AtomicU64::new(0),
                model_provider_circuit_open_rejections: AtomicU64::new(0),
            },
            journal_store,
            revoked_certificate_count,
            model_provider,
        }))
    }

    pub fn record_denied(&self) {
        self.counters.denied_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_admin_status_request(&self) {
        self.counters.admin_status_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[allow(clippy::result_large_err)]
    fn record_journal_event_blocking(
        &self,
        request: &JournalAppendRequest,
    ) -> Result<crate::journal::JournalAppendOutcome, Status> {
        let outcome = match self.journal_store.append(request) {
            Ok(outcome) => outcome,
            Err(JournalError::DuplicateEventId { event_id }) => {
                return Err(Status::already_exists(format!(
                    "journal event already exists: {event_id}"
                )));
            }
            Err(error) => {
                self.counters.journal_persist_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Status::internal(format!(
                    "failed to persist journal event '{}': {error}",
                    request.event_id
                )));
            }
        };
        self.counters.journal_events.fetch_add(1, Ordering::Relaxed);
        if outcome.redacted {
            self.counters.journal_redacted_events.fetch_add(1, Ordering::Relaxed);
        }
        if outcome.write_duration.as_millis() > JOURNAL_WRITE_LATENCY_BUDGET_MS {
            warn!(
                event_id = %request.event_id,
                write_duration_ms = outcome.write_duration.as_millis(),
                budget_ms = JOURNAL_WRITE_LATENCY_BUDGET_MS,
                "journal write exceeded latency budget"
            );
        }
        Ok(outcome)
    }

    #[allow(clippy::result_large_err)]
    async fn record_journal_event(
        self: &Arc<Self>,
        request: JournalAppendRequest,
    ) -> Result<crate::journal::JournalAppendOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.record_journal_event_blocking(&request))
            .await
            .map_err(|_| Status::internal("journal write worker panicked"))?
    }

    pub fn status_snapshot(
        &self,
        context: RequestContext,
        auth_config: &GatewayAuthConfig,
    ) -> GatewayStatusSnapshot {
        let latest_event_hash = self.journal_store.latest_hash().ok().flatten();
        GatewayStatusSnapshot {
            service: "palyrad",
            status: "ok",
            version: self.build.version.clone(),
            git_hash: self.build.git_hash.clone(),
            build_profile: self.build.build_profile.clone(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            transport: TransportSnapshot {
                grpc_bind_addr: self.config.grpc_bind_addr.clone(),
                grpc_port: self.config.grpc_port,
                quic_bind_addr: self.config.quic_bind_addr.clone(),
                quic_port: self.config.quic_port,
                quic_enabled: self.config.quic_enabled,
            },
            security: SecuritySnapshot {
                deny_by_default: true,
                admin_auth_required: self.config.admin_auth_required,
                admin_token_configured: auth_config.admin_token.is_some(),
                orchestrator_runloop_v1_enabled: self.config.orchestrator_runloop_v1_enabled,
                node_rpc_mtls_required: self.config.node_rpc_mtls_required,
                revoked_certificate_count: self.revoked_certificate_count,
            },
            storage: StorageSnapshot {
                journal_db_path: self.journal_config.db_path.to_string_lossy().into_owned(),
                journal_hash_chain_enabled: self.journal_config.hash_chain_enabled,
                latest_event_hash,
            },
            model_provider: self.model_provider.status_snapshot(),
            counters: self.counters.snapshot(),
            request_context: context,
        }
    }

    #[allow(clippy::result_large_err)]
    pub async fn status_snapshot_async(
        self: &Arc<Self>,
        context: RequestContext,
        auth_config: GatewayAuthConfig,
    ) -> Result<GatewayStatusSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.status_snapshot(context, &auth_config))
            .await
            .map_err(|_| Status::internal("status snapshot worker panicked"))
    }

    #[allow(clippy::result_large_err)]
    fn recent_journal_snapshot_blocking(
        &self,
        limit: usize,
    ) -> Result<JournalRecentSnapshot, Status> {
        let limit = limit.clamp(1, MAX_JOURNAL_RECENT_EVENTS);
        let events = self.journal_store.recent(limit).map_err(|error| {
            Status::internal(format!("failed to load recent journal events: {error}"))
        })?;
        let total_events =
            self.journal_store.total_events().map_err(|error| {
                Status::internal(format!("failed to count journal events: {error}"))
            })? as u64;
        Ok(JournalRecentSnapshot {
            total_events,
            hash_chain_enabled: self.journal_config.hash_chain_enabled,
            events,
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn recent_journal_snapshot(
        self: &Arc<Self>,
        limit: usize,
    ) -> Result<JournalRecentSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.recent_journal_snapshot_blocking(limit))
            .await
            .map_err(|_| Status::internal("journal read worker panicked"))?
    }

    #[must_use]
    pub const fn is_orchestrator_runloop_enabled(&self) -> bool {
        self.config.orchestrator_runloop_v1_enabled
    }

    #[allow(clippy::result_large_err)]
    pub async fn execute_model_provider(
        self: &Arc<Self>,
        request: ProviderRequest,
    ) -> Result<crate::model_provider::ProviderResponse, Status> {
        self.counters.model_provider_requests.fetch_add(1, Ordering::Relaxed);
        match self.model_provider.complete(request).await {
            Ok(response) => {
                if response.retry_count > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(response.retry_count as u64, Ordering::Relaxed);
                }
                Ok(response)
            }
            Err(error) => {
                self.counters.model_provider_failures.fetch_add(1, Ordering::Relaxed);
                if error.retry_count() > 0 {
                    self.counters
                        .model_provider_retry_attempts
                        .fetch_add(error.retry_count() as u64, Ordering::Relaxed);
                }
                if error.is_circuit_open() {
                    self.counters
                        .model_provider_circuit_open_rejections
                        .fetch_add(1, Ordering::Relaxed);
                }
                Err(map_provider_error(error))
            }
        }
    }

    #[allow(clippy::result_large_err)]
    fn upsert_orchestrator_session_blocking(
        &self,
        request: &OrchestratorSessionUpsertRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .upsert_orchestrator_session(request)
            .map_err(|error| map_orchestrator_store_error("upsert orchestrator session", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn upsert_orchestrator_session(
        self: &Arc<Self>,
        request: OrchestratorSessionUpsertRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.upsert_orchestrator_session_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator session worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn start_orchestrator_run_blocking(
        &self,
        request: &OrchestratorRunStartRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .start_orchestrator_run(request)
            .map_err(|error| map_orchestrator_store_error("start orchestrator run", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn start_orchestrator_run(
        self: &Arc<Self>,
        request: OrchestratorRunStartRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.start_orchestrator_run_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator run worker panicked"))??;
        self.counters.orchestrator_runs_started.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn update_orchestrator_run_state_blocking(
        &self,
        run_id: &str,
        state: RunLifecycleState,
        error_message: Option<&str>,
    ) -> Result<(), Status> {
        self.journal_store
            .update_orchestrator_run_state(run_id, state, error_message)
            .map_err(|error| map_orchestrator_store_error("update orchestrator run state", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn update_orchestrator_run_state(
        self: &Arc<Self>,
        run_id: String,
        state: RunLifecycleState,
        error_message: Option<String>,
    ) -> Result<(), Status> {
        let state_ref = Arc::clone(self);
        let error_message_ref = error_message.clone();
        tokio::task::spawn_blocking(move || {
            state_ref.update_orchestrator_run_state_blocking(
                run_id.as_str(),
                state,
                error_message_ref.as_deref(),
            )
        })
        .await
        .map_err(|_| Status::internal("orchestrator run state worker panicked"))??;
        if state == RunLifecycleState::Done {
            self.counters.orchestrator_runs_completed.fetch_add(1, Ordering::Relaxed);
        } else if state == RunLifecycleState::Cancelled {
            self.counters.orchestrator_runs_cancelled.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn add_orchestrator_usage_blocking(
        &self,
        delta: &OrchestratorUsageDelta,
    ) -> Result<(), Status> {
        self.journal_store
            .add_orchestrator_usage(delta)
            .map_err(|error| map_orchestrator_store_error("update orchestrator usage", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn add_orchestrator_usage(
        self: &Arc<Self>,
        delta: OrchestratorUsageDelta,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.add_orchestrator_usage_blocking(&delta))
            .await
            .map_err(|_| Status::internal("orchestrator usage worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn append_orchestrator_tape_event_blocking(
        &self,
        request: &OrchestratorTapeAppendRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .append_orchestrator_tape_event(request)
            .map_err(|error| map_orchestrator_store_error("append orchestrator tape event", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn append_orchestrator_tape_event(
        self: &Arc<Self>,
        request: OrchestratorTapeAppendRequest,
    ) -> Result<(), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.append_orchestrator_tape_event_blocking(&request)
        })
        .await
        .map_err(|_| Status::internal("orchestrator tape worker panicked"))??;
        self.counters.orchestrator_tape_events.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn request_orchestrator_cancel_blocking(
        &self,
        request: &OrchestratorCancelRequest,
    ) -> Result<(), Status> {
        self.journal_store
            .request_orchestrator_cancel(request)
            .map_err(|error| map_orchestrator_store_error("request orchestrator cancel", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn request_orchestrator_cancel(
        self: &Arc<Self>,
        request: OrchestratorCancelRequest,
    ) -> Result<RunCancelSnapshot, Status> {
        let state = Arc::clone(self);
        let run_id = request.run_id.clone();
        let reason = request.reason.clone();
        tokio::task::spawn_blocking(move || state.request_orchestrator_cancel_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator cancel worker panicked"))??;
        self.counters.orchestrator_cancel_requests.fetch_add(1, Ordering::Relaxed);
        Ok(RunCancelSnapshot { run_id, cancel_requested: true, reason })
    }

    #[allow(clippy::result_large_err)]
    fn is_orchestrator_cancel_requested_blocking(&self, run_id: &str) -> Result<bool, Status> {
        self.journal_store
            .is_orchestrator_cancel_requested(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator cancel flag", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn is_orchestrator_cancel_requested(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<bool, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.is_orchestrator_cancel_requested_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator cancel read worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_run_status_snapshot_blocking(
        &self,
        run_id: &str,
    ) -> Result<Option<OrchestratorRunStatusSnapshot>, Status> {
        self.journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_run_status_snapshot(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<Option<OrchestratorRunStatusSnapshot>, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_run_status_snapshot_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator snapshot worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn orchestrator_tape_snapshot_blocking(&self, run_id: &str) -> Result<RunTapeSnapshot, Status> {
        let run_exists = self
            .journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))?
            .is_some();
        if !run_exists {
            return Err(Status::not_found(format!("orchestrator run not found: {run_id}")));
        }
        let events = self
            .journal_store
            .orchestrator_tape(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator tape", error))?;
        Ok(RunTapeSnapshot { run_id: run_id.to_owned(), events })
    }

    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_tape_snapshot(
        self: &Arc<Self>,
        run_id: String,
    ) -> Result<RunTapeSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_tape_snapshot_blocking(run_id.as_str())
        })
        .await
        .map_err(|_| Status::internal("orchestrator tape snapshot worker panicked"))?
    }
}

fn map_orchestrator_store_error(operation: &str, error: JournalError) -> Status {
    match error {
        JournalError::DuplicateRunId { run_id } => {
            Status::already_exists(format!("orchestrator run already exists: {run_id}"))
        }
        JournalError::DuplicateTapeSequence { run_id, seq } => Status::already_exists(format!(
            "orchestrator tape already contains seq={seq} for run {run_id}"
        )),
        JournalError::RunNotFound { run_id } => {
            Status::not_found(format!("orchestrator run not found: {run_id}"))
        }
        JournalError::SessionIdentityMismatch { session_id } => Status::failed_precondition(
            format!("orchestrator session identity mismatch for session: {session_id}"),
        ),
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn map_provider_error(error: ProviderError) -> Status {
    match error {
        ProviderError::CircuitOpen { retry_after_ms } => Status::unavailable(format!(
            "model provider circuit breaker is open; retry after {retry_after_ms}ms"
        )),
        ProviderError::MissingApiKey => {
            Status::failed_precondition("model provider API key is missing")
        }
        ProviderError::VisionUnsupported { provider } => {
            Status::failed_precondition(format!("provider '{provider}' does not support vision"))
        }
        ProviderError::RequestFailed { message, retryable, retry_count } => {
            let status_message = format!(
                "model provider request failed after {retry_count} retries (retryable={retryable}): {message}"
            );
            if retryable {
                Status::unavailable(status_message)
            } else {
                Status::internal(status_message)
            }
        }
        ProviderError::InvalidResponse { message, retry_count } => Status::internal(format!(
            "model provider response invalid after {retry_count} retries: {message}"
        )),
        ProviderError::StatePoisoned => Status::internal("model provider state lock poisoned"),
    }
}

#[derive(Clone)]
pub struct GatewayServiceImpl {
    state: Arc<GatewayRuntimeState>,
    auth: GatewayAuthConfig,
}

impl GatewayServiceImpl {
    #[must_use]
    pub fn new(state: Arc<GatewayRuntimeState>, auth: GatewayAuthConfig) -> Self {
        Self { state, auth }
    }

    #[allow(clippy::result_large_err)]
    fn authorize_rpc(
        &self,
        metadata: &MetadataMap,
        method: &'static str,
    ) -> Result<RequestContext, Status> {
        authorize_metadata(metadata, &self.auth).map_err(|error| {
            self.state.record_denied();
            warn!(method, error = %error, "gateway rpc authorization denied");
            Status::permission_denied(error.to_string())
        })
    }
}

#[tonic::async_trait]
impl gateway_v1::gateway_service_server::GatewayService for GatewayServiceImpl {
    type RunStreamStream = ReceiverStream<Result<common_v1::RunStreamEvent, Status>>;

    async fn get_health(
        &self,
        _request: Request<gateway_v1::HealthRequest>,
    ) -> Result<Response<gateway_v1::HealthResponse>, Status> {
        Ok(Response::new(gateway_v1::HealthResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            service: "palyrad".to_owned(),
            status: "ok".to_owned(),
            version: self.state.build.version.clone(),
            git_hash: self.state.build.git_hash.clone(),
            build_profile: self.state.build.build_profile.clone(),
            uptime_seconds: self.state.started_at.elapsed().as_secs(),
        }))
    }

    async fn append_event(
        &self,
        request: Request<gateway_v1::AppendEventRequest>,
    ) -> Result<Response<gateway_v1::AppendEventResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "AppendEvent")?;
        self.state.counters.append_event_requests.fetch_add(1, Ordering::Relaxed);

        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let event = payload.event.ok_or_else(|| Status::invalid_argument("event is required"))?;
        if event.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition(
                "event uses an unsupported protocol major version",
            ));
        }
        let event_id = if let Some(id) = event.event_id.and_then(|value| non_empty(value.ulid)) {
            validate_canonical_id(&id)
                .map_err(|_| Status::invalid_argument("event.event_id must be a canonical ULID"))?;
            id
        } else {
            Ulid::new().to_string()
        };
        let session_id = canonical_id(event.session_id, "event.session_id")?;
        let run_id = canonical_id(event.run_id, "event.run_id")?;
        if event.timestamp_unix_ms <= 0 {
            return Err(Status::invalid_argument(
                "event.timestamp_unix_ms must be a unix timestamp",
            ));
        }
        if event.kind == common_v1::journal_event::EventKind::Unspecified as i32 {
            return Err(Status::invalid_argument("event.kind must be specified"));
        }
        if event.actor == common_v1::journal_event::EventActor::Unspecified as i32 {
            return Err(Status::invalid_argument("event.actor must be specified"));
        }

        let journal_outcome = self
            .state
            .record_journal_event(JournalAppendRequest {
                event_id: event_id.clone(),
                session_id,
                run_id,
                kind: event.kind,
                actor: event.actor,
                timestamp_unix_ms: event.timestamp_unix_ms,
                payload_json: event.payload_json,
                principal: context.principal.clone(),
                device_id: context.device_id.clone(),
                channel: context.channel.clone(),
            })
            .await?;

        info!(
            method = "AppendEvent",
            principal = %context.principal,
            device_id = %context.device_id,
            channel = context.channel.as_deref().unwrap_or("n/a"),
            event_id = %event_id,
            redacted_payload = journal_outcome.redacted,
            hash_chain_enabled = self.state.journal_config.hash_chain_enabled,
            write_duration_ms = journal_outcome.write_duration.as_millis(),
            event_hash = journal_outcome.hash.as_deref().unwrap_or("disabled"),
            "gateway event appended"
        );

        Ok(Response::new(gateway_v1::AppendEventResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            event_id: Some(common_v1::CanonicalId { ulid: event_id }),
            accepted: true,
        }))
    }

    async fn run_stream(
        &self,
        request: Request<Streaming<common_v1::RunStreamRequest>>,
    ) -> Result<Response<Self::RunStreamStream>, Status> {
        if !self.state.is_orchestrator_runloop_enabled() {
            self.state.record_denied();
            return Err(Status::failed_precondition(
                "orchestrator run loop v1 is disabled; set PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED=true",
            ));
        }
        let context = self.authorize_rpc(request.metadata(), "RunStream")?;
        self.state.counters.run_stream_requests.fetch_add(1, Ordering::Relaxed);

        let mut stream = request.into_inner();
        let (sender, receiver) = mpsc::channel(16);
        let context_for_stream = context.clone();
        let state_for_stream = self.state.clone();

        tokio::spawn(async move {
            let mut active_session_id = None::<String>;
            let mut active_run_id = None::<String>;
            let mut run_state = RunStateMachine::default();
            let mut tape_seq = 0_i64;
            let mut in_progress_emitted = false;

            while let Some(item) = stream.next().await {
                let message = match item {
                    Ok(value) => value,
                    Err(error) => {
                        let status =
                            Status::internal(format!("failed to read run stream request: {error}"));
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                };
                if message.v != CANONICAL_PROTOCOL_MAJOR {
                    let status = Status::failed_precondition("unsupported protocol major version");
                    finalize_run_failure(
                        &sender,
                        &state_for_stream,
                        &mut run_state,
                        active_run_id.as_deref(),
                        &mut tape_seq,
                        status.message(),
                    )
                    .await;
                    let _ = sender.send(Err(status)).await;
                    return;
                }
                if message.allow_sensitive_tools {
                    state_for_stream.record_denied();
                    let status = Status::permission_denied(format!(
                        "decision=deny_by_default approval_required=true reason={SENSITIVE_TOOLS_DENY_REASON}",
                    ));
                    finalize_run_failure(
                        &sender,
                        &state_for_stream,
                        &mut run_state,
                        active_run_id.as_deref(),
                        &mut tape_seq,
                        SENSITIVE_TOOLS_DENY_REASON,
                    )
                    .await;
                    let _ = sender.send(Err(status)).await;
                    return;
                }

                let session_id = match canonical_id(message.session_id, "session_id") {
                    Ok(value) => value,
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                let run_id = match canonical_id(message.run_id, "run_id") {
                    Ok(value) => value,
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };

                if let Some(expected_session) = active_session_id.as_ref() {
                    if expected_session != &session_id {
                        let status = Status::invalid_argument(
                            "run stream cannot switch session_id mid-stream",
                        );
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }
                if let Some(expected_run) = active_run_id.as_ref() {
                    if expected_run != &run_id {
                        let status =
                            Status::invalid_argument("run stream cannot switch run_id mid-stream");
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                }

                if active_run_id.is_none() {
                    if let Err(error) = run_state.transition(RunTransition::Accept) {
                        let status = Status::internal(error.to_string());
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                    if let Err(error) = state_for_stream
                        .upsert_orchestrator_session(OrchestratorSessionUpsertRequest {
                            session_id: session_id.clone(),
                            principal: context_for_stream.principal.clone(),
                            device_id: context_for_stream.device_id.clone(),
                            channel: context_for_stream.channel.clone(),
                        })
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                    if let Err(error) = state_for_stream
                        .start_orchestrator_run(OrchestratorRunStartRequest {
                            run_id: run_id.clone(),
                            session_id: session_id.clone(),
                        })
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }

                    active_session_id = Some(session_id.clone());
                    active_run_id = Some(run_id.clone());

                    let accepted_message = format!(
                        "accepted session={session_id} principal={}",
                        context_for_stream.principal
                    );
                    if let Err(error) = send_status_with_tape(
                        &sender,
                        &state_for_stream,
                        run_id.as_str(),
                        &mut tape_seq,
                        common_v1::stream_status::StatusKind::Accepted,
                        accepted_message.as_str(),
                    )
                    .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                let input_envelope = message.input.unwrap_or_default();
                let input_content = input_envelope.content.unwrap_or_default();
                let input_text = input_content.text;
                let vision_requested = input_content.attachments.iter().any(|attachment| {
                    attachment.kind == common_v1::message_attachment::AttachmentKind::Image as i32
                });
                let json_mode_requested = input_envelope
                    .security
                    .as_ref()
                    .map(|security| {
                        security.labels.iter().any(|label| label.eq_ignore_ascii_case("json_mode"))
                    })
                    .unwrap_or(false);

                if is_cancel_command(input_text.as_str()) {
                    if let Err(error) = state_for_stream
                        .request_orchestrator_cancel(OrchestratorCancelRequest {
                            run_id: run_id.clone(),
                            reason: "stream_cancel_command".to_owned(),
                        })
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                match state_for_stream.is_orchestrator_cancel_requested(run_id.clone()).await {
                    Ok(true) => {
                        if let Err(error) = run_state.transition(RunTransition::Cancel) {
                            let status = Status::internal(error.to_string());
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                active_run_id.as_deref(),
                                &mut tape_seq,
                                status.message(),
                            )
                            .await;
                            let _ = sender.send(Err(status)).await;
                            return;
                        }
                        if let Err(error) = state_for_stream
                            .update_orchestrator_run_state(
                                run_id.clone(),
                                RunLifecycleState::Cancelled,
                                Some(CANCELLED_REASON.to_owned()),
                            )
                            .await
                        {
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                active_run_id.as_deref(),
                                &mut tape_seq,
                                error.message(),
                            )
                            .await;
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                        if let Err(error) = send_status_with_tape(
                            &sender,
                            &state_for_stream,
                            run_id.as_str(),
                            &mut tape_seq,
                            common_v1::stream_status::StatusKind::Failed,
                            CANCELLED_REASON,
                        )
                        .await
                        {
                            let _ = sender.send(Err(error)).await;
                        }
                        return;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                if !in_progress_emitted {
                    if let Err(error) = run_state.transition(RunTransition::StartStreaming) {
                        let status = Status::internal(error.to_string());
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                    if let Err(error) = state_for_stream
                        .update_orchestrator_run_state(
                            run_id.clone(),
                            RunLifecycleState::InProgress,
                            None,
                        )
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                    if let Err(error) = send_status_with_tape(
                        &sender,
                        &state_for_stream,
                        run_id.as_str(),
                        &mut tape_seq,
                        common_v1::stream_status::StatusKind::InProgress,
                        "streaming",
                    )
                    .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                    in_progress_emitted = true;
                }

                let provider_response = match state_for_stream
                    .execute_model_provider(ProviderRequest {
                        input_text: input_text.clone(),
                        json_mode: json_mode_requested,
                        vision_requested,
                    })
                    .await
                {
                    Ok(response) => response,
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };

                if let Err(error) = state_for_stream
                    .add_orchestrator_usage(OrchestratorUsageDelta {
                        run_id: run_id.clone(),
                        prompt_tokens_delta: provider_response.prompt_tokens,
                        completion_tokens_delta: 0,
                    })
                    .await
                {
                    finalize_run_failure(
                        &sender,
                        &state_for_stream,
                        &mut run_state,
                        active_run_id.as_deref(),
                        &mut tape_seq,
                        error.message(),
                    )
                    .await;
                    let _ = sender.send(Err(error)).await;
                    return;
                }

                for provider_event in provider_response.events {
                    match state_for_stream.is_orchestrator_cancel_requested(run_id.clone()).await {
                        Ok(true) => {
                            if let Err(error) = run_state.transition(RunTransition::Cancel) {
                                let status = Status::internal(error.to_string());
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    status.message(),
                                )
                                .await;
                                let _ = sender.send(Err(status)).await;
                                return;
                            }
                            if let Err(error) = state_for_stream
                                .update_orchestrator_run_state(
                                    run_id.clone(),
                                    RunLifecycleState::Cancelled,
                                    Some(CANCELLED_REASON.to_owned()),
                                )
                                .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                            if let Err(error) = send_status_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                common_v1::stream_status::StatusKind::Failed,
                                CANCELLED_REASON,
                            )
                            .await
                            {
                                let _ = sender.send(Err(error)).await;
                            }
                            return;
                        }
                        Ok(false) => {}
                        Err(error) => {
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                active_run_id.as_deref(),
                                &mut tape_seq,
                                error.message(),
                            )
                            .await;
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                    }

                    match provider_event {
                        ProviderEvent::ModelToken { token, is_final } => {
                            if let Err(error) = send_model_token_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                token.as_str(),
                                is_final,
                            )
                            .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                        }
                        ProviderEvent::ToolProposal { proposal_id, tool_name, input_json } => {
                            if let Err(error) = send_tool_proposal_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                tool_name.as_str(),
                                input_json,
                            )
                            .await
                            {
                                finalize_run_failure(
                                    &sender,
                                    &state_for_stream,
                                    &mut run_state,
                                    active_run_id.as_deref(),
                                    &mut tape_seq,
                                    error.message(),
                                )
                                .await;
                                let _ = sender.send(Err(error)).await;
                                return;
                            }
                        }
                    }
                }

                if provider_response.completion_tokens > 0 {
                    if let Err(error) = state_for_stream
                        .add_orchestrator_usage(OrchestratorUsageDelta {
                            run_id: run_id.clone(),
                            prompt_tokens_delta: 0,
                            completion_tokens_delta: provider_response.completion_tokens,
                        })
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            active_run_id.as_deref(),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }

            if let Some(run_id) = active_run_id {
                match state_for_stream.is_orchestrator_cancel_requested(run_id.clone()).await {
                    Ok(true) => {
                        if let Err(error) = run_state.transition(RunTransition::Cancel) {
                            let status = Status::internal(error.to_string());
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                Some(run_id.as_str()),
                                &mut tape_seq,
                                status.message(),
                            )
                            .await;
                            let _ = sender.send(Err(status)).await;
                            return;
                        }
                        if let Err(error) = state_for_stream
                            .update_orchestrator_run_state(
                                run_id.clone(),
                                RunLifecycleState::Cancelled,
                                Some(CANCELLED_REASON.to_owned()),
                            )
                            .await
                        {
                            finalize_run_failure(
                                &sender,
                                &state_for_stream,
                                &mut run_state,
                                Some(run_id.as_str()),
                                &mut tape_seq,
                                error.message(),
                            )
                            .await;
                            let _ = sender.send(Err(error)).await;
                            return;
                        }
                        if let Err(error) = send_status_with_tape(
                            &sender,
                            &state_for_stream,
                            run_id.as_str(),
                            &mut tape_seq,
                            common_v1::stream_status::StatusKind::Failed,
                            CANCELLED_REASON,
                        )
                        .await
                        {
                            let _ = sender.send(Err(error)).await;
                        }
                        return;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            Some(run_id.as_str()),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }

                if run_state.state() == RunLifecycleState::InProgress {
                    if let Err(error) = run_state.transition(RunTransition::Complete) {
                        let status = Status::internal(error.to_string());
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            Some(run_id.as_str()),
                            &mut tape_seq,
                            status.message(),
                        )
                        .await;
                        let _ = sender.send(Err(status)).await;
                        return;
                    }
                    if let Err(error) = state_for_stream
                        .update_orchestrator_run_state(
                            run_id.clone(),
                            RunLifecycleState::Done,
                            None,
                        )
                        .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            Some(run_id.as_str()),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                    if let Err(error) = send_status_with_tape(
                        &sender,
                        &state_for_stream,
                        run_id.as_str(),
                        &mut tape_seq,
                        common_v1::stream_status::StatusKind::Done,
                        "completed",
                    )
                    .await
                    {
                        finalize_run_failure(
                            &sender,
                            &state_for_stream,
                            &mut run_state,
                            Some(run_id.as_str()),
                            &mut tape_seq,
                            error.message(),
                        )
                        .await;
                        let _ = sender.send(Err(error)).await;
                    }
                }
            }
        });

        info!(
            method = "RunStream",
            principal = %context.principal,
            device_id = %context.device_id,
            channel = context.channel.as_deref().unwrap_or("n/a"),
            "gateway run stream opened"
        );

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

async fn finalize_run_failure(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_state: &mut RunStateMachine,
    active_run_id: Option<&str>,
    tape_seq: &mut i64,
    reason: &str,
) {
    let Some(run_id) = active_run_id else {
        return;
    };
    if run_state.state().is_terminal() {
        return;
    }
    if run_state.transition(RunTransition::Fail).is_err() {
        return;
    }
    let _ = runtime_state
        .update_orchestrator_run_state(
            run_id.to_owned(),
            RunLifecycleState::Failed,
            Some(reason.to_owned()),
        )
        .await;
    let _ = send_status_with_tape(
        sender,
        runtime_state,
        run_id,
        tape_seq,
        common_v1::stream_status::StatusKind::Failed,
        reason,
    )
    .await;
}

fn status_event(
    run_id: String,
    kind: common_v1::stream_status::StatusKind,
    message: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::Status(common_v1::StreamStatus {
            kind: kind as i32,
            message: message.into(),
        })),
    }
}

fn model_token_event(
    run_id: String,
    token: impl Into<String>,
    is_final: bool,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ModelToken(common_v1::ModelToken {
            token: token.into(),
            is_final,
        })),
    }
}

fn tool_proposal_event(
    run_id: String,
    proposal_id: impl Into<String>,
    tool_name: impl Into<String>,
    input_json: Vec<u8>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolProposal(common_v1::ToolProposal {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            tool_name: tool_name.into(),
            input_json,
            approval_required: true,
        })),
    }
}

#[allow(clippy::result_large_err)]
async fn send_status_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    kind: common_v1::stream_status::StatusKind,
    message: &str,
) -> Result<(), Status> {
    let event = status_event(run_id.to_owned(), kind, message.to_owned());
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "status".to_owned(),
            payload_json: status_tape_payload(kind, message),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn send_model_token_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    token: &str,
    is_final: bool,
) -> Result<(), Status> {
    let event = model_token_event(run_id.to_owned(), token.to_owned(), is_final);
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "model_token".to_owned(),
            payload_json: model_token_tape_payload(token, is_final),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn send_tool_proposal_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    input_json: Vec<u8>,
) -> Result<(), Status> {
    let event = tool_proposal_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        tool_name.to_owned(),
        input_json.clone(),
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_proposal".to_owned(),
            payload_json: tool_proposal_tape_payload(proposal_id, tool_name, input_json.as_slice()),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

fn status_tape_payload(kind: common_v1::stream_status::StatusKind, message: &str) -> String {
    json!({
        "kind": status_kind_name(kind),
        "message": message,
    })
    .to_string()
}

fn model_token_tape_payload(token: &str, is_final: bool) -> String {
    json!({
        "token": token,
        "is_final": is_final,
    })
    .to_string()
}

fn tool_proposal_tape_payload(proposal_id: &str, tool_name: &str, input_json: &[u8]) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    json!({
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "input_json": normalized_input,
        "approval_required": true,
    })
    .to_string()
}

const fn status_kind_name(kind: common_v1::stream_status::StatusKind) -> &'static str {
    match kind {
        common_v1::stream_status::StatusKind::Unspecified => "unspecified",
        common_v1::stream_status::StatusKind::Accepted => "accepted",
        common_v1::stream_status::StatusKind::InProgress => "in_progress",
        common_v1::stream_status::StatusKind::Done => "done",
        common_v1::stream_status::StatusKind::Failed => "failed",
    }
}

#[allow(clippy::result_large_err)]
fn canonical_id(
    value: Option<common_v1::CanonicalId>,
    field_name: &'static str,
) -> Result<String, Status> {
    let id = value
        .and_then(|id| non_empty(id.ulid))
        .ok_or_else(|| Status::invalid_argument(format!("{field_name} is required")))?;
    validate_canonical_id(id.as_str())
        .map_err(|_| Status::invalid_argument(format!("{field_name} must be a canonical ULID")))?;
    Ok(id)
}

pub fn authorize_headers(headers: &HeaderMap, auth: &GatewayAuthConfig) -> Result<(), AuthError> {
    if !auth.require_auth {
        return Ok(());
    }
    let token = auth.admin_token.as_ref().ok_or(AuthError::MissingConfiguredToken)?;
    let candidate =
        extract_bearer_token(headers.get(AUTHORIZATION).and_then(|value| value.to_str().ok()))
            .ok_or(AuthError::InvalidAuthorizationHeader)?;
    if constant_time_eq(token.as_bytes(), candidate.as_bytes()) {
        Ok(())
    } else {
        Err(AuthError::InvalidToken)
    }
}

pub fn request_context_from_headers(headers: &HeaderMap) -> Result<RequestContext, AuthError> {
    request_context_from_header_resolver(|name| {
        headers.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned)
    })
}

fn authorize_metadata(
    metadata: &MetadataMap,
    auth: &GatewayAuthConfig,
) -> Result<RequestContext, AuthError> {
    if auth.require_auth {
        let token = auth.admin_token.as_ref().ok_or(AuthError::MissingConfiguredToken)?;
        let candidate = extract_bearer_token(
            metadata.get(AUTHORIZATION.as_str()).and_then(|value| value.to_str().ok()),
        )
        .ok_or(AuthError::InvalidAuthorizationHeader)?;
        if !constant_time_eq(token.as_bytes(), candidate.as_bytes()) {
            return Err(AuthError::InvalidToken);
        }
    }

    request_context_from_header_resolver(|name| {
        metadata.get(name).and_then(|value| value.to_str().ok()).map(ToOwned::to_owned)
    })
}

fn request_context_from_header_resolver<F>(resolver: F) -> Result<RequestContext, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let principal = require_context_value(&resolver, HEADER_PRINCIPAL)?;
    let device_id = require_context_value(&resolver, HEADER_DEVICE_ID)?;
    validate_canonical_id(device_id.as_str()).map_err(|_| AuthError::InvalidDeviceId)?;
    let channel = optional_context_value(&resolver, HEADER_CHANNEL)?;

    Ok(RequestContext { principal, device_id, channel })
}

fn require_context_value<F>(resolver: &F, key: &'static str) -> Result<String, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let value = resolver(key).ok_or(AuthError::MissingContext(key))?;
    let value = value.trim();
    if value.is_empty() {
        return Err(AuthError::EmptyContext(key));
    }
    Ok(value.to_owned())
}

fn optional_context_value<F>(resolver: &F, key: &'static str) -> Result<Option<String>, AuthError>
where
    F: Fn(&'static str) -> Option<String>,
{
    let Some(value) = resolver(key) else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        return Err(AuthError::EmptyContext(key));
    }
    Ok(Some(value.to_owned()))
}

fn extract_bearer_token(raw: Option<&str>) -> Option<&str> {
    let value = raw?;
    value.strip_prefix("Bearer ").filter(|token| !token.trim().is_empty())
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0_u8;
    for (lhs, rhs) in left.iter().zip(right.iter()) {
        diff |= lhs ^ rhs;
    }
    diff == 0
}

fn non_empty(input: String) -> Option<String> {
    if input.trim().is_empty() {
        None
    } else {
        Some(input)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::http::{header::AUTHORIZATION, HeaderMap, HeaderValue};

    use crate::journal::{JournalAppendRequest, JournalConfig, JournalStore};

    use super::{
        authorize_headers, request_context_from_headers, AuthError, GatewayAuthConfig,
        GatewayJournalConfigSnapshot, GatewayRuntimeConfigSnapshot, GatewayRuntimeState,
        RequestContext, HEADER_CHANNEL, HEADER_DEVICE_ID, HEADER_PRINCIPAL,
    };

    fn unique_temp_journal_path() -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("palyra-gateway-unit-{nonce}-{}.sqlite3", std::process::id()))
    }

    fn build_test_runtime_state(hash_chain_enabled: bool) -> std::sync::Arc<GatewayRuntimeState> {
        let db_path = unique_temp_journal_path();
        let journal_store =
            JournalStore::open(JournalConfig { db_path: db_path.clone(), hash_chain_enabled })
                .expect("journal store should initialize");
        GatewayRuntimeState::new(
            GatewayRuntimeConfigSnapshot {
                grpc_bind_addr: "127.0.0.1".to_owned(),
                grpc_port: 7443,
                quic_bind_addr: "127.0.0.1".to_owned(),
                quic_port: 7444,
                quic_enabled: true,
                orchestrator_runloop_v1_enabled: true,
                node_rpc_mtls_required: true,
                admin_auth_required: true,
            },
            GatewayJournalConfigSnapshot { db_path, hash_chain_enabled },
            journal_store,
            0,
        )
        .expect("runtime state should initialize")
    }

    #[test]
    fn authorize_headers_rejects_missing_token_when_required() {
        let auth = GatewayAuthConfig { require_auth: true, admin_token: Some("secret".to_owned()) };
        let headers = HeaderMap::new();
        let result = authorize_headers(&headers, &auth);
        assert_eq!(result, Err(AuthError::InvalidAuthorizationHeader));
    }

    #[test]
    fn authorize_headers_accepts_matching_bearer_token() {
        let auth = GatewayAuthConfig { require_auth: true, admin_token: Some("secret".to_owned()) };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer secret"));
        let result = authorize_headers(&headers, &auth);
        assert!(result.is_ok(), "matching bearer token should be accepted");
    }

    #[test]
    fn request_context_from_headers_validates_device_id() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_PRINCIPAL, HeaderValue::from_static("user:ops"));
        headers.insert(HEADER_DEVICE_ID, HeaderValue::from_static("invalid-id"));
        let result = request_context_from_headers(&headers);
        assert_eq!(result, Err(AuthError::InvalidDeviceId));
    }

    #[test]
    fn request_context_from_headers_extracts_expected_fields() {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_PRINCIPAL, HeaderValue::from_static("user:ops"));
        headers.insert(HEADER_DEVICE_ID, HeaderValue::from_static("01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        headers.insert(HEADER_CHANNEL, HeaderValue::from_static("cli"));
        let context = request_context_from_headers(&headers).expect("context should parse");
        assert_eq!(
            context,
            RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            }
        );
    }

    #[test]
    fn status_snapshot_reports_journal_counters_and_storage_metadata() {
        let state = build_test_runtime_state(true);

        state
            .record_journal_event_blocking(&JournalAppendRequest {
                event_id: "01ARZ3NDEKTSV4RRFFQ69G5FB0".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                kind: 1,
                actor: 1,
                timestamp_unix_ms: 1_730_000_000_000,
                payload_json: br#"{"token":"SECRET","safe":"ok"}"#.to_vec(),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("journal record should succeed");

        let status = state.status_snapshot(
            RequestContext {
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            },
            &GatewayAuthConfig { require_auth: true, admin_token: Some("token".to_owned()) },
        );
        assert_eq!(
            status.counters.journal_events, 1,
            "status should report persisted journal count"
        );
        assert_eq!(status.counters.journal_redacted_events, 1, "status should report redactions");
        assert!(status.storage.journal_hash_chain_enabled, "hash-chain flag should be surfaced");
        assert!(
            status.security.orchestrator_runloop_v1_enabled,
            "status should expose orchestrator runloop flag"
        );
        assert!(
            status.storage.latest_event_hash.is_some(),
            "latest hash should be available when hash-chain is enabled"
        );
    }

    #[test]
    fn recent_journal_snapshot_returns_events_for_admin_surface() {
        let state = build_test_runtime_state(false);

        for index in 0..3 {
            state
                .record_journal_event_blocking(&JournalAppendRequest {
                    event_id: format!("01ARZ3NDEKTSV4RRFFQ69G5FD{index}"),
                    session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                    run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                    kind: 1,
                    actor: 1,
                    timestamp_unix_ms: 1_730_000_000_000 + index,
                    payload_json: format!(r#"{{"index":{index}}}"#).into_bytes(),
                    principal: "user:ops".to_owned(),
                    device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                    channel: Some("cli".to_owned()),
                })
                .expect("journal record should succeed");
        }

        let snapshot = state
            .recent_journal_snapshot_blocking(1000)
            .expect("recent journal snapshot should be returned");
        assert_eq!(snapshot.total_events, 3);
        assert_eq!(snapshot.events.len(), 3);
        assert!(
            snapshot.events[0].event_id.ends_with('2'),
            "recent events should be returned in descending order"
        );
    }
}
