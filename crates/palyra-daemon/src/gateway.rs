use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::http::{header::AUTHORIZATION, HeaderMap};
use palyra_common::{build_metadata, validate_canonical_id, CANONICAL_PROTOCOL_MAJOR};
use serde::Serialize;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::{interval, MissedTickBehavior};
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};
use tracing::{info, warn};
use ulid::Ulid;

use crate::{
    journal::{
        JournalAppendRequest, JournalError, JournalEventRecord, JournalStore,
        OrchestratorCancelRequest, OrchestratorRunStartRequest, OrchestratorRunStatusSnapshot,
        OrchestratorSessionRecord, OrchestratorSessionResolveOutcome,
        OrchestratorSessionResolveRequest, OrchestratorTapeAppendRequest, OrchestratorTapeRecord,
        OrchestratorUsageDelta,
    },
    model_provider::{
        ModelProvider, ProviderError, ProviderEvent, ProviderRequest, ProviderStatusSnapshot,
    },
    orchestrator::{is_cancel_command, RunLifecycleState, RunStateMachine, RunTransition},
    tool_protocol::{
        decide_tool_call, denied_execution_outcome, execute_tool_call, tool_policy_snapshot,
        tool_requires_approval, ToolCallConfig, ToolCallPolicySnapshot,
    },
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

        pub mod node {
            pub mod v1 {
                tonic::include_proto!("palyra.node.v1");
            }
        }
    }
}

use proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};

pub const HEADER_PRINCIPAL: &str = "x-palyra-principal";
pub const HEADER_DEVICE_ID: &str = "x-palyra-device-id";
pub const HEADER_CHANNEL: &str = "x-palyra-channel";
const MAX_JOURNAL_RECENT_EVENTS: usize = 100;
const MAX_SESSIONS_PAGE_LIMIT: usize = 500;
const JOURNAL_WRITE_LATENCY_BUDGET_MS: u128 = 25;
const TOOL_EXECUTION_LATENCY_BUDGET_MS: u128 = 200;
const MIN_TAPE_PAGE_LIMIT: usize = 1;
const SENSITIVE_TOOLS_DENY_REASON: &str =
    "allow_sensitive_tools=true is denied by default and requires explicit approvals";
const CANCELLED_REASON: &str = "cancelled by request";
const APPROVAL_CHANNEL_UNAVAILABLE_REASON: &str =
    "approval required but no interactive approval channel is available for this run";
const APPROVAL_DENIED_REASON: &str = "tool execution denied by explicit client approval response";
const MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN: usize = 1_024;

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
    pub max_tape_entries_per_response: usize,
    pub max_tape_bytes_per_response: usize,
    pub tool_call: ToolCallConfig,
}

#[derive(Debug, Clone)]
struct ToolApprovalOutcome {
    approved: bool,
    reason: String,
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
    tool_proposals: AtomicU64,
    tool_decisions_allowed: AtomicU64,
    tool_decisions_denied: AtomicU64,
    tool_execution_attempts: AtomicU64,
    tool_execution_failures: AtomicU64,
    tool_execution_timeouts: AtomicU64,
    tool_attestations_emitted: AtomicU64,
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
    pub tool_call_policy: ToolCallPolicySnapshot,
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
    pub tool_proposals: u64,
    pub tool_decisions_allowed: u64,
    pub tool_decisions_denied: u64,
    pub tool_execution_attempts: u64,
    pub tool_execution_failures: u64,
    pub tool_execution_timeouts: u64,
    pub tool_attestations_emitted: u64,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_after_seq: Option<i64>,
    pub limit: usize,
    pub max_response_bytes: usize,
    pub returned_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_after_seq: Option<i64>,
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
            tool_proposals: self.tool_proposals.load(Ordering::Relaxed),
            tool_decisions_allowed: self.tool_decisions_allowed.load(Ordering::Relaxed),
            tool_decisions_denied: self.tool_decisions_denied.load(Ordering::Relaxed),
            tool_execution_attempts: self.tool_execution_attempts.load(Ordering::Relaxed),
            tool_execution_failures: self.tool_execution_failures.load(Ordering::Relaxed),
            tool_execution_timeouts: self.tool_execution_timeouts.load(Ordering::Relaxed),
            tool_attestations_emitted: self.tool_attestations_emitted.load(Ordering::Relaxed),
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
                tool_proposals: AtomicU64::new(0),
                tool_decisions_allowed: AtomicU64::new(0),
                tool_decisions_denied: AtomicU64::new(0),
                tool_execution_attempts: AtomicU64::new(0),
                tool_execution_failures: AtomicU64::new(0),
                tool_execution_timeouts: AtomicU64::new(0),
                tool_attestations_emitted: AtomicU64::new(0),
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
            tool_call_policy: tool_policy_snapshot(&self.config.tool_call),
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
    fn resolve_orchestrator_session_blocking(
        &self,
        request: &OrchestratorSessionResolveRequest,
    ) -> Result<OrchestratorSessionResolveOutcome, Status> {
        self.journal_store
            .resolve_orchestrator_session(request)
            .map_err(|error| map_orchestrator_store_error("resolve orchestrator session", error))
    }

    #[allow(clippy::result_large_err)]
    pub async fn resolve_orchestrator_session(
        self: &Arc<Self>,
        request: OrchestratorSessionResolveRequest,
    ) -> Result<OrchestratorSessionResolveOutcome, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || state.resolve_orchestrator_session_blocking(&request))
            .await
            .map_err(|_| Status::internal("orchestrator session resolve worker panicked"))?
    }

    #[allow(clippy::result_large_err)]
    fn list_orchestrator_sessions_blocking(
        &self,
        after_session_key: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let limit = requested_limit.unwrap_or(100).clamp(1, MAX_SESSIONS_PAGE_LIMIT);
        let mut sessions = self
            .journal_store
            .list_orchestrator_sessions(after_session_key.as_deref(), limit.saturating_add(1))
            .map_err(|error| map_orchestrator_store_error("list orchestrator sessions", error))?;
        let has_more = sessions.len() > limit;
        if has_more {
            sessions.truncate(limit);
        }
        let next_after_session_key = if has_more {
            sessions.last().map(|session| session.session_key.clone())
        } else {
            None
        };
        Ok((sessions, next_after_session_key))
    }

    #[allow(clippy::result_large_err)]
    pub async fn list_orchestrator_sessions(
        self: &Arc<Self>,
        after_session_key: Option<String>,
        requested_limit: Option<usize>,
    ) -> Result<(Vec<OrchestratorSessionRecord>, Option<String>), Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.list_orchestrator_sessions_blocking(after_session_key, requested_limit)
        })
        .await
        .map_err(|_| Status::internal("orchestrator session list worker panicked"))?
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
    fn orchestrator_tape_snapshot_blocking(
        &self,
        run_id: &str,
        after_seq: Option<i64>,
        requested_limit: Option<usize>,
    ) -> Result<RunTapeSnapshot, Status> {
        let run_exists = self
            .journal_store
            .orchestrator_run_status_snapshot(run_id)
            .map_err(|error| map_orchestrator_store_error("load orchestrator run snapshot", error))?
            .is_some();
        if !run_exists {
            return Err(Status::not_found(format!("orchestrator run not found: {run_id}")));
        }
        let limit = requested_limit
            .unwrap_or(self.config.max_tape_entries_per_response)
            .clamp(MIN_TAPE_PAGE_LIMIT, self.config.max_tape_entries_per_response);
        let fetched_events = self
            .journal_store
            .orchestrator_tape_page(run_id, after_seq, limit.saturating_add(1))
            .map_err(|error| map_orchestrator_store_error("load orchestrator tape", error))?;
        let mut events = Vec::with_capacity(limit);
        let mut returned_bytes = 0_usize;
        let mut has_more = false;

        for record in fetched_events {
            if events.len() >= limit {
                has_more = true;
                break;
            }
            let sanitized_payload =
                crate::journal::redact_payload_json(record.payload_json.as_bytes()).map_err(
                    |error| map_orchestrator_store_error("redact orchestrator tape payload", error),
                )?;
            let payload_bytes = sanitized_payload.len();
            if events.is_empty() && payload_bytes > self.config.max_tape_bytes_per_response {
                return Err(Status::resource_exhausted(format!(
                    "single orchestrator tape event exceeds response byte limit ({payload_bytes} > {})",
                    self.config.max_tape_bytes_per_response
                )));
            }
            if returned_bytes.saturating_add(payload_bytes)
                > self.config.max_tape_bytes_per_response
            {
                has_more = true;
                break;
            }
            returned_bytes = returned_bytes.saturating_add(payload_bytes);
            events.push(OrchestratorTapeRecord {
                seq: record.seq,
                event_type: record.event_type,
                payload_json: sanitized_payload,
            });
        }

        let next_after_seq = if has_more { events.last().map(|event| event.seq) } else { None };
        Ok(RunTapeSnapshot {
            run_id: run_id.to_owned(),
            requested_after_seq: after_seq,
            limit,
            max_response_bytes: self.config.max_tape_bytes_per_response,
            returned_bytes,
            next_after_seq,
            events,
        })
    }

    #[allow(clippy::result_large_err)]
    pub async fn orchestrator_tape_snapshot(
        self: &Arc<Self>,
        run_id: String,
        after_seq: Option<i64>,
        limit: Option<usize>,
    ) -> Result<RunTapeSnapshot, Status> {
        let state = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            state.orchestrator_tape_snapshot_blocking(run_id.as_str(), after_seq, limit)
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
        JournalError::PayloadTooLarge { payload_kind, actual_bytes, max_bytes } => {
            Status::invalid_argument(format!(
                "{payload_kind} payload exceeds maximum size ({actual_bytes} > {max_bytes})"
            ))
        }
        JournalError::SessionIdentityMismatch { session_id } => Status::failed_precondition(
            format!("orchestrator session identity mismatch for session: {session_id}"),
        ),
        JournalError::SessionNotFound { selector } => {
            Status::not_found(format!("orchestrator session not found for selector: {selector}"))
        }
        JournalError::InvalidSessionSelector { reason } => {
            Status::invalid_argument(format!("invalid orchestrator session selector: {reason}"))
        }
        other => Status::internal(format!("{operation} failed: {other}")),
    }
}

fn apply_tool_approval_outcome(
    mut decision: crate::tool_protocol::ToolDecision,
    tool_name: &str,
    approval: Option<&ToolApprovalOutcome>,
) -> crate::tool_protocol::ToolDecision {
    if !(decision.allowed && decision.approval_required) {
        return decision;
    }

    let Some(approval) = approval else {
        decision.allowed = false;
        decision.reason = format!(
            "{APPROVAL_CHANNEL_UNAVAILABLE_REASON}; tool={tool_name}; original_reason={}",
            decision.reason
        );
        return decision;
    };

    if approval.approved {
        decision.reason = format!(
            "explicit approval granted for tool={tool_name}; approval_reason={}; original_reason={}",
            approval.reason, decision.reason
        );
        return decision;
    }

    decision.allowed = false;
    decision.reason = format!(
        "{APPROVAL_DENIED_REASON}; tool={tool_name}; approval_reason={}; original_reason={}",
        approval.reason, decision.reason
    );
    decision
}

#[allow(clippy::result_large_err)]
async fn await_tool_approval_response(
    stream: &mut Streaming<common_v1::RunStreamRequest>,
    expected_session_id: &str,
    expected_run_id: &str,
    proposal_id: &str,
) -> Result<ToolApprovalOutcome, Status> {
    while let Some(item) = stream.next().await {
        let message = item.map_err(|error| {
            Status::internal(format!("failed to read approval stream item: {error}"))
        })?;
        if message.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }

        let message_session_id = canonical_id(message.session_id, "session_id")?;
        if message_session_id != expected_session_id {
            return Err(Status::invalid_argument(
                "run stream cannot switch session_id while awaiting tool approval response",
            ));
        }
        let message_run_id = canonical_id(message.run_id, "run_id")?;
        if message_run_id != expected_run_id {
            return Err(Status::invalid_argument(
                "run stream cannot switch run_id while awaiting tool approval response",
            ));
        }
        if message.input.is_some() {
            return Err(Status::invalid_argument(
                "received prompt payload while waiting for tool approval response",
            ));
        }

        let Some(response) = message.tool_approval_response else {
            continue;
        };
        let response_proposal_id =
            canonical_id(response.proposal_id, "tool_approval_response.proposal_id")?;
        if response_proposal_id != proposal_id {
            return Err(Status::invalid_argument(
                "tool approval response proposal_id does not match pending tool proposal",
            ));
        }

        let reason = non_empty(response.reason).unwrap_or_else(|| {
            if response.approved {
                "approved_by_client".to_owned()
            } else {
                "denied_by_client".to_owned()
            }
        });
        return Ok(ToolApprovalOutcome { approved: response.approved, reason });
    }

    Ok(ToolApprovalOutcome {
        approved: false,
        reason: APPROVAL_CHANNEL_UNAVAILABLE_REASON.to_owned(),
    })
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

fn session_summary_message(session: &OrchestratorSessionRecord) -> gateway_v1::SessionSummary {
    gateway_v1::SessionSummary {
        session_id: Some(common_v1::CanonicalId { ulid: session.session_id.clone() }),
        session_key: session.session_key.clone(),
        session_label: session.session_label.clone().unwrap_or_default(),
        created_at_unix_ms: session.created_at_unix_ms,
        updated_at_unix_ms: session.updated_at_unix_ms,
        last_run_id: session
            .last_run_id
            .as_ref()
            .map(|run_id| common_v1::CanonicalId { ulid: run_id.clone() }),
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

    async fn abort_run(
        &self,
        request: Request<gateway_v1::AbortRunRequest>,
    ) -> Result<Response<gateway_v1::AbortRunResponse>, Status> {
        let _context = self.authorize_rpc(request.metadata(), "AbortRun")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let run_id = canonical_id(payload.run_id, "run_id")?;
        let reason = non_empty(payload.reason).unwrap_or_else(|| "grpc_abort_requested".to_owned());
        let snapshot = self
            .state
            .request_orchestrator_cancel(OrchestratorCancelRequest {
                run_id: run_id.clone(),
                reason: reason.clone(),
            })
            .await?;
        Ok(Response::new(gateway_v1::AbortRunResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            run_id: Some(common_v1::CanonicalId { ulid: snapshot.run_id }),
            cancel_requested: snapshot.cancel_requested,
            reason: snapshot.reason,
        }))
    }

    async fn list_sessions(
        &self,
        request: Request<gateway_v1::ListSessionsRequest>,
    ) -> Result<Response<gateway_v1::ListSessionsResponse>, Status> {
        let _context = self.authorize_rpc(request.metadata(), "ListSessions")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let after_session_key = non_empty(payload.after_session_key);
        let requested_limit = if payload.limit == 0 { None } else { Some(payload.limit as usize) };
        let (sessions, next_after_session_key) =
            self.state.list_orchestrator_sessions(after_session_key, requested_limit).await?;
        Ok(Response::new(gateway_v1::ListSessionsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            sessions: sessions.iter().map(session_summary_message).collect(),
            next_after_session_key: next_after_session_key.unwrap_or_default(),
        }))
    }

    async fn resolve_session(
        &self,
        request: Request<gateway_v1::ResolveSessionRequest>,
    ) -> Result<Response<gateway_v1::ResolveSessionResponse>, Status> {
        let context = self.authorize_rpc(request.metadata(), "ResolveSession")?;
        let payload = request.into_inner();
        if payload.v != CANONICAL_PROTOCOL_MAJOR {
            return Err(Status::failed_precondition("unsupported protocol major version"));
        }
        let session_id = optional_canonical_id(payload.session_id, "session_id")?;
        let session_key = non_empty(payload.session_key);
        let session_label = non_empty(payload.session_label);
        let outcome = self
            .state
            .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
                session_id,
                session_key,
                session_label,
                principal: context.principal,
                device_id: context.device_id,
                channel: context.channel,
                require_existing: payload.require_existing,
                reset_session: payload.reset_session,
            })
            .await?;
        Ok(Response::new(gateway_v1::ResolveSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            session: Some(session_summary_message(&outcome.session)),
            created: outcome.created,
            reset_applied: outcome.reset_applied,
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
            let mut model_token_tape_events = 0_usize;
            let mut model_token_compaction_emitted = false;
            let mut in_progress_emitted = false;
            let mut remaining_tool_budget = state_for_stream.config.tool_call.max_calls_per_run;

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
                    let resolved_session = state_for_stream
                        .resolve_orchestrator_session(OrchestratorSessionResolveRequest {
                            session_id: Some(session_id.clone()),
                            session_key: non_empty(message.session_key.clone()),
                            session_label: non_empty(message.session_label.clone()),
                            principal: context_for_stream.principal.clone(),
                            device_id: context_for_stream.device_id.clone(),
                            channel: context_for_stream.channel.clone(),
                            require_existing: message.require_existing,
                            reset_session: message.reset_session,
                        })
                        .await;
                    let resolved_session = match resolved_session {
                        Ok(outcome) => outcome,
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
                    if resolved_session.session.session_id != session_id {
                        let status = Status::failed_precondition(
                            "resolved session_id does not match RunStream session_id",
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

                let mut provider_future =
                    Box::pin(state_for_stream.execute_model_provider(ProviderRequest {
                        input_text: input_text.clone(),
                        json_mode: json_mode_requested,
                        vision_requested,
                    }));
                let mut cancel_poll = interval(Duration::from_millis(100));
                cancel_poll.set_missed_tick_behavior(MissedTickBehavior::Delay);

                let provider_response = loop {
                    tokio::select! {
                        provider_result = &mut provider_future => {
                            match provider_result {
                                Ok(response) => break response,
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
                        }
                        _ = cancel_poll.tick() => {
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
                        }
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
                                &mut model_token_tape_events,
                                &mut model_token_compaction_emitted,
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
                            let proposal_approval_required =
                                tool_requires_approval(tool_name.as_str());
                            state_for_stream
                                .counters
                                .tool_proposals
                                .fetch_add(1, Ordering::Relaxed);
                            if let Err(error) = send_tool_proposal_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                tool_name.as_str(),
                                input_json.as_slice(),
                                proposal_approval_required,
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

                            let approval_outcome = if proposal_approval_required {
                                if let Err(error) = send_tool_approval_request_with_tape(
                                    &sender,
                                    &state_for_stream,
                                    run_id.as_str(),
                                    &mut tape_seq,
                                    proposal_id.as_str(),
                                    tool_name.as_str(),
                                    input_json.as_slice(),
                                    true,
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

                                let response = match await_tool_approval_response(
                                    &mut stream,
                                    session_id.as_str(),
                                    run_id.as_str(),
                                    proposal_id.as_str(),
                                )
                                .await
                                {
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

                                if let Err(error) = send_tool_approval_response_with_tape(
                                    &sender,
                                    &state_for_stream,
                                    run_id.as_str(),
                                    &mut tape_seq,
                                    proposal_id.as_str(),
                                    response.approved,
                                    response.reason.as_str(),
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
                                Some(response)
                            } else {
                                None
                            };

                            let decision = decide_tool_call(
                                &state_for_stream.config.tool_call,
                                &mut remaining_tool_budget,
                                context_for_stream.principal.as_str(),
                                tool_name.as_str(),
                            );
                            let decision = apply_tool_approval_outcome(
                                decision,
                                tool_name.as_str(),
                                approval_outcome.as_ref(),
                            );
                            if decision.allowed {
                                state_for_stream
                                    .counters
                                    .tool_decisions_allowed
                                    .fetch_add(1, Ordering::Relaxed);
                            } else {
                                state_for_stream
                                    .counters
                                    .tool_decisions_denied
                                    .fetch_add(1, Ordering::Relaxed);
                                state_for_stream.record_denied();
                            }

                            if let Err(error) = send_tool_decision_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                decision.allowed,
                                decision.reason.as_str(),
                                decision.approval_required,
                                decision.policy_enforced,
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
                            let session_id = if let Some(session_id) = active_session_id.as_deref()
                            {
                                session_id
                            } else {
                                let status = Status::internal(
                                    "run stream internal invariant violated: missing session_id while recording policy decision",
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
                            };
                            if let Err(error) = record_policy_decision_journal_event(
                                &state_for_stream,
                                &context_for_stream,
                                session_id,
                                run_id.as_str(),
                                proposal_id.as_str(),
                                tool_name.as_str(),
                                decision.allowed,
                                decision.reason.as_str(),
                                decision.approval_required,
                                decision.policy_enforced,
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

                            let execution_outcome = if decision.allowed {
                                state_for_stream
                                    .counters
                                    .tool_execution_attempts
                                    .fetch_add(1, Ordering::Relaxed);
                                let started_at = Instant::now();
                                let outcome = execute_tool_call(
                                    &state_for_stream.config.tool_call,
                                    proposal_id.as_str(),
                                    tool_name.as_str(),
                                    input_json.as_slice(),
                                )
                                .await;
                                if started_at.elapsed().as_millis()
                                    > TOOL_EXECUTION_LATENCY_BUDGET_MS
                                {
                                    warn!(
                                        run_id = %run_id,
                                        proposal_id = %proposal_id,
                                        tool_name = %tool_name,
                                        execution_duration_ms = started_at.elapsed().as_millis(),
                                        budget_ms = TOOL_EXECUTION_LATENCY_BUDGET_MS,
                                        "tool execution exceeded latency budget"
                                    );
                                }
                                if !outcome.success {
                                    state_for_stream
                                        .counters
                                        .tool_execution_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                if outcome.attestation.timed_out {
                                    state_for_stream
                                        .counters
                                        .tool_execution_timeouts
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                outcome
                            } else {
                                denied_execution_outcome(
                                    proposal_id.as_str(),
                                    tool_name.as_str(),
                                    input_json.as_slice(),
                                    decision.reason.as_str(),
                                )
                            };

                            if let Err(error) = send_tool_result_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                execution_outcome.success,
                                execution_outcome.output_json.as_slice(),
                                execution_outcome.error.as_str(),
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

                            if let Err(error) = send_tool_attestation_with_tape(
                                &sender,
                                &state_for_stream,
                                run_id.as_str(),
                                &mut tape_seq,
                                proposal_id.as_str(),
                                execution_outcome.attestation.attestation_id.as_str(),
                                execution_outcome.attestation.execution_sha256.as_str(),
                                execution_outcome.attestation.executed_at_unix_ms,
                                execution_outcome.attestation.timed_out,
                                execution_outcome.attestation.executor.as_str(),
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
                            state_for_stream
                                .counters
                                .tool_attestations_emitted
                                .fetch_add(1, Ordering::Relaxed);
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
    approval_required: bool,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolProposal(common_v1::ToolProposal {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            tool_name: tool_name.into(),
            input_json,
            approval_required,
        })),
    }
}

fn tool_approval_request_event(
    run_id: String,
    proposal_id: impl Into<String>,
    tool_name: impl Into<String>,
    input_json: Vec<u8>,
    approval_required: bool,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolApprovalRequest(
            common_v1::ToolApprovalRequest {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                tool_name: tool_name.into(),
                input_json,
                approval_required,
            },
        )),
    }
}

fn tool_approval_response_event(
    run_id: String,
    proposal_id: impl Into<String>,
    approved: bool,
    reason: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolApprovalResponse(
            common_v1::ToolApprovalResponse {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                approved,
                reason: reason.into(),
            },
        )),
    }
}

fn tool_decision_event(
    run_id: String,
    proposal_id: impl Into<String>,
    allowed: bool,
    reason: impl Into<String>,
    approval_required: bool,
    policy_enforced: bool,
) -> common_v1::RunStreamEvent {
    let kind = if allowed {
        common_v1::tool_decision::DecisionKind::Allow
    } else {
        common_v1::tool_decision::DecisionKind::Deny
    };
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolDecision(common_v1::ToolDecision {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            kind: kind as i32,
            reason: reason.into(),
            approval_required,
            policy_enforced,
        })),
    }
}

fn tool_result_event(
    run_id: String,
    proposal_id: impl Into<String>,
    success: bool,
    output_json: Vec<u8>,
    error: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolResult(common_v1::ToolResult {
            proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
            success,
            output_json,
            error: error.into(),
        })),
    }
}

fn tool_attestation_event(
    run_id: String,
    proposal_id: impl Into<String>,
    attestation_id: impl Into<String>,
    execution_sha256: impl Into<String>,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: impl Into<String>,
) -> common_v1::RunStreamEvent {
    common_v1::RunStreamEvent {
        v: CANONICAL_PROTOCOL_MAJOR,
        run_id: Some(common_v1::CanonicalId { ulid: run_id }),
        body: Some(common_v1::run_stream_event::Body::ToolAttestation(
            common_v1::ToolAttestation {
                proposal_id: Some(common_v1::CanonicalId { ulid: proposal_id.into() }),
                attestation_id: Some(common_v1::CanonicalId { ulid: attestation_id.into() }),
                execution_sha256: execution_sha256.into(),
                executed_at_unix_ms,
                timed_out,
                executor: executor.into(),
            },
        )),
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
#[allow(clippy::too_many_arguments)]
async fn send_model_token_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    token_tape_events: &mut usize,
    compaction_emitted: &mut bool,
    token: &str,
    is_final: bool,
) -> Result<(), Status> {
    let event = model_token_event(run_id.to_owned(), token.to_owned(), is_final);
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    if !is_final && *token_tape_events >= MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN {
        if !*compaction_emitted {
            compact_model_token_tape_stub(runtime_state, run_id, tape_seq).await?;
            *compaction_emitted = true;
        }
        return Ok(());
    }
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "model_token".to_owned(),
            payload_json: model_token_tape_payload(token, is_final),
        })
        .await?;
    *tape_seq += 1;
    *token_tape_events += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn compact_model_token_tape_stub(
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
) -> Result<(), Status> {
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "model_token_compaction".to_owned(),
            payload_json: model_token_compaction_tape_payload(MAX_MODEL_TOKEN_TAPE_EVENTS_PER_RUN),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_proposal_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> Result<(), Status> {
    let event = tool_proposal_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        tool_name.to_owned(),
        input_json.to_vec(),
        approval_required,
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
            payload_json: tool_proposal_tape_payload(
                proposal_id,
                tool_name,
                input_json,
                approval_required,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_approval_request_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> Result<(), Status> {
    let event = tool_approval_request_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        tool_name.to_owned(),
        input_json.to_vec(),
        approval_required,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_approval_request".to_owned(),
            payload_json: tool_approval_request_tape_payload(
                proposal_id,
                tool_name,
                input_json,
                approval_required,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
async fn send_tool_approval_response_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    approved: bool,
    reason: &str,
) -> Result<(), Status> {
    let event = tool_approval_response_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        approved,
        reason.to_owned(),
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_approval_response".to_owned(),
            payload_json: tool_approval_response_tape_payload(proposal_id, approved, reason),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_decision_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Result<(), Status> {
    let event = tool_decision_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        allowed,
        reason.to_owned(),
        approval_required,
        policy_enforced,
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_decision".to_owned(),
            payload_json: tool_decision_tape_payload(
                proposal_id,
                allowed,
                reason,
                approval_required,
                policy_enforced,
            ),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_result_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    success: bool,
    output_json: &[u8],
    error: &str,
) -> Result<(), Status> {
    let event = tool_result_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        success,
        output_json.to_vec(),
        error.to_owned(),
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_result".to_owned(),
            payload_json: tool_result_tape_payload(proposal_id, success, output_json, error),
        })
        .await?;
    *tape_seq += 1;
    Ok(())
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn send_tool_attestation_with_tape(
    sender: &mpsc::Sender<Result<common_v1::RunStreamEvent, Status>>,
    runtime_state: &Arc<GatewayRuntimeState>,
    run_id: &str,
    tape_seq: &mut i64,
    proposal_id: &str,
    attestation_id: &str,
    execution_sha256: &str,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: &str,
) -> Result<(), Status> {
    let event = tool_attestation_event(
        run_id.to_owned(),
        proposal_id.to_owned(),
        attestation_id.to_owned(),
        execution_sha256.to_owned(),
        executed_at_unix_ms,
        timed_out,
        executor.to_owned(),
    );
    sender
        .send(Ok(event))
        .await
        .map_err(|_| Status::cancelled("run stream response channel closed"))?;
    runtime_state
        .append_orchestrator_tape_event(OrchestratorTapeAppendRequest {
            run_id: run_id.to_owned(),
            seq: *tape_seq,
            event_type: "tool_attestation".to_owned(),
            payload_json: tool_attestation_tape_payload(
                proposal_id,
                attestation_id,
                execution_sha256,
                executed_at_unix_ms,
                timed_out,
                executor,
            ),
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
        "is_final": is_final,
        "token": token,
    })
    .to_string()
}

fn model_token_compaction_tape_payload(max_model_token_events: usize) -> String {
    json!({
        "kind": "token_cap_reached",
        "max_model_token_tape_events": max_model_token_events,
        "compaction_hook": "stub",
    })
    .to_string()
}

fn tool_proposal_tape_payload(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    json!({
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "input_json": normalized_input,
        "approval_required": approval_required,
    })
    .to_string()
}

fn tool_approval_request_tape_payload(
    proposal_id: &str,
    tool_name: &str,
    input_json: &[u8],
    approval_required: bool,
) -> String {
    let normalized_input = serde_json::from_slice::<Value>(input_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(input_json).to_string() }));
    json!({
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "input_json": normalized_input,
        "approval_required": approval_required,
    })
    .to_string()
}

fn tool_approval_response_tape_payload(proposal_id: &str, approved: bool, reason: &str) -> String {
    json!({
        "proposal_id": proposal_id,
        "approved": approved,
        "reason": reason,
    })
    .to_string()
}

fn tool_decision_tape_payload(
    proposal_id: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> String {
    json!({
        "proposal_id": proposal_id,
        "kind": if allowed { "allow" } else { "deny" },
        "reason": reason,
        "approval_required": approval_required,
        "policy_enforced": policy_enforced,
    })
    .to_string()
}

#[allow(clippy::result_large_err)]
#[allow(clippy::too_many_arguments)]
async fn record_policy_decision_journal_event(
    runtime_state: &Arc<GatewayRuntimeState>,
    context: &RequestContext,
    session_id: &str,
    run_id: &str,
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Result<(), Status> {
    runtime_state
        .record_journal_event(JournalAppendRequest {
            event_id: Ulid::new().to_string(),
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            kind: common_v1::journal_event::EventKind::ToolProposed as i32,
            actor: common_v1::journal_event::EventActor::System as i32,
            timestamp_unix_ms: current_unix_ms(),
            payload_json: tool_decision_journal_payload(
                proposal_id,
                tool_name,
                allowed,
                reason,
                approval_required,
                policy_enforced,
            ),
            principal: context.principal.clone(),
            device_id: context.device_id.clone(),
            channel: context.channel.clone(),
        })
        .await
        .map(|_| ())
}

fn tool_decision_journal_payload(
    proposal_id: &str,
    tool_name: &str,
    allowed: bool,
    reason: &str,
    approval_required: bool,
    policy_enforced: bool,
) -> Vec<u8> {
    json!({
        "event": "policy_decision",
        "proposal_id": proposal_id,
        "tool_name": tool_name,
        "kind": if allowed { "allow" } else { "deny" },
        "reason": reason,
        "approval_required": approval_required,
        "policy_enforced": policy_enforced,
    })
    .to_string()
    .into_bytes()
}

fn tool_result_tape_payload(
    proposal_id: &str,
    success: bool,
    output_json: &[u8],
    error: &str,
) -> String {
    let normalized_output = serde_json::from_slice::<Value>(output_json)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(output_json).to_string() }));
    json!({
        "proposal_id": proposal_id,
        "success": success,
        "output_json": normalized_output,
        "error": error,
    })
    .to_string()
}

fn tool_attestation_tape_payload(
    proposal_id: &str,
    attestation_id: &str,
    execution_sha256: &str,
    executed_at_unix_ms: i64,
    timed_out: bool,
    executor: &str,
) -> String {
    json!({
        "proposal_id": proposal_id,
        "attestation_id": attestation_id,
        "execution_sha256": execution_sha256,
        "executed_at_unix_ms": executed_at_unix_ms,
        "timed_out": timed_out,
        "executor": executor,
    })
    .to_string()
}

fn current_unix_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as i64
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

#[allow(clippy::result_large_err)]
fn optional_canonical_id(
    value: Option<common_v1::CanonicalId>,
    field_name: &'static str,
) -> Result<Option<String>, Status> {
    let Some(value) = value else {
        return Ok(None);
    };
    let id = non_empty(value.ulid)
        .ok_or_else(|| Status::invalid_argument(format!("{field_name} must be non-empty")))?;
    validate_canonical_id(id.as_str())
        .map_err(|_| Status::invalid_argument(format!("{field_name} must be a canonical ULID")))?;
    Ok(Some(id))
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

    use crate::journal::{
        JournalAppendRequest, JournalConfig, JournalStore, OrchestratorRunStartRequest,
        OrchestratorSessionUpsertRequest, OrchestratorTapeAppendRequest,
    };

    use super::{
        apply_tool_approval_outcome, authorize_headers, request_context_from_headers, AuthError,
        GatewayAuthConfig, GatewayJournalConfigSnapshot, GatewayRuntimeConfigSnapshot,
        GatewayRuntimeState, RequestContext, ToolApprovalOutcome, HEADER_CHANNEL, HEADER_DEVICE_ID,
        HEADER_PRINCIPAL,
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
        let journal_store = JournalStore::open(JournalConfig {
            db_path: db_path.clone(),
            hash_chain_enabled,
            max_payload_bytes: 256 * 1024,
        })
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
                max_tape_entries_per_response: 1_000,
                max_tape_bytes_per_response: 2 * 1024 * 1024,
                tool_call: crate::tool_protocol::ToolCallConfig {
                    allowed_tools: vec!["palyra.echo".to_owned()],
                    max_calls_per_run: 4,
                    execution_timeout_ms: 250,
                    process_runner: crate::sandbox_runner::SandboxProcessRunnerPolicy {
                        enabled: false,
                        workspace_root: PathBuf::from("."),
                        allowed_executables: Vec::new(),
                        allowed_egress_hosts: Vec::new(),
                        allowed_dns_suffixes: Vec::new(),
                        cpu_time_limit_ms: 2_000,
                        memory_limit_bytes: 256 * 1024 * 1024,
                        max_output_bytes: 64 * 1024,
                    },
                    wasm_runtime: crate::wasm_plugin_runner::WasmPluginRunnerPolicy {
                        enabled: false,
                        max_module_size_bytes: 256 * 1024,
                        fuel_budget: 10_000_000,
                        max_memory_bytes: 64 * 1024 * 1024,
                        max_table_elements: 100_000,
                        max_instances: 256,
                        allowed_http_hosts: Vec::new(),
                        allowed_secrets: Vec::new(),
                        allowed_storage_prefixes: Vec::new(),
                        allowed_channels: Vec::new(),
                    },
                },
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

    #[test]
    fn approval_required_decision_is_denied_without_interactive_channel() {
        let decision = crate::tool_protocol::ToolDecision {
            allowed: true,
            reason: "allowlisted by policy".to_owned(),
            approval_required: true,
            policy_enforced: true,
        };
        let enforced = apply_tool_approval_outcome(decision, "palyra.process.run", None);
        assert!(!enforced.allowed, "allowed decisions must be denied until approval is granted");
        assert!(
            enforced.reason.contains("approval required"),
            "denial reason should explain why execution was blocked"
        );
    }

    #[test]
    fn approval_required_decision_is_allowed_with_explicit_approval() {
        let decision = crate::tool_protocol::ToolDecision {
            allowed: true,
            reason: "allowlisted by policy".to_owned(),
            approval_required: true,
            policy_enforced: true,
        };
        let approval = ToolApprovalOutcome { approved: true, reason: "allow_once".to_owned() };
        let enforced = apply_tool_approval_outcome(decision, "palyra.process.run", Some(&approval));
        assert!(enforced.allowed, "explicit approval should keep allow decisions allowed");
        assert!(
            enforced.reason.contains("explicit approval granted"),
            "allow reason should preserve approval context"
        );
    }

    #[test]
    fn orchestrator_tape_snapshot_paginates_and_redacts_payloads() {
        let state = build_test_runtime_state(false);
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                session_key: "session:test".to_owned(),
                session_label: Some("Test session".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("orchestrator session should be upserted");
        state
            .journal_store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("orchestrator run should start");
        state
            .journal_store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 0,
                event_type: "status".to_owned(),
                payload_json: r#"{"kind":"accepted"}"#.to_owned(),
            })
            .expect("first tape event should persist");
        state
            .journal_store
            .append_orchestrator_tape_event(&OrchestratorTapeAppendRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                seq: 1,
                event_type: "tool_result".to_owned(),
                payload_json: r#"{"token":"secret-value","ok":true}"#.to_owned(),
            })
            .expect("second tape event should persist");

        let first_page = state
            .orchestrator_tape_snapshot_blocking("01ARZ3NDEKTSV4RRFFQ69G5FAX", None, Some(1))
            .expect("first tape page should succeed");
        assert_eq!(first_page.events.len(), 1);
        assert_eq!(first_page.events[0].seq, 0);
        assert_eq!(first_page.next_after_seq, Some(0));

        let second_page = state
            .orchestrator_tape_snapshot_blocking(
                "01ARZ3NDEKTSV4RRFFQ69G5FAX",
                first_page.next_after_seq,
                Some(2),
            )
            .expect("second tape page should succeed");
        assert_eq!(second_page.events.len(), 1);
        assert_eq!(second_page.events[0].seq, 1);
        assert!(
            !second_page.events[0].payload_json.contains("secret-value"),
            "tape snapshots must redact sensitive token values"
        );
        assert!(
            second_page.events[0].payload_json.contains("<redacted>"),
            "redacted marker should be present in tape payloads"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn model_token_tape_compaction_stub_emits_marker_event() {
        let state = build_test_runtime_state(false);
        state
            .journal_store
            .upsert_orchestrator_session(&OrchestratorSessionUpsertRequest {
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
                session_key: "session:test".to_owned(),
                session_label: Some("Test session".to_owned()),
                principal: "user:ops".to_owned(),
                device_id: "01ARZ3NDEKTSV4RRFFQ69G5FAV".to_owned(),
                channel: Some("cli".to_owned()),
            })
            .expect("orchestrator session should be upserted");
        state
            .journal_store
            .start_orchestrator_run(&OrchestratorRunStartRequest {
                run_id: "01ARZ3NDEKTSV4RRFFQ69G5FAX".to_owned(),
                session_id: "01ARZ3NDEKTSV4RRFFQ69G5FAW".to_owned(),
            })
            .expect("orchestrator run should start");

        let mut tape_seq = 0_i64;
        super::compact_model_token_tape_stub(&state, "01ARZ3NDEKTSV4RRFFQ69G5FAX", &mut tape_seq)
            .await
            .expect("compaction stub should append marker tape event");
        assert_eq!(tape_seq, 1);

        let tape = state
            .journal_store
            .orchestrator_tape("01ARZ3NDEKTSV4RRFFQ69G5FAX")
            .expect("orchestrator tape should be queryable");
        assert_eq!(tape.len(), 1);
        assert_eq!(tape[0].event_type, "model_token_compaction");
        assert!(
            tape[0].payload_json.contains("token_cap_reached"),
            "marker payload should describe compaction trigger"
        );
    }
}
