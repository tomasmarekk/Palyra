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
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};
use tracing::{info, warn};
use ulid::Ulid;

use crate::journal::{JournalAppendRequest, JournalError, JournalEventRecord, JournalStore};

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
const MAX_MODEL_TOKENS_PER_EVENT: usize = 16;
const MAX_JOURNAL_RECENT_EVENTS: usize = 100;
const JOURNAL_WRITE_LATENCY_BUDGET_MS: u128 = 25;
const SENSITIVE_TOOLS_DENY_REASON: &str =
    "allow_sensitive_tools=true is denied by default and requires explicit approvals";

#[derive(Debug, Clone)]
pub struct GatewayRuntimeConfigSnapshot {
    pub grpc_bind_addr: String,
    pub grpc_port: u16,
    pub quic_bind_addr: String,
    pub quic_port: u16,
    pub quic_enabled: bool,
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

#[derive(Debug)]
pub struct GatewayRuntimeState {
    started_at: Instant,
    build: BuildSnapshot,
    config: GatewayRuntimeConfigSnapshot,
    journal_config: GatewayJournalConfigSnapshot,
    counters: RuntimeCounters,
    journal_store: JournalStore,
    revoked_certificate_count: usize,
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
}

#[derive(Debug, Clone, Serialize)]
pub struct JournalRecentSnapshot {
    pub total_events: u64,
    pub hash_chain_enabled: bool,
    pub events: Vec<JournalEventRecord>,
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
        }
    }
}

impl GatewayRuntimeState {
    pub fn new(
        config: GatewayRuntimeConfigSnapshot,
        journal_config: GatewayJournalConfigSnapshot,
        journal_store: JournalStore,
        revoked_certificate_count: usize,
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
            },
            journal_store,
            revoked_certificate_count,
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
                node_rpc_mtls_required: self.config.node_rpc_mtls_required,
                revoked_certificate_count: self.revoked_certificate_count,
            },
            storage: StorageSnapshot {
                journal_db_path: self.journal_config.db_path.to_string_lossy().into_owned(),
                journal_hash_chain_enabled: self.journal_config.hash_chain_enabled,
                latest_event_hash,
            },
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
        let context = self.authorize_rpc(request.metadata(), "RunStream")?;
        self.state.counters.run_stream_requests.fetch_add(1, Ordering::Relaxed);

        let mut stream = request.into_inner();
        let (sender, receiver) = mpsc::channel(16);
        let context_for_stream = context.clone();
        let state_for_stream = self.state.clone();

        tokio::spawn(async move {
            let mut last_run_id = None::<String>;
            let mut accepted_emitted = false;
            while let Some(item) = stream.next().await {
                let message = match item {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = sender
                            .send(Err(Status::internal(format!(
                                "failed to read run stream request: {error}"
                            ))))
                            .await;
                        return;
                    }
                };
                if message.v != CANONICAL_PROTOCOL_MAJOR {
                    let _ = sender
                        .send(Err(Status::failed_precondition(
                            "unsupported protocol major version",
                        )))
                        .await;
                    return;
                }
                if message.allow_sensitive_tools {
                    state_for_stream.record_denied();
                    let _ = sender
                        .send(Err(Status::permission_denied(format!(
                            "decision=deny_by_default approval_required=true reason={SENSITIVE_TOOLS_DENY_REASON}",
                        ))))
                        .await;
                    return;
                }

                let session_id = match canonical_id(message.session_id, "session_id") {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                let run_id = match canonical_id(message.run_id, "run_id") {
                    Ok(value) => value,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                };
                last_run_id = Some(run_id.clone());

                if !accepted_emitted {
                    if sender
                        .send(Ok(status_event(
                            run_id.clone(),
                            common_v1::stream_status::StatusKind::Accepted,
                            format!(
                                "accepted session={session_id} principal={}",
                                context_for_stream.principal
                            ),
                        )))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    accepted_emitted = true;
                }

                if sender
                    .send(Ok(status_event(
                        run_id.clone(),
                        common_v1::stream_status::StatusKind::InProgress,
                        "streaming",
                    )))
                    .await
                    .is_err()
                {
                    return;
                }

                let mut emitted_token = false;
                if let Some(text) = message
                    .input
                    .and_then(|input| input.content)
                    .and_then(|content| non_empty(content.text))
                {
                    let mut tokens =
                        text.split_whitespace().take(MAX_MODEL_TOKENS_PER_EVENT).peekable();
                    if tokens.peek().is_none() {
                        let _ =
                            sender.send(Ok(model_token_event(run_id.clone(), "ack", true))).await;
                    } else {
                        while let Some(token) = tokens.next() {
                            let is_final = tokens.peek().is_none();
                            if sender
                                .send(Ok(model_token_event(run_id.clone(), token, is_final)))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    emitted_token = true;
                }
                if !emitted_token {
                    let _ = sender.send(Ok(model_token_event(run_id, "ack", true))).await;
                }
            }

            if let Some(run_id) = last_run_id {
                let _ = sender
                    .send(Ok(status_event(
                        run_id,
                        common_v1::stream_status::StatusKind::Done,
                        "completed",
                    )))
                    .await;
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
