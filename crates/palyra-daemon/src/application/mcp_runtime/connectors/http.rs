//! Persistent Streamable HTTP and shared remote-session machinery.
//!
//! All network I/O is delegated to a host port that must enforce egress,
//! credential, redirect, DNS, timeout, and response-size policy.

use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::mpsc;

use super::sse::BoundedSseDecoder;
use super::{
    catalog_digest, catalog_request, classify_http_status, decode_value, encode_callback_response,
    encode_session_request, encode_value, ensure_pinned_session, evidence, initialize_message,
    initialized_notification, now_unix_ms, parse_catalog_response, parse_initialize_response,
    parse_runtime_event, session_expiry, CatalogTracker, McpConnectorCatalogState,
    McpConnectorEvidenceHandle, McpConnectorLimits, McpConnectorPortError, McpHttpSessionResponse,
    INITIALIZE_ID, PROMPTS_LIST_ID, RESOURCES_LIST_ID, TOOLS_LIST_ID,
};
use crate::application::mcp_runtime::{
    McpConnectRequest, McpConnectedSession, McpServerCallbackResponse, McpServerNotification,
    McpSessionConnector, McpSessionReader, McpSessionRequest, McpSessionTransportKind,
    McpSessionWriter, McpTransportError, McpTransportEvent, McpTransportSession,
};

/// Request to establish and initialize a host-governed remote MCP session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHttpSessionOpenRequest {
    /// Host-configured event or Streamable HTTP endpoint identity.
    pub endpoint_id: String,
    /// Paired request endpoint for SSE, otherwise `None`.
    pub paired_request_endpoint_id: Option<String>,
    /// Stable MCP server identity.
    pub server_id: String,
    /// Runtime generation that owns the session.
    pub runtime_generation: u64,
    /// JSON-RPC initialize body.
    pub body: Vec<u8>,
    /// Maximum bytes the port may allocate for a response.
    pub max_response_bytes: usize,
}

/// Request sent through a pinned remote MCP session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHttpSessionExchangeRequest {
    /// Host-configured POST or Streamable HTTP endpoint identity.
    pub endpoint_id: String,
    /// Stable MCP server identity.
    pub server_id: String,
    /// Runtime generation that owns the session.
    pub runtime_generation: u64,
    /// Exact server-issued session identifier.
    pub session_id: String,
    /// JSON-RPC request, response, or notification body.
    pub body: Vec<u8>,
    /// Maximum bytes the port may allocate for a response.
    pub max_response_bytes: usize,
}

/// Request for the next persistent HTTP/SSE event chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHttpSessionEventRequest {
    /// Host-configured event endpoint identity.
    pub endpoint_id: String,
    /// Stable MCP server identity.
    pub server_id: String,
    /// Runtime generation that owns the session.
    pub runtime_generation: u64,
    /// Exact server-issued session identifier.
    pub session_id: String,
    /// Last accepted SSE event cursor.
    pub last_event_id: Option<String>,
    /// Maximum bytes the port may allocate for one chunk.
    pub max_response_bytes: usize,
}

/// Request to deterministically release a remote session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHttpSessionCloseRequest {
    /// Host-configured event or Streamable HTTP endpoint identity.
    pub endpoint_id: String,
    /// Stable MCP server identity.
    pub server_id: String,
    /// Runtime generation that owns the session.
    pub runtime_generation: u64,
    /// Exact server-issued session identifier.
    pub session_id: String,
    /// Maximum bytes the port may allocate for the close response.
    pub max_response_bytes: usize,
}

/// Host-owned remote session port.
#[async_trait]
pub trait McpHttpSessionPort: Send + Sync {
    /// Opens, initializes, and returns the first bounded response.
    async fn open(
        &self,
        request: &McpHttpSessionOpenRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError>;

    /// Sends one request through the exact pinned session.
    async fn exchange(
        &self,
        request: &McpHttpSessionExchangeRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError>;

    /// Reads the next bounded event chunk.
    ///
    /// Implementations must make this method cancellation safe.
    async fn next_event(
        &self,
        request: &McpHttpSessionEventRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError>;

    /// Closes the exact pinned session and releases host resources.
    async fn close(
        &self,
        request: &McpHttpSessionCloseRequest,
    ) -> Result<McpHttpSessionResponse, McpConnectorPortError>;
}

/// Configuration for a persistent Streamable HTTP MCP connector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHttpConnectorConfig {
    /// Host-configured Streamable HTTP endpoint identity.
    pub endpoint_id: String,
    /// Restored durable catalog state.
    pub catalog_state: McpConnectorCatalogState,
    /// Bounded body, queue, and expiry limits.
    pub limits: McpConnectorLimits,
}

/// Persistent Streamable HTTP connector.
pub struct McpHttpConnector {
    inner: RemoteConnector,
}

impl McpHttpConnector {
    /// Creates a connector without opening a network session.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for invalid endpoint identity or limits.
    pub fn new(
        port: Arc<dyn McpHttpSessionPort>,
        config: McpHttpConnectorConfig,
    ) -> Result<Self, McpTransportError> {
        let endpoint_id = config.endpoint_id;
        let inner = RemoteConnector::new(
            port,
            RemoteConnectorConfig {
                transport: McpSessionTransportKind::StreamableHttp,
                event_endpoint_id: endpoint_id.clone(),
                request_endpoint_id: endpoint_id,
                catalog_state: config.catalog_state,
                limits: config.limits,
            },
        )?;
        Ok(Self { inner })
    }

    /// Returns the shared redaction-safe evidence handle.
    pub fn evidence(&self) -> McpConnectorEvidenceHandle {
        self.inner.evidence()
    }
}

#[async_trait]
impl McpSessionConnector for McpHttpConnector {
    async fn connect(
        &self,
        request: &McpConnectRequest,
    ) -> Result<Box<dyn McpTransportSession>, McpTransportError> {
        if request.transport != McpSessionTransportKind::StreamableHttp {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.http.transport_mismatch",
            });
        }
        Ok(Box::new(self.inner.connect(request).await?))
    }
}

#[derive(Debug, Clone)]
pub(super) struct RemoteConnectorConfig {
    pub(super) transport: McpSessionTransportKind,
    pub(super) event_endpoint_id: String,
    pub(super) request_endpoint_id: String,
    pub(super) catalog_state: McpConnectorCatalogState,
    pub(super) limits: McpConnectorLimits,
}

pub(super) struct RemoteConnector {
    port: Arc<dyn McpHttpSessionPort>,
    config: RemoteConnectorConfig,
    catalog: Arc<Mutex<CatalogTracker>>,
    evidence: McpConnectorEvidenceHandle,
}

impl RemoteConnector {
    pub(super) fn new(
        port: Arc<dyn McpHttpSessionPort>,
        config: RemoteConnectorConfig,
    ) -> Result<Self, McpTransportError> {
        config.limits.validate()?;
        config.catalog_state.validate()?;
        if !valid_endpoint_id(&config.event_endpoint_id)
            || !valid_endpoint_id(&config.request_endpoint_id)
            || config.transport == McpSessionTransportKind::Stdio
        {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.http.invalid_endpoint",
            });
        }
        let catalog = CatalogTracker::new(config.catalog_state.clone())?;
        let evidence = McpConnectorEvidenceHandle::new(config.limits.max_stderr_tail_bytes);
        Ok(Self { port, config, catalog: Arc::new(Mutex::new(catalog)), evidence })
    }

    pub(super) fn evidence(&self) -> McpConnectorEvidenceHandle {
        self.evidence.clone()
    }

    pub(super) async fn connect(
        &self,
        request: &McpConnectRequest,
    ) -> Result<McpConnectedSession, McpTransportError> {
        request.validate()?;
        if request.transport != self.config.transport {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.http.transport_mismatch",
            });
        }
        let initialize_body = encode_value(&initialize_message(&request.initialize)?)?;
        let open = self
            .port
            .open(&McpHttpSessionOpenRequest {
                endpoint_id: self.config.event_endpoint_id.clone(),
                paired_request_endpoint_id: (self.config.transport
                    == McpSessionTransportKind::ServerSentEvents)
                    .then(|| self.config.request_endpoint_id.clone()),
                server_id: request.server_id.clone(),
                runtime_generation: request.runtime_generation,
                body: initialize_body,
                max_response_bytes: self.config.limits.max_http_body_bytes,
            })
            .await
            .map_err(McpConnectorPortError::into_transport_error)?;
        self.validate_response(&open, request.runtime_generation, None)?;
        let session_id = open.session_id.clone().ok_or(McpTransportError::InvalidHandshake)?;
        let initialize_value = decode_remote_json(&open, self.config.limits.max_http_body_bytes)?;
        if initialize_value.get("id").and_then(Value::as_str) != Some(INITIALIZE_ID) {
            return Err(McpTransportError::InvalidHandshake);
        }
        let mut initialize_result = parse_initialize_response(&initialize_value)?;
        let expiry = Arc::new(AtomicI64::new(session_expiry(
            open.expires_at_unix_ms,
            self.config.limits.session_idle_timeout_ms,
        )?));

        self.exchange_notification(request, &session_id, &expiry, initialized_notification())
            .await?;
        let tools = self
            .exchange_catalog(request, &session_id, &expiry, TOOLS_LIST_ID, "tools/list")
            .await?;
        let resources = self
            .exchange_catalog(request, &session_id, &expiry, RESOURCES_LIST_ID, "resources/list")
            .await?;
        let prompts = self
            .exchange_catalog(request, &session_id, &expiry, PROMPTS_LIST_ID, "prompts/list")
            .await?;
        initialize_result.catalog_digest = catalog_digest(&tools, &resources, &prompts)?;
        {
            let mut catalog = self.catalog.lock().map_err(|_| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.http.catalog_lock_poisoned".to_owned(),
            })?;
            catalog.observe_initial(&initialize_result.catalog_digest)?;
        }

        let (response_sender, response_receiver) =
            mpsc::channel(self.config.limits.response_queue_capacity);
        let writer = RemoteSessionWriter {
            port: Arc::clone(&self.port),
            transport: self.config.transport,
            request_endpoint_id: self.config.request_endpoint_id.clone(),
            event_endpoint_id: self.config.event_endpoint_id.clone(),
            server_id: request.server_id.clone(),
            runtime_generation: request.runtime_generation,
            session_id: session_id.clone(),
            max_body_bytes: self.config.limits.max_http_body_bytes,
            idle_timeout_ms: self.config.limits.session_idle_timeout_ms,
            expiry: Arc::clone(&expiry),
            response_sender,
            evidence: self.evidence.clone(),
            closed: false,
        };
        let reader = RemoteSessionReader {
            port: Arc::clone(&self.port),
            transport: self.config.transport,
            event_endpoint_id: self.config.event_endpoint_id.clone(),
            server_id: request.server_id.clone(),
            runtime_generation: request.runtime_generation,
            session_id,
            max_body_bytes: self.config.limits.max_http_body_bytes,
            max_sse_event_bytes: self.config.limits.max_sse_event_bytes,
            idle_timeout_ms: self.config.limits.session_idle_timeout_ms,
            expiry,
            responses: response_receiver,
            catalog: Arc::clone(&self.catalog),
            evidence: self.evidence.clone(),
            sse: BoundedSseDecoder::new(self.config.limits.max_sse_event_bytes),
            last_event_id: open.last_event_id,
        };
        McpConnectedSession::new(initialize_result, Box::new(writer), Box::new(reader))
    }

    async fn exchange_notification(
        &self,
        request: &McpConnectRequest,
        session_id: &str,
        expiry: &AtomicI64,
        value: Value,
    ) -> Result<(), McpTransportError> {
        let response = self.exchange(request, session_id, encode_value(&value)?).await?;
        update_expiry(
            expiry,
            response.expires_at_unix_ms,
            self.config.limits.session_idle_timeout_ms,
        )?;
        Ok(())
    }

    async fn exchange_catalog(
        &self,
        request: &McpConnectRequest,
        session_id: &str,
        expiry: &AtomicI64,
        id: &'static str,
        method: &'static str,
    ) -> Result<Value, McpTransportError> {
        let response =
            self.exchange(request, session_id, encode_value(&catalog_request(id, method))?).await?;
        update_expiry(
            expiry,
            response.expires_at_unix_ms,
            self.config.limits.session_idle_timeout_ms,
        )?;
        let value = decode_remote_json(&response, self.config.limits.max_http_body_bytes)?;
        parse_catalog_response(&value, id)
    }

    async fn exchange(
        &self,
        request: &McpConnectRequest,
        session_id: &str,
        body: Vec<u8>,
    ) -> Result<McpHttpSessionResponse, McpTransportError> {
        let response = self
            .port
            .exchange(&McpHttpSessionExchangeRequest {
                endpoint_id: self.config.request_endpoint_id.clone(),
                server_id: request.server_id.clone(),
                runtime_generation: request.runtime_generation,
                session_id: session_id.to_owned(),
                body,
                max_response_bytes: self.config.limits.max_http_body_bytes,
            })
            .await
            .map_err(McpConnectorPortError::into_transport_error)?;
        self.validate_response(&response, request.runtime_generation, Some(session_id))?;
        Ok(response)
    }

    fn validate_response(
        &self,
        response: &McpHttpSessionResponse,
        runtime_generation: u64,
        pinned_session_id: Option<&str>,
    ) -> Result<(), McpTransportError> {
        response.validate(self.config.limits.max_http_body_bytes)?;
        if let Some(reason_code) = classify_http_status(response.status) {
            self.evidence.record(evidence(
                self.config.transport,
                runtime_generation,
                reason_code,
                Some(response.status),
                pinned_session_id.or(response.session_id.as_deref()),
            ));
            return Err(McpTransportError::Unavailable { reason_code: reason_code.to_owned() });
        }
        if let Some(pinned_session_id) = pinned_session_id {
            ensure_pinned_session(pinned_session_id, response.session_id.as_deref())?;
        }
        Ok(())
    }
}

struct QueuedRemoteBody {
    content_type: String,
    body: Vec<u8>,
    last_event_id: Option<String>,
    stream_closed: bool,
}

struct RemoteSessionWriter {
    port: Arc<dyn McpHttpSessionPort>,
    transport: McpSessionTransportKind,
    request_endpoint_id: String,
    event_endpoint_id: String,
    server_id: String,
    runtime_generation: u64,
    session_id: String,
    max_body_bytes: usize,
    idle_timeout_ms: u64,
    expiry: Arc<AtomicI64>,
    response_sender: mpsc::Sender<QueuedRemoteBody>,
    evidence: McpConnectorEvidenceHandle,
    closed: bool,
}

#[async_trait]
impl McpSessionWriter for RemoteSessionWriter {
    async fn send_request(&mut self, request: McpSessionRequest) -> Result<(), McpTransportError> {
        self.exchange(encode_session_request(&request)?).await
    }

    async fn send_callback_response(
        &mut self,
        response: McpServerCallbackResponse,
    ) -> Result<(), McpTransportError> {
        self.exchange(encode_callback_response(&response)?).await
    }

    async fn close(&mut self) -> Result<(), McpTransportError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        let response = self
            .port
            .close(&McpHttpSessionCloseRequest {
                endpoint_id: self.event_endpoint_id.clone(),
                server_id: self.server_id.clone(),
                runtime_generation: self.runtime_generation,
                session_id: self.session_id.clone(),
                max_response_bytes: self.max_body_bytes,
            })
            .await
            .map_err(McpConnectorPortError::into_transport_error)?;
        response.validate(self.max_body_bytes)?;
        ensure_pinned_session(&self.session_id, response.session_id.as_deref())?;
        if let Some(reason_code) = classify_http_status(response.status) {
            self.record(reason_code, Some(response.status));
            return Err(McpTransportError::Unavailable { reason_code: reason_code.to_owned() });
        }
        self.record("mcp.runtime.http.clean_close", Some(response.status));
        Ok(())
    }
}

impl RemoteSessionWriter {
    async fn exchange(&mut self, body: Vec<u8>) -> Result<(), McpTransportError> {
        ensure_not_expired(
            &self.expiry,
            self.transport,
            self.runtime_generation,
            &self.evidence,
            &self.session_id,
        )?;
        if body.len() > self.max_body_bytes {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.http.request_too_large",
            });
        }
        let response = self
            .port
            .exchange(&McpHttpSessionExchangeRequest {
                endpoint_id: self.request_endpoint_id.clone(),
                server_id: self.server_id.clone(),
                runtime_generation: self.runtime_generation,
                session_id: self.session_id.clone(),
                body,
                max_response_bytes: self.max_body_bytes,
            })
            .await
            .map_err(McpConnectorPortError::into_transport_error)?;
        response.validate(self.max_body_bytes)?;
        ensure_pinned_session(&self.session_id, response.session_id.as_deref())?;
        if let Some(reason_code) = classify_http_status(response.status) {
            self.record(reason_code, Some(response.status));
            return Err(McpTransportError::Unavailable { reason_code: reason_code.to_owned() });
        }
        update_expiry(&self.expiry, response.expires_at_unix_ms, self.idle_timeout_ms)?;
        if !response.body.is_empty() || response.stream_closed {
            self.response_sender
                .try_send(QueuedRemoteBody {
                    content_type: response.content_type,
                    body: response.body,
                    last_event_id: response.last_event_id,
                    stream_closed: response.stream_closed,
                })
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => McpTransportError::Unavailable {
                        reason_code: "mcp.runtime.http.response_backpressure".to_owned(),
                    },
                    mpsc::error::TrySendError::Closed(_) => McpTransportError::Unavailable {
                        reason_code: "mcp.runtime.http.reader_closed".to_owned(),
                    },
                })?;
        }
        Ok(())
    }

    fn record(&self, reason_code: &str, status: Option<u16>) {
        self.evidence.record(evidence(
            self.transport,
            self.runtime_generation,
            reason_code,
            status,
            Some(&self.session_id),
        ));
    }
}

struct RemoteSessionReader {
    port: Arc<dyn McpHttpSessionPort>,
    transport: McpSessionTransportKind,
    event_endpoint_id: String,
    server_id: String,
    runtime_generation: u64,
    session_id: String,
    max_body_bytes: usize,
    max_sse_event_bytes: usize,
    idle_timeout_ms: u64,
    expiry: Arc<AtomicI64>,
    responses: mpsc::Receiver<QueuedRemoteBody>,
    catalog: Arc<Mutex<CatalogTracker>>,
    evidence: McpConnectorEvidenceHandle,
    sse: BoundedSseDecoder,
    last_event_id: Option<String>,
}

#[async_trait]
impl McpSessionReader for RemoteSessionReader {
    async fn next_event(&mut self) -> Result<McpTransportEvent, McpTransportError> {
        loop {
            if let Some(event) = self.pop_sse_event()? {
                return self.route_value(event);
            }
            match self.responses.try_recv() {
                Ok(response) => {
                    if let Some(event) = self.accept_body(response)? {
                        return self.route_value(event);
                    }
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    return Ok(McpTransportEvent::Closed {
                        reason_code: "mcp.runtime.http.writer_closed".to_owned(),
                    });
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            ensure_not_expired(
                &self.expiry,
                self.transport,
                self.runtime_generation,
                &self.evidence,
                &self.session_id,
            )?;
            let response = self
                .port
                .next_event(&McpHttpSessionEventRequest {
                    endpoint_id: self.event_endpoint_id.clone(),
                    server_id: self.server_id.clone(),
                    runtime_generation: self.runtime_generation,
                    session_id: self.session_id.clone(),
                    last_event_id: self.last_event_id.clone(),
                    max_response_bytes: self.max_body_bytes,
                })
                .await
                .map_err(McpConnectorPortError::into_transport_error)?;
            response.validate(self.max_body_bytes)?;
            ensure_pinned_session(&self.session_id, response.session_id.as_deref())?;
            if let Some(reason_code) = classify_http_status(response.status) {
                self.record(reason_code, Some(response.status));
                return Err(McpTransportError::Unavailable { reason_code: reason_code.to_owned() });
            }
            update_expiry(&self.expiry, response.expires_at_unix_ms, self.idle_timeout_ms)?;
            if let Some(event) = self.accept_body(QueuedRemoteBody {
                content_type: response.content_type,
                body: response.body,
                last_event_id: response.last_event_id,
                stream_closed: response.stream_closed,
            })? {
                return self.route_value(event);
            }
        }
    }
}

impl RemoteSessionReader {
    fn accept_body(
        &mut self,
        response: QueuedRemoteBody,
    ) -> Result<Option<Value>, McpTransportError> {
        if let Some(last_event_id) = response.last_event_id {
            self.last_event_id = Some(last_event_id);
        }
        if response.body.is_empty() {
            if response.stream_closed {
                return Err(McpTransportError::Unavailable {
                    reason_code: "mcp.runtime.http.stream_closed".to_owned(),
                });
            }
            return Ok(None);
        }
        if is_sse_content_type(&response.content_type)
            || self.transport == McpSessionTransportKind::ServerSentEvents
        {
            if response.body.len() > self.max_sse_event_bytes {
                return Err(McpTransportError::MalformedFrame {
                    reason_code: "mcp.runtime.sse.chunk_too_large",
                });
            }
            self.sse.push(&response.body)?;
            return self.pop_sse_event();
        }
        if !is_json_content_type(&response.content_type) {
            return Err(McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.http.content_type_invalid",
            });
        }
        decode_value(&response.body, self.max_body_bytes).map(Some)
    }

    fn pop_sse_event(&mut self) -> Result<Option<Value>, McpTransportError> {
        let Some(event) = self.sse.pop_event() else {
            return Ok(None);
        };
        if let Some(id) = event.id {
            self.last_event_id = (!id.is_empty()).then_some(id);
        }
        if event.event_type.as_deref() == Some("ping") {
            return Ok(None);
        }
        decode_value(&event.data, self.max_sse_event_bytes).map(Some)
    }

    fn route_value(&mut self, value: Value) -> Result<McpTransportEvent, McpTransportError> {
        let epoch = self
            .catalog
            .lock()
            .map_err(|_| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.http.catalog_lock_poisoned".to_owned(),
            })?
            .epoch();
        let event = parse_runtime_event(value, self.runtime_generation, epoch)?;
        if matches!(
            event,
            McpTransportEvent::Notification {
                notification: McpServerNotification::CatalogChanged { .. },
                ..
            }
        ) {
            self.catalog
                .lock()
                .map_err(|_| McpTransportError::Unavailable {
                    reason_code: "mcp.runtime.http.catalog_lock_poisoned".to_owned(),
                })?
                .advance_notification()?;
        }
        Ok(event)
    }

    fn record(&self, reason_code: &str, status: Option<u16>) {
        self.evidence.record(evidence(
            self.transport,
            self.runtime_generation,
            reason_code,
            status,
            Some(&self.session_id),
        ));
    }
}

fn decode_remote_json(
    response: &McpHttpSessionResponse,
    max_body_bytes: usize,
) -> Result<Value, McpTransportError> {
    if !is_json_content_type(&response.content_type) {
        return Err(McpTransportError::MalformedFrame {
            reason_code: "mcp.runtime.http.content_type_invalid",
        });
    }
    decode_value(&response.body, max_body_bytes)
}

fn update_expiry(
    expiry: &AtomicI64,
    server_expiry: Option<i64>,
    idle_timeout_ms: u64,
) -> Result<(), McpTransportError> {
    expiry.store(session_expiry(server_expiry, idle_timeout_ms)?, Ordering::Release);
    Ok(())
}

fn ensure_not_expired(
    expiry: &AtomicI64,
    transport: McpSessionTransportKind,
    runtime_generation: u64,
    evidence_handle: &McpConnectorEvidenceHandle,
    session_id: &str,
) -> Result<(), McpTransportError> {
    if now_unix_ms() < expiry.load(Ordering::Acquire) {
        return Ok(());
    }
    let reason_code = "mcp.runtime.http.session_expired";
    evidence_handle.record(evidence(
        transport,
        runtime_generation,
        reason_code,
        Some(410),
        Some(session_id),
    ));
    Err(McpTransportError::Unavailable { reason_code: reason_code.to_owned() })
}

fn valid_endpoint_id(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= 128
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
}

fn is_json_content_type(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"))
}

fn is_sse_content_type(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("text/event-stream"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_classification_distinguishes_auth_missing_and_expired_sessions() {
        assert_eq!(classify_http_status(401), Some("mcp.runtime.http.authorization_required"));
        assert_eq!(classify_http_status(404), Some("mcp.runtime.http.session_not_found"));
        assert_eq!(classify_http_status(410), Some("mcp.runtime.http.session_expired"));
        assert_eq!(classify_http_status(204), None);
    }

    #[test]
    fn expired_session_fails_before_another_port_call() {
        let expiry = AtomicI64::new(now_unix_ms().saturating_sub(1));
        let evidence = McpConnectorEvidenceHandle::new(128);
        let error = ensure_not_expired(
            &expiry,
            McpSessionTransportKind::StreamableHttp,
            3,
            &evidence,
            "session-a",
        )
        .expect_err("expired session is rejected");
        assert!(matches!(
            error,
            McpTransportError::Unavailable {
                reason_code
            } if reason_code == "mcp.runtime.http.session_expired"
        ));
        assert_eq!(evidence.snapshot().reconnect_events.len(), 1);
    }

    #[test]
    fn changed_session_id_is_rejected() {
        let error = ensure_pinned_session("session-a", Some("session-b"))
            .expect_err("session replacement is rejected");
        assert!(matches!(
            error,
            McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.http.session_id_changed"
            }
        ));
    }
}
