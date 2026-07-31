//! Host-governed persistent MCP protocol connectors.
//!
//! The adapters in this module own framing and protocol state only. Process
//! creation, HTTP execution, credentials, egress policy, and cleanup remain
//! behind injected host ports.

mod http;
mod sse;
mod stdio;

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_common::redaction::redact_diagnostic_text;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{
    McpCallbackResponsePayload, McpElicitationRequest, McpInitializeRequest, McpInitializeResult,
    McpRemoteError, McpResponsePayload, McpSamplingRequest, McpServerCallbackRequest,
    McpServerCallbackResponse, McpServerCallbackType, McpServerNotification, McpSessionRequest,
    McpSessionTransportKind, McpTransportError, McpTransportEvent,
};

pub use http::{
    McpHttpConnector, McpHttpConnectorConfig, McpHttpSessionCloseRequest,
    McpHttpSessionEventRequest, McpHttpSessionExchangeRequest, McpHttpSessionOpenRequest,
    McpHttpSessionPort,
};
pub use sse::{McpSseConnector, McpSseConnectorConfig};
pub use stdio::{
    McpByteReader, McpByteWriter, McpLaunchedProcessSession, McpProcessCloseEvidence,
    McpProcessControl, McpProcessLaunchRequest, McpProcessLauncher, McpStdioConnector,
    McpStdioConnectorConfig,
};

pub(super) const JSONRPC_VERSION: &str = "2.0";
pub(super) const INITIALIZE_ID: &str = "palyra.initialize";
pub(super) const TOOLS_LIST_ID: &str = "palyra.catalog.tools";
pub(super) const RESOURCES_LIST_ID: &str = "palyra.catalog.resources";
pub(super) const PROMPTS_LIST_ID: &str = "palyra.catalog.prompts";

const MAX_EVIDENCE_EVENTS: usize = 128;
const MAX_REASON_CODE_BYTES: usize = 192;
const MAX_SESSION_ID_BYTES: usize = 512;

/// Bounds applied independently of concrete process or HTTP implementations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConnectorLimits {
    /// Maximum encoded JSON-RPC frame.
    pub max_frame_bytes: usize,
    /// Maximum HTTP response or event chunk.
    pub max_http_body_bytes: usize,
    /// Maximum decoded SSE event.
    pub max_sse_event_bytes: usize,
    /// Maximum retained redacted stderr tail.
    pub max_stderr_tail_bytes: usize,
    /// Bounded internal response/event queue.
    pub response_queue_capacity: usize,
    /// Idle lifetime used when a remote server supplies no earlier expiry.
    pub session_idle_timeout_ms: u64,
}

impl Default for McpConnectorLimits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 1024 * 1024,
            max_http_body_bytes: 1024 * 1024,
            max_sse_event_bytes: 1024 * 1024,
            max_stderr_tail_bytes: 16 * 1024,
            response_queue_capacity: 128,
            session_idle_timeout_ms: 60_000,
        }
    }
}

impl McpConnectorLimits {
    /// Validates non-zero bounded connector limits.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for unsafe limits.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        if self.max_frame_bytes == 0
            || self.max_http_body_bytes == 0
            || self.max_sse_event_bytes == 0
            || self.max_stderr_tail_bytes == 0
            || self.response_queue_capacity == 0
            || self.session_idle_timeout_ms == 0
        {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.connector.invalid_limits",
            });
        }
        Ok(())
    }
}

/// Catalog state restored before a connector establishes its first generation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct McpConnectorCatalogState {
    /// Last durable catalog epoch.
    pub catalog_epoch: u64,
    /// Digest represented by the last durable epoch.
    pub catalog_digest: Option<String>,
}

impl McpConnectorCatalogState {
    /// Validates epoch and lowercase SHA-256 consistency.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for inconsistent state.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        let valid_digest = self.catalog_digest.as_deref().is_none_or(is_sha256);
        let consistent =
            (self.catalog_epoch == 0 && self.catalog_digest.is_none()) || self.catalog_epoch > 0;
        if !valid_digest || !consistent {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.connector.invalid_catalog_state",
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct CatalogTracker {
    epoch: u64,
    digest: Option<String>,
}

impl CatalogTracker {
    pub(super) fn new(state: McpConnectorCatalogState) -> Result<Self, McpTransportError> {
        state.validate()?;
        Ok(Self { epoch: state.catalog_epoch, digest: state.catalog_digest })
    }

    pub(super) fn observe_initial(&mut self, digest: &str) -> Result<u64, McpTransportError> {
        if !is_sha256(digest) {
            return Err(McpTransportError::InvalidHandshake);
        }
        if self.epoch == 0 || self.digest.as_deref() != Some(digest) {
            self.epoch =
                self.epoch.checked_add(1).ok_or_else(|| McpTransportError::Unavailable {
                    reason_code: "mcp.runtime.catalog_epoch_exhausted".to_owned(),
                })?;
        }
        self.digest = Some(digest.to_owned());
        Ok(self.epoch)
    }

    pub(super) fn advance_notification(&mut self) -> Result<u64, McpTransportError> {
        self.epoch = self.epoch.checked_add(1).ok_or_else(|| McpTransportError::Unavailable {
            reason_code: "mcp.runtime.catalog_epoch_exhausted".to_owned(),
        })?;
        Ok(self.epoch)
    }

    pub(super) fn epoch(&self) -> u64 {
        self.epoch
    }
}

/// Redaction-safe connector failure or cleanup observation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpReconnectEvidence {
    /// Transport that emitted the observation.
    pub transport: McpSessionTransportKind,
    /// Runtime generation.
    pub runtime_generation: u64,
    /// Stable failure or closure reason.
    pub reason_code: String,
    /// HTTP status when applicable.
    pub http_status: Option<u16>,
    /// SHA-256 of a remote session identifier when applicable.
    pub session_id_sha256: Option<String>,
    /// Evidence time.
    pub occurred_at_unix_ms: i64,
}

/// Bounded redaction-safe connector diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConnectorEvidenceSnapshot {
    /// Redacted tail of process stderr.
    pub stderr_tail_redacted: String,
    /// Recent reconnect, expiry, and cleanup observations.
    pub reconnect_events: Vec<McpReconnectEvidence>,
    /// Number of evidence events dropped from the bounded history.
    pub dropped_reconnect_events: u64,
}

/// Shared read handle for connector diagnostics.
#[derive(Debug, Clone)]
pub struct McpConnectorEvidenceHandle {
    inner: Arc<Mutex<EvidenceState>>,
}

impl McpConnectorEvidenceHandle {
    pub(super) fn new(max_stderr_tail_bytes: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(EvidenceState {
                max_stderr_tail_bytes,
                stderr_tail_redacted: String::new(),
                reconnect_events: VecDeque::new(),
                dropped_reconnect_events: 0,
            })),
        }
    }

    /// Returns a consistent redaction-safe diagnostic snapshot.
    pub fn snapshot(&self) -> McpConnectorEvidenceSnapshot {
        self.inner.lock().map_or_else(
            |_| McpConnectorEvidenceSnapshot {
                stderr_tail_redacted: String::new(),
                reconnect_events: Vec::new(),
                dropped_reconnect_events: 0,
            },
            |state| McpConnectorEvidenceSnapshot {
                stderr_tail_redacted: state.stderr_tail_redacted.clone(),
                reconnect_events: state.reconnect_events.iter().cloned().collect(),
                dropped_reconnect_events: state.dropped_reconnect_events,
            },
        )
    }

    pub(super) fn append_stderr(&self, bytes: &[u8]) {
        let decoded = String::from_utf8_lossy(bytes);
        let redacted = redact_diagnostic_text(&decoded);
        if let Ok(mut state) = self.inner.lock() {
            let max_stderr_tail_bytes = state.max_stderr_tail_bytes;
            state.stderr_tail_redacted.push_str(&redacted);
            truncate_front_utf8(&mut state.stderr_tail_redacted, max_stderr_tail_bytes);
        }
    }

    pub(super) fn record(&self, evidence: McpReconnectEvidence) {
        if let Ok(mut state) = self.inner.lock() {
            if state.reconnect_events.len() >= MAX_EVIDENCE_EVENTS {
                state.reconnect_events.pop_front();
                state.dropped_reconnect_events = state.dropped_reconnect_events.saturating_add(1);
            }
            state.reconnect_events.push_back(evidence);
        }
    }
}

#[derive(Debug)]
struct EvidenceState {
    max_stderr_tail_bytes: usize,
    stderr_tail_redacted: String,
    reconnect_events: VecDeque<McpReconnectEvidence>,
    dropped_reconnect_events: u64,
}

/// Host-port failure containing only a stable redaction-safe reason.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("mcp connector host port failed: {reason_code}")]
pub struct McpConnectorPortError {
    /// Stable reason code supplied by the host adapter.
    pub reason_code: String,
}

impl McpConnectorPortError {
    pub(super) fn into_transport_error(self) -> McpTransportError {
        McpTransportError::Unavailable { reason_code: sanitize_reason_code(&self.reason_code) }
    }
}

/// One response returned by the host-governed HTTP session port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpHttpSessionResponse {
    /// HTTP status.
    pub status: u16,
    /// Session identifier returned by the MCP server.
    pub session_id: Option<String>,
    /// Response content type.
    pub content_type: String,
    /// Bounded response bytes.
    pub body: Vec<u8>,
    /// Server-declared absolute session expiry.
    pub expires_at_unix_ms: Option<i64>,
    /// SSE last-event cursor when returned out of band.
    pub last_event_id: Option<String>,
    /// Whether the host observed deterministic end-of-stream.
    pub stream_closed: bool,
}

impl McpHttpSessionResponse {
    pub(super) fn validate(&self, max_body_bytes: usize) -> Result<(), McpTransportError> {
        if self.body.len() > max_body_bytes
            || !(100..=599).contains(&self.status)
            || self.content_type.len() > 256
            || self.content_type.chars().any(char::is_control)
            || self.session_id.as_deref().is_some_and(|session_id| !valid_session_id(session_id))
            || self.last_event_id.as_deref().is_some_and(|event_id| !valid_cursor(event_id))
        {
            return Err(McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.http.response_invalid",
            });
        }
        Ok(())
    }
}

pub(super) fn initialize_message(
    request: &McpInitializeRequest,
) -> Result<Value, McpTransportError> {
    request.validate()?;
    let protocol_version =
        request.supported_protocol_versions.first().ok_or(McpTransportError::InvalidRequest {
            reason_code: "mcp.runtime.initialize.protocol_missing",
        })?;
    Ok(json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": INITIALIZE_ID,
        "method": "initialize",
        "params": {
            "protocolVersion": protocol_version,
            "capabilities": request.capabilities,
            "clientInfo": {
                "name": request.client_name,
                "version": request.client_version,
            },
        },
    }))
}

pub(super) fn initialized_notification() -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "method": "notifications/initialized",
        "params": {},
    })
}

pub(super) fn catalog_request(id: &'static str, method: &'static str) -> Value {
    json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": id,
        "method": method,
        "params": {},
    })
}

pub(super) fn encode_session_request(
    request: &McpSessionRequest,
) -> Result<Vec<u8>, McpTransportError> {
    request.validate()?;
    encode_value(&json!({
        "jsonrpc": JSONRPC_VERSION,
        "id": request.request_id,
        "method": request.method,
        "params": request.params_json,
    }))
}

pub(super) fn encode_callback_response(
    response: &McpServerCallbackResponse,
) -> Result<Vec<u8>, McpTransportError> {
    response.validate()?;
    let value = match &response.payload {
        McpCallbackResponsePayload::Success(result) => json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": response.callback_id,
            "result": result,
        }),
        McpCallbackResponsePayload::Rejected { reason_code, safe_message } => json!({
            "jsonrpc": JSONRPC_VERSION,
            "id": response.callback_id,
            "error": {
                "code": -32000,
                "message": safe_message,
                "data": {"reason_code": reason_code},
            },
        }),
    };
    encode_value(&value)
}

pub(super) fn encode_value(value: &Value) -> Result<Vec<u8>, McpTransportError> {
    serde_json::to_vec(value).map_err(|_| McpTransportError::InvalidRequest {
        reason_code: "mcp.runtime.connector.encode_failed",
    })
}

pub(super) fn decode_value(bytes: &[u8], max_bytes: usize) -> Result<Value, McpTransportError> {
    if bytes.is_empty() || bytes.len() > max_bytes {
        return Err(McpTransportError::MalformedFrame {
            reason_code: "mcp.runtime.connector.frame_size",
        });
    }
    serde_json::from_slice(bytes).map_err(|_| McpTransportError::MalformedFrame {
        reason_code: "mcp.runtime.connector.invalid_json",
    })
}

pub(super) fn parse_initialize_response(
    value: &Value,
) -> Result<McpInitializeResult, McpTransportError> {
    validate_jsonrpc(value)?;
    if value.get("id").and_then(Value::as_str) != Some(INITIALIZE_ID)
        || value.get("error").is_some()
    {
        return Err(McpTransportError::InvalidHandshake);
    }
    let result = value.get("result").ok_or(McpTransportError::InvalidHandshake)?;
    let protocol_version = result
        .get("protocolVersion")
        .and_then(Value::as_str)
        .ok_or(McpTransportError::InvalidHandshake)?;
    let server_info = result.get("serverInfo").ok_or(McpTransportError::InvalidHandshake)?;
    let server_name = server_info
        .get("name")
        .and_then(Value::as_str)
        .ok_or(McpTransportError::InvalidHandshake)?;
    let server_version = server_info
        .get("version")
        .and_then(Value::as_str)
        .ok_or(McpTransportError::InvalidHandshake)?;
    let capabilities_json =
        result.get("capabilities").cloned().ok_or(McpTransportError::InvalidHandshake)?;
    Ok(McpInitializeResult {
        protocol_version: protocol_version.to_owned(),
        server_name: server_name.to_owned(),
        server_version: server_version.to_owned(),
        capabilities_json,
        catalog_digest: "0".repeat(64),
    })
}

pub(super) fn parse_catalog_response(
    value: &Value,
    expected_id: &str,
) -> Result<Value, McpTransportError> {
    validate_jsonrpc(value)?;
    if value.get("id").and_then(Value::as_str) != Some(expected_id) {
        return Err(McpTransportError::InvalidHandshake);
    }
    if let Some(error) = value.get("error") {
        if error.get("code").and_then(Value::as_i64) == Some(-32601) {
            return Ok(json!({"unsupported": true}));
        }
        return Err(McpTransportError::InvalidHandshake);
    }
    value.get("result").cloned().ok_or(McpTransportError::InvalidHandshake)
}

pub(super) fn catalog_digest(
    tools: &Value,
    resources: &Value,
    prompts: &Value,
) -> Result<String, McpTransportError> {
    let encoded = encode_value(&json!({
        "tools": tools,
        "resources": resources,
        "prompts": prompts,
    }))?;
    Ok(hex::encode(Sha256::digest(encoded)))
}

pub(super) fn parse_runtime_event(
    value: Value,
    runtime_generation: u64,
    catalog_epoch: u64,
) -> Result<McpTransportEvent, McpTransportError> {
    validate_jsonrpc(&value)?;
    if let Some(request_id) = value.get("id").and_then(Value::as_u64) {
        if let Some(method) = value.get("method").and_then(Value::as_str) {
            return parse_callback(
                request_id,
                method,
                value.get("params").cloned().unwrap_or_else(|| json!({})),
                runtime_generation,
                catalog_epoch,
            );
        }
        let payload = if let Some(result) = value.get("result") {
            McpResponsePayload::Success(result.clone())
        } else {
            let error = value.get("error").ok_or(McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.connector.response_missing_payload",
            })?;
            let code = error.get("code").and_then(Value::as_i64).ok_or(
                McpTransportError::MalformedFrame {
                    reason_code: "mcp.runtime.connector.response_error_invalid",
                },
            )?;
            let safe_message =
                error.get("message").and_then(Value::as_str).map(redact_diagnostic_text).ok_or(
                    McpTransportError::MalformedFrame {
                        reason_code: "mcp.runtime.connector.response_error_invalid",
                    },
                )?;
            McpResponsePayload::Error(McpRemoteError {
                code,
                safe_message,
                data_json: error.get("data").cloned(),
            })
        };
        return Ok(McpTransportEvent::Response { request_id, runtime_generation, payload });
    }
    let method =
        value.get("method").and_then(Value::as_str).ok_or(McpTransportError::MalformedFrame {
            reason_code: "mcp.runtime.connector.method_missing",
        })?;
    let params = value.get("params").cloned().unwrap_or_else(|| json!({}));
    let notification = parse_notification(method, params)?;
    Ok(McpTransportEvent::Notification { runtime_generation, notification })
}

fn parse_callback(
    callback_id: u64,
    method: &str,
    params: Value,
    runtime_generation: u64,
    catalog_epoch: u64,
) -> Result<McpTransportEvent, McpTransportError> {
    let callback = match method {
        "sampling/createMessage" => {
            let max_output_tokens = params.get("maxTokens").and_then(Value::as_u64).ok_or(
                McpTransportError::MalformedFrame {
                    reason_code: "mcp.runtime.connector.sampling_invalid",
                },
            )?;
            let requested_tools = params
                .get("tools")
                .and_then(Value::as_array)
                .map(|tools| {
                    tools
                        .iter()
                        .map(|tool| {
                            tool.get("name").and_then(Value::as_str).map(str::to_owned).ok_or(
                                McpTransportError::MalformedFrame {
                                    reason_code: "mcp.runtime.connector.sampling_tool_invalid",
                                },
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default();
            McpServerCallbackType::Sampling(McpSamplingRequest {
                input_json: params,
                requested_tools,
                max_output_tokens,
            })
        }
        "elicitation/create" => {
            let prompt =
                params.get("message").and_then(Value::as_str).map(redact_diagnostic_text).ok_or(
                    McpTransportError::MalformedFrame {
                        reason_code: "mcp.runtime.connector.elicitation_invalid",
                    },
                )?;
            let response_schema_json = params.get("requestedSchema").cloned().ok_or(
                McpTransportError::MalformedFrame {
                    reason_code: "mcp.runtime.connector.elicitation_invalid",
                },
            )?;
            McpServerCallbackType::Elicitation(McpElicitationRequest {
                prompt,
                response_schema_json,
            })
        }
        "roots/list" => McpServerCallbackType::RootsList,
        _ => {
            return Err(McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.connector.callback_method_unsupported",
            });
        }
    };
    Ok(McpTransportEvent::Callback(McpServerCallbackRequest {
        callback_id,
        runtime_generation,
        catalog_epoch,
        // The actor overwrites all three authority bindings before policy.
        principal_id: "host-pinned".to_owned(),
        session_id: "host-pinned".to_owned(),
        origin: "host-pinned".to_owned(),
        callback,
    }))
}

fn parse_notification(
    method: &str,
    params: Value,
) -> Result<McpServerNotification, McpTransportError> {
    let notification = match method {
        "notifications/tools/list_changed" => McpServerNotification::CatalogChanged {
            surface: "tools".to_owned(),
            catalog_digest: None,
        },
        "notifications/resources/list_changed" => McpServerNotification::CatalogChanged {
            surface: "resources".to_owned(),
            catalog_digest: None,
        },
        "notifications/prompts/list_changed" => McpServerNotification::CatalogChanged {
            surface: "prompts".to_owned(),
            catalog_digest: None,
        },
        "notifications/progress" => {
            let token = params.get("progressToken").and_then(value_to_token).ok_or(
                McpTransportError::MalformedFrame {
                    reason_code: "mcp.runtime.connector.progress_invalid",
                },
            )?;
            let completed = params.get("progress").and_then(Value::as_u64).ok_or(
                McpTransportError::MalformedFrame {
                    reason_code: "mcp.runtime.connector.progress_invalid",
                },
            )?;
            let total = params.get("total").and_then(Value::as_u64);
            McpServerNotification::Progress { token, completed, total }
        }
        "notifications/message" => {
            let level = params.get("level").and_then(Value::as_str).ok_or(
                McpTransportError::MalformedFrame {
                    reason_code: "mcp.runtime.connector.log_invalid",
                },
            )?;
            let message = params.get("data").map(Value::to_string).unwrap_or_default();
            McpServerNotification::Log {
                level: level.to_owned(),
                safe_message: redact_diagnostic_text(&message),
            }
        }
        _ => McpServerNotification::Other { method: method.to_owned(), params_json: params },
    };
    notification.validate()?;
    Ok(notification)
}

fn value_to_token(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

fn validate_jsonrpc(value: &Value) -> Result<(), McpTransportError> {
    if !value.is_object() || value.get("jsonrpc").and_then(Value::as_str) != Some(JSONRPC_VERSION) {
        return Err(McpTransportError::MalformedFrame {
            reason_code: "mcp.runtime.connector.jsonrpc_invalid",
        });
    }
    Ok(())
}

pub(super) fn classify_http_status(status: u16) -> Option<&'static str> {
    match status {
        200..=299 => None,
        401 => Some("mcp.runtime.http.authorization_required"),
        404 => Some("mcp.runtime.http.session_not_found"),
        410 => Some("mcp.runtime.http.session_expired"),
        408 | 429 => Some("mcp.runtime.http.retryable"),
        500..=599 => Some("mcp.runtime.http.server_error"),
        _ => Some("mcp.runtime.http.rejected"),
    }
}

pub(super) fn ensure_pinned_session(
    expected: &str,
    observed: Option<&str>,
) -> Result<(), McpTransportError> {
    if observed.is_some_and(|observed| observed != expected) {
        return Err(McpTransportError::MalformedFrame {
            reason_code: "mcp.runtime.http.session_id_changed",
        });
    }
    Ok(())
}

pub(super) fn session_expiry(
    server_expiry: Option<i64>,
    idle_timeout_ms: u64,
) -> Result<i64, McpTransportError> {
    let idle_timeout_ms = i64::try_from(idle_timeout_ms).map_err(|_| {
        McpTransportError::InvalidRequest { reason_code: "mcp.runtime.http.idle_timeout_overflow" }
    })?;
    let local_expiry = now_unix_ms().checked_add(idle_timeout_ms).ok_or_else(|| {
        McpTransportError::Unavailable {
            reason_code: "mcp.runtime.http.expiry_overflow".to_owned(),
        }
    })?;
    Ok(server_expiry.map_or(local_expiry, |expiry| expiry.min(local_expiry)))
}

pub(super) fn session_id_sha256(session_id: &str) -> String {
    hex::encode(Sha256::digest(session_id.as_bytes()))
}

pub(super) fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(1)
        .max(1)
}

pub(super) fn evidence(
    transport: McpSessionTransportKind,
    runtime_generation: u64,
    reason_code: impl Into<String>,
    http_status: Option<u16>,
    session_id: Option<&str>,
) -> McpReconnectEvidence {
    McpReconnectEvidence {
        transport,
        runtime_generation,
        reason_code: reason_code.into(),
        http_status,
        session_id_sha256: session_id.map(session_id_sha256),
        occurred_at_unix_ms: now_unix_ms(),
    }
}

fn sanitize_reason_code(value: &str) -> String {
    if !value.trim().is_empty()
        && value.len() <= MAX_REASON_CODE_BYTES
        && value.chars().all(|character| {
            character.is_ascii_lowercase()
                || character.is_ascii_digit()
                || matches!(character, '.' | '_' | '-')
        })
    {
        value.to_owned()
    } else {
        "mcp.runtime.connector.host_port_failed".to_owned()
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_session_id(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_SESSION_ID_BYTES
        && !value.chars().any(char::is_control)
}

fn valid_cursor(value: &str) -> bool {
    !value.is_empty() && value.len() <= MAX_SESSION_ID_BYTES && !value.chars().any(char::is_control)
}

fn truncate_front_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut split_at = value.len().saturating_sub(max_bytes);
    while split_at < value.len() && !value.is_char_boundary(split_at) {
        split_at = split_at.saturating_add(1);
    }
    value.drain(..split_at);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_parser_routes_response_notification_and_callback() {
        let response =
            parse_runtime_event(json!({"jsonrpc": "2.0", "id": 7, "result": {"ok": true}}), 3, 4)
                .expect("response parses");
        assert!(matches!(
            response,
            McpTransportEvent::Response { request_id: 7, runtime_generation: 3, .. }
        ));

        let notification = parse_runtime_event(
            json!({
                "jsonrpc": "2.0",
                "method": "notifications/tools/list_changed",
                "params": {}
            }),
            3,
            4,
        )
        .expect("notification parses");
        assert!(matches!(
            notification,
            McpTransportEvent::Notification {
                notification: McpServerNotification::CatalogChanged { .. },
                ..
            }
        ));

        let callback = parse_runtime_event(
            json!({
                "jsonrpc": "2.0",
                "id": 9,
                "method": "roots/list",
                "params": {}
            }),
            3,
            4,
        )
        .expect("callback parses");
        assert!(matches!(
            callback,
            McpTransportEvent::Callback(McpServerCallbackRequest {
                callback_id: 9,
                runtime_generation: 3,
                catalog_epoch: 4,
                ..
            })
        ));
    }

    #[test]
    fn stderr_evidence_is_redacted_and_bounded() {
        let evidence = McpConnectorEvidenceHandle::new(32);
        evidence.append_stderr(b"authorization: Bearer secret-value\n");
        let snapshot = evidence.snapshot();
        assert!(snapshot.stderr_tail_redacted.len() <= 32);
        assert!(!snapshot.stderr_tail_redacted.contains("secret-value"));
    }
}
