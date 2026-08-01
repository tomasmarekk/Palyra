//! Persistent MCP transport boundary shared by stdio, Streamable HTTP, and SSE.
//!
//! A connector returns split reader and writer halves, but both halves remain
//! owned by one session actor. Concrete adapters must apply the host's process,
//! egress, credential, and resource-governance services before connecting.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;

const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_METHOD_BYTES: usize = 256;
const MAX_PROTOCOL_VERSIONS: usize = 16;
const MAX_JSON_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_CALLBACK_TOOLS: usize = 128;

/// Persistent transport selected for an MCP server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpSessionTransportKind {
    /// A sandboxed child process using framed standard input and output.
    Stdio,
    /// A negotiated Streamable HTTP session.
    StreamableHttp,
    /// A long-lived server-sent events session with a paired request endpoint.
    ServerSentEvents,
}

/// Client capabilities advertised during MCP initialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct McpProtocolCapabilities {
    /// Whether the host accepts sampling callbacks.
    pub sampling: bool,
    /// Whether the host accepts elicitation callbacks.
    pub elicitation: bool,
    /// Whether the host exposes workspace roots.
    pub roots: bool,
    /// Whether the host processes catalog-change notifications.
    pub catalog_notifications: bool,
}

/// Initialization payload sent once for each persistent transport generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpInitializeRequest {
    /// Stable client implementation name.
    pub client_name: String,
    /// Client build or protocol adapter version.
    pub client_version: String,
    /// Supported protocol versions ordered by host preference.
    pub supported_protocol_versions: Vec<String>,
    /// Host-owned callback and notification capabilities.
    pub capabilities: McpProtocolCapabilities,
}

impl McpInitializeRequest {
    /// Validates bounded initialization metadata.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for empty or oversized metadata.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        if !valid_identifier(&self.client_name)
            || !valid_identifier(&self.client_version)
            || self.supported_protocol_versions.is_empty()
            || self.supported_protocol_versions.len() > MAX_PROTOCOL_VERSIONS
            || self.supported_protocol_versions.iter().any(|version| !valid_identifier(version))
        {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.initialize.invalid",
            });
        }
        Ok(())
    }
}

/// Negotiated server metadata returned by MCP initialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpInitializeResult {
    /// Negotiated MCP protocol version.
    pub protocol_version: String,
    /// Redaction-safe server implementation name.
    pub server_name: String,
    /// Redaction-safe server implementation version.
    pub server_version: String,
    /// Server capabilities represented as bounded protocol JSON.
    pub capabilities_json: Value,
    /// Digest produced after initial tools/resources/prompts discovery.
    pub catalog_digest: String,
}

impl McpInitializeResult {
    /// Validates the negotiated response before it becomes runtime state.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidHandshake`] for malformed or oversized metadata.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        if !valid_identifier(&self.protocol_version)
            || !valid_identifier(&self.server_name)
            || !valid_identifier(&self.server_version)
            || !valid_digest(&self.catalog_digest)
            || encoded_len(&self.capabilities_json)? > MAX_JSON_PAYLOAD_BYTES
        {
            return Err(McpTransportError::InvalidHandshake);
        }
        Ok(())
    }
}

/// Generation-pinned request used to establish a persistent session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpConnectRequest {
    /// Stable server identity.
    pub server_id: String,
    /// Transport selected by validated host configuration.
    pub transport: McpSessionTransportKind,
    /// New runtime generation owned by the actor.
    pub runtime_generation: u64,
    /// Maximum time allowed for transport setup and initialization.
    pub handshake_timeout_ms: u64,
    /// MCP initialization payload.
    pub initialize: McpInitializeRequest,
}

impl McpConnectRequest {
    /// Validates a transport connection plan.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for invalid identifiers or limits.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        self.initialize.validate()?;
        if !valid_identifier(&self.server_id)
            || self.runtime_generation == 0
            || self.handshake_timeout_ms == 0
        {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.connect.invalid",
            });
        }
        Ok(())
    }
}

/// One JSON-RPC request routed through a persistent session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSessionRequest {
    /// Actor-issued request identifier unique for its lifetime.
    pub request_id: u64,
    /// Runtime generation that owns the request.
    pub runtime_generation: u64,
    /// Catalog epoch observed when the host prepared the request.
    pub catalog_epoch: u64,
    /// MCP method.
    pub method: String,
    /// Bounded request parameters.
    pub params_json: Value,
}

impl McpSessionRequest {
    /// Validates a request before it reaches a transport adapter.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for malformed or oversized input.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        if self.request_id == 0
            || self.runtime_generation == 0
            || self.catalog_epoch == 0
            || !valid_method(&self.method)
            || encoded_len(&self.params_json)? > MAX_JSON_PAYLOAD_BYTES
        {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.request.invalid",
            });
        }
        Ok(())
    }
}

/// Redaction-safe JSON-RPC error returned by an MCP server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpRemoteError {
    /// JSON-RPC error code.
    pub code: i64,
    /// Sanitized human-readable message.
    pub safe_message: String,
    /// Optional bounded structured details.
    pub data_json: Option<Value>,
}

/// Success or protocol error payload for a routed request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpResponsePayload {
    /// Successful JSON result.
    Success(Value),
    /// Server-declared JSON-RPC error.
    Error(McpRemoteError),
}

/// Tool-catalog, resource, prompt, progress, or log notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpServerNotification {
    /// A catalog surface changed and prepared calls must observe a new epoch.
    CatalogChanged {
        /// Protocol surface such as `tools`, `resources`, or `prompts`.
        surface: String,
        /// Optional digest supplied by the transport adapter.
        catalog_digest: Option<String>,
    },
    /// Bounded progress notification.
    Progress {
        /// Opaque host-safe progress token.
        token: String,
        /// Current progress units.
        completed: u64,
        /// Optional total progress units.
        total: Option<u64>,
    },
    /// Sanitized server diagnostic.
    Log {
        /// Protocol log level.
        level: String,
        /// Redacted message.
        safe_message: String,
    },
    /// Recognized but currently host-opaque notification.
    Other {
        /// MCP notification method.
        method: String,
        /// Bounded notification parameters.
        params_json: Value,
    },
}

impl McpServerNotification {
    /// Validates an untrusted notification before publication.
    ///
    /// # Errors
    /// Returns [`McpTransportError::MalformedFrame`] when bounds or invariants fail.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        let valid = match self {
            Self::CatalogChanged { surface, catalog_digest } => {
                valid_identifier(surface) && catalog_digest.as_deref().is_none_or(valid_digest)
            }
            Self::Progress { token, completed, total } => {
                valid_identifier(token) && total.is_none_or(|total| *completed <= total)
            }
            Self::Log { level, safe_message } => {
                valid_identifier(level) && safe_message.len() <= 8 * 1024
            }
            Self::Other { method, params_json } => {
                valid_method(method) && encoded_len(params_json)? <= MAX_JSON_PAYLOAD_BYTES
            }
        };
        if !valid {
            return Err(McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.notification.invalid",
            });
        }
        Ok(())
    }
}

/// Host-owned sampling callback request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSamplingRequest {
    /// Redacted prompt/messages in protocol form.
    pub input_json: Value,
    /// Explicit tools requested by the server; empty means no tools.
    pub requested_tools: Vec<String>,
    /// Hard output-token ceiling requested from the host.
    pub max_output_tokens: u64,
}

/// Host-owned structured elicitation callback request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpElicitationRequest {
    /// Redacted user-facing prompt.
    pub prompt: String,
    /// Bounded response schema.
    pub response_schema_json: Value,
}

/// Callback class requested by an MCP server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpServerCallbackType {
    /// A bounded model sampling request.
    Sampling(McpSamplingRequest),
    /// A structured user elicitation request.
    Elicitation(McpElicitationRequest),
    /// A request for policy-filtered host roots.
    RootsList,
}

/// Generation-pinned callback request routed to host policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerCallbackRequest {
    /// Server-issued callback identifier.
    pub callback_id: u64,
    /// Runtime generation that received the callback.
    pub runtime_generation: u64,
    /// Catalog epoch active when the callback arrived.
    pub catalog_epoch: u64,
    /// Host-authorized principal binding.
    pub principal_id: String,
    /// Host-authorized session binding.
    pub session_id: String,
    /// Host-authorized origin binding.
    pub origin: String,
    /// Typed callback payload.
    pub callback: McpServerCallbackType,
}

impl McpServerCallbackRequest {
    /// Validates callback identity, bounds, and explicit tool scope.
    ///
    /// # Errors
    /// Returns [`McpTransportError::MalformedFrame`] for malformed callback input.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        let callback_valid = match &self.callback {
            McpServerCallbackType::Sampling(request) => {
                request.max_output_tokens > 0
                    && request.requested_tools.len() <= MAX_CALLBACK_TOOLS
                    && request.requested_tools.iter().all(|tool| valid_identifier(tool))
                    && encoded_len(&request.input_json)? <= MAX_JSON_PAYLOAD_BYTES
            }
            McpServerCallbackType::Elicitation(request) => {
                !request.prompt.trim().is_empty()
                    && request.prompt.len() <= 16 * 1024
                    && encoded_len(&request.response_schema_json)? <= MAX_JSON_PAYLOAD_BYTES
            }
            McpServerCallbackType::RootsList => true,
        };
        if self.callback_id == 0
            || self.runtime_generation == 0
            || self.catalog_epoch == 0
            || !valid_identifier(&self.principal_id)
            || !valid_identifier(&self.session_id)
            || !valid_identifier(&self.origin)
            || !callback_valid
        {
            return Err(McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.callback.invalid",
            });
        }
        Ok(())
    }
}

/// Host callback response payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpCallbackResponsePayload {
    /// Callback was authorized and completed.
    Success(Value),
    /// Callback was denied or failed with a safe reason.
    Rejected {
        /// Stable host-owned reason code.
        reason_code: String,
        /// Sanitized diagnostic suitable for the external server.
        safe_message: String,
    },
}

/// Generation-pinned callback response returned to the transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpServerCallbackResponse {
    /// Server-issued callback identifier.
    pub callback_id: u64,
    /// Runtime generation that owns the response.
    pub runtime_generation: u64,
    /// Host-produced response.
    pub payload: McpCallbackResponsePayload,
}

impl McpServerCallbackResponse {
    /// Validates a callback response before sending it to an external server.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for malformed host output.
    pub fn validate(&self) -> Result<(), McpTransportError> {
        let payload_valid = match &self.payload {
            McpCallbackResponsePayload::Success(value) => {
                encoded_len(value)? <= MAX_JSON_PAYLOAD_BYTES
            }
            McpCallbackResponsePayload::Rejected { reason_code, safe_message } => {
                valid_identifier(reason_code) && safe_message.len() <= 8 * 1024
            }
        };
        if self.callback_id == 0 || self.runtime_generation == 0 || !payload_valid {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.callback_response.invalid",
            });
        }
        Ok(())
    }
}

/// Event decoded by a persistent transport reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum McpTransportEvent {
    /// Response to an actor-issued request.
    Response {
        /// Actor-issued request identifier.
        request_id: u64,
        /// Runtime generation observed on the transport.
        runtime_generation: u64,
        /// Response payload.
        payload: McpResponsePayload,
    },
    /// Server notification.
    Notification {
        /// Runtime generation observed on the transport.
        runtime_generation: u64,
        /// Notification payload.
        notification: McpServerNotification,
    },
    /// Server-to-host callback request.
    Callback(McpServerCallbackRequest),
    /// Clean remote session closure.
    Closed {
        /// Stable, sanitized closure reason.
        reason_code: String,
    },
}

/// Writer half of one persistent MCP session.
#[async_trait]
pub trait McpSessionWriter: Send {
    /// Sends one generation-pinned request without waiting for its response.
    async fn send_request(&mut self, request: McpSessionRequest) -> Result<(), McpTransportError>;

    /// Sends a host callback response.
    async fn send_callback_response(
        &mut self,
        response: McpServerCallbackResponse,
    ) -> Result<(), McpTransportError>;

    /// Gracefully closes the session and releases its transport resources.
    async fn close(&mut self) -> Result<(), McpTransportError>;
}

/// Reader half of one persistent MCP session.
#[async_trait]
pub trait McpSessionReader: Send {
    /// Waits for the next decoded transport event.
    ///
    /// Implementations must make this method cancellation safe because the
    /// actor selects it against commands and request deadlines.
    async fn next_event(&mut self) -> Result<McpTransportEvent, McpTransportError>;
}

/// Observable state of one actor-owned persistent transport generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpTransportHealthState {
    /// The transport is connected and has no outstanding keepalive probe.
    Connected,
    /// A bounded host ping is awaiting its response.
    KeepalivePending,
    /// Transport health failed and reconnect is required.
    Degraded,
    /// The transport was closed by an orderly drain.
    Closed,
}

/// Bounded transport liveness evidence published through actor diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpTransportHealth {
    /// Generation that exclusively owns this transport.
    pub runtime_generation: u64,
    /// Current health state.
    pub state: McpTransportHealthState,
    /// Time at which initialization completed.
    pub connected_at_unix_ms: i64,
    /// Most recent validated frame or successful write time.
    pub last_activity_at_unix_ms: i64,
    /// Most recent keepalive request time.
    pub last_keepalive_at_unix_ms: Option<i64>,
    /// Successfully acknowledged keepalive count.
    pub successful_keepalives: u64,
    /// Keepalive failures observed before reconnect.
    pub failed_keepalives: u64,
}

/// Connected session returned after transport setup and MCP initialization.
pub struct McpConnectedSession {
    initialize_result: McpInitializeResult,
    writer: Box<dyn McpSessionWriter>,
    reader: Box<dyn McpSessionReader>,
}

impl McpConnectedSession {
    /// Creates a connected session from validated split transport halves.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidHandshake`] for invalid negotiated metadata.
    pub fn new(
        initialize_result: McpInitializeResult,
        writer: Box<dyn McpSessionWriter>,
        reader: Box<dyn McpSessionReader>,
    ) -> Result<Self, McpTransportError> {
        initialize_result.validate()?;
        Ok(Self { initialize_result, writer, reader })
    }

    /// Splits the initialized session into metadata and actor-owned I/O halves.
    pub fn into_parts(
        self,
    ) -> (McpInitializeResult, Box<dyn McpSessionWriter>, Box<dyn McpSessionReader>) {
        (self.initialize_result, self.writer, self.reader)
    }
}

/// Actor-owned initialized transport session.
///
/// Implementations transfer both I/O halves exactly once so no second owner can
/// issue requests for the same runtime generation.
pub trait McpTransportSession: Send {
    /// Consumes the session into negotiated metadata and exclusive I/O halves.
    fn into_parts(
        self: Box<Self>,
    ) -> (McpInitializeResult, Box<dyn McpSessionWriter>, Box<dyn McpSessionReader>);
}

impl McpTransportSession for McpConnectedSession {
    fn into_parts(
        self: Box<Self>,
    ) -> (McpInitializeResult, Box<dyn McpSessionWriter>, Box<dyn McpSessionReader>) {
        (*self).into_parts()
    }
}

/// Factory for persistent stdio, Streamable HTTP, or SSE sessions.
#[async_trait]
pub trait McpSessionConnector: Send + Sync {
    /// Connects and initializes exactly one runtime generation.
    ///
    /// The adapter must enforce the host's sandbox or egress plan and return
    /// only after protocol initialization succeeds.
    async fn connect(
        &self,
        request: &McpConnectRequest,
    ) -> Result<Box<dyn McpTransportSession>, McpTransportError>;
}

/// Persistent transport failure.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum McpTransportError {
    /// Host configuration or actor-produced request is invalid.
    #[error("invalid mcp transport request: {reason_code}")]
    InvalidRequest {
        /// Stable reason code.
        reason_code: &'static str,
    },
    /// MCP initialization failed validation.
    #[error("invalid mcp initialization handshake")]
    InvalidHandshake,
    /// Setup or initialization exceeded its deadline.
    #[error("mcp transport handshake timed out")]
    HandshakeTimedOut,
    /// A decoded frame violated the bounded protocol contract.
    #[error("malformed mcp transport frame: {reason_code}")]
    MalformedFrame {
        /// Stable reason code.
        reason_code: &'static str,
    },
    /// The transport closed or failed.
    #[error("mcp transport unavailable: {reason_code}")]
    Unavailable {
        /// Stable, sanitized reason code.
        reason_code: String,
    },
}

impl McpTransportError {
    /// Returns a stable redaction-safe reason code.
    pub fn reason_code(&self) -> &str {
        match self {
            Self::InvalidRequest { reason_code } | Self::MalformedFrame { reason_code } => {
                reason_code
            }
            Self::InvalidHandshake => "mcp.runtime.handshake.invalid",
            Self::HandshakeTimedOut => "mcp.runtime.handshake.timeout",
            Self::Unavailable { reason_code } => reason_code,
        }
    }
}

fn encoded_len(value: &Value) -> Result<usize, McpTransportError> {
    serde_json::to_vec(value)
        .map(|encoded| encoded.len())
        .map_err(|_| McpTransportError::MalformedFrame { reason_code: "mcp.runtime.json.invalid" })
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_IDENTIFIER_BYTES
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
}

fn valid_method(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_METHOD_BYTES
        && value.chars().all(|character| {
            character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-' | ':' | '/')
        })
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_requires_explicit_bounded_tool_scope() {
        let mut request = McpServerCallbackRequest {
            callback_id: 1,
            runtime_generation: 2,
            catalog_epoch: 3,
            principal_id: "principal-1".to_owned(),
            session_id: "session-1".to_owned(),
            origin: "mcp:test".to_owned(),
            callback: McpServerCallbackType::Sampling(McpSamplingRequest {
                input_json: serde_json::json!({"messages": []}),
                requested_tools: vec!["read_file".to_owned()],
                max_output_tokens: 128,
            }),
        };
        assert_eq!(request.validate(), Ok(()));

        if let McpServerCallbackType::Sampling(sampling) = &mut request.callback {
            sampling.requested_tools = vec!["tool".to_owned(); MAX_CALLBACK_TOOLS + 1];
        }
        assert_eq!(
            request.validate(),
            Err(McpTransportError::MalformedFrame { reason_code: "mcp.runtime.callback.invalid" })
        );
    }

    #[test]
    fn initialize_rejects_unbounded_capability_payload() {
        let result = McpInitializeResult {
            protocol_version: "2025-06-18".to_owned(),
            server_name: "test-server".to_owned(),
            server_version: "1.0.0".to_owned(),
            capabilities_json: Value::String("x".repeat(MAX_JSON_PAYLOAD_BYTES + 1)),
            catalog_digest: "a".repeat(64),
        };
        assert_eq!(result.validate(), Err(McpTransportError::InvalidHandshake));
    }
}
