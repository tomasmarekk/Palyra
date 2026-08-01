//! Persistent newline-delimited JSON-RPC over a host-launched process.
//!
//! The connector never creates a process directly. A host launcher supplies
//! already governed stdin, stdout, stderr, and deterministic cleanup handles.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use serde_json::Value;

use super::{
    catalog_digest, catalog_request, decode_value, encode_callback_response,
    encode_session_request, encode_value, evidence, initialize_message, initialized_notification,
    parse_catalog_response, parse_initialize_response, parse_runtime_event, CatalogTracker,
    McpConnectorCatalogState, McpConnectorEvidenceHandle, McpConnectorLimits,
    McpConnectorPortError, INITIALIZE_ID, PROMPTS_LIST_ID, RESOURCES_LIST_ID, TOOLS_LIST_ID,
};
use crate::application::mcp_runtime::{
    McpConnectRequest, McpConnectedSession, McpServerCallbackResponse, McpServerNotification,
    McpSessionConnector, McpSessionReader, McpSessionRequest, McpSessionTransportKind,
    McpSessionWriter, McpTransportError, McpTransportEvent, McpTransportSession,
};

const MAX_HANDSHAKE_SIDE_FRAMES: usize = 64;

/// Opaque host-governed process launch request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpProcessLaunchRequest {
    /// Stable MCP server identity.
    pub server_id: String,
    /// Runtime generation that will own the process.
    pub runtime_generation: u64,
    /// Host configuration profile containing executable, sandbox, env, and lease policy.
    pub launch_profile_id: String,
    /// Maximum stdout or stderr chunk requested from the host.
    pub max_chunk_bytes: usize,
}

/// Async bounded byte source supplied by the host process service.
#[async_trait]
pub trait McpByteReader: Send {
    /// Reads at most `max_bytes`; `None` is deterministic EOF.
    ///
    /// Implementations must make this operation cancellation safe.
    async fn read_chunk(
        &mut self,
        max_bytes: usize,
    ) -> Result<Option<Vec<u8>>, McpConnectorPortError>;
}

/// Async byte sink supplied by the host process service.
#[async_trait]
pub trait McpByteWriter: Send {
    /// Writes one complete newline-delimited frame.
    async fn write_frame(&mut self, frame: &[u8]) -> Result<(), McpConnectorPortError>;

    /// Closes process stdin without terminating unrelated resources.
    async fn close(&mut self) -> Result<(), McpConnectorPortError>;
}

/// Redaction-safe cleanup result returned by the host process supervisor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpProcessCloseEvidence {
    /// Whether the owned process reached a terminal state.
    pub process_exited: bool,
    /// Number of owned descendants remaining after cleanup.
    pub descendants_remaining: u32,
    /// Stable cleanup reason.
    pub reason_code: String,
}

/// Host-owned process lifecycle control.
#[async_trait]
pub trait McpProcessControl: Send {
    /// Terminates the exact process lease and returns orphan evidence.
    async fn close(&mut self) -> Result<McpProcessCloseEvidence, McpConnectorPortError>;
}

/// Governed process session returned by [`McpProcessLauncher`].
pub struct McpLaunchedProcessSession {
    stdin: Box<dyn McpByteWriter>,
    stdout: Box<dyn McpByteReader>,
    stderr: Option<Box<dyn McpByteReader>>,
    control: Box<dyn McpProcessControl>,
}

impl McpLaunchedProcessSession {
    /// Creates a process session from host-governed I/O and cleanup handles.
    pub fn new(
        stdin: Box<dyn McpByteWriter>,
        stdout: Box<dyn McpByteReader>,
        stderr: Option<Box<dyn McpByteReader>>,
        control: Box<dyn McpProcessControl>,
    ) -> Self {
        Self { stdin, stdout, stderr, control }
    }
}

/// Host process launcher that enforces sandbox, env, lease, and resource policy.
#[async_trait]
pub trait McpProcessLauncher: Send + Sync {
    /// Launches one exact runtime generation.
    async fn launch(
        &self,
        request: &McpProcessLaunchRequest,
    ) -> Result<McpLaunchedProcessSession, McpConnectorPortError>;
}

/// Configuration for a persistent stdio MCP connector.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpStdioConnectorConfig {
    /// Opaque host launch profile.
    pub launch_profile_id: String,
    /// Restored catalog state mirrored with the runtime actor.
    pub catalog_state: McpConnectorCatalogState,
    /// Bounded framing and evidence limits.
    pub limits: McpConnectorLimits,
}

impl McpStdioConnectorConfig {
    fn validate(&self) -> Result<(), McpTransportError> {
        self.catalog_state.validate()?;
        self.limits.validate()?;
        if self.launch_profile_id.trim().is_empty() || self.launch_profile_id.len() > 128 {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.stdio.invalid_launch_profile",
            });
        }
        Ok(())
    }
}

/// Persistent newline-delimited JSON-RPC connector.
pub struct McpStdioConnector {
    launcher: Arc<dyn McpProcessLauncher>,
    config: McpStdioConnectorConfig,
    catalog: Arc<Mutex<CatalogTracker>>,
    evidence: McpConnectorEvidenceHandle,
}

impl McpStdioConnector {
    /// Creates a connector without launching a process.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for invalid bounds or profile identity.
    pub fn new(
        launcher: Arc<dyn McpProcessLauncher>,
        config: McpStdioConnectorConfig,
    ) -> Result<Self, McpTransportError> {
        config.validate()?;
        let catalog = CatalogTracker::new(config.catalog_state.clone())?;
        let evidence = McpConnectorEvidenceHandle::new(config.limits.max_stderr_tail_bytes);
        Ok(Self { launcher, config, catalog: Arc::new(Mutex::new(catalog)), evidence })
    }

    /// Returns the shared redaction-safe evidence handle.
    pub fn evidence(&self) -> McpConnectorEvidenceHandle {
        self.evidence.clone()
    }
}

#[async_trait]
impl McpSessionConnector for McpStdioConnector {
    async fn connect(
        &self,
        request: &McpConnectRequest,
    ) -> Result<Box<dyn McpTransportSession>, McpTransportError> {
        request.validate()?;
        if request.transport != McpSessionTransportKind::Stdio {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.stdio.transport_mismatch",
            });
        }
        let process = self
            .launcher
            .launch(&McpProcessLaunchRequest {
                server_id: request.server_id.clone(),
                runtime_generation: request.runtime_generation,
                launch_profile_id: self.config.launch_profile_id.clone(),
                max_chunk_bytes: self.config.limits.max_frame_bytes,
            })
            .await
            .map_err(McpConnectorPortError::into_transport_error)?;
        let McpLaunchedProcessSession { mut stdin, mut stdout, stderr, control } = process;
        let mut decoder = BoundedLineDecoder::new(self.config.limits.max_frame_bytes);
        let mut side_frames = VecDeque::new();

        write_json_line(
            &mut stdin,
            &initialize_message(&request.initialize)?,
            self.config.limits.max_frame_bytes,
        )
        .await?;
        let initialize_value = read_matching_response(
            &mut stdout,
            &mut decoder,
            INITIALIZE_ID,
            &mut side_frames,
            self.config.limits.max_frame_bytes,
        )
        .await?;
        let mut initialize_result = parse_initialize_response(&initialize_value)?;
        write_json_line(
            &mut stdin,
            &initialized_notification(),
            self.config.limits.max_frame_bytes,
        )
        .await?;

        let tools = request_catalog(
            &mut stdin,
            &mut stdout,
            &mut decoder,
            &mut side_frames,
            TOOLS_LIST_ID,
            "tools/list",
            self.config.limits.max_frame_bytes,
        )
        .await?;
        let resources = request_catalog(
            &mut stdin,
            &mut stdout,
            &mut decoder,
            &mut side_frames,
            RESOURCES_LIST_ID,
            "resources/list",
            self.config.limits.max_frame_bytes,
        )
        .await?;
        let prompts = request_catalog(
            &mut stdin,
            &mut stdout,
            &mut decoder,
            &mut side_frames,
            PROMPTS_LIST_ID,
            "prompts/list",
            self.config.limits.max_frame_bytes,
        )
        .await?;
        initialize_result.catalog_digest = catalog_digest(&tools, &resources, &prompts)?;
        {
            let mut catalog = self.catalog.lock().map_err(|_| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.stdio.catalog_lock_poisoned".to_owned(),
            })?;
            catalog.observe_initial(&initialize_result.catalog_digest)?;
        }

        let writer = StdioSessionWriter {
            stdin: Some(stdin),
            control: Some(control),
            runtime_generation: request.runtime_generation,
            max_frame_bytes: self.config.limits.max_frame_bytes,
            evidence: self.evidence.clone(),
        };
        let reader = StdioSessionReader {
            stdout,
            stderr,
            decoder,
            side_frames,
            runtime_generation: request.runtime_generation,
            max_frame_bytes: self.config.limits.max_frame_bytes,
            catalog: Arc::clone(&self.catalog),
            evidence: self.evidence.clone(),
        };
        Ok(Box::new(McpConnectedSession::new(
            initialize_result,
            Box::new(writer),
            Box::new(reader),
        )?))
    }
}

struct StdioSessionWriter {
    stdin: Option<Box<dyn McpByteWriter>>,
    control: Option<Box<dyn McpProcessControl>>,
    runtime_generation: u64,
    max_frame_bytes: usize,
    evidence: McpConnectorEvidenceHandle,
}

#[async_trait]
impl McpSessionWriter for StdioSessionWriter {
    async fn send_request(&mut self, request: McpSessionRequest) -> Result<(), McpTransportError> {
        let encoded = encode_session_request(&request)?;
        self.write_encoded(encoded).await
    }

    async fn send_callback_response(
        &mut self,
        response: McpServerCallbackResponse,
    ) -> Result<(), McpTransportError> {
        let encoded = encode_callback_response(&response)?;
        self.write_encoded(encoded).await
    }

    async fn close(&mut self) -> Result<(), McpTransportError> {
        let stdin_result =
            if let Some(mut stdin) = self.stdin.take() { stdin.close().await } else { Ok(()) };
        let close_result = if let Some(mut control) = self.control.take() {
            control.close().await
        } else {
            Ok(McpProcessCloseEvidence {
                process_exited: true,
                descendants_remaining: 0,
                reason_code: "mcp.runtime.stdio.already_closed".to_owned(),
            })
        };
        match close_result {
            Ok(close) => {
                self.evidence.record(evidence(
                    McpSessionTransportKind::Stdio,
                    self.runtime_generation,
                    close.reason_code.clone(),
                    None,
                    None,
                ));
                if !close.process_exited || close.descendants_remaining > 0 {
                    return Err(McpTransportError::Unavailable {
                        reason_code: "mcp.runtime.stdio.cleanup_incomplete".to_owned(),
                    });
                }
            }
            Err(error) => return Err(error.into_transport_error()),
        }
        stdin_result.map_err(McpConnectorPortError::into_transport_error)
    }
}

impl StdioSessionWriter {
    async fn write_encoded(&mut self, mut encoded: Vec<u8>) -> Result<(), McpTransportError> {
        if encoded.len().checked_add(1).is_none_or(|length| length > self.max_frame_bytes) {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.stdio.frame_too_large",
            });
        }
        encoded.push(b'\n');
        self.stdin
            .as_mut()
            .ok_or_else(|| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.stdio.closed".to_owned(),
            })?
            .write_frame(&encoded)
            .await
            .map_err(McpConnectorPortError::into_transport_error)
    }
}

struct StdioSessionReader {
    stdout: Box<dyn McpByteReader>,
    stderr: Option<Box<dyn McpByteReader>>,
    decoder: BoundedLineDecoder,
    side_frames: VecDeque<Value>,
    runtime_generation: u64,
    max_frame_bytes: usize,
    catalog: Arc<Mutex<CatalogTracker>>,
    evidence: McpConnectorEvidenceHandle,
}

#[async_trait]
impl McpSessionReader for StdioSessionReader {
    async fn next_event(&mut self) -> Result<McpTransportEvent, McpTransportError> {
        loop {
            if let Some(value) = self.side_frames.pop_front() {
                return self.route_value(value);
            }
            if let Some(frame) = self.decoder.pop_frame() {
                return self.route_value(decode_value(&frame, self.max_frame_bytes)?);
            }
            if let Some(stderr) = self.stderr.as_mut() {
                tokio::select! {
                    stdout = self.stdout.read_chunk(self.max_frame_bytes) => {
                        let stdout =
                            stdout.map_err(McpConnectorPortError::into_transport_error)?;
                        if !self.accept_stdout(stdout)? {
                            return Ok(McpTransportEvent::Closed {
                                reason_code: "mcp.runtime.stdio.stdout_closed".to_owned(),
                            });
                        }
                    }
                    stderr_chunk = stderr.read_chunk(self.max_frame_bytes) => {
                        match stderr_chunk.map_err(McpConnectorPortError::into_transport_error)? {
                            Some(chunk) => {
                                if chunk.len() > self.max_frame_bytes {
                                    return Err(McpTransportError::MalformedFrame {
                                        reason_code: "mcp.runtime.stdio.stderr_chunk_too_large",
                                    });
                                }
                                self.evidence.append_stderr(&chunk);
                            }
                            None => self.stderr = None,
                        }
                    }
                }
            } else {
                let stdout = self
                    .stdout
                    .read_chunk(self.max_frame_bytes)
                    .await
                    .map_err(McpConnectorPortError::into_transport_error)?;
                if !self.accept_stdout(stdout)? {
                    return Ok(McpTransportEvent::Closed {
                        reason_code: "mcp.runtime.stdio.stdout_closed".to_owned(),
                    });
                }
            }
        }
    }
}

impl StdioSessionReader {
    fn accept_stdout(&mut self, chunk: Option<Vec<u8>>) -> Result<bool, McpTransportError> {
        let Some(chunk) = chunk else {
            return Ok(false);
        };
        if chunk.len() > self.max_frame_bytes {
            return Err(McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.stdio.stdout_chunk_too_large",
            });
        }
        self.decoder.push(&chunk)?;
        Ok(true)
    }

    fn route_value(&mut self, value: Value) -> Result<McpTransportEvent, McpTransportError> {
        let epoch = self
            .catalog
            .lock()
            .map_err(|_| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.stdio.catalog_lock_poisoned".to_owned(),
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
                    reason_code: "mcp.runtime.stdio.catalog_lock_poisoned".to_owned(),
                })?
                .advance_notification()?;
        }
        Ok(event)
    }
}

async fn write_json_line(
    writer: &mut Box<dyn McpByteWriter>,
    value: &Value,
    max_frame_bytes: usize,
) -> Result<(), McpTransportError> {
    let mut encoded = encode_value(value)?;
    if encoded.len().checked_add(1).is_none_or(|length| length > max_frame_bytes) {
        return Err(McpTransportError::InvalidRequest {
            reason_code: "mcp.runtime.stdio.handshake_frame_too_large",
        });
    }
    encoded.push(b'\n');
    writer.write_frame(&encoded).await.map_err(McpConnectorPortError::into_transport_error)
}

async fn request_catalog(
    stdin: &mut Box<dyn McpByteWriter>,
    stdout: &mut Box<dyn McpByteReader>,
    decoder: &mut BoundedLineDecoder,
    side_frames: &mut VecDeque<Value>,
    id: &'static str,
    method: &'static str,
    max_frame_bytes: usize,
) -> Result<Value, McpTransportError> {
    write_json_line(stdin, &catalog_request(id, method), max_frame_bytes).await?;
    let response =
        read_matching_response(stdout, decoder, id, side_frames, max_frame_bytes).await?;
    parse_catalog_response(&response, id)
}

async fn read_matching_response(
    stdout: &mut Box<dyn McpByteReader>,
    decoder: &mut BoundedLineDecoder,
    expected_id: &str,
    side_frames: &mut VecDeque<Value>,
    max_frame_bytes: usize,
) -> Result<Value, McpTransportError> {
    loop {
        if let Some(frame) = decoder.pop_frame() {
            let value = decode_value(&frame, max_frame_bytes)?;
            if value.get("id").and_then(Value::as_str) == Some(expected_id) {
                return Ok(value);
            }
            if side_frames.len() >= MAX_HANDSHAKE_SIDE_FRAMES {
                return Err(McpTransportError::MalformedFrame {
                    reason_code: "mcp.runtime.stdio.handshake_side_frames_exhausted",
                });
            }
            side_frames.push_back(value);
            continue;
        }
        let chunk = stdout
            .read_chunk(max_frame_bytes)
            .await
            .map_err(McpConnectorPortError::into_transport_error)?
            .ok_or_else(|| McpTransportError::Unavailable {
                reason_code: "mcp.runtime.stdio.handshake_eof".to_owned(),
            })?;
        if chunk.len() > max_frame_bytes {
            return Err(McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.stdio.handshake_chunk_too_large",
            });
        }
        decoder.push(&chunk)?;
    }
}

#[derive(Debug)]
struct BoundedLineDecoder {
    max_frame_bytes: usize,
    current: Vec<u8>,
    ready: VecDeque<Vec<u8>>,
}

impl BoundedLineDecoder {
    fn new(max_frame_bytes: usize) -> Self {
        Self { max_frame_bytes, current: Vec::new(), ready: VecDeque::new() }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<(), McpTransportError> {
        for byte in chunk {
            if *byte == b'\n' {
                if self.current.last() == Some(&b'\r') {
                    self.current.pop();
                }
                if self.current.is_empty() {
                    continue;
                }
                self.ready.push_back(std::mem::take(&mut self.current));
            } else {
                if self.current.len() >= self.max_frame_bytes {
                    self.current.clear();
                    self.ready.clear();
                    return Err(McpTransportError::MalformedFrame {
                        reason_code: "mcp.runtime.stdio.frame_too_large",
                    });
                }
                self.current.push(*byte);
            }
        }
        Ok(())
    }

    fn pop_frame(&mut self) -> Option<Vec<u8>> {
        self.ready.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn line_decoder_rejects_frame_before_unbounded_growth() {
        let mut decoder = BoundedLineDecoder::new(8);
        let error = decoder.push(b"123456789").expect_err("oversized frame is rejected");
        assert!(matches!(
            error,
            McpTransportError::MalformedFrame { reason_code: "mcp.runtime.stdio.frame_too_large" }
        ));
    }

    struct RecordingWriter {
        closes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl McpByteWriter for RecordingWriter {
        async fn write_frame(&mut self, _frame: &[u8]) -> Result<(), McpConnectorPortError> {
            Ok(())
        }

        async fn close(&mut self) -> Result<(), McpConnectorPortError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct RecordingControl {
        closes: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl McpProcessControl for RecordingControl {
        async fn close(&mut self) -> Result<McpProcessCloseEvidence, McpConnectorPortError> {
            self.closes.fetch_add(1, Ordering::SeqCst);
            Ok(McpProcessCloseEvidence {
                process_exited: true,
                descendants_remaining: 0,
                reason_code: "mcp.runtime.stdio.clean_close".to_owned(),
            })
        }
    }

    #[tokio::test]
    async fn close_releases_stdin_and_exact_process_control_once() {
        let stdin_closes = Arc::new(AtomicUsize::new(0));
        let process_closes = Arc::new(AtomicUsize::new(0));
        let mut writer = StdioSessionWriter {
            stdin: Some(Box::new(RecordingWriter { closes: Arc::clone(&stdin_closes) })),
            control: Some(Box::new(RecordingControl { closes: Arc::clone(&process_closes) })),
            runtime_generation: 3,
            max_frame_bytes: 1024,
            evidence: McpConnectorEvidenceHandle::new(128),
        };

        writer.close().await.expect("first close succeeds");
        writer.close().await.expect("second close is idempotent");
        assert_eq!(stdin_closes.load(Ordering::SeqCst), 1);
        assert_eq!(process_closes.load(Ordering::SeqCst), 1);
    }
}
