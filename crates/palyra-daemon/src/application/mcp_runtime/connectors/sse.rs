//! Bounded SSE framing and the paired-endpoint persistent MCP connector.

use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;

use super::{
    http::{McpHttpSessionPort, RemoteConnector, RemoteConnectorConfig},
    McpConnectorCatalogState, McpConnectorEvidenceHandle, McpConnectorLimits,
};
use crate::application::mcp_runtime::{
    McpConnectRequest, McpSessionConnector, McpSessionTransportKind, McpTransportError,
    McpTransportSession,
};

/// Configuration for an SSE MCP session with a paired request endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpSseConnectorConfig {
    /// Host-configured endpoint used to receive the SSE stream.
    pub event_endpoint_id: String,
    /// Host-configured endpoint used for JSON-RPC POST requests.
    pub request_endpoint_id: String,
    /// Restored durable catalog state.
    pub catalog_state: McpConnectorCatalogState,
    /// Bounded framing and expiry limits.
    pub limits: McpConnectorLimits,
}

/// Persistent MCP connector using SSE plus a paired request endpoint.
pub struct McpSseConnector {
    inner: RemoteConnector,
}

impl McpSseConnector {
    /// Creates an SSE connector without opening a network session.
    ///
    /// # Errors
    /// Returns [`McpTransportError::InvalidRequest`] for invalid endpoint identities or limits.
    pub fn new(
        port: Arc<dyn McpHttpSessionPort>,
        config: McpSseConnectorConfig,
    ) -> Result<Self, McpTransportError> {
        let inner = RemoteConnector::new(
            port,
            RemoteConnectorConfig {
                transport: McpSessionTransportKind::ServerSentEvents,
                event_endpoint_id: config.event_endpoint_id,
                request_endpoint_id: config.request_endpoint_id,
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
impl McpSessionConnector for McpSseConnector {
    async fn connect(
        &self,
        request: &McpConnectRequest,
    ) -> Result<Box<dyn McpTransportSession>, McpTransportError> {
        if request.transport != McpSessionTransportKind::ServerSentEvents {
            return Err(McpTransportError::InvalidRequest {
                reason_code: "mcp.runtime.sse.transport_mismatch",
            });
        }
        Ok(Box::new(self.inner.connect(request).await?))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct SseEvent {
    pub(super) event_type: Option<String>,
    pub(super) data: Vec<u8>,
    pub(super) id: Option<String>,
}

#[derive(Debug)]
pub(super) struct BoundedSseDecoder {
    max_event_bytes: usize,
    line: Vec<u8>,
    event_type: Option<String>,
    data: Vec<u8>,
    id: Option<String>,
    ready: VecDeque<SseEvent>,
}

impl BoundedSseDecoder {
    pub(super) fn new(max_event_bytes: usize) -> Self {
        Self {
            max_event_bytes,
            line: Vec::new(),
            event_type: None,
            data: Vec::new(),
            id: None,
            ready: VecDeque::new(),
        }
    }

    pub(super) fn push(&mut self, chunk: &[u8]) -> Result<(), McpTransportError> {
        for byte in chunk {
            if *byte == b'\n' {
                if self.line.last() == Some(&b'\r') {
                    self.line.pop();
                }
                self.finish_line()?;
            } else {
                if self.line.len() >= self.max_event_bytes {
                    self.reset();
                    return Err(McpTransportError::MalformedFrame {
                        reason_code: "mcp.runtime.sse.line_too_large",
                    });
                }
                self.line.push(*byte);
            }
        }
        Ok(())
    }

    pub(super) fn pop_event(&mut self) -> Option<SseEvent> {
        self.ready.pop_front()
    }

    fn finish_line(&mut self) -> Result<(), McpTransportError> {
        if self.line.is_empty() {
            if !self.data.is_empty() {
                if self.data.last() == Some(&b'\n') {
                    self.data.pop();
                }
                self.ready.push_back(SseEvent {
                    event_type: self.event_type.take(),
                    data: std::mem::take(&mut self.data),
                    id: self.id.take(),
                });
            } else {
                self.event_type = None;
            }
            return Ok(());
        }
        if self.line.first() == Some(&b':') {
            self.line.clear();
            return Ok(());
        }
        let line = std::mem::take(&mut self.line);
        let split = line.iter().position(|byte| *byte == b':').unwrap_or(line.len());
        let field = std::str::from_utf8(&line[..split]).map_err(|_| {
            McpTransportError::MalformedFrame { reason_code: "mcp.runtime.sse.field_invalid_utf8" }
        })?;
        let mut value = line.get(split.saturating_add(1)..).unwrap_or_default();
        if value.first() == Some(&b' ') {
            value = &value[1..];
        }
        match field {
            "data" => {
                let new_len = self
                    .data
                    .len()
                    .checked_add(value.len())
                    .and_then(|length| length.checked_add(1))
                    .ok_or(McpTransportError::MalformedFrame {
                        reason_code: "mcp.runtime.sse.event_too_large",
                    })?;
                if new_len > self.max_event_bytes {
                    self.reset();
                    return Err(McpTransportError::MalformedFrame {
                        reason_code: "mcp.runtime.sse.event_too_large",
                    });
                }
                self.data.extend_from_slice(value);
                self.data.push(b'\n');
            }
            "event" => self.event_type = Some(parse_bounded_utf8(value)?),
            "id" => {
                if value.contains(&0) {
                    self.reset();
                    return Err(McpTransportError::MalformedFrame {
                        reason_code: "mcp.runtime.sse.cursor_invalid",
                    });
                }
                self.id = Some(parse_bounded_utf8(value)?);
            }
            "retry" | "" => {}
            _ => {}
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.line.clear();
        self.event_type = None;
        self.data.clear();
        self.id = None;
        self.ready.clear();
    }
}

fn parse_bounded_utf8(value: &[u8]) -> Result<String, McpTransportError> {
    std::str::from_utf8(value).map(str::to_owned).map_err(|_| McpTransportError::MalformedFrame {
        reason_code: "mcp.runtime.sse.value_invalid_utf8",
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sse_parser_preserves_multiline_data_and_cursor() {
        let mut decoder = BoundedSseDecoder::new(256);
        decoder
            .push(b"id: event-7\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\ndata: \"id\":7}\n\n")
            .expect("event parses");
        let event = decoder.pop_event().expect("event is emitted");
        assert_eq!(event.id.as_deref(), Some("event-7"));
        assert_eq!(event.event_type.as_deref(), Some("message"));
        assert_eq!(event.data, b"{\"jsonrpc\":\"2.0\",\n\"id\":7}");
    }

    #[test]
    fn sse_parser_rejects_malicious_oversized_event() {
        let mut decoder = BoundedSseDecoder::new(16);
        let chunk = format!("data: {}\n\n", "x".repeat(32));
        let error = decoder.push(chunk.as_bytes()).expect_err("oversized event is rejected");
        assert!(matches!(
            error,
            McpTransportError::MalformedFrame {
                reason_code: "mcp.runtime.sse.line_too_large" | "mcp.runtime.sse.event_too_large"
            }
        ));
    }
}
