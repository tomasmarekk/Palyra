//! Bounded incremental decoding for control-plane NDJSON responses.
//!
//! The stream owns the open HTTP response and retains only a caller-limited
//! amount of unread framing data while yielding one JSON value at a time.

use reqwest::Response;
use serde_json::Value;

use crate::{
    errors::{ControlPlaneClientError, ErrorEnvelope},
    transport::fallback_error_message,
};

const DEFAULT_MAX_STREAM_BUFFER_BYTES: usize = 1024 * 1024;
const DEFAULT_MAX_ERROR_BODY_BYTES: usize = 64 * 1024;

/// Memory limits applied while opening and consuming an NDJSON response.
///
/// `max_stream_buffer_bytes` caps unread framing bytes retained between
/// [`ControlPlaneNdjsonStream::next_value`] calls. The buffer may include more
/// than one complete line when the transport coalesces frames.
/// `max_error_body_bytes` independently caps a non-success response body read
/// before it is mapped to [`ControlPlaneClientError::Http`]. A zero limit
/// accepts only an empty corresponding body or stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NdjsonStreamLimits {
    max_stream_buffer_bytes: usize,
    max_error_body_bytes: usize,
}

impl NdjsonStreamLimits {
    /// Creates explicit stream-buffer and error-body limits, in bytes.
    #[must_use]
    pub const fn new(max_stream_buffer_bytes: usize, max_error_body_bytes: usize) -> Self {
        Self { max_stream_buffer_bytes, max_error_body_bytes }
    }

    /// Returns the maximum unread NDJSON bytes retained between values.
    #[must_use]
    pub const fn max_stream_buffer_bytes(self) -> usize {
        self.max_stream_buffer_bytes
    }

    /// Returns the maximum non-success response body read for error mapping.
    #[must_use]
    pub const fn max_error_body_bytes(self) -> usize {
        self.max_error_body_bytes
    }
}

impl Default for NdjsonStreamLimits {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_STREAM_BUFFER_BYTES, DEFAULT_MAX_ERROR_BODY_BYTES)
    }
}

/// An open control-plane response decoded as newline-delimited JSON values.
///
/// Call [`next_value`](Self::next_value) repeatedly to process events while the
/// response remains open. Dropping the handle cancels further response reads.
/// Empty lines are ignored, both LF and CRLF delimiters are accepted, and a
/// final JSON value does not require a trailing newline.
// INTENTIONAL: no `Debug` implementation. The unread buffer can contain model
// output, tool arguments, or other sensitive event payloads.
pub struct ControlPlaneNdjsonStream {
    response: Response,
    decoder: NdjsonDecoder,
}

impl ControlPlaneNdjsonStream {
    pub(crate) async fn from_response(
        mut response: Response,
        limits: NdjsonStreamLimits,
    ) -> Result<Self, ControlPlaneClientError> {
        if !response.status().is_success() {
            return Err(map_error_response(&mut response, limits.max_error_body_bytes()).await);
        }
        Ok(Self { response, decoder: NdjsonDecoder::new(limits.max_stream_buffer_bytes()) })
    }

    /// Reads and decodes the next JSON value without waiting for stream closure.
    ///
    /// The method retains at most the configured stream-buffer limit between
    /// calls. After it returns `Ok(None)`, subsequent calls also return
    /// `Ok(None)`. A framing, JSON, size-limit, or transport error terminates
    /// the stream and does not expose the offending payload in its message.
    ///
    /// # Errors
    /// Returns [`ControlPlaneClientError::Decode`] when response bytes cannot
    /// be read, the configured buffer limit is exceeded, or one line is not a
    /// valid JSON value.
    pub async fn next_value(&mut self) -> Result<Option<Value>, ControlPlaneClientError> {
        loop {
            match self.decoder.decode_available()? {
                DecodeProgress::Value(value) => return Ok(Some(value)),
                DecodeProgress::Finished => return Ok(None),
                DecodeProgress::NeedInput => {}
            }

            match self.response.chunk().await {
                Ok(Some(chunk)) => self.decoder.push(chunk.as_ref())?,
                Ok(None) => self.decoder.finish(),
                Err(error) => {
                    self.decoder.terminate();
                    return Err(ControlPlaneClientError::Decode(error.to_string()));
                }
            }
        }
    }
}

async fn map_error_response(
    response: &mut Response,
    max_body_bytes: usize,
) -> ControlPlaneClientError {
    let status = response.status().as_u16();
    match read_bounded_body(response, max_body_bytes).await {
        Ok(body) => http_error_from_body(status, body.as_deref()),
        Err(error) => error,
    }
}

fn http_error_from_body(status: u16, body: Option<&[u8]>) -> ControlPlaneClientError {
    let Some(body) = body else {
        return ControlPlaneClientError::Http {
            status,
            message: format!("request failed with HTTP {status}"),
            envelope: None,
        };
    };
    let envelope = serde_json::from_slice::<ErrorEnvelope>(body).ok();
    let message = envelope.as_ref().map_or_else(
        || fallback_error_message(status, String::from_utf8_lossy(body).as_ref()),
        |value| value.error.clone(),
    );
    ControlPlaneClientError::Http { status, message, envelope }
}

async fn read_bounded_body(
    response: &mut Response,
    max_body_bytes: usize,
) -> Result<Option<Vec<u8>>, ControlPlaneClientError> {
    let mut body = Vec::with_capacity(max_body_bytes.min(8 * 1024));
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| ControlPlaneClientError::Decode(error.to_string()))?
    {
        let Some(next_len) = body.len().checked_add(chunk.len()) else {
            return Ok(None);
        };
        if next_len > max_body_bytes {
            return Ok(None);
        }
        body.extend_from_slice(chunk.as_ref());
    }
    Ok(Some(body))
}

struct NdjsonDecoder {
    buffer: Vec<u8>,
    max_buffer_bytes: usize,
    state: DecoderState,
}

impl NdjsonDecoder {
    const fn new(max_buffer_bytes: usize) -> Self {
        Self { buffer: Vec::new(), max_buffer_bytes, state: DecoderState::Open }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<(), ControlPlaneClientError> {
        if self.state != DecoderState::Open {
            return Ok(());
        }
        let Some(next_len) = self.buffer.len().checked_add(chunk.len()) else {
            self.terminate();
            return Err(buffer_limit_error(self.max_buffer_bytes));
        };
        if next_len > self.max_buffer_bytes {
            self.terminate();
            return Err(buffer_limit_error(self.max_buffer_bytes));
        }
        self.buffer.extend_from_slice(chunk);
        Ok(())
    }

    fn finish(&mut self) {
        if self.state == DecoderState::Open {
            self.state = DecoderState::EndOfInput;
        }
    }

    fn terminate(&mut self) {
        self.buffer.clear();
        self.state = DecoderState::Terminated;
    }

    fn decode_available(&mut self) -> Result<DecodeProgress, ControlPlaneClientError> {
        loop {
            if self.state == DecoderState::Terminated {
                return Ok(DecodeProgress::Finished);
            }
            if let Some(newline) = self.buffer.iter().position(|byte| *byte == b'\n') {
                let decoded = decode_line(strip_carriage_return(&self.buffer[..newline]));
                self.buffer.drain(..=newline);
                match decoded {
                    Ok(Some(value)) => return Ok(DecodeProgress::Value(value)),
                    Ok(None) => continue,
                    Err(error) => {
                        self.terminate();
                        return Err(error);
                    }
                }
            }
            if self.state == DecoderState::EndOfInput {
                let final_line = std::mem::take(&mut self.buffer);
                self.state = DecoderState::Terminated;
                return decode_line(strip_carriage_return(final_line.as_slice()))
                    .map(|value| value.map_or(DecodeProgress::Finished, DecodeProgress::Value));
            }
            return Ok(DecodeProgress::NeedInput);
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DecoderState {
    Open,
    EndOfInput,
    Terminated,
}

enum DecodeProgress {
    Value(Value),
    NeedInput,
    Finished,
}

fn strip_carriage_return(line: &[u8]) -> &[u8] {
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn decode_line(line: &[u8]) -> Result<Option<Value>, ControlPlaneClientError> {
    if line.iter().all(|byte| byte.is_ascii_whitespace()) {
        return Ok(None);
    }
    serde_json::from_slice(line).map(Some).map_err(|error| {
        ControlPlaneClientError::Decode(format!(
            "invalid NDJSON value at line {}, column {}",
            error.line(),
            error.column()
        ))
    })
}

fn buffer_limit_error(max_buffer_bytes: usize) -> ControlPlaneClientError {
    ControlPlaneClientError::Decode(format!(
        "NDJSON response exceeded the {max_buffer_bytes}-byte buffer limit"
    ))
}

#[cfg(test)]
mod tests {
    use std::{
        future::Future as _,
        task::{Context, Poll, Waker},
    };

    use super::*;
    use crate::{ControlPlaneClient, ControlPlaneClientConfig};

    #[test]
    fn default_limits_match_the_bounded_runner_profile() {
        let limits = NdjsonStreamLimits::default();

        assert_eq!(limits.max_stream_buffer_bytes(), 1024 * 1024);
        assert_eq!(limits.max_error_body_bytes(), 64 * 1024);
    }

    #[test]
    fn client_rejects_cross_origin_stream_before_sending_request() {
        let client =
            ControlPlaneClient::new(ControlPlaneClientConfig::new("http://127.0.0.1:8787/"))
                .expect("local base URL should be valid");
        let body = serde_json::json!({"prompt": "safe"});
        let mut future = std::pin::pin!(client.post_ndjson_stream(
            "https://untrusted.example/run",
            &body,
            NdjsonStreamLimits::default(),
        ));
        let mut context = Context::from_waker(Waker::noop());

        let Poll::Ready(result) = future.as_mut().poll(&mut context) else {
            panic!("cross-origin validation should finish before network I/O");
        };
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("cross-origin endpoint must be rejected"),
        };

        assert!(matches!(error, ControlPlaneClientError::InvalidBaseUrl(_)));
    }

    #[test]
    fn decoder_handles_lines_split_across_transport_chunks() {
        let mut decoder = NdjsonDecoder::new(128);
        decoder.push(br#"{"event":"par"#).expect("first chunk should fit");
        assert!(matches!(
            decoder.decode_available().expect("partial line should remain valid"),
            DecodeProgress::NeedInput
        ));

        decoder.push(b"tial\"}\n").expect("second chunk should fit");
        let DecodeProgress::Value(value) =
            decoder.decode_available().expect("completed line should decode")
        else {
            panic!("completed line should yield one value");
        };

        assert_eq!(value, serde_json::json!({"event": "partial"}));
    }

    #[test]
    fn decoder_accepts_crlf_and_unterminated_final_line() {
        let mut decoder = NdjsonDecoder::new(128);
        decoder.push(b"{\"first\":1}\r\n{\"second\":2}").expect("response should fit");

        let DecodeProgress::Value(first) =
            decoder.decode_available().expect("CRLF line should decode")
        else {
            panic!("CRLF line should yield one value");
        };
        decoder.finish();
        let DecodeProgress::Value(second) =
            decoder.decode_available().expect("final line should decode at EOF")
        else {
            panic!("unterminated final line should yield one value");
        };

        assert_eq!(first, serde_json::json!({"first": 1}));
        assert_eq!(second, serde_json::json!({"second": 2}));
        assert!(matches!(
            decoder.decode_available().expect("finished decoder should stay exhausted"),
            DecodeProgress::Finished
        ));
    }

    #[test]
    fn decoder_ignores_empty_lines() {
        let mut decoder = NdjsonDecoder::new(128);
        decoder.push(b"\n \t\r\n{\"event\":true}\n").expect("response should fit");

        let DecodeProgress::Value(value) =
            decoder.decode_available().expect("non-empty line should decode")
        else {
            panic!("blank lines should be skipped");
        };

        assert_eq!(value, serde_json::json!({"event": true}));
    }

    #[test]
    fn decoder_rejects_oversized_buffer_without_echoing_payload() {
        let mut decoder = NdjsonDecoder::new(8);
        let error = decoder.push(b"secret-value").expect_err("oversized response must be rejected");
        let message = error.to_string();

        assert!(message.contains("8-byte buffer limit"), "unexpected error: {message}");
        assert!(!message.contains("secret-value"), "error exposed response bytes: {message}");
        assert!(matches!(
            decoder.decode_available().expect("failed decoder should be terminal"),
            DecodeProgress::Finished
        ));
    }

    #[test]
    fn decoder_rejects_malformed_json_without_echoing_payload() {
        let mut decoder = NdjsonDecoder::new(128);
        decoder.push(b"{\"token\":secret}\n").expect("response should fit");
        let error = match decoder.decode_available() {
            Err(error) => error,
            Ok(_) => panic!("invalid JSON must be rejected"),
        };
        let message = error.to_string();

        assert!(message.contains("invalid NDJSON value"), "unexpected error: {message}");
        assert!(!message.contains("secret"), "error exposed response bytes: {message}");
    }

    #[test]
    fn error_body_maps_to_existing_http_error_contract() {
        let body = br#"{
            "error":"request was rejected",
            "code":"qa_rejected",
            "category":"validation",
            "retryable":false,
            "redacted":true
        }"#;
        let error = http_error_from_body(422, Some(body));

        let ControlPlaneClientError::Http { status, message, envelope } = error else {
            panic!("error should keep the HTTP contract");
        };
        assert_eq!(status, 422);
        assert_eq!(message, "request was rejected");
        assert_eq!(envelope.map(|value| value.code).as_deref(), Some("qa_rejected"));
    }

    #[test]
    fn truncated_error_body_maps_to_generic_http_error() {
        let error = http_error_from_body(503, None);

        let ControlPlaneClientError::Http { status, message, envelope } = error else {
            panic!("error should keep the HTTP contract");
        };
        assert_eq!(status, 503);
        assert_eq!(message, "request failed with HTTP 503");
        assert!(envelope.is_none());
    }
}
