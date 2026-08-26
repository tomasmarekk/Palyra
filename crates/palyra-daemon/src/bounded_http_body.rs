//! Byte-bounded response readers for remote provider and OAuth protocols.
//!
//! A request timeout limits wall-clock duration, not the amount of data a peer
//! can deliver during that interval. Every remote body is therefore streamed
//! through these helpers before parsing or sanitization.

use anyhow::{Context, Result};
use reqwest::Response;

pub(crate) const MAX_PROVIDER_JSON_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_PROVIDER_SSE_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_PROVIDER_DISCOVERY_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_PROVIDER_PROBE_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_OAUTH_RESPONSE_BYTES: usize = 256 * 1024;
pub(crate) const MAX_REMOTE_ERROR_RESPONSE_BYTES: usize = 64 * 1024;

/// Collects a response body under both declared-length and streamed-byte caps.
///
/// # Errors
/// Returns an error when the declared or observed body exceeds `max_bytes`, the
/// byte count overflows, or the transport fails while reading a chunk.
pub(crate) async fn read_response_bytes_limited(
    mut response: Response,
    max_bytes: usize,
    body_kind: &str,
) -> Result<Vec<u8>> {
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if response.content_length().is_some_and(|length| length > max_bytes_u64) {
        anyhow::bail!(
            "{body_kind} declared a response body larger than the {max_bytes}-byte limit"
        );
    }

    let initial_capacity = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok())
        .unwrap_or_default()
        .min(max_bytes);
    let mut body = Vec::with_capacity(initial_capacity);
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("failed to read {body_kind} response body"))?
    {
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or_else(|| anyhow::anyhow!("{body_kind} response byte count overflowed"))?;
        if next_len > max_bytes {
            anyhow::bail!("{body_kind} response body exceeded the {max_bytes}-byte limit");
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Collects a bounded response body and validates that it is UTF-8.
///
/// # Errors
/// Returns an error from [`read_response_bytes_limited`] or when the bounded
/// body is not valid UTF-8.
pub(crate) async fn read_response_text_limited(
    response: Response,
    max_bytes: usize,
    body_kind: &str,
) -> Result<String> {
    let body = read_response_bytes_limited(response, max_bytes, body_kind).await?;
    String::from_utf8(body)
        .with_context(|| format!("{body_kind} response body was not valid UTF-8"))
}

/// Reads a remote error body under the smallest protocol cap.
///
/// Read failures become a local placeholder so callers can preserve the
/// original HTTP status classification without exposing or allocating an
/// unbounded provider payload.
pub(crate) async fn read_remote_error_text(response: Response, body_kind: &str) -> String {
    read_response_text_limited(response, MAX_REMOTE_ERROR_RESPONSE_BYTES, body_kind)
        .await
        .unwrap_or_else(|error| format!("<{body_kind} unavailable: {error}>"))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Read, Write},
        net::TcpListener,
        thread,
    };

    use super::read_response_bytes_limited;

    fn serve_once(response: &'static [u8]) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("test listener should bind");
        let address = listener.local_addr().expect("test listener address should resolve");
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("test request should connect");
            let mut request = [0_u8; 1_024];
            let _ = stream.read(&mut request).expect("test request should be readable");
            stream.write_all(response).expect("test response should be written");
        });
        (format!("http://{address}"), handle)
    }

    #[tokio::test]
    async fn rejects_oversized_declared_body_before_collection() {
        let (url, handle) = serve_once(
            b"HTTP/1.1 200 OK\r\nContent-Length: 9\r\nConnection: close\r\n\r\n123456789",
        );
        let response = reqwest::get(url).await.expect("test response should arrive");

        let error = read_response_bytes_limited(response, 8, "test JSON")
            .await
            .expect_err("oversized declared response must be rejected");

        assert!(error.to_string().contains("declared"));
        assert!(error.to_string().contains("8-byte limit"));
        handle.join().expect("test server should exit");
    }

    #[tokio::test]
    async fn rejects_chunked_body_when_cumulative_limit_is_crossed() {
        let (url, handle) = serve_once(
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\n1234\r\n4\r\n5678\r\n0\r\n\r\n",
        );
        let response = reqwest::get(url).await.expect("test response should arrive");

        let error = read_response_bytes_limited(response, 7, "test SSE")
            .await
            .expect_err("oversized chunked response must be rejected");

        assert!(error.to_string().contains("exceeded"));
        assert!(error.to_string().contains("7-byte limit"));
        handle.join().expect("test server should exit");
    }
}
