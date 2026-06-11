//! Outbound payload construction for the Discord adapter.
//!
//! Builds message JSON and multipart uploads, enforces upload content-type/size policy, and
//! splits long text into Discord-sized chunks while keeping Markdown code fences balanced so
//! each chunk renders correctly on its own.

use base64::Engine as _;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::protocol::{AttachmentKind, AttachmentRef, OutboundMessageRequest};

use super::{DISCORD_MAX_UPLOAD_BYTES, DISCORD_UPLOAD_ALLOWED_CONTENT_TYPES};

/// Percent-encodes a URL path component using the RFC 3986 unreserved set.
pub(super) fn percent_encode_component(raw: &str) -> String {
    let mut encoded = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push_str(format!("{byte:02X}").as_str());
        }
    }
    encoded
}

/// One validated file destined for a multipart upload part.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DiscordMultipartAttachment {
    pub filename: String,
    pub content_type: String,
    pub bytes: Vec<u8>,
}

/// Validates and decodes the upload-requested attachments of an outbound request.
///
/// # Errors
/// Returns a user-visible reason string when an attachment is missing required fields, has a
/// policy-blocked content type, carries invalid base64, or exceeds the upload size cap.
pub(super) fn collect_discord_upload_files(
    request: &OutboundMessageRequest,
) -> Result<Vec<DiscordMultipartAttachment>, String> {
    let mut files = Vec::new();
    for attachment in &request.attachments {
        if !attachment.upload_requested {
            continue;
        }
        let Some(content_type) =
            attachment.content_type.as_deref().map(str::trim).filter(|value| !value.is_empty())
        else {
            return Err("discord upload attachment requires content_type".to_owned());
        };
        if !DISCORD_UPLOAD_ALLOWED_CONTENT_TYPES.contains(&content_type) {
            return Err(format!(
                "discord upload content type '{content_type}' is blocked by policy"
            ));
        }
        let Some(inline_base64) =
            attachment.inline_base64.as_deref().map(str::trim).filter(|value| !value.is_empty())
        else {
            return Err("discord upload attachment requires inline_base64".to_owned());
        };
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(inline_base64)
            .map_err(|_| "discord upload attachment inline_base64 is invalid".to_owned())?;
        if bytes.len() > DISCORD_MAX_UPLOAD_BYTES {
            return Err(format!(
                "discord upload attachment exceeds max bytes ({}/{DISCORD_MAX_UPLOAD_BYTES})",
                bytes.len()
            ));
        }
        files.push(DiscordMultipartAttachment {
            filename: sanitize_discord_upload_filename(
                attachment.filename.as_deref(),
                content_type,
            ),
            content_type: content_type.to_owned(),
            bytes,
        });
    }
    Ok(files)
}

/// Builds the JSON body for a message create call (also used as `payload_json` in multipart
/// uploads).
pub(super) fn build_discord_message_payload(
    request: &OutboundMessageRequest,
    upload_files: &[DiscordMultipartAttachment],
) -> Value {
    // The deterministic nonce plus enforce_nonce makes Discord itself deduplicate resends of
    // the same envelope, backstopping the adapter's local idempotency cache.
    let mut payload = json!({
        "content": request.text,
        "nonce": discord_message_nonce(request),
        "enforce_nonce": true,
    });
    if !upload_files.is_empty() {
        payload["attachments"] = Value::Array(
            upload_files
                .iter()
                .enumerate()
                .map(|(index, file)| {
                    json!({
                        "id": index.to_string(),
                        "filename": file.filename,
                    })
                })
                .collect(),
        );
    }
    if let Some(in_reply_to_message_id) =
        request.in_reply_to_message_id.as_deref().map(str::trim).filter(|value| !value.is_empty())
    {
        // fail_if_not_exists=false degrades a reply to a plain message when the referenced
        // message was deleted, instead of failing the whole delivery.
        payload["message_reference"] = json!({
            "message_id": in_reply_to_message_id,
            "fail_if_not_exists": false,
        });
    }
    payload
}

/// Derives a stable per-envelope nonce from the request's routing identity (not its text, so
/// edits to retry payloads still dedupe).
fn discord_message_nonce(request: &OutboundMessageRequest) -> String {
    stable_fingerprint_hex(
        json!({
            "envelope_id": request.envelope_id,
            "connector_id": request.connector_id,
            "conversation_id": request.conversation_id,
            "thread_id": request.reply_thread_id,
        })
        .to_string()
        .as_bytes(),
    )
}

/// Reduces a filename to a safe ASCII subset, falling back to a content-type-derived default
/// when nothing usable remains.
fn sanitize_discord_upload_filename(raw: Option<&str>, content_type: &str) -> String {
    let mut sanitized = raw
        .unwrap_or_default()
        .chars()
        .map(
            |ch| if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') { ch } else { '_' },
        )
        .collect::<String>()
        .trim_matches('.')
        .to_owned();
    if sanitized.is_empty() {
        sanitized = match content_type {
            "image/png" => "attachment.png".to_owned(),
            "image/jpeg" => "attachment.jpg".to_owned(),
            "image/webp" => "attachment.webp".to_owned(),
            "image/gif" => "attachment.gif".to_owned(),
            "application/json" => "attachment.json".to_owned(),
            "text/plain" => "attachment.txt".to_owned(),
            _ => "attachment.bin".to_owned(),
        };
    }
    sanitized
}

/// Extracts the message id from a message create/edit response body.
pub(super) fn parse_discord_message_id(raw_body: &str) -> Option<String> {
    serde_json::from_str::<Value>(raw_body)
        .ok()
        .and_then(|payload| payload.get("id").and_then(Value::as_str).map(ToOwned::to_owned))
}

/// Builds a deterministic stand-in message id when the API response carried none, so the
/// idempotency cache and receipts still get a stable identifier.
pub(super) fn fallback_native_message_id(request: &OutboundMessageRequest) -> String {
    let fingerprint = json!({
        "envelope_id": request.envelope_id,
        "connector_id": request.connector_id,
        "conversation_id": request.conversation_id,
        "text": request.text,
        "thread_id": request.reply_thread_id,
    });
    format!("discord-{}", stable_fingerprint_hex(fingerprint.to_string().as_bytes()))
}

fn stable_fingerprint_hex(payload: &[u8]) -> String {
    let digest = Sha256::digest(payload);
    digest[..16].iter().map(|byte| format!("{byte:02x}")).collect()
}

/// A Markdown code fence opened earlier in the text and not yet closed.
#[derive(Debug, Clone)]
pub(super) struct OpenFence {
    pub(super) indent: String,
    pub(super) marker: char,
    pub(super) marker_len: usize,
    pub(super) open_line: String,
}

/// Splits `text` into chunks within the char and line budgets, closing any open code fence
/// at each chunk boundary and reopening it in the next chunk so every chunk renders as valid
/// Markdown on its own.
pub(super) fn chunk_discord_text(text: &str, max_chars: usize, max_lines: usize) -> Vec<String> {
    let body = text.trim_end_matches('\r');
    if body.trim().is_empty() {
        return Vec::new();
    }

    let max_chars = max_chars.max(1);
    let max_lines = max_lines.max(1);
    if char_len(body) <= max_chars && count_lines(body) <= max_lines {
        return vec![body.to_owned()];
    }

    let mut chunks = Vec::new();
    let mut current = String::new();
    let mut current_lines = 0_usize;
    let mut open_fence: Option<OpenFence> = None;

    for line in body.split('\n') {
        let fence_info = parse_fence_line(line);
        let was_inside_fence = open_fence.is_some();
        let mut next_open_fence = open_fence.clone();
        if let Some(fence_info) = fence_info {
            if open_fence.is_none() {
                next_open_fence = Some(fence_info);
            } else if let Some(existing) = open_fence.as_ref() {
                if existing.marker == fence_info.marker
                    && fence_info.marker_len >= existing.marker_len
                {
                    next_open_fence = None;
                }
            }
        }

        // Reserve room for the synthetic closing fence line that flush_chunk may append, so
        // appending it never pushes a chunk over the limits.
        let reserve_chars = next_open_fence
            .as_ref()
            .map(|fence| char_len(close_fence_line(fence).as_str()) + 1)
            .unwrap_or(0);
        let reserve_lines = if next_open_fence.is_some() { 1 } else { 0 };

        let char_limit = max_chars.saturating_sub(reserve_chars).max(1);
        let line_limit = max_lines.saturating_sub(reserve_lines).max(1);
        let prefix_len = if current.is_empty() { 0 } else { char_len(current.as_str()) + 1 };
        let segment_limit = char_limit.saturating_sub(prefix_len).max(1);
        let segments = split_long_line(line, segment_limit, was_inside_fence);

        for (segment_index, segment) in segments.iter().enumerate() {
            let is_continuation = segment_index > 0;
            let delimiter = if is_continuation || current.is_empty() { "" } else { "\n" };
            let addition = format!("{delimiter}{segment}");
            let next_length = char_len(current.as_str()) + char_len(addition.as_str());
            let next_line_count = current_lines + if is_continuation { 0 } else { 1 };
            if (next_length > char_limit || next_line_count > line_limit) && !current.is_empty() {
                flush_chunk(&mut current, &mut current_lines, &open_fence, &mut chunks);
            }

            if current.is_empty() {
                current.push_str(segment);
                current_lines = 1;
            } else {
                current.push_str(delimiter);
                current.push_str(segment);
                if !is_continuation {
                    current_lines = current_lines.saturating_add(1);
                }
            }
        }

        open_fence = next_open_fence;
    }

    if !current.is_empty() {
        let payload = ensure_balanced_fences(close_fence_if_needed(current.as_str(), &open_fence));
        if !payload.trim().is_empty() {
            chunks.push(payload);
        }
    }

    chunks
}

/// Finalizes `current` into `chunks`, closing an open fence in the emitted chunk and
/// reopening it as the seed of the next chunk.
fn flush_chunk(
    current: &mut String,
    current_lines: &mut usize,
    open_fence: &Option<OpenFence>,
    chunks: &mut Vec<String>,
) {
    if current.is_empty() {
        return;
    }
    let payload = ensure_balanced_fences(close_fence_if_needed(current.as_str(), open_fence));
    if !payload.trim().is_empty() {
        chunks.push(payload);
    }
    current.clear();
    *current_lines = 0;
    if let Some(open_fence) = open_fence {
        current.push_str(open_fence.open_line.as_str());
        *current_lines = 1;
    }
}

/// Parses a line as a CommonMark fence opener (up to three spaces of indent, then three or
/// more backticks or tildes).
pub(super) fn parse_fence_line(line: &str) -> Option<OpenFence> {
    let mut chars = line.chars().peekable();
    let mut indent = String::new();
    while indent.len() < 3 {
        let Some(next) = chars.peek().copied() else {
            break;
        };
        if next == ' ' {
            indent.push(next);
            chars.next();
            continue;
        }
        break;
    }
    let marker = chars.peek().copied()?;
    if marker != '`' && marker != '~' {
        return None;
    }
    let mut marker_len = 0_usize;
    while let Some(next) = chars.peek().copied() {
        if next == marker {
            marker_len = marker_len.saturating_add(1);
            chars.next();
        } else {
            break;
        }
    }
    if marker_len < 3 {
        return None;
    }
    Some(OpenFence { indent, marker, marker_len, open_line: line.to_owned() })
}

fn close_fence_line(open_fence: &OpenFence) -> String {
    format!("{}{}", open_fence.indent, open_fence.marker.to_string().repeat(open_fence.marker_len))
}

fn close_fence_if_needed(text: &str, open_fence: &Option<OpenFence>) -> String {
    let Some(open_fence) = open_fence else {
        return text.to_owned();
    };
    let close_line = close_fence_line(open_fence);
    if text.is_empty() {
        return close_line;
    }
    if text.ends_with('\n') {
        format!("{text}{close_line}")
    } else {
        format!("{text}\n{close_line}")
    }
}

fn ensure_balanced_fences(chunk: String) -> String {
    let mut open_fence: Option<OpenFence> = None;
    for line in chunk.split('\n') {
        let Some(fence_info) = parse_fence_line(line) else {
            continue;
        };
        if open_fence.is_none() {
            open_fence = Some(fence_info);
            continue;
        }
        if let Some(existing) = open_fence.as_ref() {
            if existing.marker == fence_info.marker && fence_info.marker_len >= existing.marker_len
            {
                open_fence = None;
            }
        }
    }
    let Some(open_fence) = open_fence else {
        return chunk;
    };
    let close_line = close_fence_line(&open_fence);
    if chunk.ends_with('\n') {
        format!("{chunk}{close_line}")
    } else {
        format!("{chunk}\n{close_line}")
    }
}

/// Splits an over-long line into segments of at most `limit` chars, preferring whitespace
/// break points outside code fences (`preserve_whitespace` keeps fenced content verbatim).
fn split_long_line(line: &str, limit: usize, preserve_whitespace: bool) -> Vec<String> {
    let limit = limit.max(1);
    if char_len(line) <= limit {
        return vec![line.to_owned()];
    }

    let chars = line.chars().collect::<Vec<_>>();
    let mut out = Vec::new();
    let mut start = 0_usize;
    while start < chars.len() {
        let remaining = chars.len().saturating_sub(start);
        if remaining <= limit {
            out.push(chars[start..].iter().collect::<String>());
            break;
        }
        let window_end = start.saturating_add(limit);
        if preserve_whitespace {
            out.push(chars[start..window_end].iter().collect::<String>());
            start = window_end;
            continue;
        }

        let mut split_index = None;
        for index in (start..window_end).rev() {
            if chars[index].is_whitespace() {
                split_index = Some(index);
                break;
            }
        }
        let split_at = split_index.filter(|index| *index > start).unwrap_or(window_end);
        out.push(chars[start..split_at].iter().collect::<String>());
        start = split_at;
    }
    out
}

fn count_lines(text: &str) -> usize {
    if text.is_empty() {
        return 0;
    }
    text.split('\n').count()
}

fn char_len(text: &str) -> usize {
    text.chars().count()
}

/// Appends an attachment-metadata summary block to the outbound text for attachments that
/// are referenced but not uploaded.
pub(super) fn with_attachment_context(text: &str, attachments: &[AttachmentRef]) -> String {
    let Some(summary) = render_attachment_context(attachments) else {
        return text.to_owned();
    };
    let trimmed = text.trim_end();
    if trimmed.is_empty() {
        summary
    } else {
        format!("{trimmed}\n\n{summary}")
    }
}

fn render_attachment_context(attachments: &[AttachmentRef]) -> Option<String> {
    if attachments.is_empty() {
        return None;
    }
    let mut lines = Vec::with_capacity(attachments.len().saturating_add(1));
    lines.push("[attachment-metadata]".to_owned());
    for (index, attachment) in attachments.iter().enumerate() {
        lines.push(format!("- {}: {}", index.saturating_add(1), summarize_attachment(attachment)));
    }
    Some(lines.join("\n"))
}

fn summarize_attachment(attachment: &AttachmentRef) -> String {
    let kind = match attachment.kind {
        AttachmentKind::Image => "image",
        AttachmentKind::File => "file",
    };
    let source = attachment
        .url
        .as_deref()
        .or(attachment.artifact_ref.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let filename = attachment
        .filename
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let content_type = attachment
        .content_type
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("unknown");
    let size = attachment
        .size_bytes
        .map(|value| value.to_string())
        .unwrap_or_else(|| "unknown".to_owned());
    format!(
        "kind={kind}, filename={filename}, content_type={content_type}, size_bytes={size}, source={source}"
    )
}
