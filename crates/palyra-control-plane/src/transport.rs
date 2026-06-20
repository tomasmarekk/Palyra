//! Small wire-level helpers shared by the HTTP client.
//!
//! Kept dependency-free on purpose: percent-encoding is implemented locally
//! rather than pulling in a URL-encoding crate for a handful of call sites.

use serde_json::Value;

/// Builds the user-facing message for a non-success response whose body did not
/// decode as an [`ErrorEnvelope`](crate::ErrorEnvelope).
///
/// Bodies that are empty or longer than 256 bytes collapse to a generic message
/// so HTML error pages and other oversized payloads never leak into error text.
/// Short bodies are still redacted because upstream validation errors can echo
/// submitted credentials.
pub(crate) fn fallback_error_message(status: u16, body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return format!("request failed with HTTP {status}");
    }
    if trimmed.len() > 256 {
        return format!("request failed with HTTP {status}");
    }
    redact_fallback_error_body(trimmed)
}

fn redact_fallback_error_body(body: &str) -> String {
    if let Ok(mut value) = serde_json::from_str::<Value>(body) {
        redact_json_secrets(&mut value, None);
        return value.to_string();
    }
    redact_prefixed_secret_tokens(body)
}

fn redact_json_secrets(value: &mut Value, key_context: Option<&str>) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map {
                redact_json_secrets(entry, Some(key.as_str()));
            }
        }
        Value::Array(entries) => {
            for entry in entries {
                redact_json_secrets(entry, key_context);
            }
        }
        Value::String(raw) => {
            if key_context.is_some_and(is_sensitive_json_key) {
                *raw = "<redacted>".to_owned();
            } else {
                *raw = redact_prefixed_secret_tokens(raw.as_str());
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn is_sensitive_json_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', ' '], "_");
    normalized.contains("api_key")
        || normalized.contains("apikey")
        || normalized.contains("access_token")
        || normalized.contains("refresh_token")
        || normalized.contains("authorization")
        || normalized.contains("client_secret")
        || normalized.ends_with("_secret")
}

fn redact_prefixed_secret_tokens(raw: &str) -> String {
    let mut redacted = raw.to_owned();
    for prefix in ["sk-", "sk_", "xai-"] {
        redacted = redact_tokens_with_prefix(redacted.as_str(), prefix);
    }
    redacted
}

fn redact_tokens_with_prefix(raw: &str, prefix: &str) -> String {
    let mut output = String::with_capacity(raw.len());
    let mut index = 0;
    while let Some(relative_start) = raw[index..].find(prefix) {
        let start = index + relative_start;
        output.push_str(&raw[index..start]);
        let mut end = start + prefix.len();
        for (offset, ch) in raw[end..].char_indices() {
            if is_secret_token_char(ch) {
                end = start + prefix.len() + offset + ch.len_utf8();
            } else {
                break;
            }
        }
        if end.saturating_sub(start) >= prefix.len() + 4 {
            output.push_str("<redacted>");
        } else {
            output.push_str(&raw[start..end]);
        }
        index = end;
    }
    output.push_str(&raw[index..]);
    output
}

fn is_secret_token_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.')
}

/// Percent-encodes every byte outside the RFC 3986 unreserved set.
///
/// Deliberately stricter than browser encoding: `/`, `&`, and `=` are escaped
/// too, so caller-supplied identifiers can never inject extra path segments or
/// query parameters.
pub(crate) fn urlencoding(raw: &str) -> String {
    let mut encoded = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            _ => encoded.push_str(format!("%{byte:02X}").as_str()),
        }
    }
    encoded
}
