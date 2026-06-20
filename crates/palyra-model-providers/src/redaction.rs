//! Secret-safe projection of upstream provider error text.
//!
//! Remote response bodies are hostile input: this module collapses noisy
//! whitespace, redacts credential-shaped fragments, and bounds output before
//! any caller can log, persist, or display provider errors.

/// Makes a remote error body safe to log/persist: collapses whitespace,
/// redacts credential-shaped substrings, and truncates to 240 characters.
///
/// Truncation counts Unicode scalar values, not bytes, so multi-byte text
/// can never be cut on a partial code point.
#[must_use]
pub fn sanitize_remote_error(body: &str) -> String {
    let collapsed = body.replace(['\r', '\n', '\t'], " ");
    let trimmed = collapsed.trim();
    if trimmed.is_empty() {
        return "<empty>".to_owned();
    }
    let redacted = redact_remote_error_secrets(trimmed);
    const MAX_CHARS: usize = 240;
    if redacted.chars().count() <= MAX_CHARS {
        redacted
    } else {
        let truncated: String = redacted.chars().take(MAX_CHARS).collect();
        format!("{truncated}…")
    }
}

// Byte-level scan that blanks bearer header values, provider API key prefixes,
// and key=value credential pairs. It operates on bytes so mixed/invalid UTF-8
// bodies never panic the sanitizer path.
fn redact_remote_error_secrets(raw: &str) -> String {
    const REDACTED: &[u8] = b"<redacted>";
    const KV_PATTERNS: [&[u8]; 3] = [b"api_key=", b"token=", b"secret="];

    let source = raw.as_bytes();
    let mut output = Vec::with_capacity(source.len());
    let mut index = 0;

    while index < source.len() {
        if starts_with_ascii_case_insensitive(source, index, b"bearer ") {
            output.extend_from_slice(b"Bearer ");
            output.extend_from_slice(REDACTED);
            index += b"bearer ".len();
            while index < source.len() && is_bearer_token_byte(source[index]) {
                index += 1;
            }
            continue;
        }

        if starts_with_ascii_case_insensitive(source, index, b"sk-") {
            let mut end = index + b"sk-".len();
            while end < source.len() && is_sk_token_byte(source[end]) {
                end += 1;
            }
            if end.saturating_sub(index + b"sk-".len()) >= 8 {
                output.extend_from_slice(REDACTED);
                index = end;
                continue;
            }
        }

        let mut matched_kv = false;
        for pattern in KV_PATTERNS {
            if starts_with_ascii_case_insensitive(source, index, pattern) {
                output.extend_from_slice(&source[index..index + pattern.len()]);
                index += pattern.len();
                let value_start = index;
                while index < source.len() && !is_secret_value_delimiter(source[index]) {
                    index += 1;
                }
                if index > value_start {
                    output.extend_from_slice(REDACTED);
                }
                matched_kv = true;
                break;
            }
        }
        if matched_kv {
            continue;
        }

        output.push(source[index]);
        index += 1;
    }

    String::from_utf8_lossy(output.as_slice()).into_owned()
}

fn starts_with_ascii_case_insensitive(source: &[u8], offset: usize, pattern: &[u8]) -> bool {
    if source.len().saturating_sub(offset) < pattern.len() {
        return false;
    }
    source[offset..offset + pattern.len()]
        .iter()
        .zip(pattern.iter())
        .all(|(left, right)| left.eq_ignore_ascii_case(right))
}

fn is_bearer_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~' | b'+' | b'/' | b'=')
}

fn is_sk_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_')
}

fn is_secret_value_delimiter(byte: u8) -> bool {
    byte.is_ascii_whitespace()
        || matches!(byte, b'&' | b',' | b';' | b'"' | b'\'' | b')' | b']' | b'}')
}

#[cfg(test)]
mod tests {
    use super::sanitize_remote_error;

    #[test]
    fn truncates_multibyte_text_without_panicking() {
        let input = format!("{}{}", "ž".repeat(260), "sk-should-not-appear-1234567890");

        let sanitized = sanitize_remote_error(input.as_str());

        assert!(sanitized.ends_with('…'));
        assert!(sanitized.is_char_boundary(sanitized.len()));
        assert!(!sanitized.contains("sk-should-not-appear"));
    }

    #[test]
    fn redacts_common_secret_patterns() {
        let input = "Bearer abc.def/ghi== api_key=sk-test123456789 token=tok123 secret=value sk-live123456789";

        let sanitized = sanitize_remote_error(input);

        assert_eq!(
            sanitized,
            "Bearer <redacted> api_key=<redacted> token=<redacted> secret=<redacted> <redacted>"
        );
    }

    #[test]
    fn empty_body_is_explicit() {
        assert_eq!(sanitize_remote_error(" \n\t "), "<empty>");
    }
}
