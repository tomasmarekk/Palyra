//! Small wire-level helpers shared by the HTTP client.
//!
//! Kept dependency-free on purpose: percent-encoding is implemented locally
//! rather than pulling in a URL-encoding crate for a handful of call sites.

/// Builds the user-facing message for a non-success response whose body did not
/// decode as an [`ErrorEnvelope`](crate::ErrorEnvelope).
///
/// Bodies that are empty or longer than 256 bytes collapse to a generic message
/// so HTML error pages and other oversized payloads never leak into error text.
pub(crate) fn fallback_error_message(status: u16, body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return format!("request failed with HTTP {status}");
    }
    if trimmed.len() > 256 {
        return format!("request failed with HTTP {status}");
    }
    trimmed.to_owned()
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
