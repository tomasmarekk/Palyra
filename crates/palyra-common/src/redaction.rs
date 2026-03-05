use std::borrow::Cow;

pub const REDACTED: &str = "<redacted>";

const SENSITIVE_KEY_MARKERS: &[&str] = &[
    "access_token",
    "api_key",
    "apikey",
    "authorization",
    "bearer",
    "client_secret",
    "cookie",
    "credential",
    "password",
    "private_key",
    "refresh_token",
    "secret",
    "session",
    "set_cookie",
    "token",
    "vault_ref",
];

#[must_use]
pub fn is_sensitive_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    SENSITIVE_KEY_MARKERS.iter().any(|marker| normalized.contains(marker))
}

#[must_use]
pub fn redact_token(value: &str) -> String {
    if value.trim().is_empty() {
        String::new()
    } else {
        REDACTED.to_owned()
    }
}

#[must_use]
pub fn redact_cookie(value: &str) -> String {
    if value.trim().is_empty() {
        String::new()
    } else {
        REDACTED.to_owned()
    }
}

#[must_use]
pub fn redact_header(name: &str, value: &str) -> String {
    if is_sensitive_key(name) {
        return REDACTED.to_owned();
    }

    let normalized_name = normalize_key(name);
    if normalized_name == "location" || normalized_name == "referer" {
        return redact_url(value);
    }

    redact_auth_error(value)
}

#[must_use]
pub fn redact_url(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let (base_and_query, fragment) = split_once(trimmed, '#');
    let (base, query) = split_once(base_and_query, '?');
    let mut output = redact_url_userinfo(base);

    if let Some(query) = query {
        let redacted = redact_query_pairs(query);
        if !redacted.is_empty() {
            output.push('?');
            output.push_str(redacted.as_str());
        }
    }

    if let Some(fragment) = fragment {
        output.push('#');
        output.push_str(redact_query_pairs(fragment).as_str());
    }
    output
}

#[must_use]
pub fn redact_auth_error(message: &str) -> String {
    let mut output = String::with_capacity(message.len());
    let mut token = String::new();
    let mut redact_next_bearer = false;

    for ch in message.chars() {
        if ch.is_whitespace() {
            flush_redacted_token(
                token.as_str(),
                redact_next_bearer,
                &mut output,
                &mut redact_next_bearer,
            );
            token.clear();
            output.push(ch);
            continue;
        }
        token.push(ch);
    }

    flush_redacted_token(token.as_str(), redact_next_bearer, &mut output, &mut redact_next_bearer);
    output
}

fn flush_redacted_token(
    token: &str,
    redact_next_bearer: bool,
    output: &mut String,
    next_bearer_state: &mut bool,
) {
    if token.is_empty() {
        return;
    }

    if redact_next_bearer {
        let (core, suffix) = split_trailing_punctuation(token);
        if !core.is_empty() {
            output.push_str(REDACTED);
        }
        output.push_str(suffix);
        *next_bearer_state = false;
        return;
    }

    let (core, suffix) = split_trailing_punctuation(token);
    let processed = redact_assignment_token(core);
    output.push_str(processed.as_ref());
    output.push_str(suffix);

    *next_bearer_state = core.eq_ignore_ascii_case("bearer");
}

fn redact_assignment_token(token: &str) -> Cow<'_, str> {
    if token.is_empty() {
        return Cow::Borrowed(token);
    }
    if let Some((key, separator, value)) = split_assignment(token) {
        if is_sensitive_key(key) && !value.is_empty() {
            return Cow::Owned(format!("{key}{separator}{REDACTED}"));
        }
    }
    Cow::Borrowed(token)
}

fn split_assignment(token: &str) -> Option<(&str, char, &str)> {
    for separator in ['=', ':'] {
        if let Some(index) = token.find(separator) {
            let key = token[..index].trim_matches('"').trim_matches('\'');
            let value = token[index + 1..].trim_matches('"').trim_matches('\'');
            return Some((key, separator, value));
        }
    }
    None
}

fn split_trailing_punctuation(token: &str) -> (&str, &str) {
    let bytes = token.as_bytes();
    let mut index = bytes.len();
    while index > 0 {
        let value = bytes[index - 1];
        if matches!(value, b',' | b';' | b'.' | b')' | b']' | b'}') {
            index -= 1;
            continue;
        }
        break;
    }
    token.split_at(index)
}

fn split_once(value: &str, delimiter: char) -> (&str, Option<&str>) {
    if let Some((left, right)) = value.split_once(delimiter) {
        (left, Some(right))
    } else {
        (value, None)
    }
}

fn split_query_pair(pair: &str) -> (&str, &str) {
    if let Some(index) = pair.find('=') {
        (&pair[..index], &pair[index + 1..])
    } else {
        (pair, "")
    }
}

fn redact_query_pairs(raw: &str) -> String {
    let mut redacted_pairs = Vec::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = split_query_pair(pair);
        if is_sensitive_key(key) {
            redacted_pairs.push(format!("{key}={REDACTED}"));
        } else if value.is_empty() {
            redacted_pairs.push(key.to_owned());
        } else {
            redacted_pairs.push(format!("{key}={value}"));
        }
    }
    redacted_pairs.join("&")
}

fn redact_url_userinfo(base: &str) -> String {
    let Some((scheme, rest)) = base.split_once("://") else {
        return base.to_owned();
    };
    let authority_end = rest.find('/').unwrap_or(rest.len());
    let (authority, suffix) = rest.split_at(authority_end);
    let redacted_authority =
        authority.rsplit_once('@').map(|(_, host_and_port)| host_and_port).unwrap_or(authority);
    format!("{scheme}://{redacted_authority}{suffix}")
}

fn normalize_key(key: &str) -> String {
    let mut normalized = String::with_capacity(key.len());
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('_');
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::{is_sensitive_key, redact_auth_error, redact_header, redact_url, REDACTED};

    #[test]
    fn sensitive_key_detection_matches_common_markers() {
        assert!(is_sensitive_key("Authorization"));
        assert!(is_sensitive_key("x-api-key"));
        assert!(is_sensitive_key("session_token"));
        assert!(!is_sensitive_key("timeout_ms"));
    }

    #[test]
    fn header_redaction_masks_sensitive_headers() {
        assert_eq!(redact_header("authorization", "Bearer topsecret"), REDACTED);
        assert_eq!(redact_header("set-cookie", "session=alpha"), REDACTED);
    }

    #[test]
    fn header_redaction_redacts_location_query_secrets() {
        let redacted = redact_header(
            "location",
            "https://example.test/callback?state=ok&access_token=very-secret#done",
        );
        assert_eq!(redacted, "https://example.test/callback?state=ok&access_token=<redacted>#done");
    }

    #[test]
    fn url_redaction_masks_sensitive_query_values_only() {
        let redacted =
            redact_url("https://example.test/path?token=abc123&mode=full&refresh_token=qwe");
        assert_eq!(
            redacted,
            "https://example.test/path?token=<redacted>&mode=full&refresh_token=<redacted>"
        );
    }

    #[test]
    fn url_redaction_removes_embedded_userinfo_credentials() {
        let redacted = redact_url("https://user:pass@example.test/path");
        assert_eq!(redacted, "https://example.test/path");
        assert!(!redacted.contains("user:pass@"));
    }

    #[test]
    fn url_redaction_masks_sensitive_fragment_values() {
        let redacted = redact_url("https://example.test/callback#access_token=very-secret&mode=ok");
        assert_eq!(redacted, "https://example.test/callback#access_token=<redacted>&mode=ok");
    }

    #[test]
    fn auth_error_redaction_masks_bearer_and_token_assignments() {
        let redacted = redact_auth_error(
            "provider failed: Bearer secret-token authorization=topsecret token=abc123 code=429",
        );
        assert!(
            redacted.contains("Bearer <redacted>"),
            "bearer value should be redacted: {redacted}"
        );
        assert!(redacted.contains("authorization=<redacted>"));
        assert!(redacted.contains("token=<redacted>"));
        assert!(
            redacted.contains("code=429"),
            "non-sensitive diagnostic values should remain visible: {redacted}"
        );
    }
}
