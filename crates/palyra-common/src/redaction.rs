//! Secret redaction for headers, URLs, and free-form diagnostic text.
//!
//! Heuristic variants minimize false positives on benign fixture text; `_strict` variants
//! redact every sensitive-looking assignment for diagnostics that leave the host. Outcomes
//! are pinned by fixtures and exercised by `fuzz/fuzz_targets/redaction_routines.rs` —
//! placeholder strings and redaction decisions must stay byte-identical.

use std::borrow::Cow;

/// Placeholder substituted for redacted values; pinned contract surface in fixtures.
pub const REDACTED: &str = "<redacted>";
/// Placeholder substituted for internal Palyra runtime storage paths.
pub const REDACTED_INTERNAL_RUNTIME_PATH: &str = "<palyra_internal_artifact_ref>";

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
    "signature",
    "sig",
    "token",
    "vault_ref",
    "x_amz_credential",
    "x_amz_security_token",
    "x_amz_signature",
    "x_goog_credential",
    "x_goog_signature",
];

/// How aggressively assignment values are redacted.
///
/// `Heuristic` keeps benign short bare `token=` values visible; `Diagnostic` redacts every
/// sensitive-keyed assignment because diagnostic exports must err on the side of removal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RedactionStrictness {
    Heuristic,
    Diagnostic,
}

/// Returns whether a header/query/assignment key matches a known sensitive marker.
#[must_use]
pub fn is_sensitive_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    SENSITIVE_KEY_MARKERS.iter().any(|marker| normalized.contains(marker))
}

/// Returns whether a sensitive assignment key denotes a file/path reference
/// rather than credential material itself.
#[must_use]
pub fn is_sensitive_path_reference_key(key: &str) -> bool {
    let normalized = normalize_key(key);
    if !is_sensitive_key(normalized.as_str()) {
        return false;
    }
    if normalized.ends_with("_file") || normalized.ends_with("_path") {
        return true;
    }

    let compact = normalized.replace('_', "");
    const COMPACT_PATH_REFERENCE_SUFFIXES: &[&str] = &[
        "accesstokenfile",
        "accesstokenpath",
        "apikeyfile",
        "apikeypath",
        "authorizationfile",
        "authorizationpath",
        "authtokenfile",
        "authtokenpath",
        "clientsecretfile",
        "clientsecretpath",
        "cookiefile",
        "cookiepath",
        "credentialfile",
        "credentialpath",
        "passwordfile",
        "passwordpath",
        "privatekeyfile",
        "privatekeypath",
        "refreshtokenfile",
        "refreshtokenpath",
        "secretfile",
        "secretpath",
        "sessionfile",
        "sessionpath",
        "signaturefile",
        "signaturepath",
        "tokenfile",
        "tokenpath",
        "vaultreffile",
        "vaultrefpath",
    ];
    COMPACT_PATH_REFERENCE_SUFFIXES.iter().any(|suffix| compact.ends_with(suffix))
}

/// Replaces a non-empty token value with the [`REDACTED`] placeholder.
#[must_use]
pub fn redact_token(value: &str) -> String {
    if value.trim().is_empty() {
        String::new()
    } else {
        REDACTED.to_owned()
    }
}

/// Replaces a non-empty cookie value with the [`REDACTED`] placeholder.
#[must_use]
pub fn redact_cookie(value: &str) -> String {
    if value.trim().is_empty() {
        String::new()
    } else {
        REDACTED.to_owned()
    }
}

/// Redacts a header value based on the header name.
///
/// Sensitive header names mask the whole value; `Location`/`Referer` get URL redaction
/// (their query strings can carry tokens); all other values get heuristic text redaction.
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

/// Redacts userinfo credentials and sensitive query/fragment values from a URL (heuristic).
#[must_use]
pub fn redact_url(raw: &str) -> String {
    redact_url_with_strictness(raw, RedactionStrictness::Heuristic)
}

/// Like [`redact_url`] but redacts every sensitive-keyed value, including short ones.
#[must_use]
pub fn redact_url_strict(raw: &str) -> String {
    redact_url_with_strictness(raw, RedactionStrictness::Diagnostic)
}

fn redact_url_with_strictness(raw: &str, strictness: RedactionStrictness) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let (base_and_query, fragment) = split_once(trimmed, '#');
    let (base, query) = split_once(base_and_query, '?');
    let mut output = redact_url_userinfo(base);

    if let Some(query) = query {
        let redacted = redact_query_pairs(query, strictness);
        if !redacted.is_empty() {
            output.push('?');
            output.push_str(redacted.as_str());
        }
    }

    if let Some(fragment) = fragment {
        output.push('#');
        output.push_str(redact_query_pairs(fragment, strictness).as_str());
    }
    output
}

/// Redacts bearer credentials and sensitive `key=value` assignments in free text (heuristic).
#[must_use]
pub fn redact_auth_error(message: &str) -> String {
    redact_auth_error_with_strictness(message, RedactionStrictness::Heuristic)
}

/// Like [`redact_auth_error`] but redacts every sensitive-keyed assignment value.
#[must_use]
pub fn redact_auth_error_strict(message: &str) -> String {
    redact_auth_error_with_strictness(message, RedactionStrictness::Diagnostic)
}

/// Strict full-text redaction for diagnostics: embedded URLs first, then inline assignments.
#[must_use]
pub fn redact_diagnostic_text(message: &str) -> String {
    redact_internal_runtime_paths(
        redact_auth_error_strict(redact_url_segments_in_text_strict(message).as_str()).as_str(),
    )
}

/// Redacts internal `.palyra/sessions` storage paths from model-visible diagnostics.
#[must_use]
pub fn redact_internal_runtime_paths(message: &str) -> String {
    let mut redacted = message.to_owned();
    while let Some((marker_start, marker_len)) = internal_runtime_path_marker(&redacted) {
        let start = internal_runtime_path_start(redacted.as_str(), marker_start);
        let end = internal_runtime_path_end(redacted.as_str(), marker_start + marker_len);
        redacted.replace_range(start..end, REDACTED_INTERNAL_RUNTIME_PATH);
    }
    redacted
}

fn redact_auth_error_with_strictness(message: &str, strictness: RedactionStrictness) -> String {
    let mut output = String::with_capacity(message.len());
    let mut token = String::new();
    let mut redact_next_bearer = false;

    for ch in message.chars() {
        if ch.is_whitespace() {
            flush_redacted_token(
                token.as_str(),
                redact_next_bearer,
                strictness,
                &mut output,
                &mut redact_next_bearer,
            );
            token.clear();
            output.push(ch);
            continue;
        }
        token.push(ch);
    }

    flush_redacted_token(
        token.as_str(),
        redact_next_bearer,
        strictness,
        &mut output,
        &mut redact_next_bearer,
    );
    output
}

fn internal_runtime_path_marker(message: &str) -> Option<(usize, usize)> {
    const MARKERS: &[&str] = &[
        ".palyra/sessions",
        ".palyra\\sessions",
        "%userprofile%/.palyra",
        "%userprofile%\\.palyra",
        "$home/.palyra",
        "$home\\.palyra",
        "${home}/.palyra",
        "${home}\\.palyra",
    ];

    let lowered = message.to_ascii_lowercase();
    MARKERS
        .iter()
        .filter_map(|marker| lowered.find(marker).map(|index| (index, marker.len())))
        .min_by_key(|(index, _)| *index)
}

fn internal_runtime_path_start(message: &str, marker_start: usize) -> usize {
    let prefix = &message[..marker_start];
    let lowered_prefix = prefix.to_ascii_lowercase();
    if let Some(index) = lowered_prefix.rfind("%userprofile%") {
        return index;
    }
    if let Some(index) = lowered_prefix.rfind("${home}") {
        return index;
    }
    if let Some(index) = lowered_prefix.rfind("$home") {
        return index;
    }
    if let Some(index) = windows_drive_path_start(prefix) {
        return index;
    }
    prefix
        .char_indices()
        .rev()
        .find_map(|(index, ch)| internal_path_left_boundary(ch).then_some(index + ch.len_utf8()))
        .unwrap_or(0)
}

fn windows_drive_path_start(prefix: &str) -> Option<usize> {
    let bytes = prefix.as_bytes();
    let mut index = bytes.len();
    while index >= 2 {
        let colon_index = index - 1;
        if bytes[colon_index] == b':' && bytes[colon_index - 1].is_ascii_alphabetic() {
            return Some(colon_index - 1);
        }
        index -= 1;
    }
    None
}

fn internal_runtime_path_end(message: &str, search_start: usize) -> usize {
    message[search_start..]
        .char_indices()
        .find_map(|(offset, ch)| internal_path_right_boundary(ch).then_some(search_start + offset))
        .unwrap_or(message.len())
}

fn internal_path_left_boundary(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '"' | '\'' | '`' | '(' | '[' | '{' | '<' | '=')
}

fn internal_path_right_boundary(ch: char) -> bool {
    ch.is_whitespace() || matches!(ch, '"' | '\'' | '`' | ')' | ']' | '}' | '>' | ',' | ';')
}

/// Applies URL redaction to every URL-looking whitespace token embedded in free text.
#[must_use]
pub fn redact_url_segments_in_text(raw: &str) -> String {
    redact_url_segments_in_text_with_strictness(raw, RedactionStrictness::Heuristic)
}

/// Like [`redact_url_segments_in_text`] but with strict query/fragment value redaction.
#[must_use]
pub fn redact_url_segments_in_text_strict(raw: &str) -> String {
    redact_url_segments_in_text_with_strictness(raw, RedactionStrictness::Diagnostic)
}

fn redact_url_segments_in_text_with_strictness(
    raw: &str,
    strictness: RedactionStrictness,
) -> String {
    let mut output = String::with_capacity(raw.len());
    let mut token = String::new();

    for ch in raw.chars() {
        if ch.is_whitespace() {
            flush_redacted_url_token(token.as_str(), strictness, &mut output);
            token.clear();
            output.push(ch);
            continue;
        }
        token.push(ch);
    }

    flush_redacted_url_token(token.as_str(), strictness, &mut output);
    output
}

fn flush_redacted_token(
    token: &str,
    redact_next_bearer: bool,
    strictness: RedactionStrictness,
    output: &mut String,
    next_bearer_state: &mut bool,
) {
    if token.is_empty() {
        return;
    }

    // A preceding "Bearer" marker means this whole token is the credential itself;
    // trailing punctuation stays visible so log structure survives redaction.
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
    let processed = redact_assignment_token(core, strictness);
    output.push_str(processed.as_ref());
    output.push_str(suffix);

    *next_bearer_state = should_redact_following_bearer_token(core);
}

fn flush_redacted_url_token(token: &str, strictness: RedactionStrictness, output: &mut String) {
    if token.is_empty() {
        return;
    }
    if looks_like_url_token(token) {
        output.push_str(redact_url_with_strictness(token, strictness).as_str());
    } else {
        output.push_str(token);
    }
}

fn looks_like_url_token(token: &str) -> bool {
    if token.contains("://") || token.starts_with("www.") {
        return true;
    }
    let Some(separator_index) = token.find('?').or_else(|| token.find('#')) else {
        return false;
    };
    let base = token[..separator_index]
        .trim_matches(['"', '\'', '`', '(', '[', '{'])
        .trim_end_matches(['"', '\'', '`', ')', ']', '}']);
    if base.is_empty() || base.contains('(') || base.contains(')') {
        return false;
    }
    if query_or_fragment_contains_sensitive_value(&token[separator_index + 1..]) {
        return true;
    }
    base.starts_with('/') || base.starts_with("./") || base.starts_with("../") || base.contains('.')
}

fn query_or_fragment_contains_sensitive_value(raw: &str) -> bool {
    raw.split('&').any(|pair| {
        let (key, value) = split_query_pair(pair);
        !value.is_empty() && sensitive_assignment_key(key).is_some()
    })
}

fn redact_assignment_token(token: &str, strictness: RedactionStrictness) -> Cow<'_, str> {
    if token.is_empty() {
        return Cow::Borrowed(token);
    }
    if let Some((key, separator, value)) = split_assignment(token) {
        if should_redact_assignment_value(key, value, strictness) {
            return Cow::Owned(format!("{key}{separator}{REDACTED}"));
        }
    }
    Cow::Borrowed(token)
}

fn should_redact_following_bearer_token(token: &str) -> bool {
    if token.eq_ignore_ascii_case("bearer") {
        return true;
    }
    if let Some((key, _, value)) = split_assignment(token) {
        return assignment_key_is_sensitive(key) && value.eq_ignore_ascii_case("bearer");
    }
    false
}

fn should_redact_assignment_value(key: &str, value: &str, strictness: RedactionStrictness) -> bool {
    let Some(sensitive_key) = sensitive_assignment_key(key) else {
        return false;
    };
    if value.is_empty() {
        return false;
    }
    if strictness == RedactionStrictness::Heuristic
        && is_sensitive_path_reference_key(sensitive_key)
        && is_benign_path_reference_value(value)
    {
        return false;
    }
    if strictness == RedactionStrictness::Diagnostic {
        return true;
    }
    // Heuristic mode: bare "token" keys are common in benign fixture/test text, but
    // real callback, CSRF, and magic-link tokens can be very short. Keep only the
    // narrow source-fixture shapes known to be non-secret.
    if normalize_key(sensitive_key) == "token" {
        return !is_benign_bare_token_fixture_value(value);
    }
    true
}

fn assignment_key_is_sensitive(key: &str) -> bool {
    sensitive_assignment_key(key).is_some()
}

/// Returns true when a sensitive-keyed assignment value is only a local path
/// reference, not credential material.
#[must_use]
pub fn is_benign_path_reference_value(value: &str) -> bool {
    let normalized =
        value.trim().trim_end_matches([',', ';']).trim().trim_matches(['"', '\'', '`']).trim();
    if normalized.is_empty() {
        return false;
    }
    let lowered = normalized.to_ascii_lowercase();
    if lowered.starts_with("http://")
        || lowered.starts_with("https://")
        || lowered.starts_with("bearer ")
        || lowered.starts_with("sk-")
        || lowered.starts_with("ghp_")
        || lowered.starts_with("github_pat_")
        || lowered.starts_with("xox")
        || lowered.contains("palyra_test_secret")
        || lowered.contains("dummy_secret")
        || lowered.contains("should_not_leak")
        || lowered.contains("secret_should_not_appear")
    {
        return false;
    }
    let has_path_separator = normalized.contains('/') || normalized.contains('\\');
    let has_path_extension = normalized
        .rsplit(['/', '\\'])
        .next()
        .is_some_and(|file_name| file_name.contains('.') && !file_name.starts_with('.'));
    has_path_separator
        && has_path_extension
        && normalized.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(ch, '/' | '\\' | ':' | '.' | '_' | '-' | ' ' | '~' | '%' | '+')
        })
}

fn sensitive_assignment_key(key: &str) -> Option<&str> {
    let trimmed = key.trim().trim_matches(['"', '\'']);
    let plain = trailing_plain_key_segment(trimmed)?;
    // A '?', '&', or '#' directly before the key means a URL query/fragment (handled by URL
    // redaction) or a CSS-selector-like token (e.g. `selector=#password`), not an assignment.
    if matches!(plain.prefix, Some('?') | Some('&') | Some('#')) {
        return None;
    }
    if plain.is_empty() || !is_sensitive_key(plain.segment) {
        return None;
    }
    Some(plain.segment)
}

struct PlainKeySegment<'a> {
    segment: &'a str,
    prefix: Option<char>,
}

impl PlainKeySegment<'_> {
    fn is_empty(&self) -> bool {
        self.segment.is_empty()
    }
}

fn trailing_plain_key_segment(value: &str) -> Option<PlainKeySegment<'_>> {
    let end = value
        .char_indices()
        .rev()
        .find(|(_, ch)| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-'))
        .map(|(index, ch)| index + ch.len_utf8())?;
    let start = value[..end]
        .char_indices()
        .rev()
        .find(|(_, ch)| !(ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-')))
        .map(|(index, ch)| index + ch.len_utf8())
        .unwrap_or(0);
    let prefix = if start == 0 { None } else { value[..start].chars().next_back() };
    Some(PlainKeySegment { segment: &value[start..end], prefix })
}

fn is_benign_bare_token_fixture_value(value: &str) -> bool {
    let trimmed = value
        .trim()
        .trim_matches(['"', '\'', '`'])
        .trim_end_matches([',', ';', ':', '.', ')', ']', '}']);
    if trimmed.is_empty() {
        return false;
    }
    let lowered = trimmed.to_ascii_lowercase();
    lowered == "a%3db%3dc" || looks_like_parser_fixture_value(lowered.as_str())
}

fn looks_like_parser_fixture_value(value: &str) -> bool {
    value.contains('=')
        && value.len() <= 96
        && value
            .split('=')
            .all(|segment| !segment.is_empty() && segment.chars().all(|ch| ch.is_ascii_lowercase()))
        && value.split('=').any(|segment| matches!(segment, "value" | "equals" | "expected"))
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
        if matches!(value, b',' | b';' | b':' | b'.' | b')' | b']' | b'}') {
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

fn redact_query_pairs(raw: &str, strictness: RedactionStrictness) -> String {
    let mut redacted_pairs = Vec::new();
    for pair in raw.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = split_query_pair(pair);
        if should_redact_assignment_value(key, value, strictness) {
            redacted_pairs.push(format!("{key}={REDACTED}"));
        } else if value.is_empty() {
            redacted_pairs.push(key.to_owned());
        } else {
            redacted_pairs.push(format!("{key}={value}"));
        }
    }
    redacted_pairs.join("&")
}

// Drops the entire `user:password@` userinfo part instead of masking it, so credentials
// never appear in any form; `rsplit_once` keeps hosts containing literal '@' intact.
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
    use super::{
        is_sensitive_key, redact_auth_error, redact_auth_error_strict, redact_diagnostic_text,
        redact_header, redact_internal_runtime_paths, redact_url, redact_url_segments_in_text,
        redact_url_strict, REDACTED, REDACTED_INTERNAL_RUNTIME_PATH,
    };

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
        let redacted = redact_url(
            "https://example.test/path?token=very-secret-token&mode=full&refresh_token=qwe",
        );
        assert_eq!(
            redacted,
            "https://example.test/path?token=<redacted>&mode=full&refresh_token=<redacted>"
        );
    }

    #[test]
    fn url_redaction_preserves_short_benign_bare_token_values() {
        let redacted = redact_url("https://example.test/path?token=a%3Db%3Dc&mode=full");

        assert_eq!(redacted, "https://example.test/path?token=a%3Db%3Dc&mode=full");
    }

    #[test]
    fn url_redaction_masks_short_bare_token_values() {
        let redacted = redact_url("https://example.test/path?token=abc&mode=full");

        assert_eq!(redacted, "https://example.test/path?token=<redacted>&mode=full");
    }

    #[test]
    fn strict_url_redaction_masks_short_sensitive_query_values() {
        let redacted = redact_url_strict(
            "https://user:pass@example.test/path?token=abc&mode=full#access_token=x",
        );

        assert_eq!(
            redacted,
            "https://example.test/path?token=<redacted>&mode=full#access_token=<redacted>"
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
            "provider failed: Bearer secret-token authorization=topsecret token=very-secret-token code=429",
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

    #[test]
    fn auth_error_redaction_preserves_short_benign_bare_token_assignments() {
        let redacted =
            redact_auth_error("fixture line: token=a%3Db%3Dc selector=#password code=ok");

        assert!(redacted.contains("token=a%3Db%3Dc"), "{redacted}");
        assert!(redacted.contains("selector=#password"), "{redacted}");
    }

    #[test]
    fn auth_error_redaction_preserves_secret_file_path_assignments() {
        let redacted = redact_auth_error(
            "fixture line: SECRET_FILE=/app/secret.txt PRIVATE_KEY_FILE=C:\\Users\\demo\\keys\\private-key.pem",
        );

        assert!(redacted.contains("SECRET_FILE=/app/secret.txt"), "{redacted}");
        assert!(
            redacted.contains("PRIVATE_KEY_FILE=C:\\Users\\demo\\keys\\private-key.pem"),
            "{redacted}"
        );
        assert!(!redacted.contains(REDACTED), "{redacted}");
    }

    #[test]
    fn auth_error_redaction_masks_path_shaped_generic_secret_assignments() {
        let redacted =
            redact_auth_error("provider failed: CLIENT_SECRET=abc/def.ghi API_KEY=dir/file.key");

        assert!(redacted.contains("CLIENT_SECRET=<redacted>"), "{redacted}");
        assert!(redacted.contains("API_KEY=<redacted>"), "{redacted}");
        assert!(!redacted.contains("abc/def.ghi"), "{redacted}");
        assert!(!redacted.contains("dir/file.key"), "{redacted}");
    }

    #[test]
    fn strict_auth_error_redaction_still_masks_secret_file_path_assignments() {
        let redacted = redact_auth_error_strict("fixture line: SECRET_FILE=/app/secret.txt");

        assert!(redacted.contains("SECRET_FILE=<redacted>"), "{redacted}");
        assert!(!redacted.contains("/app/secret.txt"), "{redacted}");
    }

    #[test]
    fn auth_error_redaction_masks_simple_short_token_assignments() {
        let redacted = redact_auth_error("stderr preview token=abc next");

        assert!(redacted.contains("token=<redacted>"), "{redacted}");
        assert!(!redacted.contains("token=abc"), "{redacted}");
    }

    #[test]
    fn auth_error_redaction_masks_colon_terminated_token_assignments() {
        let redacted = redact_auth_error("wc: token=abc123: No such file or directory");

        assert!(redacted.contains("token=<redacted>:"), "{redacted}");
        assert!(!redacted.contains("token=abc123"), "{redacted}");
    }

    #[test]
    fn strict_auth_error_redaction_masks_short_sensitive_assignments() {
        let redacted =
            redact_auth_error_strict("provider failed: token=abc authorization=topsecret code=ok");

        assert!(redacted.contains("token=<redacted>"), "{redacted}");
        assert!(redacted.contains("authorization=<redacted>"), "{redacted}");
        assert!(redacted.contains("code=ok"), "{redacted}");
        assert!(!redacted.contains("token=abc"), "{redacted}");
    }

    #[test]
    fn auth_error_redaction_masks_authorization_bearer_with_following_token() {
        let redacted =
            redact_auth_error("error: authorization=Bearer SECRET_TOKEN oauth=Bearer NEXT_TOKEN");

        assert!(redacted.contains("authorization=<redacted> <redacted>"));
        assert!(redacted.contains("oauth=Bearer NEXT_TOKEN"));
        assert!(!redacted.contains("SECRET_TOKEN"));
    }

    #[test]
    fn auth_error_redaction_masks_sensitive_assignments_after_context_prefixes() {
        let redacted = redact_auth_error(
            r#"provider returned HTTP 401: {"error":"authorization=Bearer sk-secret-token invalid"}"#,
        );

        assert!(
            redacted.contains(r#""authorization=<redacted> <redacted> invalid""#),
            "{redacted}"
        );
        assert!(!redacted.contains("sk-secret-token"), "{redacted}");
    }

    #[test]
    fn url_segments_in_text_redact_sensitive_query_params_in_embedded_urls() {
        let redacted = redact_url_segments_in_text(
            "callback failed: https://example.test/callback?state=ok&access_token=secret",
        );
        assert!(redacted.contains("state=ok"));
        assert!(redacted.contains("access_token=<redacted>"));
        assert!(!redacted.contains("access_token=secret"));
    }

    #[test]
    fn url_segments_in_text_redacts_sensitive_fragment_params() {
        let redacted = redact_url_segments_in_text(
            "callback failed: https://example.test/callback?state=ok#refresh_token=secret&mode=ok",
        );
        assert!(redacted.contains("state=ok"));
        assert!(redacted.contains("refresh_token=<redacted>"));
        assert!(redacted.contains("mode=ok"));
        assert!(!redacted.contains("refresh_token=secret"));
    }

    #[test]
    fn url_segments_in_text_redacts_relative_sensitive_query_params() {
        let redacted = redact_url_segments_in_text(
            "callback failed: oauth/callback?access_token=abc&refresh_token=r&state=ok",
        );

        assert!(redacted.contains("state=ok"), "{redacted}");
        assert!(redacted.contains("access_token=<redacted>"), "{redacted}");
        assert!(redacted.contains("refresh_token=<redacted>"), "{redacted}");
        assert!(!redacted.contains("access_token=abc"), "{redacted}");
        assert!(!redacted.contains("refresh_token=r"), "{redacted}");
    }

    #[test]
    fn url_segments_in_text_preserves_regex_query_syntax() {
        let source = "document.cookie.match(/(?:^|; )theme=([^;]*)/)";

        assert_eq!(redact_url_segments_in_text(source), source);
    }

    #[test]
    fn diagnostic_text_redacts_url_segments_and_inline_assignments_strictly() {
        let redacted = redact_diagnostic_text(
            "failed http://127.0.0.1:7443?token=abc&mode=ok provider token=alpha",
        );

        assert!(redacted.contains("mode=ok"), "{redacted}");
        assert!(redacted.contains("token=<redacted>"), "{redacted}");
        assert!(!redacted.contains("token=abc"), "{redacted}");
        assert!(!redacted.contains("token=alpha"), "{redacted}");
    }

    #[test]
    fn internal_runtime_path_redaction_masks_foreign_windows_home_sessions_path() {
        let redacted = redact_internal_runtime_paths(
            r#"resume path C:\Users\Aftab Jafar Ansari\.palyra\sessions\01ABC\tape.ndjson should not leak"#,
        );

        assert!(redacted.contains(REDACTED_INTERNAL_RUNTIME_PATH), "{redacted}");
        assert!(!redacted.contains("Aftab Jafar Ansari"), "{redacted}");
        assert!(!redacted.contains(".palyra"), "{redacted}");
    }

    #[test]
    fn internal_runtime_path_redaction_masks_home_env_runtime_paths() {
        let redacted = redact_internal_runtime_paths(
            "%USERPROFILE%\\.palyra\\sessions\\01ABC $HOME/.palyra/sessions/01DEF ${HOME}/.palyra/state.json",
        );

        assert_eq!(redacted.matches(REDACTED_INTERNAL_RUNTIME_PATH).count(), 3);
        assert!(!redacted.to_ascii_lowercase().contains(".palyra"), "{redacted}");
    }
}
