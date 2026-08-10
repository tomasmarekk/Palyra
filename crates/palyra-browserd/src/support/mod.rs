//! Browserd inspection, diagnostics, and redaction helpers.
//!
//! All text leaving the daemon through proto responses (URLs, headers, console/network logs,
//! DOM/accessibility snapshots) must pass through the redaction and byte-budget helpers here.

use crate::*;

/// Extracts the trimmed `<title>` content from an HTML body, if present.
pub(crate) fn extract_html_title(body: &str) -> Option<&str> {
    // ASCII lowercasing preserves byte offsets, so indices found in the lowered copy slice the
    // original body correctly.
    let lower = body.to_ascii_lowercase();
    let start = lower.find("<title>")?;
    let end = lower[start + 7..].find("</title>")?;
    Some(body[start + 7..start + 7 + end].trim())
}

/// Like [`truncate_utf8_bytes`], additionally reporting whether truncation happened.
pub(crate) fn truncate_utf8_bytes_with_flag(raw: &str, max_bytes: usize) -> (String, bool) {
    let truncated = truncate_utf8_bytes(raw, max_bytes);
    let was_truncated = truncated.len() < raw.len();
    (truncated, was_truncated)
}

/// Appends entries to a tab's network log, then trims oldest-first to the entry/byte budgets.
pub(crate) fn append_network_log_entries(
    tab: &mut BrowserTabRecord,
    entries: &[NetworkLogEntryInternal],
    max_entries: usize,
    max_bytes: u64,
) {
    let mut total_bytes =
        tab.network_log.iter().map(estimate_network_log_entry_internal_bytes).sum::<usize>();
    for entry in entries {
        total_bytes = total_bytes.saturating_add(estimate_network_log_entry_internal_bytes(entry));
        tab.network_log.push_back(entry.clone());
    }
    trim_network_log_to_budget(
        &mut tab.network_log,
        &mut total_bytes,
        max_entries,
        max_bytes as usize,
    );
}

fn trim_network_log_to_budget(
    network_log: &mut VecDeque<NetworkLogEntryInternal>,
    total_bytes: &mut usize,
    max_entries: usize,
    max_bytes: usize,
) {
    while network_log.len() > max_entries {
        if let Some(entry) = network_log.pop_front() {
            *total_bytes =
                total_bytes.saturating_sub(estimate_network_log_entry_internal_bytes(&entry));
        } else {
            break;
        }
    }
    while *total_bytes > max_bytes {
        if let Some(entry) = network_log.pop_front() {
            *total_bytes =
                total_bytes.saturating_sub(estimate_network_log_entry_internal_bytes(&entry));
        } else {
            break;
        }
    }
}

/// Rough memory footprint of one network log entry; the fixed offsets overestimate per-field
/// overhead so byte budgets err on the small side.
pub(crate) fn estimate_network_log_entry_internal_bytes(entry: &NetworkLogEntryInternal) -> usize {
    let headers_bytes = entry
        .headers
        .iter()
        .map(|header| header.name.len() + header.value.len() + 8)
        .sum::<usize>();
    entry.request_url.len() + entry.timing_bucket.len() + headers_bytes + 64
}

/// Builds a console log from `entries` capped to the entry and byte budgets (oldest dropped
/// first once over the byte budget).
pub(crate) fn clamp_console_log_entries<I>(
    entries: I,
    max_entries: usize,
    max_bytes: u64,
) -> VecDeque<BrowserConsoleEntryInternal>
where
    I: IntoIterator<Item = BrowserConsoleEntryInternal>,
{
    let mut console_log = VecDeque::new();
    let mut total_bytes = 0usize;
    for entry in entries.into_iter().take(max_entries) {
        total_bytes = total_bytes.saturating_add(estimate_console_entry_internal_bytes(&entry));
        console_log.push_back(entry);
    }
    trim_console_log_to_budget(&mut console_log, &mut total_bytes, max_entries, max_bytes as usize);
    console_log
}

fn trim_console_log_to_budget(
    console_log: &mut VecDeque<BrowserConsoleEntryInternal>,
    total_bytes: &mut usize,
    max_entries: usize,
    max_bytes: usize,
) {
    while console_log.len() > max_entries {
        if let Some(entry) = console_log.pop_front() {
            *total_bytes =
                total_bytes.saturating_sub(estimate_console_entry_internal_bytes(&entry));
        } else {
            break;
        }
    }
    while *total_bytes > max_bytes {
        if let Some(entry) = console_log.pop_front() {
            *total_bytes =
                total_bytes.saturating_sub(estimate_console_entry_internal_bytes(&entry));
        } else {
            break;
        }
    }
}

fn estimate_console_entry_internal_bytes(entry: &BrowserConsoleEntryInternal) -> usize {
    entry.kind.len()
        + entry.message.len()
        + entry.source.len()
        + entry.stack_trace.len()
        + entry.page_url.len()
        + 64
}

/// Appends one console entry to a tab, then trims oldest-first to the entry/byte budgets.
pub(crate) fn append_console_log_entry(
    tab: &mut BrowserTabRecord,
    entry: BrowserConsoleEntryInternal,
    max_entries: usize,
    max_bytes: u64,
) {
    let mut total_bytes =
        tab.console_log.iter().map(estimate_console_entry_internal_bytes).sum::<usize>();
    total_bytes = total_bytes.saturating_add(estimate_console_entry_internal_bytes(&entry));
    tab.console_log.push_back(entry);
    trim_console_log_to_budget(
        &mut tab.console_log,
        &mut total_bytes,
        max_entries,
        max_bytes as usize,
    );
}

/// Converts a network log entry into its redacted proto form.
///
/// Header values are re-sanitized on the way out and the request URL is query-redacted;
/// headers are omitted entirely unless `include_headers` is set.
pub(crate) fn network_log_entry_to_proto(
    entry: NetworkLogEntryInternal,
    include_headers: bool,
) -> browser_v1::NetworkLogEntry {
    let headers = if include_headers {
        entry
            .headers
            .into_iter()
            .map(|header| browser_v1::NetworkLogHeader {
                v: CANONICAL_PROTOCOL_MAJOR,
                name: truncate_utf8_bytes(header.name.to_ascii_lowercase().as_str(), 128),
                value: sanitize_single_network_header(
                    header.name.to_ascii_lowercase().as_str(),
                    header.value.as_str(),
                ),
            })
            .collect()
    } else {
        Vec::new()
    };
    browser_v1::NetworkLogEntry {
        v: CANONICAL_PROTOCOL_MAJOR,
        request_url: normalize_url_with_redaction(entry.request_url.as_str()),
        status_code: u32::from(entry.status_code),
        timing_bucket: entry.timing_bucket,
        latency_ms: entry.latency_ms,
        captured_at_unix_ms: entry.captured_at_unix_ms,
        headers,
    }
}

fn estimate_network_log_payload_bytes(entries: &[browser_v1::NetworkLogEntry]) -> usize {
    entries.iter().map(estimate_network_log_proto_entry_bytes).sum::<usize>() + 2
}

fn estimate_network_log_proto_entry_bytes(entry: &browser_v1::NetworkLogEntry) -> usize {
    let headers = entry.headers.iter().map(estimate_network_log_proto_header_bytes).sum::<usize>();
    entry.request_url.len() + entry.timing_bucket.len() + headers + 64
}

fn estimate_network_log_proto_header_bytes(header: &browser_v1::NetworkLogHeader) -> usize {
    header.name.len() + header.value.len() + 8
}

/// Drops the oldest proto network entries until the estimated payload fits the byte budget.
///
/// Returns `true` if anything was removed.
pub(crate) fn truncate_network_log_payload(
    entries: &mut Vec<browser_v1::NetworkLogEntry>,
    max_payload_bytes: usize,
) -> bool {
    let mut truncated = false;
    while !entries.is_empty()
        && estimate_network_log_payload_bytes(entries.as_slice()) > max_payload_bytes
    {
        entries.remove(0);
        truncated = true;
    }
    truncated
}

/// Converts a console entry into its redacted, byte-capped proto form.
pub(crate) fn console_entry_to_proto(
    entry: &BrowserConsoleEntryInternal,
) -> browser_v1::BrowserConsoleEntry {
    browser_v1::BrowserConsoleEntry {
        v: CANONICAL_PROTOCOL_MAJOR,
        severity: entry.severity.to_proto(),
        kind: truncate_utf8_bytes(entry.kind.as_str(), MAX_INSPECT_CONSOLE_KIND_BYTES),
        message: sanitize_debug_text(entry.message.as_str(), MAX_CONSOLE_MESSAGE_BYTES),
        captured_at_unix_ms: entry.captured_at_unix_ms,
        source: sanitize_debug_text(entry.source.as_str(), MAX_CONSOLE_SOURCE_BYTES),
        stack_trace: sanitize_debug_text(entry.stack_trace.as_str(), MAX_CONSOLE_STACK_BYTES),
        page_url: normalize_url_with_redaction(entry.page_url.as_str()),
    }
}

fn estimate_console_log_payload_bytes(entries: &[browser_v1::BrowserConsoleEntry]) -> usize {
    entries.iter().map(estimate_console_log_proto_entry_bytes).sum::<usize>() + 2
}

fn estimate_console_log_proto_entry_bytes(entry: &browser_v1::BrowserConsoleEntry) -> usize {
    entry.kind.len()
        + entry.message.len()
        + entry.source.len()
        + entry.stack_trace.len()
        + entry.page_url.len()
        + 64
}

/// Drops the oldest proto console entries until the estimated payload fits the byte budget.
///
/// Returns `true` if anything was removed.
pub(crate) fn truncate_console_log_payload(
    entries: &mut Vec<browser_v1::BrowserConsoleEntry>,
    max_payload_bytes: usize,
) -> bool {
    let mut truncated = false;
    while !entries.is_empty()
        && estimate_console_log_payload_bytes(entries.as_slice()) > max_payload_bytes
    {
        entries.remove(0);
        truncated = true;
    }
    truncated
}

/// Buckets a request latency into the coarse timing labels used by the network log.
pub(crate) fn timing_bucket_for_latency(latency_ms: u64) -> &'static str {
    if latency_ms <= 100 {
        "lt_100ms"
    } else if latency_ms <= 500 {
        "100_500ms"
    } else if latency_ms <= 2_000 {
        "500ms_2s"
    } else {
        "gt_2s"
    }
}

/// Converts response headers into sanitized log entries, sorted by name and count-capped.
pub(crate) fn sanitize_network_headers(
    headers: &reqwest::header::HeaderMap,
) -> Vec<NetworkLogHeaderInternal> {
    let mut output = headers
        .iter()
        .take(MAX_NETWORK_LOG_HEADER_COUNT)
        .map(|(name, value)| {
            let header_name = name.as_str().to_ascii_lowercase();
            let raw_value = value.to_str().unwrap_or("<non_utf8>");
            let sanitized = sanitize_single_network_header(header_name.as_str(), raw_value);
            NetworkLogHeaderInternal { name: header_name, value: sanitized }
        })
        .collect::<Vec<_>>();
    output.sort_by(|left, right| left.name.cmp(&right.name));
    output
}

/// Sanitizes one header value: URL-shaped values get URL redaction, sensitive names or values
/// are replaced wholesale, and the rest is byte-capped.
pub(crate) fn sanitize_single_network_header(name: &str, raw_value: &str) -> String {
    if name.eq_ignore_ascii_case("location")
        || raw_value.starts_with("http://")
        || raw_value.starts_with("https://")
    {
        return normalize_url_with_redaction(raw_value);
    }
    if is_sensitive_header_name(name) || contains_sensitive_material(raw_value) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw_value, MAX_NETWORK_LOG_HEADER_VALUE_BYTES)
}

fn is_sensitive_header_name(name: &str) -> bool {
    matches!(
        name,
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "x-api-key"
            | "x-auth-token"
            | "x-csrf-token"
    ) || name.contains("token")
        || name.contains("secret")
        || name.contains("password")
}

/// Reports whether free text contains credential-shaped substrings and must be redacted.
pub(crate) fn contains_sensitive_material(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    [
        "bearer ",
        "token=",
        "access_token=",
        "id_token=",
        "refresh_token=",
        "session=",
        "password=",
        "passwd=",
        "secret=",
        "api_key=",
        "apikey=",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn is_sensitive_debug_key(raw_key: &str) -> bool {
    let key = raw_key.trim().to_ascii_lowercase();
    matches!(
        key.as_str(),
        "authorization"
            | "cookie"
            | "csrf"
            | "jwt"
            | "password"
            | "passwd"
            | "secret"
            | "session"
            | "session_id"
            | "set-cookie"
            | "token"
    ) || key.contains("auth")
        || key.contains("cookie")
        || key.contains("password")
        || key.contains("secret")
        || key.contains("session")
        || key.contains("token")
}

/// Sanitizes free-form diagnostic text: redacted wholesale when credential-shaped content is
/// detected, byte-capped otherwise.
pub(crate) fn sanitize_debug_text(raw: &str, max_bytes: usize) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    if contains_sensitive_material(raw) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw, max_bytes)
}

/// Sanitizes a key/value diagnostic pair, also redacting when the key itself looks sensitive
/// (cookie names, storage keys, ...).
pub(crate) fn sanitize_debug_map_value(key: &str, raw_value: &str, max_bytes: usize) -> String {
    if raw_value.trim().is_empty() {
        return String::new();
    }
    if is_sensitive_debug_key(key) || contains_sensitive_material(raw_value) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw_value, max_bytes)
}

/// Normalizes a URL for logging/protos: credentials and fragments are dropped, default ports
/// omitted, sensitive query values redacted, and the result byte-capped.
///
/// Unparseable input still gets best-effort query redaction rather than passing through raw.
pub(crate) fn normalize_url_with_redaction(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(parsed) = Url::parse(trimmed) {
        let Some(host) = parsed.host_str() else {
            return truncate_utf8_bytes(
                redact_query_from_raw(trimmed).as_str(),
                MAX_NETWORK_LOG_URL_BYTES,
            );
        };
        let mut output = format!("{}://{host}", parsed.scheme());
        if let Some(port) = parsed.port() {
            if !is_default_port(parsed.scheme(), port) {
                output.push(':');
                output.push_str(port.to_string().as_str());
            }
        }
        if parsed.path().is_empty() {
            output.push('/');
        } else {
            output.push_str(redact_sensitive_url_path(parsed.path()).as_str());
        }
        if let Some(query) = parsed.query() {
            let redacted = redact_query_pairs(query);
            if !redacted.is_empty() {
                output.push('?');
                output.push_str(redacted.as_str());
            }
        }
        return truncate_utf8_bytes(output.as_str(), MAX_NETWORK_LOG_URL_BYTES);
    }
    truncate_utf8_bytes(redact_query_from_raw(trimmed).as_str(), MAX_NETWORK_LOG_URL_BYTES)
}

fn redact_query_from_raw(raw: &str) -> String {
    let without_fragment = raw.split('#').next().unwrap_or_default();
    let Some((base, query)) = without_fragment.split_once('?') else {
        return redact_sensitive_url_path(without_fragment);
    };
    let base = redact_sensitive_url_path(base);
    let redacted = redact_query_pairs(query);
    if redacted.is_empty() {
        base
    } else {
        format!("{base}?{redacted}")
    }
}

fn redact_sensitive_url_path(path: &str) -> String {
    let mut previous_segment_is_sensitive_marker = false;
    path.split('/')
        .map(|segment| {
            let redact =
                previous_segment_is_sensitive_marker || looks_like_opaque_snapshot_secret(segment);
            previous_segment_is_sensitive_marker = is_sensitive_url_path_marker(segment);
            if redact {
                "<redacted>"
            } else {
                segment
            }
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn is_sensitive_url_path_marker(segment: &str) -> bool {
    matches!(
        segment.trim().to_ascii_lowercase().as_str(),
        "auth"
            | "invite"
            | "magic"
            | "recover"
            | "recovery"
            | "reset"
            | "session"
            | "signed"
            | "signature"
            | "token"
            | "verify"
            | "verification"
    )
}

/// Rebuilds a query string, redacting values whose key is sensitive or whose content looks
/// credential-shaped; remaining values are byte-capped.
pub(crate) fn redact_query_pairs(query: &str) -> String {
    query
        .split('&')
        .filter(|pair| !pair.trim().is_empty())
        .map(|pair| {
            let (raw_key, raw_value_opt) = pair
                .split_once('=')
                .map(|(key, value)| (key.trim(), Some(value)))
                .unwrap_or_else(|| (pair.trim(), None));
            if raw_key.is_empty() {
                return String::new();
            }
            let value = raw_value_opt.unwrap_or_default();
            let sanitized = if is_sensitive_query_key(raw_key)
                || contains_sensitive_material(value)
                || looks_like_opaque_snapshot_secret(value)
            {
                "<redacted>".to_owned()
            } else {
                truncate_utf8_bytes(value, 128)
            };
            if raw_value_opt.is_some() {
                format!("{raw_key}={sanitized}")
            } else {
                raw_key.to_owned()
            }
        })
        .filter(|pair| !pair.is_empty())
        .collect::<Vec<_>>()
        .join("&")
}

/// Renders a numbered, one-line-per-element DOM outline of the page's opening tags.
///
/// Only a fixed allowlist of attributes is emitted, each value sanitized; returns the snapshot
/// plus whether it was truncated to `max_bytes`.
pub(crate) fn build_dom_snapshot(page_body: &str, max_bytes: usize) -> (String, bool) {
    let lines = collect_opening_tags(page_body)
        .iter()
        .enumerate()
        .map(|(index, tag)| build_dom_line(index + 1, tag.as_str()))
        .collect::<Vec<_>>();
    let content = lines.join("\n");
    truncate_utf8_bytes_with_flag(content.as_str(), max_bytes)
}

fn build_dom_line(index: usize, tag: &str) -> String {
    let tag_lower = tag.to_ascii_lowercase();
    let name = html_tag_name(tag_lower.as_str()).unwrap_or("unknown");
    let mut attributes = Vec::new();
    for attr_name in [
        "id",
        "class",
        "name",
        "role",
        "aria-label",
        "type",
        "href",
        "src",
        "action",
        "title",
        "alt",
        "placeholder",
        "value",
        "checked",
        "selected",
    ] {
        let Some(value) = extract_attr_value_case_insensitive(tag, attr_name) else {
            continue;
        };
        let sanitized = sanitize_snapshot_attribute(tag, attr_name, value.as_str());
        if sanitized.is_empty() {
            continue;
        }
        attributes.push(format!("{attr_name}=\"{sanitized}\""));
    }
    if attributes.is_empty() {
        format!("{index:04} <{name}>")
    } else {
        format!("{index:04} <{name} {}>", attributes.join(" "))
    }
}

fn sanitize_snapshot_attribute(_tag: &str, attr_name: &str, raw_value: &str) -> String {
    if raw_value.trim().is_empty() {
        return String::new();
    }
    let lower = attr_name.to_ascii_lowercase();
    if matches!(lower.as_str(), "password" | "token") {
        return "<redacted>".to_owned();
    }
    if lower == "href" || lower == "src" || lower == "action" {
        return normalize_url_with_redaction(raw_value);
    }
    if lower == "value" {
        return "<redacted>".to_owned();
    }
    if snapshot_attribute_value_is_sensitive(raw_value) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw_value, 128)
}

/// Renders a flat accessibility outline (role, name, tag, selector per line) of the page.
///
/// Roles come from explicit `role` attributes or per-tag inference; names prefer ARIA/label
/// attributes over inner text. Returns the snapshot plus whether it was truncated.
pub(crate) fn build_accessibility_tree_snapshot(
    page_body: &str,
    max_bytes: usize,
) -> (String, bool) {
    let max_candidates = max_bytes.saturating_add(1).clamp(1, MAX_ACCESSIBILITY_CANDIDATES);
    let (candidates, candidate_budget_exhausted) =
        collect_accessibility_candidates(page_body, max_candidates);
    let mut lines = Vec::new();
    for (index, candidate) in candidates.iter().enumerate() {
        if let Some(line) = build_accessibility_line(
            index + 1,
            candidate.tag.as_str(),
            candidate.inner_text.as_str(),
        ) {
            lines.push(line);
        }
    }
    let content = lines.join("\n");
    let (content, byte_budget_exhausted) =
        truncate_utf8_bytes_with_flag(content.as_str(), max_bytes);
    (content, candidate_budget_exhausted || byte_budget_exhausted)
}

const MAX_ACCESSIBILITY_CANDIDATES: usize = 4_096;
const MAX_ACCESSIBILITY_INNER_TEXT_SCAN_BYTES: usize = 4_096;

struct AccessibilityCandidate {
    tag: String,
    inner_text: String,
}

fn build_accessibility_line(index: usize, tag: &str, inner_text: &str) -> Option<String> {
    let tag_lower = tag.to_ascii_lowercase();
    let role = accessibility_role_for_tag(tag, tag_lower.as_str())?;
    let tag_name = html_tag_name(tag_lower.as_str()).unwrap_or("unknown");
    let name = accessibility_name_for_tag(tag, inner_text);
    let selector = accessibility_selector_for_tag(tag);
    Some(format!("{index:04} role={role}; name={name}; tag={tag_name}; selector={selector}"))
}

fn accessibility_role_for_tag(tag: &str, tag_lower: &str) -> Option<String> {
    if let Some(explicit_role) = extract_attr_value_case_insensitive(tag, "role")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return Some(if snapshot_attribute_value_is_sensitive(explicit_role.as_str()) {
            "<redacted>".to_owned()
        } else {
            truncate_utf8_bytes(explicit_role.as_str(), 64)
        });
    }
    let tag_name = html_tag_name(tag_lower)?;
    let inferred = match tag_name {
        "a" => "link",
        "button" => "button",
        "textarea" => "textbox",
        "select" => "combobox",
        "img" => "img",
        "form" => "form",
        "nav" => "navigation",
        "main" => "main",
        "header" => "banner",
        "footer" => "contentinfo",
        "ul" | "ol" => "list",
        "li" => "listitem",
        "table" => "table",
        "tr" => "row",
        "td" => "cell",
        "th" => "columnheader",
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => "heading",
        "input" => match extract_attr_value(tag_lower, "type")
            .unwrap_or_else(|| "text".to_owned())
            .as_str()
        {
            "checkbox" => "checkbox",
            "radio" => "radio",
            "submit" | "button" | "reset" => "button",
            "search" | "email" | "url" | "tel" | "text" | "password" => "textbox",
            _ => "input",
        },
        _ => return None,
    };
    Some(inferred.to_owned())
}

fn accessibility_name_for_tag(tag: &str, inner_text: &str) -> String {
    let tag_lower = tag.to_ascii_lowercase();
    let tag_name = html_tag_name(tag_lower.as_str()).unwrap_or("unknown");
    for attr_name in ["aria-label", "title", "alt", "placeholder"] {
        if let Some(value) = extract_attr_value_case_insensitive(tag, attr_name)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
        {
            return accessibility_name_value(value.as_str());
        }
    }
    if tag_name == "input"
        && matches!(
            extract_attr_value(tag_lower.as_str(), "type")
                .unwrap_or_else(|| "text".to_owned())
                .as_str(),
            "submit" | "button" | "reset"
        )
    {
        if let Some(value) = extract_attr_value_case_insensitive(tag, "value")
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
        {
            return accessibility_name_value(value.as_str());
        }
    }
    if accessibility_name_can_use_inner_text(tag_name) && !inner_text.trim().is_empty() {
        return accessibility_name_value(inner_text);
    }
    if matches!(tag_name, "input" | "textarea" | "select" | "form") {
        if let Some(value) = extract_attr_value_case_insensitive(tag, "name")
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
        {
            return accessibility_name_value(value.as_str());
        }
    }
    if let Some(href) = extract_attr_value_case_insensitive(tag, "href")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return normalize_url_with_redaction(href.as_str());
    }
    "-".to_owned()
}

fn accessibility_name_can_use_inner_text(tag_name: &str) -> bool {
    matches!(
        tag_name,
        "a" | "button" | "h1" | "h2" | "h3" | "h4" | "h5" | "h6" | "label" | "option" | "summary"
    )
}

fn accessibility_name_value(value: &str) -> String {
    if snapshot_attribute_value_is_sensitive(value) {
        "<redacted>".to_owned()
    } else {
        truncate_utf8_bytes(value, 128)
    }
}

fn accessibility_selector_for_tag(tag: &str) -> String {
    if let Some(id) = extract_attr_value_case_insensitive(tag, "id")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        if snapshot_attribute_value_is_sensitive(id.as_str()) {
            return "<redacted>".to_owned();
        }
        return format!("#{}", truncate_utf8_bytes(id.as_str(), 96));
    }
    if let Some(name) = extract_attr_value_case_insensitive(tag, "name")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        if snapshot_attribute_value_is_sensitive(name.as_str()) {
            return "<redacted>".to_owned();
        }
        return format!("[name={}]", truncate_utf8_bytes(name.as_str(), 96));
    }
    if let Some(class) = extract_attr_value_case_insensitive(tag, "class")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        let first_class = class.split_ascii_whitespace().next().unwrap_or_default();
        if !first_class.is_empty() {
            if snapshot_attribute_value_is_sensitive(first_class) {
                return "<redacted>".to_owned();
            }
            return format!(".{}", truncate_utf8_bytes(first_class, 96));
        }
    }
    "-".to_owned()
}

fn snapshot_attribute_value_is_sensitive(value: &str) -> bool {
    contains_sensitive_material(value)
        || value
            .split(|character: char| {
                !(character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '%' | '='))
            })
            .any(looks_like_opaque_snapshot_secret)
}

fn looks_like_opaque_snapshot_secret(candidate: &str) -> bool {
    let mut alphanumeric_len = 0usize;
    let mut has_lower = false;
    let mut has_upper = false;
    let mut has_digit = false;
    let mut all_hex = true;
    let mut distinct = 0usize;
    let mut seen_ascii = [false; 128];
    for byte in candidate.bytes().filter(|byte| byte.is_ascii_alphanumeric()) {
        alphanumeric_len += 1;
        has_lower |= byte.is_ascii_lowercase();
        has_upper |= byte.is_ascii_uppercase();
        has_digit |= byte.is_ascii_digit();
        all_hex &= byte.is_ascii_hexdigit();
        let index = usize::from(byte);
        if !seen_ascii[index] {
            seen_ascii[index] = true;
            distinct += 1;
        }
    }
    if alphanumeric_len < 16 {
        return false;
    }
    let mixed_token = has_lower && has_upper && has_digit;
    let long_hex = alphanumeric_len >= 24 && all_hex;
    mixed_token || long_hex || (alphanumeric_len >= 24 && has_lower && has_upper && distinct >= 12)
}

/// Extracts whitespace-collapsed visible text from HTML (scripts, styles, and comments
/// stripped); returns the text plus whether it was truncated to `max_bytes`.
pub(crate) fn build_visible_text_snapshot(page_body: &str, max_bytes: usize) -> (String, bool) {
    let without_scripts = strip_tag_block_case_insensitive(page_body, "script");
    let without_styles = strip_tag_block_case_insensitive(without_scripts.as_str(), "style");
    let without_comments = strip_html_comments(without_styles.as_str());
    let mut visible = String::new();
    let mut inside_tag = false;
    for character in without_comments.chars() {
        if character == '<' {
            inside_tag = true;
            visible.push(' ');
            continue;
        }
        if character == '>' {
            inside_tag = false;
            visible.push(' ');
            continue;
        }
        if !inside_tag {
            visible.push(character);
        }
    }
    let collapsed = visible.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_utf8_bytes_with_flag(collapsed.as_str(), max_bytes)
}

fn collect_accessibility_candidates(
    html: &str,
    max_candidates: usize,
) -> (Vec<AccessibilityCandidate>, bool) {
    let mut candidates = Vec::new();
    let lower = html.to_ascii_lowercase();
    let mut cursor = 0usize;
    while let Some(rel_start) = html[cursor..].find('<') {
        let start = cursor + rel_start;
        let Some(rel_end) = html[start..].find('>') else {
            break;
        };
        let end = start + rel_end;
        let tag = &html[start..=end];
        if tag.starts_with("</") || tag.starts_with("<!") || tag.starts_with("<?") {
            cursor = end.saturating_add(1);
            continue;
        }
        let tag_lower = tag.to_ascii_lowercase();
        let Some(tag_name) = html_tag_name(tag_lower.as_str()) else {
            cursor = end.saturating_add(1);
            continue;
        };
        if matches!(tag_name, "script" | "style") {
            cursor = end.saturating_add(1);
            continue;
        }
        if accessibility_role_for_tag(tag, tag_lower.as_str()).is_none() {
            cursor = end.saturating_add(1);
            continue;
        }
        if candidates.len() >= max_candidates {
            return (candidates, true);
        }
        let inner_text = if accessibility_name_can_use_inner_text(tag_name) {
            accessibility_inner_text_for_tag(html, lower.as_str(), end + 1, tag_name)
        } else {
            String::new()
        };
        candidates.push(AccessibilityCandidate { tag: tag.to_owned(), inner_text });
        cursor = end.saturating_add(1);
    }
    (candidates, false)
}

fn accessibility_inner_text_for_tag(
    html: &str,
    lower_html: &str,
    content_start: usize,
    tag_name: &str,
) -> String {
    if content_start >= html.len() {
        return String::new();
    }
    let close_pattern = format!("</{tag_name}>");
    let mut scan_end =
        content_start.saturating_add(MAX_ACCESSIBILITY_INNER_TEXT_SCAN_BYTES).min(html.len());
    while !html.is_char_boundary(scan_end) {
        scan_end -= 1;
    }
    let close = lower_html[content_start..scan_end]
        .find(close_pattern.as_str())
        .map_or(scan_end, |rel_close| content_start + rel_close);
    let (text, _) = build_visible_text_snapshot(&html[content_start..close], 128);
    text
}

fn strip_tag_block_case_insensitive(input: &str, tag_name: &str) -> String {
    let mut output = String::new();
    let lower = input.to_ascii_lowercase();
    let open_pattern = format!("<{tag_name}");
    let close_pattern = format!("</{tag_name}>");
    let mut cursor = 0usize;
    while let Some(rel_open) = lower[cursor..].find(open_pattern.as_str()) {
        let open = cursor + rel_open;
        output.push_str(&input[cursor..open]);
        let Some(rel_close) = lower[open..].find(close_pattern.as_str()) else {
            cursor = input.len();
            break;
        };
        let close_start = open + rel_close;
        cursor = close_start + close_pattern.len();
    }
    if cursor < input.len() {
        output.push_str(&input[cursor..]);
    }
    output
}

fn strip_html_comments(input: &str) -> String {
    let mut output = String::new();
    let mut cursor = 0usize;
    while let Some(rel_start) = input[cursor..].find("<!--") {
        let start = cursor + rel_start;
        output.push_str(&input[cursor..start]);
        let Some(rel_end) = input[start + 4..].find("-->") else {
            cursor = input.len();
            break;
        };
        cursor = start + 4 + rel_end + 3;
    }
    if cursor < input.len() {
        output.push_str(&input[cursor..]);
    }
    output
}

fn collect_opening_tags(html: &str) -> Vec<String> {
    let mut tags = Vec::new();
    let mut cursor = 0usize;
    while let Some(rel_start) = html[cursor..].find('<') {
        let start = cursor + rel_start;
        let Some(rel_end) = html[start..].find('>') else {
            break;
        };
        let end = start + rel_end;
        let tag = &html[start..=end];
        if tag.starts_with("</") || tag.starts_with("<!") || tag.starts_with("<?") {
            cursor = end.saturating_add(1);
            continue;
        }
        let tag_lower = tag.to_ascii_lowercase();
        if matches!(html_tag_name(tag_lower.as_str()), Some("script" | "style")) {
            cursor = end.saturating_add(1);
            continue;
        }
        tags.push(tag.to_owned());
        cursor = end.saturating_add(1);
    }
    tags
}

#[cfg(test)]
mod tests {
    use super::{
        build_accessibility_tree_snapshot, build_dom_snapshot, build_visible_text_snapshot,
        normalize_url_with_redaction, MAX_ACCESSIBILITY_CANDIDATES,
    };

    #[test]
    fn visible_text_snapshot_extracts_page_text() {
        let html = r#"<html><body><h1>Palyra Browser Visible Text</h1><p>Fixture paragraph visible to the browser snapshot.</p><button>Click me</button></body></html>"#;

        let (visible_text, truncated) = build_visible_text_snapshot(html, 4096);

        assert!(!truncated);
        assert_eq!(
            visible_text,
            "Palyra Browser Visible Text Fixture paragraph visible to the browser snapshot. Click me"
        );
    }

    #[test]
    fn accessibility_tree_prefers_visible_text_over_ids_for_common_controls() {
        let html = r#"<html><body><h1 id="main-title">Palyra Browser Visible Text</h1><button id="action">Click me</button></body></html>"#;

        let (accessibility_tree, truncated) = build_accessibility_tree_snapshot(html, 4096);

        assert!(!truncated);
        assert!(
            accessibility_tree.contains(
                "role=heading; name=Palyra Browser Visible Text; tag=h1; selector=#main-title"
            ),
            "{accessibility_tree}"
        );
        assert!(
            accessibility_tree.contains("role=button; name=Click me; tag=button; selector=#action"),
            "{accessibility_tree}"
        );
        assert!(!accessibility_tree.contains("name=main-title"), "{accessibility_tree}");
        assert!(!accessibility_tree.contains("name=action"), "{accessibility_tree}");
    }

    #[test]
    fn snapshots_redact_exact_case_opaque_attribute_values() {
        let secret = "Qx7Vn2Lm9Pk4Rt8Ws3Yz6Aa1";
        let html = format!(
            r#"<a id="{secret}" aria-label="{secret}" href="/reset/{secret}">Reset account</a>"#
        );

        let (dom_snapshot, dom_truncated) = build_dom_snapshot(html.as_str(), 4096);
        let (accessibility_tree, accessibility_truncated) =
            build_accessibility_tree_snapshot(html.as_str(), 4096);

        assert!(!dom_truncated);
        assert!(!accessibility_truncated);
        assert!(!dom_snapshot.contains(secret), "{dom_snapshot}");
        assert!(!accessibility_tree.contains(secret), "{accessibility_tree}");
        assert!(dom_snapshot.contains("<redacted>"), "{dom_snapshot}");
        assert!(accessibility_tree.contains("<redacted>"), "{accessibility_tree}");
    }

    #[test]
    fn url_redaction_masks_sensitive_and_opaque_path_segments() {
        let secret = "Qx7Vn2Lm9Pk4Rt8Ws3Yz6Aa1";
        let redacted = normalize_url_with_redaction(
            format!("https://example.test/reset/{secret}/done?mode=ok").as_str(),
        );

        assert!(!redacted.contains(secret), "{redacted}");
        assert_eq!(redacted, "https://example.test/reset/<redacted>/done?mode=ok");
    }

    #[test]
    fn accessibility_tree_skips_deep_non_accessible_markup_before_extracting_names() {
        let depth = 20_000;
        let html =
            format!("{}<button>Ready</button>{}", "<div>".repeat(depth), "</div>".repeat(depth));

        let (accessibility_tree, truncated) = build_accessibility_tree_snapshot(&html, 4096);

        assert!(!truncated);
        assert!(accessibility_tree.contains("role=button; name=Ready"), "{accessibility_tree}");
    }

    #[test]
    fn accessibility_tree_caps_candidate_processing() {
        let html = "<button>Ready</button>".repeat(MAX_ACCESSIBILITY_CANDIDATES + 1);

        let (accessibility_tree, truncated) = build_accessibility_tree_snapshot(&html, usize::MAX);

        assert!(truncated);
        assert_eq!(accessibility_tree.lines().count(), MAX_ACCESSIBILITY_CANDIDATES);
    }
}
