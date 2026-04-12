//! Browserd inspection, diagnostics, and redaction helpers.

use crate::*;

pub(crate) fn extract_html_title(body: &str) -> Option<&str> {
    let lower = body.to_ascii_lowercase();
    let start = lower.find("<title>")?;
    let end = lower[start + 7..].find("</title>")?;
    Some(body[start + 7..start + 7 + end].trim())
}

pub(crate) fn truncate_utf8_bytes_with_flag(raw: &str, max_bytes: usize) -> (String, bool) {
    let truncated = truncate_utf8_bytes(raw, max_bytes);
    let was_truncated = truncated.len() < raw.len();
    (truncated, was_truncated)
}

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

pub(crate) fn clamp_network_log_entries<I>(
    entries: I,
    max_entries: usize,
    max_bytes: u64,
) -> VecDeque<NetworkLogEntryInternal>
where
    I: IntoIterator<Item = NetworkLogEntryInternal>,
{
    let mut network_log = VecDeque::new();
    let mut total_bytes = 0usize;
    for entry in entries.into_iter().take(max_entries) {
        total_bytes = total_bytes.saturating_add(estimate_network_log_entry_internal_bytes(&entry));
        network_log.push_back(entry);
    }
    trim_network_log_to_budget(&mut network_log, &mut total_bytes, max_entries, max_bytes as usize);
    network_log
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

pub(crate) fn estimate_network_log_entry_internal_bytes(entry: &NetworkLogEntryInternal) -> usize {
    let headers_bytes = entry
        .headers
        .iter()
        .map(|header| header.name.len() + header.value.len() + 8)
        .sum::<usize>();
    entry.request_url.len() + entry.timing_bucket.len() + headers_bytes + 64
}

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

pub(crate) fn sanitize_debug_text(raw: &str, max_bytes: usize) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    if contains_sensitive_material(raw) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw, max_bytes)
}

pub(crate) fn sanitize_debug_map_value(key: &str, raw_value: &str, max_bytes: usize) -> String {
    if raw_value.trim().is_empty() {
        return String::new();
    }
    if is_sensitive_debug_key(key) || contains_sensitive_material(raw_value) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw_value, max_bytes)
}

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
            output.push_str(parsed.path());
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
        return without_fragment.to_owned();
    };
    let redacted = redact_query_pairs(query);
    if redacted.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{redacted}")
    }
}

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
            let sanitized = if is_sensitive_query_key(raw_key) || contains_sensitive_material(value)
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
    ] {
        let Some(value) = extract_attr_value(tag_lower.as_str(), attr_name) else {
            continue;
        };
        let sanitized = sanitize_snapshot_attribute(attr_name, value.as_str());
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

fn sanitize_snapshot_attribute(attr_name: &str, raw_value: &str) -> String {
    if raw_value.trim().is_empty() {
        return String::new();
    }
    let lower = attr_name.to_ascii_lowercase();
    if matches!(lower.as_str(), "value" | "password" | "token") {
        return "<redacted>".to_owned();
    }
    if lower == "href" || lower == "src" || lower == "action" {
        return normalize_url_with_redaction(raw_value);
    }
    if contains_sensitive_material(raw_value) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw_value, 128)
}

pub(crate) fn build_accessibility_tree_snapshot(
    page_body: &str,
    max_bytes: usize,
) -> (String, bool) {
    let mut lines = Vec::new();
    for (index, tag) in collect_opening_tags(page_body).iter().enumerate() {
        if let Some(line) = build_accessibility_line(index + 1, tag.as_str()) {
            lines.push(line);
        }
    }
    let content = lines.join("\n");
    truncate_utf8_bytes_with_flag(content.as_str(), max_bytes)
}

fn build_accessibility_line(index: usize, tag: &str) -> Option<String> {
    let tag_lower = tag.to_ascii_lowercase();
    let role = accessibility_role_for_tag(tag_lower.as_str())?;
    let tag_name = html_tag_name(tag_lower.as_str()).unwrap_or("unknown");
    let name = accessibility_name_for_tag(tag_lower.as_str());
    let selector = accessibility_selector_for_tag(tag_lower.as_str());
    Some(format!("{index:04} role={role}; name={name}; tag={tag_name}; selector={selector}"))
}

fn accessibility_role_for_tag(tag_lower: &str) -> Option<String> {
    if let Some(explicit_role) = extract_attr_value(tag_lower, "role")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return Some(truncate_utf8_bytes(explicit_role.as_str(), 64));
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

fn accessibility_name_for_tag(tag_lower: &str) -> String {
    for attr_name in ["aria-label", "title", "alt", "placeholder", "name", "id"] {
        if let Some(value) = extract_attr_value(tag_lower, attr_name)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
        {
            if contains_sensitive_material(value.as_str()) {
                return "<redacted>".to_owned();
            }
            return truncate_utf8_bytes(value.as_str(), 128);
        }
    }
    if let Some(href) = extract_attr_value(tag_lower, "href")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return normalize_url_with_redaction(href.as_str());
    }
    "-".to_owned()
}

fn accessibility_selector_for_tag(tag_lower: &str) -> String {
    if let Some(id) = extract_attr_value(tag_lower, "id")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return format!("#{}", truncate_utf8_bytes(id.as_str(), 96));
    }
    if let Some(name) = extract_attr_value(tag_lower, "name")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return format!("[name={}]", truncate_utf8_bytes(name.as_str(), 96));
    }
    if let Some(class) = extract_attr_value(tag_lower, "class")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        let first_class = class.split_ascii_whitespace().next().unwrap_or_default();
        if !first_class.is_empty() {
            return format!(".{}", truncate_utf8_bytes(first_class, 96));
        }
    }
    "-".to_owned()
}

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
