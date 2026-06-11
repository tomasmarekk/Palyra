//! Browser action budgeting, allowlist enforcement, and action-log proto mapping.
//!
//! Every session action passes through [`consume_action_budget_and_snapshot`] before execution
//! and [`finalize_session_action`] afterwards; together they enforce per-session action caps,
//! sliding-window rate limits, and the optional per-session domain allowlist.

use crate::*;

/// Converts an internal action-log entry into its redacted proto form.
///
/// Free-text fields are sanitized and byte-capped; URLs go through query redaction.
pub(crate) fn action_log_entry_to_proto(
    entry: &BrowserActionLogEntryInternal,
) -> browser_v1::BrowserActionLogEntry {
    browser_v1::BrowserActionLogEntry {
        v: CANONICAL_PROTOCOL_MAJOR,
        action_id: entry.action_id.clone(),
        action_name: truncate_utf8_bytes(entry.action_name.as_str(), MAX_INSPECT_ACTION_NAME_BYTES),
        selector: action_log_selector_to_proto(entry.selector.as_str()),
        success: entry.success,
        outcome: sanitize_debug_text(entry.outcome.as_str(), MAX_INSPECT_ACTION_OUTCOME_BYTES),
        error: sanitize_debug_text(entry.error.as_str(), MAX_INSPECT_ACTION_ERROR_BYTES),
        started_at_unix_ms: entry.started_at_unix_ms,
        completed_at_unix_ms: entry.completed_at_unix_ms,
        attempts: entry.attempts,
        page_url: normalize_url_with_redaction(entry.page_url.as_str()),
    }
}

/// Redacts a selector for the action log: URL-shaped selectors (e.g. download hrefs) can carry
/// secrets in query strings, so they get URL redaction instead of plain text sanitization.
fn action_log_selector_to_proto(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if Url::parse(trimmed).is_ok_and(|url| matches!(url.scheme(), "http" | "https" | "file")) {
        let redacted = normalize_url_with_redaction(trimmed);
        return truncate_utf8_bytes(redacted.as_str(), MAX_INSPECT_ACTION_SELECTOR_BYTES);
    }
    sanitize_debug_text(trimmed, MAX_INSPECT_ACTION_SELECTOR_BYTES)
}

/// Summarizes a tab's console log into per-severity counts for the diagnostics proto.
pub(crate) fn page_diagnostics_to_proto(
    tab: &BrowserTabRecord,
) -> browser_v1::BrowserPageDiagnostics {
    let warning_count = tab
        .console_log
        .iter()
        .filter(|entry| entry.severity == BrowserDiagnosticSeverityInternal::Warn)
        .count();
    let error_count = tab
        .console_log
        .iter()
        .filter(|entry| entry.severity == BrowserDiagnosticSeverityInternal::Error)
        .count();
    let last_event_unix_ms =
        tab.console_log.iter().map(|entry| entry.captured_at_unix_ms).max().unwrap_or(0);
    browser_v1::BrowserPageDiagnostics {
        v: CANONICAL_PROTOCOL_MAJOR,
        page_url: normalize_url_with_redaction(tab.last_url.as_deref().unwrap_or_default()),
        page_title: truncate_utf8_bytes(tab.last_title.as_str(), MAX_CONSOLE_SOURCE_BYTES),
        console_entry_count: u32::try_from(tab.console_log.len()).unwrap_or(u32::MAX),
        warning_count: u32::try_from(warning_count).unwrap_or(u32::MAX),
        error_count: u32::try_from(error_count).unwrap_or(u32::MAX),
        last_event_unix_ms,
    }
}

/// Converts the session cookie jar into proto domains with redacted cookie values.
///
/// Domains and cookie names are sorted so output is deterministic across `HashMap` iteration
/// orders; domains with no cookies are omitted.
pub(crate) fn cookie_jar_to_proto(
    cookie_jar: &HashMap<String, HashMap<String, String>>,
) -> Vec<browser_v1::SessionCookieDomain> {
    let mut domains = cookie_jar.iter().collect::<Vec<_>>();
    domains.sort_by(|left, right| left.0.cmp(right.0));
    domains
        .into_iter()
        .filter_map(|(domain, cookies)| {
            let mut entries = cookies.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let cookies = entries
                .into_iter()
                .map(|(name, value)| browser_v1::SessionCookieEntry {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    name: truncate_utf8_bytes(name.as_str(), 128),
                    value: sanitize_debug_map_value(
                        name.as_str(),
                        value.as_str(),
                        MAX_INSPECT_COOKIE_VALUE_BYTES,
                    ),
                })
                .collect::<Vec<_>>();
            if cookies.is_empty() {
                None
            } else {
                Some(browser_v1::SessionCookieDomain {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    domain: truncate_utf8_bytes(domain.as_str(), 256),
                    cookies,
                })
            }
        })
        .collect()
}

/// Converts session storage state into proto origins with redacted values.
///
/// Origins and keys are sorted for deterministic output; origins with no entries are omitted.
pub(crate) fn storage_entries_to_proto(
    storage_entries: &HashMap<String, HashMap<String, String>>,
) -> Vec<browser_v1::SessionStorageOrigin> {
    let mut origins = storage_entries.iter().collect::<Vec<_>>();
    origins.sort_by(|left, right| left.0.cmp(right.0));
    origins
        .into_iter()
        .filter_map(|(origin, entries)| {
            let mut values = entries.iter().collect::<Vec<_>>();
            values.sort_by(|left, right| left.0.cmp(right.0));
            let entries = values
                .into_iter()
                .map(|(key, value)| browser_v1::SessionStorageEntry {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    key: truncate_utf8_bytes(key.as_str(), 256),
                    value: sanitize_debug_map_value(
                        key.as_str(),
                        value.as_str(),
                        MAX_INSPECT_STORAGE_VALUE_BYTES,
                    ),
                })
                .collect::<Vec<_>>();
            if entries.is_empty() {
                None
            } else {
                Some(browser_v1::SessionStorageOrigin {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    origin: truncate_utf8_bytes(origin.as_str(), MAX_NETWORK_LOG_URL_BYTES),
                    entries,
                })
            }
        })
        .collect()
}

// The fixed per-entry/per-domain offsets approximate proto framing overhead; they deliberately
// overestimate so payload truncation errs on the small side.
fn estimate_cookie_payload_bytes(domains: &[browser_v1::SessionCookieDomain]) -> usize {
    domains
        .iter()
        .map(|domain| {
            domain.domain.len()
                + domain
                    .cookies
                    .iter()
                    .map(|cookie| cookie.name.len() + cookie.value.len() + 16)
                    .sum::<usize>()
                + 24
        })
        .sum::<usize>()
        + 2
}

/// Drops cookies from the trailing domain until the estimated payload fits the byte budget.
///
/// Returns `true` if anything was removed.
pub(crate) fn truncate_cookie_payload(
    domains: &mut Vec<browser_v1::SessionCookieDomain>,
    max_payload_bytes: usize,
) -> bool {
    let mut truncated = false;
    while !domains.is_empty()
        && estimate_cookie_payload_bytes(domains.as_slice()) > max_payload_bytes
    {
        if let Some(domain) = domains.last_mut() {
            domain.cookies.pop();
            if domain.cookies.is_empty() {
                domains.pop();
            }
        }
        truncated = true;
    }
    truncated
}

// Same overestimating framing heuristic as estimate_cookie_payload_bytes.
fn estimate_storage_payload_bytes(origins: &[browser_v1::SessionStorageOrigin]) -> usize {
    origins
        .iter()
        .map(|origin| {
            origin.origin.len()
                + origin
                    .entries
                    .iter()
                    .map(|entry| entry.key.len() + entry.value.len() + 16)
                    .sum::<usize>()
                + 24
        })
        .sum::<usize>()
        + 2
}

/// Drops storage entries from the trailing origin until the estimated payload fits the budget.
///
/// Returns `true` if anything was removed.
pub(crate) fn truncate_storage_payload(
    origins: &mut Vec<browser_v1::SessionStorageOrigin>,
    max_payload_bytes: usize,
) -> bool {
    let mut truncated = false;
    while !origins.is_empty()
        && estimate_storage_payload_bytes(origins.as_slice()) > max_payload_bytes
    {
        if let Some(origin) = origins.last_mut() {
            origin.entries.pop();
            if origin.entries.is_empty() {
                origins.pop();
            }
        }
        truncated = true;
    }
    truncated
}

/// Point-in-time copy of the session fields an action handler needs after the sessions lock is
/// released.
#[derive(Debug, Clone)]
pub(crate) struct ActionSessionSnapshot {
    pub(crate) budget: SessionBudget,
    pub(crate) page_body: String,
    pub(crate) allow_downloads: bool,
    pub(crate) current_url: Option<String>,
    pub(crate) allow_private_targets: bool,
    pub(crate) profile_id: Option<String>,
    pub(crate) private_profile: bool,
}

/// Outcome data recorded for a completed (successful or failed) browser action.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FinalizeActionRequest<'a> {
    pub(crate) action_name: &'a str,
    pub(crate) selector: &'a str,
    pub(crate) success: bool,
    pub(crate) outcome: &'a str,
    pub(crate) error: &'a str,
    pub(crate) started_at_unix_ms: u64,
    pub(crate) attempts: u32,
    pub(crate) capture_failure_screenshot: bool,
    pub(crate) max_failure_screenshot_bytes: u64,
}

/// Validates and charges one action against the session's budget, returning a snapshot.
///
/// Enforces the domain allowlist, the per-session action cap, and the sliding-window rate
/// limit, then records the action. In Chromium mode the active tab snapshot is refreshed first
/// so the allowlist check sees the real current URL.
///
/// # Errors
/// Returns an error string when the session or active tab is missing, the allowlist or an
/// action budget/rate limit blocks the action, or (with `require_page_body`) no page has been
/// loaded yet.
pub(crate) async fn consume_action_budget_and_snapshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    require_page_body: bool,
) -> Result<ActionSessionSnapshot, String> {
    if matches!(runtime.engine_mode, BrowserEngineMode::Chromium) {
        // The sessions lock must not be held across the Chromium refresh await: grab the tab
        // id, release, refresh, then re-validate the session below (it may expire meanwhile).
        let active_tab_id = {
            let sessions = runtime.sessions.lock().await;
            let Some(session) = sessions.get(session_id) else {
                return Err("session_not_found".to_owned());
            };
            session.active_tab_id.clone()
        };
        chromium_refresh_tab_snapshot(runtime, session_id, active_tab_id.as_str()).await?;
    }

    let mut sessions = runtime.sessions.lock().await;
    let Some(session) = sessions.get_mut(session_id) else {
        return Err("session_not_found".to_owned());
    };
    session.last_active = Instant::now();
    enforce_action_domain_allowlist(session)?;
    let page_body = session
        .active_tab()
        .map(|tab| tab.last_page_body.clone())
        .ok_or_else(|| "active_tab_not_found".to_owned())?;
    if require_page_body && page_body.trim().is_empty() {
        return Err("navigate must succeed before performing this browser action".to_owned());
    }

    let now = Instant::now();
    let rate_window = Duration::from_millis(session.budget.action_rate_window_ms.max(1));
    while let Some(front) = session.action_window.front().copied() {
        if now.saturating_duration_since(front) > rate_window {
            session.action_window.pop_front();
        } else {
            break;
        }
    }
    if session.action_count >= session.budget.max_actions_per_session {
        return Err(format!(
            "session action budget exceeded ({} >= {})",
            session.action_count, session.budget.max_actions_per_session
        ));
    }
    if session.action_window.len() as u64 >= session.budget.max_actions_per_window {
        return Err(format!(
            "session action rate limit exceeded ({} per {}ms)",
            session.budget.max_actions_per_window, session.budget.action_rate_window_ms
        ));
    }
    session.action_count = session.action_count.saturating_add(1);
    session.action_window.push_back(now);

    Ok(ActionSessionSnapshot {
        budget: session.budget.clone(),
        page_body,
        allow_downloads: session.allow_downloads,
        current_url: session.active_tab().and_then(|tab| tab.last_url.clone()),
        allow_private_targets: session.allow_private_targets,
        profile_id: session.profile_id.clone(),
        private_profile: session.private_profile,
    })
}

/// Checks the active tab's host against the session's action domain allowlist.
///
/// An empty allowlist permits everything. Matching is exact or dot-suffix, so allowing
/// `example.com` also allows `sub.example.com`.
///
/// # Errors
/// Returns an error string when the session has no active URL, the host cannot be resolved,
/// or the host is not allowlisted.
pub(crate) fn enforce_action_domain_allowlist(
    session: &BrowserSessionRecord,
) -> Result<(), String> {
    if session.action_allowed_domains.is_empty() {
        return Ok(());
    }
    let Some(current_url) = session.active_tab().and_then(|tab| tab.last_url.as_deref()) else {
        return Err(
            "action domain allowlist is configured but session has no active URL".to_owned()
        );
    };
    let current_host = Url::parse(current_url)
        .ok()
        .and_then(|url| url.host_str().map(|value| value.to_ascii_lowercase()))
        .ok_or_else(|| "failed to resolve host for action domain allowlist check".to_owned())?;
    if session.action_allowed_domains.iter().any(|domain| {
        current_host == *domain || current_host.ends_with(format!(".{domain}").as_str())
    }) {
        return Ok(());
    }
    Err(format!("current page host '{current_host}' is blocked by action domain allowlist"))
}

/// Normalizes, sorts, and dedupes a raw allowlist; entries that cannot become a valid host are
/// silently dropped.
pub(crate) fn normalize_action_allowed_domains(values: &[String]) -> Vec<String> {
    let mut domains = values
        .iter()
        .filter_map(|value| normalize_single_allowed_domain(value.as_str()))
        .collect::<Vec<_>>();
    domains.sort();
    domains.dedup();
    domains
}

/// Normalizes one allowlist entry to a bare lowercase host, or `None` if it cannot be one.
///
/// Accepts full URLs (the host is extracted) or bare `host[:port][/path]` strings; the result
/// must consist of ASCII alphanumerics, `.`, and `-`.
pub(crate) fn normalize_single_allowed_domain(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let from_url = Url::parse(trimmed).ok().and_then(|url| url.host_str().map(str::to_owned));
    let value = from_url.unwrap_or_else(|| {
        trimmed
            .split('/')
            .next()
            .unwrap_or_default()
            .split(':')
            .next()
            .unwrap_or_default()
            .to_owned()
    });
    let normalized = value.trim().trim_matches('.').to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-') {
        Some(normalized)
    } else {
        None
    }
}

/// Records an action's outcome in the session log and optionally captures a failure screenshot.
///
/// Failures are mirrored into the tab console log. Returns the proto log entry plus screenshot
/// bytes and MIME type (entry is `None` and the rest empty when the session is gone; screenshot
/// fields are empty when none was captured).
pub(crate) async fn finalize_session_action(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    request: FinalizeActionRequest<'_>,
) -> (Option<browser_v1::BrowserActionLogEntry>, Vec<u8>, String) {
    let (action_log, screenshot_max_bytes) = {
        let mut sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id) else {
            return (None, Vec::new(), String::new());
        };
        let entry = BrowserActionLogEntryInternal {
            action_id: Ulid::new().to_string(),
            action_name: request.action_name.to_owned(),
            selector: request.selector.to_owned(),
            success: request.success,
            outcome: request.outcome.to_owned(),
            error: request.error.to_owned(),
            started_at_unix_ms: request.started_at_unix_ms,
            completed_at_unix_ms: current_unix_ms(),
            attempts: request.attempts,
            page_url: session.active_tab().and_then(|tab| tab.last_url.clone()).unwrap_or_default(),
        };
        session.last_active = Instant::now();
        session.action_log.push_back(entry.clone());
        while session.action_log.len() > session.budget.max_action_log_entries {
            session.action_log.pop_front();
        }
        if !request.success {
            let page_url =
                session.active_tab().and_then(|tab| tab.last_url.clone()).unwrap_or_default();
            if let Some(tab) = session.active_tab_mut() {
                append_console_log_entry(
                    tab,
                    BrowserConsoleEntryInternal {
                        severity: BrowserDiagnosticSeverityInternal::Error,
                        kind: "browser_action_failure".to_owned(),
                        message: format!(
                            "{} failed: {}",
                            request.action_name,
                            if request.error.trim().is_empty() {
                                request.outcome
                            } else {
                                request.error
                            }
                        ),
                        captured_at_unix_ms: current_unix_ms(),
                        source: format!("browser.action.{}", request.action_name),
                        stack_trace: String::new(),
                        page_url,
                    },
                    DEFAULT_MAX_CONSOLE_LOG_ENTRIES,
                    DEFAULT_MAX_CONSOLE_LOG_BYTES,
                );
            }
        }
        let screenshot_max_bytes = if !request.success && request.capture_failure_screenshot {
            Some(if request.max_failure_screenshot_bytes == 0 {
                session.budget.max_screenshot_bytes
            } else {
                request.max_failure_screenshot_bytes.min(session.budget.max_screenshot_bytes)
            })
        } else {
            None
        };
        (
            Some(browser_v1::BrowserActionLogEntry {
                v: CANONICAL_PROTOCOL_MAJOR,
                action_id: entry.action_id,
                action_name: entry.action_name,
                selector: entry.selector,
                success: entry.success,
                outcome: entry.outcome,
                error: entry.error,
                started_at_unix_ms: entry.started_at_unix_ms,
                completed_at_unix_ms: entry.completed_at_unix_ms,
                attempts: entry.attempts,
                page_url: entry.page_url,
            }),
            screenshot_max_bytes,
        )
    };
    // Screenshot capture awaits the Chromium engine, so it must run after the sessions lock is
    // released; the byte cap was resolved above while the lock was still held.
    let (failure_screenshot_bytes, failure_screenshot_mime_type) =
        if let Some(max_bytes) = screenshot_max_bytes {
            capture_action_failure_screenshot(runtime, session_id, max_bytes).await
        } else {
            (Vec::new(), String::new())
        };
    (action_log, failure_screenshot_bytes, failure_screenshot_mime_type)
}

// Simulated mode returns a fixed 1x1 PNG so callers exercise the same payload path as Chromium.
async fn capture_action_failure_screenshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    max_bytes: u64,
) -> (Vec<u8>, String) {
    if runtime.engine_mode == BrowserEngineMode::Chromium {
        return match chromium_screenshot(runtime, session_id).await {
            Ok(image_bytes) if (image_bytes.len() as u64) <= max_bytes => {
                (image_bytes, "image/png".to_owned())
            }
            _ => (Vec::new(), String::new()),
        };
    }
    if (ONE_BY_ONE_PNG.len() as u64) <= max_bytes {
        (ONE_BY_ONE_PNG.to_vec(), "image/png".to_owned())
    } else {
        (Vec::new(), String::new())
    }
}
