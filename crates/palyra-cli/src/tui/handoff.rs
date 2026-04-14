use super::percent_encode_component;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TuiCrossSurfaceHandoff {
    pub(crate) section: String,
    pub(crate) session_id: Option<String>,
    pub(crate) run_id: Option<String>,
    pub(crate) device_id: Option<String>,
    pub(crate) objective_id: Option<String>,
    pub(crate) canvas_id: Option<String>,
    pub(crate) intent: Option<String>,
    pub(crate) source: Option<String>,
}

impl Default for TuiCrossSurfaceHandoff {
    fn default() -> Self {
        Self {
            section: "overview".to_owned(),
            session_id: None,
            run_id: None,
            device_id: None,
            objective_id: None,
            canvas_id: None,
            intent: None,
            source: None,
        }
    }
}

const HANDOFF_PARAM_ORDER: &[(&str, fn(&TuiCrossSurfaceHandoff) -> Option<&str>)] = &[
    ("sessionId", |payload| payload.session_id.as_deref()),
    ("runId", |payload| payload.run_id.as_deref()),
    ("deviceId", |payload| payload.device_id.as_deref()),
    ("objectiveId", |payload| payload.objective_id.as_deref()),
    ("canvasId", |payload| payload.canvas_id.as_deref()),
    ("intent", |payload| payload.intent.as_deref()),
    ("source", |payload| payload.source.as_deref()),
];

pub(crate) fn build_console_handoff_path(payload: &TuiCrossSurfaceHandoff) -> String {
    let base_path = match normalize_section(payload.section.as_str()) {
        "chat" => "/#/chat",
        "approvals" => "/#/control/approvals",
        "access" => "/#/settings/access",
        "browser" => "/#/browser",
        "onboarding" => "/#/settings/profiles",
        "overview" | "home" => "/#/control/overview",
        other if other.starts_with('/') => other,
        other => return format!("/#/control/{other}"),
    };
    let mut params = Vec::new();
    for (key, resolve) in HANDOFF_PARAM_ORDER {
        if let Some(value) = resolve(payload).and_then(normalize_value) {
            params.push(format!("{key}={}", percent_encode_component(value)));
        }
    }
    if params.is_empty() {
        base_path.to_owned()
    } else {
        format!("{base_path}?{}", params.join("&"))
    }
}

#[cfg(test)]
pub(crate) fn parse_console_handoff(raw: &str) -> TuiCrossSurfaceHandoff {
    let candidate = raw.split_once("/#").map(|(_, fragment)| fragment).unwrap_or(raw).trim();
    let (path, query) = candidate.split_once('?').unwrap_or((candidate, ""));
    let mut handoff = TuiCrossSurfaceHandoff {
        section: section_from_path(path).unwrap_or("overview").to_owned(),
        ..TuiCrossSurfaceHandoff::default()
    };
    for pair in query.split('&').filter(|value| !value.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let value = normalize_value(value).map(percent_decode_component);
        match key {
            "section" => {
                if let Some(value) = value {
                    handoff.section = normalize_section(value.as_str()).to_owned();
                }
            }
            "sessionId" => handoff.session_id = value,
            "runId" => handoff.run_id = value,
            "deviceId" => handoff.device_id = value,
            "objectiveId" => handoff.objective_id = value,
            "canvasId" => handoff.canvas_id = value,
            "intent" => handoff.intent = value,
            "source" => handoff.source = value,
            _ => {}
        }
    }
    handoff
}

fn normalize_section(raw: &str) -> &str {
    normalize_value(raw).unwrap_or("overview")
}

fn normalize_value(raw: &str) -> Option<&str> {
    let trimmed = raw.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

#[cfg(test)]
fn section_from_path(path: &str) -> Option<&'static str> {
    match path {
        "/#/chat" => Some("chat"),
        "/#/browser" => Some("browser"),
        "/#/control/approvals" => Some("approvals"),
        "/#/settings/access" => Some("access"),
        "/#/settings/profiles" => Some("onboarding"),
        "/#/control/overview" => Some("overview"),
        "/chat" => Some("chat"),
        "/browser" => Some("browser"),
        "/control/approvals" => Some("approvals"),
        "/settings/access" => Some("access"),
        "/settings/profiles" => Some("onboarding"),
        "/control/overview" | "/" => Some("overview"),
        _ => None,
    }
}

#[cfg(test)]
fn percent_decode_component(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = String::with_capacity(value.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let high = bytes[index + 1];
            let low = bytes[index + 2];
            let hex = [high, low];
            if let Ok(hex) = std::str::from_utf8(&hex) {
                if let Ok(value) = u8::from_str_radix(hex, 16) {
                    decoded.push(char::from(value));
                    index += 3;
                    continue;
                }
            }
        }
        decoded.push(char::from(bytes[index]));
        index += 1;
    }
    decoded
}

#[cfg(test)]
mod tests {
    use super::{build_console_handoff_path, parse_console_handoff, TuiCrossSurfaceHandoff};

    #[test]
    fn build_console_handoff_path_preserves_phase1_context_order() {
        let payload = TuiCrossSurfaceHandoff {
            section: "browser".to_owned(),
            session_id: Some("session-1".to_owned()),
            run_id: Some("run-1".to_owned()),
            device_id: Some("device-1".to_owned()),
            objective_id: Some("objective-1".to_owned()),
            canvas_id: Some("canvas-1".to_owned()),
            intent: Some("reopen_canvas".to_owned()),
            source: Some("tui".to_owned()),
        };
        assert_eq!(
            build_console_handoff_path(&payload),
            "/#/browser?sessionId=session-1&runId=run-1&deviceId=device-1&objectiveId=objective-1&canvasId=canvas-1&intent=reopen_canvas&source=tui"
        );
    }

    #[test]
    fn parse_console_handoff_round_trips_browser_context() {
        let payload = parse_console_handoff(
            "/#/browser?sessionId=session-1&runId=run-1&canvasId=canvas-1&intent=reopen_canvas&source=tui",
        );
        assert_eq!(payload.section, "browser");
        assert_eq!(payload.session_id.as_deref(), Some("session-1"));
        assert_eq!(payload.run_id.as_deref(), Some("run-1"));
        assert_eq!(payload.canvas_id.as_deref(), Some("canvas-1"));
        assert_eq!(payload.intent.as_deref(), Some("reopen_canvas"));
        assert_eq!(payload.source.as_deref(), Some("tui"));
    }
}
