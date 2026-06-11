//! User-visible TUI status strings with optional pseudo-localization.
//!
//! Central string table for status lines, transcript entries, and popup hints
//! so tests can pin exact wording in one place. The `qps-ploc` locale wraps
//! and lengthens strings to surface truncation issues without translating.

/// Locale applied to every user-visible string produced by this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TuiLocale {
    En,
    QpsPloc,
}

impl TuiLocale {
    /// Canonical locale tag, e.g. for UX telemetry payloads.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::En => "en",
            Self::QpsPloc => "qps-ploc",
        }
    }
}

/// Resolves the TUI locale from `PALYRA_TUI_LOCALE`, defaulting to English.
pub(crate) fn resolve_tui_locale() -> TuiLocale {
    match std::env::var("PALYRA_TUI_LOCALE")
        .ok()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "qps-ploc" | "en-xa" => TuiLocale::QpsPloc,
        _ => TuiLocale::En,
    }
}

/// Status line shown once the TUI is connected.
pub(crate) fn connected(locale: TuiLocale) -> String {
    render(locale, "Connected")
}

/// Transcript entry recorded when the session bootstraps.
pub(crate) fn connected_entry(locale: TuiLocale) -> String {
    render(locale, "Connected.")
}

/// Status line when the model catalog cannot be fetched at startup.
pub(crate) fn connected_model_catalog_unavailable(locale: TuiLocale, error: &str) -> String {
    render(locale, format!("Connected; model catalog unavailable: {error}").as_str())
}

/// Status line when the slash-palette entity catalogs cannot be fetched at startup.
pub(crate) fn connected_slash_catalog_unavailable(locale: TuiLocale, error: &str) -> String {
    render(locale, format!("Connected; slash catalogs unavailable: {error}").as_str())
}

/// Status line when an active run stream finishes cleanly.
pub(crate) fn run_completed(locale: TuiLocale) -> String {
    render(locale, "Run completed")
}

/// Status line when an interrupt-redirect prompt is about to start.
pub(crate) fn starting_redirected_prompt(locale: TuiLocale) -> String {
    render(locale, "Starting redirected prompt")
}

/// Status line when the final assistant token arrives.
pub(crate) fn assistant_response_completed(locale: TuiLocale) -> String {
    render(locale, "Assistant response completed")
}

/// Status line when a tool approval request arrives for `tool_name`.
pub(crate) fn approval_required(locale: TuiLocale, tool_name: &str) -> String {
    render(locale, format!("Approval required for {tool_name}").as_str())
}

/// Risk line shown inside the approval popup.
pub(crate) fn approval_risk(locale: TuiLocale, risk_level: &str) -> String {
    render(locale, format!("Risk: {risk_level}").as_str())
}

/// Policy-explanation line shown inside the approval popup.
pub(crate) fn approval_policy(locale: TuiLocale, explanation: &str) -> String {
    render(locale, format!("Why: {explanation}").as_str())
}

/// Approval-popup hint pointing repeat approvals at the web console.
pub(crate) fn approval_manage_posture_hint(locale: TuiLocale) -> String {
    render(
        locale,
        "If this repeats, review Tool Permissions in the web console before widening posture",
    )
}

/// Approval-popup key hint for allowing the action once.
pub(crate) fn approval_allow_once_hint(locale: TuiLocale) -> String {
    render(locale, "y / Enter = allow once")
}

/// Approval-popup key hint for denying the action.
pub(crate) fn approval_deny_hint(locale: TuiLocale) -> String {
    render(locale, "n / Esc   = deny")
}

/// Approval-popup body when the pending request disappeared.
pub(crate) fn approval_request_unavailable(locale: TuiLocale) -> String {
    render(locale, "Approval request is no longer available.")
}

/// Status line when a new prompt is rejected because a run is streaming.
pub(crate) fn run_already_in_progress(locale: TuiLocale) -> String {
    render(locale, "A run is already in progress")
}

/// Status line while a freshly submitted prompt is starting.
pub(crate) fn running_prompt(locale: TuiLocale) -> String {
    render(locale, "Running prompt")
}

/// Status line when a `!` shell request has no command text.
pub(crate) fn shell_command_empty(locale: TuiLocale) -> String {
    render(locale, "Shell command is empty")
}

/// Status line when the strict profile posture blocks local shell use.
pub(crate) fn local_shell_blocked(locale: TuiLocale) -> String {
    render(locale, "Local shell is blocked by strict profile posture")
}

/// Status line when a shell request is parked behind the opt-in confirmation.
pub(crate) fn local_shell_requires_opt_in(locale: TuiLocale) -> String {
    render(locale, "Local shell requires explicit opt-in")
}

/// Status line after a local shell command completes with `exit_code`.
pub(crate) fn shell_finished(locale: TuiLocale, exit_code: Option<i32>) -> String {
    let exit = exit_code.map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned());
    render(locale, format!("Shell finished with {exit}").as_str())
}

/// Status line after `/status` re-renders the summary.
pub(crate) fn status_refreshed(locale: TuiLocale) -> String {
    render(locale, "Status refreshed")
}

/// Status line after `/usage` re-renders the usage summary.
pub(crate) fn usage_summary_refreshed(locale: TuiLocale) -> String {
    render(locale, "Usage summary refreshed")
}

/// Status line after switching to another session.
pub(crate) fn session_switched(locale: TuiLocale) -> String {
    render(locale, "Session switched")
}

/// Status line after `/reset` clears the session history.
pub(crate) fn session_reset(locale: TuiLocale) -> String {
    render(locale, "Session reset")
}

/// Status line after a tool approval is granted once.
pub(crate) fn approval_granted_once(locale: TuiLocale) -> String {
    render(locale, "Approval granted once")
}

/// Status line after a tool approval is denied.
pub(crate) fn approval_denied(locale: TuiLocale) -> String {
    render(locale, "Approval denied")
}

/// Status line after the shell opt-in confirmation is declined.
pub(crate) fn local_shell_remains_disabled(locale: TuiLocale) -> String {
    render(locale, "Local shell remains disabled")
}

/// Status line after the shell opt-in confirmation is accepted.
pub(crate) fn local_shell_enabled_for_session(locale: TuiLocale) -> String {
    render(locale, "Local shell enabled for this TUI session")
}

/// Status line while the shell opt-in confirmation popup is open.
pub(crate) fn confirm_local_shell_opt_in(locale: TuiLocale) -> String {
    render(locale, "Confirm local shell opt-in")
}

/// Status line after `/shell on` enables the local shell.
pub(crate) fn local_shell_enabled(locale: TuiLocale) -> String {
    render(locale, "Local shell enabled")
}

/// Status line after `/shell off` disables the local shell.
pub(crate) fn local_shell_disabled(locale: TuiLocale) -> String {
    render(locale, "Local shell disabled")
}

fn render(locale: TuiLocale, raw: &str) -> String {
    match locale {
        TuiLocale::En => raw.to_owned(),
        TuiLocale::QpsPloc => pseudo_localize(raw),
    }
}

fn pseudo_localize(raw: &str) -> String {
    // Doubling vowels lengthens every string so layout truncation bugs show
    // up in pseudo-locale runs; the brackets mark unlocalized leakage.
    let expanded = raw.replace(['a', 'e', 'i', 'o', 'u'], "aa");
    format!("[~ {expanded} ~]")
}

#[cfg(test)]
mod tests {
    use super::{approval_required, connected, TuiLocale};

    #[test]
    fn english_locale_keeps_shell_statuses_plain() {
        assert_eq!(connected(TuiLocale::En), "Connected");
    }

    #[test]
    fn pseudo_locale_expands_visible_shell_strings() {
        assert!(connected(TuiLocale::QpsPloc).starts_with("[~ "));
        assert!(approval_required(TuiLocale::QpsPloc, "shell").starts_with("[~ "));
    }
}
