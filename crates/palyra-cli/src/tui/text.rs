#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TuiLocale {
    En,
    QpsPloc,
}

impl TuiLocale {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::En => "en",
            Self::QpsPloc => "qps-ploc",
        }
    }
}

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

pub(crate) fn connected(locale: TuiLocale) -> String {
    render(locale, "Connected")
}

pub(crate) fn connected_entry(locale: TuiLocale) -> String {
    render(locale, "Connected.")
}

pub(crate) fn connected_model_catalog_unavailable(locale: TuiLocale, error: &str) -> String {
    render(locale, format!("Connected; model catalog unavailable: {error}").as_str())
}

pub(crate) fn connected_slash_catalog_unavailable(locale: TuiLocale, error: &str) -> String {
    render(locale, format!("Connected; slash catalogs unavailable: {error}").as_str())
}

pub(crate) fn run_completed(locale: TuiLocale) -> String {
    render(locale, "Run completed")
}

pub(crate) fn starting_redirected_prompt(locale: TuiLocale) -> String {
    render(locale, "Starting redirected prompt")
}

pub(crate) fn assistant_response_completed(locale: TuiLocale) -> String {
    render(locale, "Assistant response completed")
}

pub(crate) fn approval_required(locale: TuiLocale, tool_name: &str) -> String {
    render(locale, format!("Approval required for {tool_name}").as_str())
}

pub(crate) fn run_already_in_progress(locale: TuiLocale) -> String {
    render(locale, "A run is already in progress")
}

pub(crate) fn running_prompt(locale: TuiLocale) -> String {
    render(locale, "Running prompt")
}

pub(crate) fn shell_command_empty(locale: TuiLocale) -> String {
    render(locale, "Shell command is empty")
}

pub(crate) fn local_shell_blocked(locale: TuiLocale) -> String {
    render(locale, "Local shell is blocked by strict profile posture")
}

pub(crate) fn local_shell_requires_opt_in(locale: TuiLocale) -> String {
    render(locale, "Local shell requires explicit opt-in")
}

pub(crate) fn shell_finished(locale: TuiLocale, exit_code: Option<i32>) -> String {
    render(
        locale,
        format!(
            "Shell finished with {}",
            exit_code.map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned())
        )
        .as_str(),
    )
}

pub(crate) fn status_refreshed(locale: TuiLocale) -> String {
    render(locale, "Status refreshed")
}

pub(crate) fn usage_summary_refreshed(locale: TuiLocale) -> String {
    render(locale, "Usage summary refreshed")
}

pub(crate) fn session_switched(locale: TuiLocale) -> String {
    render(locale, "Session switched")
}

pub(crate) fn session_reset(locale: TuiLocale) -> String {
    render(locale, "Session reset")
}

pub(crate) fn approval_granted_once(locale: TuiLocale) -> String {
    render(locale, "Approval granted once")
}

pub(crate) fn approval_denied(locale: TuiLocale) -> String {
    render(locale, "Approval denied")
}

pub(crate) fn local_shell_remains_disabled(locale: TuiLocale) -> String {
    render(locale, "Local shell remains disabled")
}

pub(crate) fn local_shell_enabled_for_session(locale: TuiLocale) -> String {
    render(locale, "Local shell enabled for this TUI session")
}

pub(crate) fn confirm_local_shell_opt_in(locale: TuiLocale) -> String {
    render(locale, "Confirm local shell opt-in")
}

pub(crate) fn local_shell_enabled(locale: TuiLocale) -> String {
    render(locale, "Local shell enabled")
}

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
