//! Bounded browser-dialog contracts shared by the CDP engine and gRPC layer.
//!
//! Dialog state is generation-fenced because a delayed model response must
//! never acknowledge a newer prompt that reused the same tab.

use crate::*;

pub(crate) const MAX_BROWSER_DIALOG_TEXT_BYTES: usize = 2 * 1024;
pub(crate) const MAX_BROWSER_DIALOG_PROMPT_BYTES: usize = 4 * 1024;
pub(crate) const DEFAULT_BROWSER_DIALOG_TIMEOUT_MS: u64 = 30_000;

/// One native JavaScript dialog observed on a live Chromium tab.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrowserDialogEvent {
    pub(crate) generation: u64,
    pub(crate) tab_id: String,
    pub(crate) dialog_type: String,
    pub(crate) message: String,
    pub(crate) default_prompt: String,
    pub(crate) page_url: String,
    pub(crate) opened_at_unix_ms: u64,
    pub(crate) expires_at_unix_ms: u64,
}

/// Supported dialog operations at the browser trust boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserDialogAction {
    Inspect,
    Accept,
    Dismiss,
    Respond,
}

impl BrowserDialogAction {
    pub(crate) fn mutates_page(self) -> bool {
        !matches!(self, Self::Inspect)
    }

    pub(crate) fn action_log_name(self) -> &'static str {
        match self {
            Self::Inspect => "dialog_inspect",
            Self::Accept => "dialog_accept",
            Self::Dismiss => "dialog_dismiss",
            Self::Respond => "dialog_respond",
        }
    }
}

/// Fail-closed limits applied to every native browser dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BrowserResilienceProfile {
    pub(crate) automatic_reconnect: bool,
    pub(crate) dialog_timeout_ms: u64,
    pub(crate) max_dialog_text_bytes: usize,
    pub(crate) max_prompt_response_bytes: usize,
}

impl Default for BrowserResilienceProfile {
    fn default() -> Self {
        Self {
            automatic_reconnect: false,
            dialog_timeout_ms: DEFAULT_BROWSER_DIALOG_TIMEOUT_MS,
            max_dialog_text_bytes: MAX_BROWSER_DIALOG_TEXT_BYTES,
            max_prompt_response_bytes: MAX_BROWSER_DIALOG_PROMPT_BYTES,
        }
    }
}

impl BrowserResilienceProfile {
    /// Resolves the separate browser resilience rollout from its operator-owned env value.
    ///
    /// # Errors
    /// Returns an error for any non-empty value other than `disabled` or `resilient`.
    pub(crate) fn from_env() -> Result<Self> {
        let Some(value) = std::env::var(BROWSER_RESILIENCE_PROFILE_ENV)
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .filter(|value| !value.is_empty())
        else {
            return Ok(Self::default());
        };
        match value.as_str() {
            "disabled" => Ok(Self::default()),
            "resilient" => Ok(Self { automatic_reconnect: true, ..Self::default() }),
            _ => {
                anyhow::bail!("{BROWSER_RESILIENCE_PROFILE_ENV} must be 'disabled' or 'resilient'")
            }
        }
    }

    pub(crate) fn name(self) -> &'static str {
        if self.automatic_reconnect {
            "resilient"
        } else {
            "disabled"
        }
    }

    #[cfg(test)]
    pub(crate) fn resilient_for_tests() -> Self {
        Self { automatic_reconnect: true, ..Self::default() }
    }
}

/// Why a pending dialog stopped being actionable without an explicit caller response.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserDialogResolutionKind {
    TimedOut,
    NavigationCleanup,
    PageCloseCleanup,
}

/// Most recent automatically resolved dialog, retained for generation-fenced diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrowserDialogResolution {
    pub(crate) event: BrowserDialogEvent,
    pub(crate) kind: BrowserDialogResolutionKind,
}

/// Per-tab dialog state fed synchronously by CDP event callbacks.
#[derive(Debug, Default)]
pub(crate) struct ChromiumDialogTracker {
    next_generation: u64,
    pending: Option<BrowserDialogEvent>,
    last_resolution: Option<BrowserDialogResolution>,
}

impl ChromiumDialogTracker {
    pub(crate) fn record_opening(
        &mut self,
        tab_id: &str,
        dialog_type: &str,
        message: &str,
        default_prompt: &str,
        page_url: &str,
        profile: BrowserResilienceProfile,
    ) -> BrowserDialogEvent {
        self.next_generation = self.next_generation.saturating_add(1).max(1);
        let opened_at_unix_ms = current_unix_ms();
        let event = BrowserDialogEvent {
            generation: self.next_generation,
            tab_id: tab_id.to_owned(),
            dialog_type: truncate_utf8_bytes(dialog_type, profile.max_dialog_text_bytes),
            message: sanitize_debug_text(message, profile.max_dialog_text_bytes),
            default_prompt: sanitize_debug_text(default_prompt, profile.max_dialog_text_bytes),
            page_url: normalize_url_with_redaction(page_url),
            opened_at_unix_ms,
            expires_at_unix_ms: opened_at_unix_ms.saturating_add(profile.dialog_timeout_ms),
        };
        self.pending = Some(event.clone());
        self.last_resolution = None;
        event
    }

    pub(crate) fn pending(&self) -> Option<BrowserDialogEvent> {
        self.pending.clone()
    }

    pub(crate) fn is_pending_generation(&self, generation: u64) -> bool {
        self.pending.as_ref().is_some_and(|event| event.generation == generation)
    }

    pub(crate) fn clear(&mut self) {
        self.pending = None;
    }

    pub(crate) fn clear_generation(&mut self, generation: u64) -> bool {
        if !self.is_pending_generation(generation) {
            return false;
        }
        self.pending = None;
        true
    }

    pub(crate) fn resolution_for_generation(
        &self,
        generation: u64,
    ) -> Option<BrowserDialogResolution> {
        self.last_resolution
            .as_ref()
            .filter(|resolution| resolution.event.generation == generation)
            .cloned()
    }

    pub(crate) fn remember_resolution(
        &mut self,
        event: BrowserDialogEvent,
        kind: BrowserDialogResolutionKind,
    ) {
        if self.pending.as_ref().is_some_and(|pending| pending.generation == event.generation) {
            self.pending = None;
        }
        self.last_resolution = Some(BrowserDialogResolution { event, kind });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialog_tracker_fences_reused_tab_generations_and_redacts_sensitive_text() {
        let mut tracker = ChromiumDialogTracker::default();
        let profile = BrowserResilienceProfile {
            automatic_reconnect: false,
            dialog_timeout_ms: 250,
            max_dialog_text_bytes: 64,
            max_prompt_response_bytes: 64,
        };
        let first = tracker.record_opening(
            "tab-1",
            "prompt",
            "authorization: Bearer top-secret",
            "safe default",
            "https://example.test/dialog?token=secret",
            profile,
        );
        let second = tracker.record_opening(
            "tab-1",
            "confirm",
            "continue?",
            "",
            "https://example.test/next",
            profile,
        );

        assert_eq!(first.message, "<redacted>");
        assert!(first.page_url.contains("<redacted>"));
        assert!(second.generation > first.generation);
        assert!(!tracker.clear_generation(first.generation));
        assert!(tracker.clear_generation(second.generation));
        assert!(tracker.pending().is_none());
    }

    #[test]
    fn dialog_tracker_retains_automatic_resolution_for_the_exact_generation() {
        let mut tracker = ChromiumDialogTracker::default();
        let event = tracker.record_opening(
            "tab-1",
            "alert",
            "continue?",
            "",
            "https://example.test/dialog",
            BrowserResilienceProfile::default(),
        );

        let resolved = tracker.pending().expect("pending dialog should resolve");
        tracker.remember_resolution(resolved.clone(), BrowserDialogResolutionKind::TimedOut);
        assert_eq!(resolved.generation, event.generation);
        assert_eq!(
            tracker
                .resolution_for_generation(event.generation)
                .expect("resolution should remain inspectable")
                .kind,
            BrowserDialogResolutionKind::TimedOut
        );
        assert!(tracker.resolution_for_generation(event.generation + 1).is_none());
    }

    #[test]
    fn dialog_action_classifies_inspection_as_read_only() {
        assert!(!BrowserDialogAction::Inspect.mutates_page());
        assert!(BrowserDialogAction::Accept.mutates_page());
        assert!(BrowserDialogAction::Dismiss.mutates_page());
        assert!(BrowserDialogAction::Respond.mutates_page());
        assert_eq!(BrowserDialogAction::Inspect.action_log_name(), "dialog_inspect");
        assert_eq!(BrowserDialogAction::Respond.action_log_name(), "dialog_respond");
    }
}
