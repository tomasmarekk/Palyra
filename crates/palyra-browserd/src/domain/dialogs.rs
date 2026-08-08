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
}

/// Fail-closed limits applied to every native browser dialog.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BrowserResilienceProfile {
    pub(crate) dialog_timeout_ms: u64,
    pub(crate) max_dialog_text_bytes: usize,
    pub(crate) max_prompt_response_bytes: usize,
}

impl Default for BrowserResilienceProfile {
    fn default() -> Self {
        Self {
            dialog_timeout_ms: DEFAULT_BROWSER_DIALOG_TIMEOUT_MS,
            max_dialog_text_bytes: MAX_BROWSER_DIALOG_TEXT_BYTES,
            max_prompt_response_bytes: MAX_BROWSER_DIALOG_PROMPT_BYTES,
        }
    }
}

/// Per-tab dialog state fed synchronously by CDP event callbacks.
#[derive(Debug, Default)]
pub(crate) struct ChromiumDialogTracker {
    next_generation: u64,
    pending: Option<BrowserDialogEvent>,
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dialog_tracker_fences_reused_tab_generations_and_redacts_sensitive_text() {
        let mut tracker = ChromiumDialogTracker::default();
        let profile = BrowserResilienceProfile {
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
    fn dialog_action_classifies_inspection_as_read_only() {
        assert!(!BrowserDialogAction::Inspect.mutates_page());
        assert!(BrowserDialogAction::Accept.mutates_page());
        assert!(BrowserDialogAction::Dismiss.mutates_page());
        assert!(BrowserDialogAction::Respond.mutates_page());
    }
}
