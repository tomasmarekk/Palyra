//! Explicit browser-session health and bounded resilience counters.
//!
//! The tracker survives Chromium runtime replacement so reconnect evidence is
//! not lost with the process or target that caused it.

use crate::*;

pub(crate) const BROWSER_SESSION_READY_REASON: &str = "browser.session.ready";
pub(crate) const BROWSER_PROCESS_RECONNECT_REASON: &str = "browser.process.reconnected";
pub(crate) const BROWSER_TARGET_RECONNECT_REASON: &str = "browser.target.reconnected";
pub(crate) const BROWSER_RECONNECTING_REASON: &str = "browser.runtime.reconnecting";
pub(crate) const BROWSER_RECONNECT_DISABLED_REASON: &str = "browser.runtime.reconnect_disabled";
pub(crate) const BROWSER_RECONNECT_FAILED_REASON: &str = "browser.runtime.reconnect_failed";
pub(crate) const BROWSER_DIALOG_TIMEOUT_REASON: &str = "browser.dialog.timeout_safe_dismiss";
pub(crate) const BROWSER_DIALOG_TIMEOUT_FAILED_REASON: &str =
    "browser.dialog.timeout_dismiss_failed";
pub(crate) const BROWSER_DIALOG_NAVIGATION_CLEANUP_REASON: &str =
    "browser.dialog.navigation_cleanup";
pub(crate) const BROWSER_DIALOG_CLOSE_CLEANUP_REASON: &str = "browser.dialog.page_close_cleanup";

/// Operator-visible health states for one logical browser session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BrowserSessionHealthState {
    Ready,
    Degraded,
    Reconnecting,
    Blocked,
}

impl BrowserSessionHealthState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Degraded => "degraded",
            Self::Reconnecting => "reconnecting",
            Self::Blocked => "blocked",
        }
    }
}

/// Read-only projection used by health and inspect responses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BrowserSessionHealthSnapshot {
    pub(crate) state: BrowserSessionHealthState,
    pub(crate) runtime_generation: u64,
    pub(crate) process_reconnect_count: u64,
    pub(crate) target_reconnect_count: u64,
    pub(crate) dialog_timeout_count: u64,
    pub(crate) dialog_navigation_cleanup_count: u64,
    pub(crate) dialog_close_cleanup_count: u64,
    pub(crate) reason_code: String,
    pub(crate) updated_at_unix_ms: u64,
}

/// Mutable health evidence retained independently from a Chromium process.
#[derive(Debug)]
pub(crate) struct BrowserSessionHealth {
    snapshot: BrowserSessionHealthSnapshot,
}

impl Default for BrowserSessionHealth {
    fn default() -> Self {
        Self {
            snapshot: BrowserSessionHealthSnapshot {
                state: BrowserSessionHealthState::Ready,
                runtime_generation: 0,
                process_reconnect_count: 0,
                target_reconnect_count: 0,
                dialog_timeout_count: 0,
                dialog_navigation_cleanup_count: 0,
                dialog_close_cleanup_count: 0,
                reason_code: BROWSER_SESSION_READY_REASON.to_owned(),
                updated_at_unix_ms: current_unix_ms(),
            },
        }
    }
}

impl BrowserSessionHealth {
    pub(crate) fn snapshot(&self) -> BrowserSessionHealthSnapshot {
        self.snapshot.clone()
    }

    pub(crate) fn mark_initial_ready(&mut self) {
        self.snapshot.runtime_generation = self.snapshot.runtime_generation.max(1);
        self.set_state(BrowserSessionHealthState::Ready, BROWSER_SESSION_READY_REASON);
    }

    pub(crate) fn mark_reconnecting(&mut self) {
        self.set_state(BrowserSessionHealthState::Reconnecting, BROWSER_RECONNECTING_REASON);
    }

    pub(crate) fn mark_process_reconnected(&mut self) {
        self.snapshot.runtime_generation =
            self.snapshot.runtime_generation.saturating_add(1).max(1);
        self.snapshot.process_reconnect_count =
            self.snapshot.process_reconnect_count.saturating_add(1);
        self.set_state(BrowserSessionHealthState::Ready, BROWSER_PROCESS_RECONNECT_REASON);
    }

    pub(crate) fn mark_target_reconnected(&mut self) {
        self.snapshot.target_reconnect_count =
            self.snapshot.target_reconnect_count.saturating_add(1);
        self.set_state(BrowserSessionHealthState::Ready, BROWSER_TARGET_RECONNECT_REASON);
    }

    pub(crate) fn mark_reconnect_disabled(&mut self) {
        self.set_state(BrowserSessionHealthState::Blocked, BROWSER_RECONNECT_DISABLED_REASON);
    }

    pub(crate) fn mark_reconnect_failed(&mut self) {
        self.set_state(BrowserSessionHealthState::Blocked, BROWSER_RECONNECT_FAILED_REASON);
    }

    pub(crate) fn record_dialog_timeout(&mut self, dismissed: bool) {
        self.snapshot.dialog_timeout_count = self.snapshot.dialog_timeout_count.saturating_add(1);
        if dismissed {
            self.set_state(BrowserSessionHealthState::Ready, BROWSER_DIALOG_TIMEOUT_REASON);
        } else {
            self.set_state(
                BrowserSessionHealthState::Degraded,
                BROWSER_DIALOG_TIMEOUT_FAILED_REASON,
            );
        }
    }

    pub(crate) fn record_dialog_navigation_cleanup(&mut self) {
        self.snapshot.dialog_navigation_cleanup_count =
            self.snapshot.dialog_navigation_cleanup_count.saturating_add(1);
        self.set_state(BrowserSessionHealthState::Ready, BROWSER_DIALOG_NAVIGATION_CLEANUP_REASON);
    }

    pub(crate) fn record_dialog_close_cleanup(&mut self) {
        self.snapshot.dialog_close_cleanup_count =
            self.snapshot.dialog_close_cleanup_count.saturating_add(1);
        self.set_state(BrowserSessionHealthState::Ready, BROWSER_DIALOG_CLOSE_CLEANUP_REASON);
    }

    fn set_state(&mut self, state: BrowserSessionHealthState, reason_code: &str) {
        self.snapshot.state = state;
        self.snapshot.reason_code = reason_code.to_owned();
        self.snapshot.updated_at_unix_ms = current_unix_ms();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_health_retains_reconnect_and_dialog_evidence() {
        let mut health = BrowserSessionHealth::default();
        health.mark_initial_ready();
        health.mark_reconnecting();
        health.mark_process_reconnected();
        health.mark_target_reconnected();
        health.record_dialog_timeout(true);
        health.record_dialog_navigation_cleanup();
        health.record_dialog_close_cleanup();

        let snapshot = health.snapshot();
        assert_eq!(snapshot.state, BrowserSessionHealthState::Ready);
        assert_eq!(snapshot.runtime_generation, 2);
        assert_eq!(snapshot.process_reconnect_count, 1);
        assert_eq!(snapshot.target_reconnect_count, 1);
        assert_eq!(snapshot.dialog_timeout_count, 1);
        assert_eq!(snapshot.dialog_navigation_cleanup_count, 1);
        assert_eq!(snapshot.dialog_close_cleanup_count, 1);
        assert_eq!(snapshot.reason_code, BROWSER_DIALOG_CLOSE_CLEANUP_REASON);
    }

    #[test]
    fn failed_timeout_and_disabled_reconnect_are_explicit_health_states() {
        let mut health = BrowserSessionHealth::default();
        health.record_dialog_timeout(false);
        assert_eq!(health.snapshot().state, BrowserSessionHealthState::Degraded);
        assert_eq!(health.snapshot().reason_code, BROWSER_DIALOG_TIMEOUT_FAILED_REASON);

        health.mark_reconnect_disabled();
        assert_eq!(health.snapshot().state, BrowserSessionHealthState::Blocked);
        assert_eq!(health.snapshot().reason_code, BROWSER_RECONNECT_DISABLED_REASON);
    }
}
