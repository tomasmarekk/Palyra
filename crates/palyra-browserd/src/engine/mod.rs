//! Browser engine internals for browserd.
//!
//! Hosts the Chromium engine module and the per-session live state shared with
//! the app layer: the browser process handle, tabs, diagnostics buffers, and
//! the session-scoped egress proxy.

pub(crate) mod chromium;

use crate::*;
pub(crate) use chromium::*;

/// Live Chromium state for one browser session.
///
/// `_profile_dir` and `_proxy` are held for their `Drop` side effects: the
/// temporary profile directory is deleted and the per-session SOCKS5 proxy
/// task is shut down when the session state is dropped.
pub(crate) struct ChromiumSessionState {
    pub(crate) browser: Arc<HeadlessBrowser>,
    /// Live tabs keyed by tab ID.
    pub(crate) tabs: HashMap<String, Arc<HeadlessTab>>,
    /// Per-tab network log buffers fed by CDP response handlers.
    pub(crate) network_logs:
        HashMap<String, Arc<std::sync::Mutex<VecDeque<NetworkLogEntryInternal>>>>,
    pub(crate) private_target_policy: Arc<ChromiumPrivateTargetPolicy>,
    /// First remote-IP guard incident, if any; consuming it terminates the session.
    pub(crate) security_incident: Arc<std::sync::Mutex<Option<String>>>,
    pub(crate) _profile_dir: TempDir,
    pub(crate) _proxy: Option<ChromiumSessionProxy>,
}
