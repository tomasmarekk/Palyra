//! In-memory console session and payload caches for the desktop app.
//!
//! These caches reduce repeated control-plane calls during companion refreshes;
//! callers own freshness policy and decide when cached payloads are acceptable.

use palyra_control_plane as control_plane;
use serde_json::Value;

/// Cached console session with CSRF metadata.
#[derive(Debug, Clone)]
pub(crate) struct ConsoleSessionCache {
    pub(crate) session: control_plane::ConsoleSession,
}

/// Cached arbitrary console JSON payload with fetch timestamp.
#[derive(Debug, Clone, Default)]
pub(crate) struct CachedConsolePayload {
    pub(crate) payload: Option<Value>,
    pub(crate) fetched_at_unix_ms: Option<i64>,
}

/// Companion-specific payload cache buckets.
#[derive(Debug, Clone, Default)]
pub(crate) struct DesktopCompanionPayloadCache {
    pub(crate) session_catalog: CachedConsolePayload,
    pub(crate) approvals: CachedConsolePayload,
    pub(crate) inventory: CachedConsolePayload,
}

/// Shared cache for console diagnostics and companion payloads.
#[derive(Debug, Clone, Default)]
pub(crate) struct ConsolePayloadCache {
    pub(crate) diagnostics: CachedConsolePayload,
    pub(crate) discord: CachedConsolePayload,
    pub(crate) companion: DesktopCompanionPayloadCache,
}
