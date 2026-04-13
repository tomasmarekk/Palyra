use palyra_control_plane as control_plane;
use serde_json::Value;

#[derive(Debug, Clone)]
pub(crate) struct ConsoleSessionCache {
    pub(crate) session: control_plane::ConsoleSession,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct CachedConsolePayload {
    pub(crate) payload: Option<Value>,
    pub(crate) fetched_at_unix_ms: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DesktopCompanionPayloadCache {
    pub(crate) session_catalog: CachedConsolePayload,
    pub(crate) approvals: CachedConsolePayload,
    pub(crate) inventory: CachedConsolePayload,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ConsolePayloadCache {
    pub(crate) diagnostics: CachedConsolePayload,
    pub(crate) discord: CachedConsolePayload,
    pub(crate) companion: DesktopCompanionPayloadCache,
}
