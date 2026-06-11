//! Channel-platform defaults: sibling media-store paths derived from the
//! connector database location, the default connector inventory, and small
//! shared helpers (payload budget, wall-clock milliseconds).

use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use palyra_connectors::{
    providers::default_instance_specs, ConnectorInstanceSpec, ConnectorSupervisorConfig,
};

/// Returns the media database path next to the connector database.
pub(super) fn media_db_path_from_connector_db_path(connector_db_path: &std::path::Path) -> PathBuf {
    let parent = connector_db_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    parent.join("media.sqlite3")
}

/// Returns the media content directory next to the connector database.
pub(super) fn media_content_root_from_connector_db_path(
    connector_db_path: &std::path::Path,
) -> PathBuf {
    let parent = connector_db_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    parent.join("media")
}

/// Connector inventory registered on first start (Discord-first runtime).
pub(super) fn default_connector_specs() -> Vec<ConnectorInstanceSpec> {
    default_instance_specs()
}

/// Payload budget advertised on RouteMessage envelopes: the connector
/// outbound body limit, so replies are chunked to what the connector can
/// actually deliver.
pub(super) fn route_message_max_payload_bytes(config: &ConnectorSupervisorConfig) -> u64 {
    u64::try_from(config.max_outbound_body_bytes).unwrap_or(u64::MAX)
}

/// Wall-clock unix milliseconds, saturating instead of failing on clock
/// anomalies.
pub(super) fn unix_ms_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().try_into().unwrap_or(i64::MAX))
        .unwrap_or_default()
}
