const SUPERVISOR_TICK_MS: u64 = 500;
const MAX_LOG_LINES_PER_SERVICE: usize = 400;
const LOG_EVENT_CHANNEL_CAPACITY: usize = 2_048;
const MAX_DIAGNOSTIC_ERRORS: usize = 25;
const DASHBOARD_SCHEME: &str = "http";
const LOOPBACK_HOST: &str = "127.0.0.1";
const CONSOLE_PRINCIPAL: &str = "admin:desktop-control-center";
const CONSOLE_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const DESKTOP_STATE_SCHEMA_VERSION: u32 = 2;
const DESKTOP_SECRET_MAX_BYTES: usize = 4_096;
const DESKTOP_SECRET_KEY_ADMIN_TOKEN: &str = "desktop_admin_token";
const DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN: &str = "desktop_browser_auth_token";

const GATEWAY_ADMIN_PORT: u16 = 7142;
const GATEWAY_GRPC_PORT: u16 = 7443;
const GATEWAY_QUIC_PORT: u16 = 7444;
const BROWSER_HEALTH_PORT: u16 = 7143;
const BROWSER_GRPC_PORT: u16 = 7543;

mod commands;
mod desktop_state;
mod openai_auth;
mod snapshot;
mod supervisor;

use snapshot::sanitize_log_line;

pub(crate) use desktop_state::{
    load_or_initialize_state_file, resolve_desktop_state_root, DesktopSecretStore, DesktopStateFile,
};
pub(crate) use supervisor::{
    normalize_optional_text, resolve_binary_path, unix_ms_now, ControlCenter,
    HealthEndpointPayload, LogLine, RuntimeConfig, ServiceKind, ServiceProcessSnapshot,
};

#[cfg(test)]
pub(crate) use reqwest::Client;
#[cfg(test)]
pub(crate) use snapshot::{
    build_snapshot_from_inputs, collect_redacted_errors, parse_discord_status,
    parse_remote_dashboard_base_url, BrowserStatusSnapshot, DashboardAccessMode,
};
#[cfg(test)]
pub(crate) use supervisor::{
    compute_backoff_ms, executable_file_name, try_enqueue_log_event, LogEvent, LogStream,
    ManagedService,
};
#[cfg(test)]
pub(crate) use tokio::sync::mpsc;
#[cfg(test)]
pub(crate) use ulid::Ulid;

#[cfg(test)]
mod tests;

pub fn run() {
    commands::run();
}
