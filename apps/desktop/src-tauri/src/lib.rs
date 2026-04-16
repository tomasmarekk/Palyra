const SUPERVISOR_TICK_MS: u64 = 500;
const MAX_LOG_LINES_PER_SERVICE: usize = 400;
const LOG_EVENT_CHANNEL_CAPACITY: usize = 2_048;
const MAX_DIAGNOSTIC_ERRORS: usize = 25;
const DASHBOARD_SCHEME: &str = "http";
const LOOPBACK_HOST: &str = "127.0.0.1";
const CONSOLE_PRINCIPAL: &str = "admin:desktop-control-center";
const CONSOLE_DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const DESKTOP_STATE_SCHEMA_VERSION: u32 = 7;
const DESKTOP_SECRET_MAX_BYTES: usize = 4_096;
const DESKTOP_SECRET_KEY_ADMIN_TOKEN: &str = "desktop_admin_token";
const DESKTOP_SECRET_KEY_BROWSER_AUTH_TOKEN: &str = "desktop_browser_auth_token";

use tokio::process::Command;

const GATEWAY_ADMIN_PORT: u16 = 7142;
const GATEWAY_GRPC_PORT: u16 = 7443;
const GATEWAY_QUIC_PORT: u16 = 7444;
const BROWSER_HEALTH_PORT: u16 = 7143;
const BROWSER_GRPC_PORT: u16 = 7543;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

mod ambient;
mod commands;
mod companion;
mod companion_console;
mod console_cache;
mod desktop_state;
mod dashboard_open;
mod features;
mod onboarding;
mod openai_auth;
mod profile_registry;
mod snapshot;
mod supervisor;

use snapshot::sanitize_log_line;

pub(crate) use desktop_state::{
    bootstrap_portable_install_environment, load_or_initialize_state_file, load_runtime_secrets,
    migrate_legacy_runtime_secrets_from_state_file, resolve_desktop_state_root,
    validate_runtime_state_root_override, DesktopCompanionSection, DesktopCompanionSurfaceMode,
    DesktopOnboardingStep, DesktopSecretStore, DesktopStateFile,
};
#[cfg(test)]
pub(crate) use desktop_state::bootstrap_portable_install_environment_for_executable;
pub(crate) use features::onboarding::connectors::discord::DiscordOnboardingRequest;
pub(crate) use onboarding::{build_desktop_refresh_payload, build_onboarding_status};
pub(crate) use supervisor::{
    normalize_optional_text, resolve_binary_path, unix_ms_now, ControlCenter,
    HealthEndpointPayload, LogLine, RuntimeConfig, ServiceKind, ServiceProcessSnapshot,
};

#[cfg(test)]
pub(crate) use reqwest::Client;
#[cfg(test)]
pub(crate) use commands::prepare_control_center_for_launch;
#[cfg(test)]
pub(crate) use snapshot::{
    build_snapshot_from_inputs, collect_redacted_errors, parse_discord_status,
    parse_remote_dashboard_base_url, BrowserStatusSnapshot, DashboardAccessMode,
};
#[cfg(test)]
pub(crate) use supervisor::{
    compute_backoff_ms, executable_file_name, try_enqueue_log_event, DesktopInstanceLock,
    LogEvent, LogStream, ManagedService,
};

pub(crate) fn configure_background_command(command: &mut Command) {
    #[cfg(windows)]
    {
        command.creation_flags(CREATE_NO_WINDOW);
    }

    #[cfg(not(windows))]
    {
        let _ = command;
    }
}
#[cfg(test)]
pub(crate) use tokio::sync::mpsc;
#[cfg(test)]
pub(crate) use ulid::Ulid;

#[cfg(test)]
mod tests;

pub fn run() {
    commands::run();
}
