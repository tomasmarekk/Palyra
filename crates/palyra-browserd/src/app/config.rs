use crate::*;

pub(crate) const DEFAULT_GRPC_PORT: u16 = 7543;
pub(crate) const DEFAULT_SESSION_IDLE_TTL_MS: u64 = 10 * 60 * 1_000;
pub(crate) const DEFAULT_MAX_SESSIONS: usize = 128;
pub(crate) const DEFAULT_MAX_NAVIGATION_TIMEOUT_MS: u64 = 15_000;
pub(crate) const DEFAULT_MAX_SESSION_LIFETIME_MS: u64 = 30 * 60 * 1_000;
pub(crate) const DEFAULT_MAX_SCREENSHOT_BYTES: u64 = 256 * 1024;
pub(crate) const DEFAULT_MAX_RESPONSE_BYTES: u64 = 512 * 1024;
pub(crate) const DEFAULT_MAX_TITLE_BYTES: u64 = 4 * 1024;
pub(crate) const DEFAULT_MAX_ACTION_TIMEOUT_MS: u64 = 5_000;
pub(crate) const DEFAULT_MAX_TYPE_INPUT_BYTES: u64 = 4 * 1024;
pub(crate) const DEFAULT_MAX_ACTIONS_PER_SESSION: u64 = 256;
pub(crate) const DEFAULT_MAX_ACTIONS_PER_WINDOW: u64 = 20;
pub(crate) const DEFAULT_ACTION_RATE_WINDOW_MS: u64 = 1_000;
pub(crate) const DEFAULT_MAX_ACTION_LOG_ENTRIES: usize = 256;
pub(crate) const DEFAULT_MAX_OBSERVE_SNAPSHOT_BYTES: u64 = 64 * 1024;
pub(crate) const DEFAULT_MAX_VISIBLE_TEXT_BYTES: u64 = 16 * 1024;
pub(crate) const DEFAULT_MAX_NETWORK_LOG_ENTRIES: usize = 256;
pub(crate) const DEFAULT_MAX_NETWORK_LOG_BYTES: u64 = 64 * 1024;
pub(crate) const DEFAULT_MAX_CONSOLE_LOG_ENTRIES: usize = 256;
pub(crate) const DEFAULT_MAX_CONSOLE_LOG_BYTES: u64 = 32 * 1024;
pub(crate) const DEFAULT_MAX_INSPECT_COOKIE_BYTES: u64 = 8 * 1024;
pub(crate) const DEFAULT_MAX_INSPECT_STORAGE_BYTES: u64 = 16 * 1024;
pub(crate) const DEFAULT_MAX_TABS_PER_SESSION: usize = 32;
pub(crate) const MAX_NETWORK_LOG_HEADER_COUNT: usize = 24;
pub(crate) const MAX_NETWORK_LOG_HEADER_VALUE_BYTES: usize = 256;
pub(crate) const MAX_NETWORK_LOG_URL_BYTES: usize = 2 * 1024;
pub(crate) const MAX_CONSOLE_MESSAGE_BYTES: usize = 1_024;
pub(crate) const MAX_CONSOLE_SOURCE_BYTES: usize = 256;
pub(crate) const MAX_CONSOLE_STACK_BYTES: usize = 1_024;
pub(crate) const MAX_INSPECT_COOKIE_VALUE_BYTES: usize = 512;
pub(crate) const MAX_INSPECT_STORAGE_VALUE_BYTES: usize = 1_024;
pub(crate) const MAX_INSPECT_ACTION_NAME_BYTES: usize = 64;
pub(crate) const MAX_INSPECT_ACTION_SELECTOR_BYTES: usize = 256;
pub(crate) const MAX_INSPECT_ACTION_OUTCOME_BYTES: usize = 256;
pub(crate) const MAX_INSPECT_ACTION_ERROR_BYTES: usize = 512;
pub(crate) const MAX_INSPECT_CONSOLE_KIND_BYTES: usize = 64;
pub(crate) const DEFAULT_ACTION_RETRY_INTERVAL_MS: u64 = 100;
pub(crate) const CLEANUP_INTERVAL_MS: u64 = 15_000;
pub(crate) const AUTHORIZATION_HEADER: &str = "authorization";
pub(crate) const PRINCIPAL_HEADER: &str = "x-palyra-principal";
pub(crate) const STATE_DIR_ENV: &str = "PALYRA_BROWSERD_STATE_DIR";
pub(crate) const STATE_KEY_ENV: &str = "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY";
pub(crate) const STATE_ROOT_ENV: &str = "PALYRA_STATE_ROOT";
pub(crate) const CHROMIUM_PATH_ENV: &str = "PALYRA_BROWSERD_CHROMIUM_PATH";
pub(crate) const CHROMIUM_ENGINE_MODE_ENV: &str = "PALYRA_BROWSERD_ENGINE_MODE";
pub(crate) const CHROMIUM_STARTUP_TIMEOUT_ENV: &str = "PALYRA_BROWSERD_CHROMIUM_STARTUP_TIMEOUT_MS";
pub(crate) const DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS: u64 = 20_000;
pub(crate) const CHROMIUM_NEW_TAB_MAX_ATTEMPTS: usize = 6;
pub(crate) const CHROMIUM_NEW_TAB_RETRY_DELAY_MS: u64 = 400;
pub(crate) const STATE_FILE_MAGIC: &[u8; 4] = b"PBS1";
pub(crate) const STATE_NONCE_LEN: usize = 12;
pub(crate) const STATE_KEY_LEN: usize = 32;
pub(crate) const STATE_TMP_EXTENSION: &str = "tmp";
pub(crate) const STATE_PROFILE_DEK_NAMESPACE: &[u8] = b"palyra.browser.profile.dek.v1";
pub(crate) const COOKIE_HEADER: &str = "cookie";
pub(crate) const SET_COOKIE_HEADER: &str = "set-cookie";
pub(crate) const PROFILE_REGISTRY_FILE_NAME: &str = "profiles.enc";
pub(crate) const PROFILE_REGISTRY_SCHEMA_VERSION: u32 = 1;
pub(crate) const PROFILE_RECORD_SCHEMA_VERSION: u32 = 2;
pub(crate) const MAX_PROFILE_NAME_BYTES: usize = 96;
pub(crate) const MAX_PROFILE_THEME_BYTES: usize = 24;
pub(crate) const MAX_PROFILES_PER_PRINCIPAL: usize = 16;
pub(crate) const MAX_PROFILE_REGISTRY_BYTES: usize = 512 * 1024;
pub(crate) const PROFILE_RECORD_HASH_NAMESPACE: &[u8] = b"palyra.browser.profile.record.v2";
pub(crate) const PROFILE_RECORD_HASH_NAMESPACE_LEGACY: &[u8] = b"palyra.browser.profile.record.v1";
pub(crate) const DOWNLOAD_MAX_TOTAL_BYTES_PER_SESSION: u64 = 32 * 1024 * 1024;
pub(crate) const DOWNLOAD_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_DOWNLOAD_ARTIFACTS_PER_SESSION: usize = 128;
pub(crate) const DOWNLOADS_DIR_ALLOWLIST: &str = "allowlist";
pub(crate) const DOWNLOADS_DIR_QUARANTINE: &str = "quarantine";
pub(crate) const DOWNLOAD_FILE_NAME_FALLBACK: &str = "download.bin";
pub(crate) const DOWNLOAD_ALLOWED_EXTENSIONS: &[&str] = &["txt", "csv", "json", "pdf", "zip", "gz"];
pub(crate) const DOWNLOAD_ALLOWED_MIME_TYPES: &[&str] = &[
    "text/plain",
    "text/csv",
    "application/json",
    "application/pdf",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
];
pub(crate) const MAX_RELAY_EXTENSION_ID_BYTES: usize = 96;
pub(crate) const MAX_RELAY_SELECTION_BYTES: usize = 8 * 1024;
pub(crate) const MAX_RELAY_PAYLOAD_BYTES: u64 = 32 * 1024;
pub(crate) const MAX_COOKIE_DOMAINS_PER_SESSION: usize = 32;
pub(crate) const MAX_COOKIES_PER_DOMAIN: usize = 32;
pub(crate) const MAX_STORAGE_ORIGINS_PER_SESSION: usize = 16;
pub(crate) const MAX_STORAGE_ENTRIES_PER_ORIGIN: usize = 32;
pub(crate) const MAX_STORAGE_ENTRY_VALUE_BYTES: usize = 4 * 1024;
pub(crate) const CHROMIUM_REMOTE_IP_GUARD_HANDLER_NAME: &str = "palyra.security.remote_ip_guard";
pub(crate) const DNS_VALIDATION_CACHE_MAX_ENTRIES: usize = 512;
pub(crate) const DNS_VALIDATION_NEGATIVE_TTL: Duration = Duration::from_secs(10);
pub(crate) const DNS_VALIDATION_METRICS_LOG_INTERVAL: u64 = 256;
pub(crate) const ONE_BY_ONE_PNG: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0,
    0, 0, 31, 21, 196, 137, 0, 0, 0, 10, 73, 68, 65, 84, 120, 156, 99, 96, 0, 0, 0, 2, 0, 1, 229,
    39, 212, 138, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum BrowserEngineMode {
    Chromium,
    Simulated,
}

impl BrowserEngineMode {
    pub(crate) fn from_env_or_default(default: Self) -> Self {
        match std::env::var(CHROMIUM_ENGINE_MODE_ENV)
            .ok()
            .map(|value| value.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("chromium") => Self::Chromium,
            Some("simulated") => Self::Simulated,
            _ => default,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ChromiumEngineConfig {
    pub(crate) executable_path: Option<PathBuf>,
    pub(crate) startup_timeout: Duration,
}

#[derive(Debug, Clone, Parser)]
#[command(name = "palyra-browserd", about = "Palyra browser service v1")]
pub(crate) struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) bind: String,
    #[arg(long, default_value_t = 7143)]
    pub(crate) port: u16,
    #[arg(long, default_value = "127.0.0.1")]
    pub(crate) grpc_bind: String,
    #[arg(long, default_value_t = DEFAULT_GRPC_PORT)]
    pub(crate) grpc_port: u16,
    #[arg(long)]
    pub(crate) auth_token: Option<String>,
    #[arg(long, default_value_t = DEFAULT_SESSION_IDLE_TTL_MS)]
    pub(crate) session_idle_ttl_ms: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_SESSIONS)]
    pub(crate) max_sessions: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_NAVIGATION_TIMEOUT_MS)]
    pub(crate) max_navigation_timeout_ms: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_SESSION_LIFETIME_MS)]
    pub(crate) max_session_lifetime_ms: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_SCREENSHOT_BYTES)]
    pub(crate) max_screenshot_bytes: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_RESPONSE_BYTES)]
    pub(crate) max_response_bytes: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_TITLE_BYTES)]
    pub(crate) max_title_bytes: u64,
    #[arg(long, value_enum, default_value_t = BrowserEngineMode::Chromium)]
    pub(crate) engine_mode: BrowserEngineMode,
    #[arg(long)]
    pub(crate) chromium_path: Option<PathBuf>,
    #[arg(long, default_value_t = DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS)]
    pub(crate) chromium_startup_timeout_ms: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct SessionBudget {
    pub(crate) max_navigation_timeout_ms: u64,
    pub(crate) max_session_lifetime_ms: u64,
    pub(crate) max_screenshot_bytes: u64,
    pub(crate) max_response_bytes: u64,
    pub(crate) max_title_bytes: u64,
    pub(crate) max_action_timeout_ms: u64,
    pub(crate) max_type_input_bytes: u64,
    pub(crate) max_actions_per_session: u64,
    pub(crate) max_actions_per_window: u64,
    pub(crate) action_rate_window_ms: u64,
    pub(crate) max_action_log_entries: usize,
    pub(crate) max_observe_snapshot_bytes: u64,
    pub(crate) max_visible_text_bytes: u64,
    pub(crate) max_network_log_entries: usize,
    pub(crate) max_network_log_bytes: u64,
    pub(crate) max_tabs_per_session: usize,
}

pub(crate) fn session_budget_to_proto(budget: &SessionBudget) -> browser_v1::SessionBudget {
    browser_v1::SessionBudget {
        max_navigation_timeout_ms: budget.max_navigation_timeout_ms,
        max_session_lifetime_ms: budget.max_session_lifetime_ms,
        max_screenshot_bytes: budget.max_screenshot_bytes,
        max_response_bytes: budget.max_response_bytes,
        max_action_timeout_ms: budget.max_action_timeout_ms,
        max_type_input_bytes: budget.max_type_input_bytes,
        max_actions_per_session: budget.max_actions_per_session,
        max_actions_per_window: budget.max_actions_per_window,
        action_rate_window_ms: budget.action_rate_window_ms,
        max_action_log_entries: budget.max_action_log_entries as u64,
        max_observe_snapshot_bytes: budget.max_observe_snapshot_bytes,
        max_visible_text_bytes: budget.max_visible_text_bytes,
        max_network_log_entries: budget.max_network_log_entries as u64,
        max_network_log_bytes: budget.max_network_log_bytes,
    }
}
