pub mod app;
pub mod domain;
pub mod engine;
pub mod infra;
pub mod persistence;
pub mod security;
pub mod support;
pub mod transport;
#[cfg(test)]
pub(crate) use app::bootstrap::enforce_non_loopback_bind_auth;
pub use app::bootstrap::run;
pub(crate) use domain::*;
pub(crate) use engine::*;
pub(crate) use persistence::*;
pub(crate) use security::*;
pub(crate) use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ffi::{OsStr, OsString},
    fs,
    io::Write,
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, LazyLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[cfg(test)]
pub(crate) use transport::grpc::BrowserServiceImpl;

pub(crate) use anyhow::{Context, Result};
pub(crate) use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
pub(crate) use base64::Engine as _;
pub(crate) use clap::Parser;
pub(crate) use headless_chrome::{
    browser::tab::RequestPausedDecision,
    protocol::cdp::{Fetch, Network, Page},
    Browser as HeadlessBrowser, LaunchOptionsBuilder, Tab as HeadlessTab,
};
pub(crate) use palyra_common::{
    build_metadata, health_response, netguard, parse_daemon_bind_socket, validate_canonical_id,
    HealthResponse, CANONICAL_PROTOCOL_MAJOR,
};
pub(crate) use reqwest::{redirect::Policy, Url};
pub(crate) use ring::{
    aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305},
    digest::{Context as DigestContext, SHA256},
    rand::{SecureRandom, SystemRandom},
};
pub(crate) use serde::{Deserialize, Serialize};
pub(crate) use tempfile::TempDir;
pub(crate) use tokio::time::{interval, MissedTickBehavior};
pub(crate) use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{oneshot, Mutex},
};
pub(crate) use tokio_stream::wrappers::TcpListenerStream;
pub(crate) use tonic::{transport::Server, Request, Response, Status};
pub(crate) use tracing::{info, warn};
pub(crate) use tracing_subscriber::EnvFilter;
pub(crate) use ulid::Ulid;

pub mod proto {
    pub mod palyra {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("palyra.common.v1");
            }
        }

        pub mod browser {
            pub mod v1 {
                tonic::include_proto!("palyra.browser.v1");
            }
        }
    }
}

pub(crate) use proto::palyra::browser::v1 as browser_v1;

const DEFAULT_GRPC_PORT: u16 = 7543;
const DEFAULT_SESSION_IDLE_TTL_MS: u64 = 10 * 60 * 1_000;
const DEFAULT_MAX_SESSIONS: usize = 128;
const DEFAULT_MAX_NAVIGATION_TIMEOUT_MS: u64 = 15_000;
const DEFAULT_MAX_SESSION_LIFETIME_MS: u64 = 30 * 60 * 1_000;
const DEFAULT_MAX_SCREENSHOT_BYTES: u64 = 256 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: u64 = 512 * 1024;
const DEFAULT_MAX_TITLE_BYTES: u64 = 4 * 1024;
const DEFAULT_MAX_ACTION_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_MAX_TYPE_INPUT_BYTES: u64 = 4 * 1024;
const DEFAULT_MAX_ACTIONS_PER_SESSION: u64 = 256;
const DEFAULT_MAX_ACTIONS_PER_WINDOW: u64 = 20;
const DEFAULT_ACTION_RATE_WINDOW_MS: u64 = 1_000;
const DEFAULT_MAX_ACTION_LOG_ENTRIES: usize = 256;
const DEFAULT_MAX_OBSERVE_SNAPSHOT_BYTES: u64 = 64 * 1024;
const DEFAULT_MAX_VISIBLE_TEXT_BYTES: u64 = 16 * 1024;
const DEFAULT_MAX_NETWORK_LOG_ENTRIES: usize = 256;
const DEFAULT_MAX_NETWORK_LOG_BYTES: u64 = 64 * 1024;
const DEFAULT_MAX_INSPECT_COOKIE_BYTES: u64 = 8 * 1024;
const DEFAULT_MAX_INSPECT_STORAGE_BYTES: u64 = 16 * 1024;
const DEFAULT_MAX_TABS_PER_SESSION: usize = 32;
const MAX_NETWORK_LOG_HEADER_COUNT: usize = 24;
const MAX_NETWORK_LOG_HEADER_VALUE_BYTES: usize = 256;
const MAX_NETWORK_LOG_URL_BYTES: usize = 2 * 1024;
const MAX_INSPECT_COOKIE_VALUE_BYTES: usize = 512;
const MAX_INSPECT_STORAGE_VALUE_BYTES: usize = 1_024;
const MAX_INSPECT_ACTION_NAME_BYTES: usize = 64;
const MAX_INSPECT_ACTION_SELECTOR_BYTES: usize = 256;
const MAX_INSPECT_ACTION_OUTCOME_BYTES: usize = 256;
const MAX_INSPECT_ACTION_ERROR_BYTES: usize = 512;
const DEFAULT_ACTION_RETRY_INTERVAL_MS: u64 = 100;
const CLEANUP_INTERVAL_MS: u64 = 15_000;
const AUTHORIZATION_HEADER: &str = "authorization";
const STATE_DIR_ENV: &str = "PALYRA_BROWSERD_STATE_DIR";
const STATE_KEY_ENV: &str = "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY";
const STATE_ROOT_ENV: &str = "PALYRA_STATE_ROOT";
const CHROMIUM_PATH_ENV: &str = "PALYRA_BROWSERD_CHROMIUM_PATH";
const CHROMIUM_ENGINE_MODE_ENV: &str = "PALYRA_BROWSERD_ENGINE_MODE";
const CHROMIUM_STARTUP_TIMEOUT_ENV: &str = "PALYRA_BROWSERD_CHROMIUM_STARTUP_TIMEOUT_MS";
const DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS: u64 = 20_000;
const CHROMIUM_NEW_TAB_MAX_ATTEMPTS: usize = 6;
const CHROMIUM_NEW_TAB_RETRY_DELAY_MS: u64 = 400;
const STATE_FILE_MAGIC: &[u8; 4] = b"PBS1";
const STATE_NONCE_LEN: usize = 12;
const STATE_KEY_LEN: usize = 32;
const STATE_TMP_EXTENSION: &str = "tmp";
const STATE_PROFILE_DEK_NAMESPACE: &[u8] = b"palyra.browser.profile.dek.v1";
const COOKIE_HEADER: &str = "cookie";
const SET_COOKIE_HEADER: &str = "set-cookie";
const PROFILE_REGISTRY_FILE_NAME: &str = "profiles.enc";
const PROFILE_REGISTRY_SCHEMA_VERSION: u32 = 1;
const PROFILE_RECORD_SCHEMA_VERSION: u32 = 2;
const MAX_PROFILE_NAME_BYTES: usize = 96;
const MAX_PROFILE_THEME_BYTES: usize = 24;
const MAX_PROFILES_PER_PRINCIPAL: usize = 16;
const MAX_PROFILE_REGISTRY_BYTES: usize = 512 * 1024;
const PROFILE_RECORD_HASH_NAMESPACE: &[u8] = b"palyra.browser.profile.record.v2";
const PROFILE_RECORD_HASH_NAMESPACE_LEGACY: &[u8] = b"palyra.browser.profile.record.v1";
const DOWNLOAD_MAX_TOTAL_BYTES_PER_SESSION: u64 = 32 * 1024 * 1024;
const DOWNLOAD_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_DOWNLOAD_ARTIFACTS_PER_SESSION: usize = 128;
const DOWNLOADS_DIR_ALLOWLIST: &str = "allowlist";
const DOWNLOADS_DIR_QUARANTINE: &str = "quarantine";
const DOWNLOAD_FILE_NAME_FALLBACK: &str = "download.bin";
const DOWNLOAD_ALLOWED_EXTENSIONS: &[&str] = &["txt", "csv", "json", "pdf", "zip", "gz"];
const DOWNLOAD_ALLOWED_MIME_TYPES: &[&str] = &[
    "text/plain",
    "text/csv",
    "application/json",
    "application/pdf",
    "application/zip",
    "application/gzip",
    "application/x-gzip",
];
const MAX_RELAY_EXTENSION_ID_BYTES: usize = 96;
const MAX_RELAY_SELECTION_BYTES: usize = 8 * 1024;
const MAX_RELAY_PAYLOAD_BYTES: u64 = 32 * 1024;
const MAX_COOKIE_DOMAINS_PER_SESSION: usize = 32;
const MAX_COOKIES_PER_DOMAIN: usize = 32;
const MAX_STORAGE_ORIGINS_PER_SESSION: usize = 16;
const MAX_STORAGE_ENTRIES_PER_ORIGIN: usize = 32;
const MAX_STORAGE_ENTRY_VALUE_BYTES: usize = 4 * 1024;
const CHROMIUM_REMOTE_IP_GUARD_HANDLER_NAME: &str = "palyra.security.remote_ip_guard";
const DNS_VALIDATION_CACHE_MAX_ENTRIES: usize = 512;
const DNS_VALIDATION_NEGATIVE_TTL: Duration = Duration::from_secs(10);
const DNS_VALIDATION_METRICS_LOG_INTERVAL: u64 = 256;
const ONE_BY_ONE_PNG: &[u8] = &[
    137, 80, 78, 71, 13, 10, 26, 10, 0, 0, 0, 13, 73, 72, 68, 82, 0, 0, 0, 1, 0, 0, 0, 1, 8, 6, 0,
    0, 0, 31, 21, 196, 137, 0, 0, 0, 10, 73, 68, 65, 84, 120, 156, 99, 96, 0, 0, 0, 2, 0, 1, 229,
    39, 212, 138, 0, 0, 0, 0, 73, 69, 78, 68, 174, 66, 96, 130,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum BrowserEngineMode {
    Chromium,
    Simulated,
}

impl BrowserEngineMode {
    fn from_env_or_default(default: Self) -> Self {
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
struct ChromiumEngineConfig {
    executable_path: Option<PathBuf>,
    startup_timeout: Duration,
}

#[derive(Debug, Clone, Parser)]
#[command(name = "palyra-browserd", about = "Palyra browser service v1")]
struct Args {
    #[arg(long, default_value = "127.0.0.1")]
    bind: String,
    #[arg(long, default_value_t = 7143)]
    port: u16,
    #[arg(long, default_value = "127.0.0.1")]
    grpc_bind: String,
    #[arg(long, default_value_t = DEFAULT_GRPC_PORT)]
    grpc_port: u16,
    #[arg(long)]
    auth_token: Option<String>,
    #[arg(long, default_value_t = DEFAULT_SESSION_IDLE_TTL_MS)]
    session_idle_ttl_ms: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_SESSIONS)]
    max_sessions: usize,
    #[arg(long, default_value_t = DEFAULT_MAX_NAVIGATION_TIMEOUT_MS)]
    max_navigation_timeout_ms: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_SESSION_LIFETIME_MS)]
    max_session_lifetime_ms: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_SCREENSHOT_BYTES)]
    max_screenshot_bytes: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_RESPONSE_BYTES)]
    max_response_bytes: u64,
    #[arg(long, default_value_t = DEFAULT_MAX_TITLE_BYTES)]
    max_title_bytes: u64,
    #[arg(long, value_enum, default_value_t = BrowserEngineMode::Chromium)]
    engine_mode: BrowserEngineMode,
    #[arg(long)]
    chromium_path: Option<PathBuf>,
    #[arg(long, default_value_t = DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS)]
    chromium_startup_timeout_ms: u64,
}

#[derive(Debug, Clone)]
struct SessionBudget {
    max_navigation_timeout_ms: u64,
    max_session_lifetime_ms: u64,
    max_screenshot_bytes: u64,
    max_response_bytes: u64,
    max_title_bytes: u64,
    max_action_timeout_ms: u64,
    max_type_input_bytes: u64,
    max_actions_per_session: u64,
    max_actions_per_window: u64,
    action_rate_window_ms: u64,
    max_action_log_entries: usize,
    max_observe_snapshot_bytes: u64,
    max_visible_text_bytes: u64,
    max_network_log_entries: usize,
    max_network_log_bytes: u64,
    max_tabs_per_session: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
enum PermissionSettingInternal {
    #[default]
    Deny,
    Allow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SessionPermissionsInternal {
    camera: PermissionSettingInternal,
    microphone: PermissionSettingInternal,
    location: PermissionSettingInternal,
}

impl Default for SessionPermissionsInternal {
    fn default() -> Self {
        Self {
            camera: PermissionSettingInternal::Deny,
            microphone: PermissionSettingInternal::Deny,
            location: PermissionSettingInternal::Deny,
        }
    }
}

impl SessionPermissionsInternal {
    fn to_proto(&self) -> browser_v1::SessionPermissions {
        browser_v1::SessionPermissions {
            v: CANONICAL_PROTOCOL_MAJOR,
            camera: permission_setting_to_proto(self.camera),
            microphone: permission_setting_to_proto(self.microphone),
            location: permission_setting_to_proto(self.location),
        }
    }

    fn apply_update(
        &mut self,
        camera: i32,
        microphone: i32,
        location: i32,
        reset_to_default: bool,
    ) {
        if reset_to_default {
            *self = Self::default();
            return;
        }
        if let Some(value) = permission_setting_from_proto(camera) {
            self.camera = value;
        }
        if let Some(value) = permission_setting_from_proto(microphone) {
            self.microphone = value;
        }
        if let Some(value) = permission_setting_from_proto(location) {
            self.location = value;
        }
    }
}

fn session_budget_to_proto(budget: &SessionBudget) -> browser_v1::SessionBudget {
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

fn instant_to_unix_ms(instant: Instant) -> u64 {
    current_unix_ms()
        .saturating_sub(u64::try_from(instant.elapsed().as_millis()).unwrap_or(u64::MAX))
}

fn session_summary_to_proto(
    session_id: &str,
    session: &BrowserSessionRecord,
) -> browser_v1::BrowserSessionSummary {
    let active_tab = session.active_tab();
    browser_v1::BrowserSessionSummary {
        v: CANONICAL_PROTOCOL_MAJOR,
        session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id.to_owned() }),
        principal: session.principal.clone(),
        channel: session.channel.clone().unwrap_or_default(),
        created_at_unix_ms: instant_to_unix_ms(session.created_at),
        last_active_unix_ms: instant_to_unix_ms(session.last_active),
        idle_ttl_ms: u64::try_from(session.idle_ttl.as_millis()).unwrap_or(u64::MAX),
        age_ms: u64::try_from(session.created_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        idle_for_ms: u64::try_from(session.last_active.elapsed().as_millis()).unwrap_or(u64::MAX),
        action_count: session.action_count,
        action_log_entries: u32::try_from(session.action_log.len()).unwrap_or(u32::MAX),
        tab_count: u32::try_from(session.tabs.len()).unwrap_or(u32::MAX),
        active_tab_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: session.active_tab_id.clone(),
        }),
        active_tab_url: normalize_url_with_redaction(
            active_tab.and_then(|tab| tab.last_url.as_deref()).unwrap_or_default(),
        ),
        active_tab_title: truncate_utf8_bytes(
            active_tab.map(|tab| tab.last_title.as_str()).unwrap_or_default(),
            session.budget.max_title_bytes as usize,
        ),
        allow_private_targets: session.allow_private_targets,
        downloads_enabled: session.allow_downloads,
        persistence_enabled: session.persistence.enabled,
        persistence_id: session.persistence.persistence_id.clone().unwrap_or_default(),
        state_restored: session.persistence.state_restored,
        profile_id: session
            .profile_id
            .clone()
            .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        private_profile: session.private_profile,
        action_allowed_domains: session.action_allowed_domains.clone(),
        permissions: Some(session.permissions.to_proto()),
    }
}

fn session_detail_to_proto(
    session_id: &str,
    session: &BrowserSessionRecord,
) -> browser_v1::BrowserSessionDetail {
    browser_v1::BrowserSessionDetail {
        v: CANONICAL_PROTOCOL_MAJOR,
        summary: Some(session_summary_to_proto(session_id, session)),
        effective_budget: Some(session_budget_to_proto(&session.budget)),
        tabs: session.list_tabs(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserActionLogEntryInternal {
    action_id: String,
    action_name: String,
    selector: String,
    success: bool,
    outcome: String,
    error: String,
    started_at_unix_ms: u64,
    completed_at_unix_ms: u64,
    attempts: u32,
    page_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetworkLogHeaderInternal {
    name: String,
    value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct NetworkLogEntryInternal {
    request_url: String,
    status_code: u16,
    timing_bucket: String,
    latency_ms: u64,
    captured_at_unix_ms: u64,
    headers: Vec<NetworkLogHeaderInternal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserTabRecord {
    tab_id: String,
    last_title: String,
    last_url: Option<String>,
    last_page_body: String,
    scroll_x: i64,
    scroll_y: i64,
    typed_inputs: HashMap<String, String>,
    network_log: VecDeque<NetworkLogEntryInternal>,
}

impl BrowserTabRecord {
    fn new(tab_id: String) -> Self {
        Self {
            tab_id,
            last_title: String::new(),
            last_url: None,
            last_page_body: String::new(),
            scroll_x: 0,
            scroll_y: 0,
            typed_inputs: HashMap::new(),
            network_log: VecDeque::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SessionPersistenceState {
    enabled: bool,
    persistence_id: Option<String>,
    state_restored: bool,
}

#[derive(Debug, Clone)]
struct BrowserSessionInit {
    principal: String,
    channel: Option<String>,
    now: Instant,
    idle_ttl: Duration,
    budget: SessionBudget,
    allow_private_targets: bool,
    allow_downloads: bool,
    action_allowed_domains: Vec<String>,
    profile_id: Option<String>,
    private_profile: bool,
    persistence: SessionPersistenceState,
}

#[derive(Debug, Clone)]
struct BrowserSessionRecord {
    principal: String,
    channel: Option<String>,
    last_active: Instant,
    created_at: Instant,
    idle_ttl: Duration,
    budget: SessionBudget,
    allow_private_targets: bool,
    allow_downloads: bool,
    action_allowed_domains: Vec<String>,
    profile_id: Option<String>,
    private_profile: bool,
    action_count: u64,
    action_window: VecDeque<Instant>,
    action_log: VecDeque<BrowserActionLogEntryInternal>,
    tabs: HashMap<String, BrowserTabRecord>,
    tab_order: Vec<String>,
    active_tab_id: String,
    permissions: SessionPermissionsInternal,
    cookie_jar: HashMap<String, HashMap<String, String>>,
    storage_entries: HashMap<String, HashMap<String, String>>,
    persistence: SessionPersistenceState,
}

impl BrowserSessionRecord {
    fn with_defaults(init: BrowserSessionInit) -> Self {
        let initial_tab_id = Ulid::new().to_string();
        let mut tabs = HashMap::new();
        tabs.insert(initial_tab_id.clone(), BrowserTabRecord::new(initial_tab_id.clone()));
        Self {
            principal: init.principal,
            channel: init.channel,
            last_active: init.now,
            created_at: init.now,
            idle_ttl: init.idle_ttl,
            budget: init.budget,
            allow_private_targets: init.allow_private_targets,
            allow_downloads: init.allow_downloads,
            action_allowed_domains: init.action_allowed_domains,
            profile_id: init.profile_id,
            private_profile: init.private_profile,
            action_count: 0,
            action_window: VecDeque::new(),
            action_log: VecDeque::new(),
            tabs,
            tab_order: vec![initial_tab_id.clone()],
            active_tab_id: initial_tab_id,
            permissions: SessionPermissionsInternal::default(),
            cookie_jar: HashMap::new(),
            storage_entries: HashMap::new(),
            persistence: init.persistence,
        }
    }

    fn active_tab(&self) -> Option<&BrowserTabRecord> {
        self.tabs.get(self.active_tab_id.as_str())
    }

    fn active_tab_mut(&mut self) -> Option<&mut BrowserTabRecord> {
        self.tabs.get_mut(self.active_tab_id.as_str())
    }

    fn create_tab(&mut self) -> String {
        let tab_id = Ulid::new().to_string();
        self.tabs.insert(tab_id.clone(), BrowserTabRecord::new(tab_id.clone()));
        self.tab_order.push(tab_id.clone());
        tab_id
    }

    fn can_create_tab(&self) -> bool {
        self.tabs.len() < self.budget.max_tabs_per_session
    }

    fn tab_to_proto(&self, tab_id: &str) -> Option<browser_v1::BrowserTab> {
        self.tabs.get(tab_id).map(|tab| browser_v1::BrowserTab {
            v: CANONICAL_PROTOCOL_MAJOR,
            tab_id: Some(proto::palyra::common::v1::CanonicalId { ulid: tab.tab_id.clone() }),
            url: normalize_url_with_redaction(tab.last_url.as_deref().unwrap_or_default()),
            title: truncate_utf8_bytes(
                tab.last_title.as_str(),
                self.budget.max_title_bytes as usize,
            ),
            active: tab_id == self.active_tab_id,
        })
    }

    fn list_tabs(&self) -> Vec<browser_v1::BrowserTab> {
        self.tab_order.iter().filter_map(|tab_id| self.tab_to_proto(tab_id)).collect()
    }

    fn close_tab(&mut self, tab_id: &str) -> Result<(String, Option<String>), String> {
        if self.tabs.len() <= 1 {
            return Err("cannot close the last remaining tab".to_owned());
        }
        if self.tabs.remove(tab_id).is_none() {
            return Err("tab_not_found".to_owned());
        }
        self.tab_order.retain(|value| value != tab_id);
        let mut switched_to = None;
        if self.active_tab_id == tab_id {
            if let Some(next_tab_id) = self.tab_order.first() {
                self.active_tab_id = next_tab_id.clone();
                switched_to = Some(next_tab_id.clone());
            } else {
                let created = self.create_tab();
                self.active_tab_id = created.clone();
                switched_to = Some(created);
            }
        }
        Ok((tab_id.to_owned(), switched_to))
    }

    fn apply_snapshot(&mut self, snapshot: PersistedSessionSnapshot) {
        let mut tabs = HashMap::new();
        for mut tab in snapshot.tabs.into_iter().take(self.budget.max_tabs_per_session) {
            if validate_canonical_id(tab.tab_id.as_str()).is_ok() {
                tab.network_log = clamp_network_log_entries(
                    tab.network_log,
                    self.budget.max_network_log_entries,
                    self.budget.max_network_log_bytes,
                );
                tabs.insert(tab.tab_id.clone(), tab);
            }
        }
        if tabs.is_empty() {
            let initial_tab_id = Ulid::new().to_string();
            tabs.insert(initial_tab_id.clone(), BrowserTabRecord::new(initial_tab_id.clone()));
            self.tab_order = vec![initial_tab_id.clone()];
            self.active_tab_id = initial_tab_id;
        } else {
            let mut tab_order = snapshot
                .tab_order
                .into_iter()
                .filter(|tab_id| tabs.contains_key(tab_id.as_str()))
                .collect::<Vec<_>>();
            let mut seen_tab_ids = tab_order.iter().cloned().collect::<HashSet<_>>();
            for tab_id in tabs.keys() {
                if seen_tab_ids.insert(tab_id.clone()) {
                    tab_order.push(tab_id.clone());
                }
            }
            self.active_tab_id = if tabs.contains_key(snapshot.active_tab_id.as_str()) {
                snapshot.active_tab_id
            } else {
                tab_order.first().cloned().unwrap_or_else(|| Ulid::new().to_string())
            };
            self.tab_order = tab_order;
        }
        self.tabs = tabs;
        self.permissions = snapshot.permissions;
        self.cookie_jar = clamp_cookie_jar(snapshot.cookie_jar);
        self.storage_entries = clamp_storage_entries(snapshot.storage_entries);
    }
}

struct BrowserRuntimeState {
    started_at: Instant,
    auth_token: Option<String>,
    engine_mode: BrowserEngineMode,
    chromium: ChromiumEngineConfig,
    default_idle_ttl: Duration,
    default_budget: SessionBudget,
    max_sessions: usize,
    state_store: Option<PersistedStateStore>,
    profile_registry_lock: Mutex<()>,
    sessions: Mutex<HashMap<String, BrowserSessionRecord>>,
    chromium_sessions: Mutex<HashMap<String, ChromiumSessionState>>,
    download_sessions: Mutex<HashMap<String, DownloadSandboxSession>>,
}

impl BrowserRuntimeState {
    fn new(args: &Args) -> Result<Self> {
        if args.session_idle_ttl_ms == 0
            || args.max_sessions == 0
            || args.chromium_startup_timeout_ms == 0
        {
            anyhow::bail!(
                "session_idle_ttl_ms, max_sessions, and chromium_startup_timeout_ms must be greater than zero"
            );
        }
        let state_store = build_state_store_from_env()?;
        let engine_mode = BrowserEngineMode::from_env_or_default(args.engine_mode);
        let chromium_path = args
            .chromium_path
            .clone()
            .or_else(|| std::env::var(CHROMIUM_PATH_ENV).ok().map(PathBuf::from));
        let chromium_startup_timeout = std::env::var(CHROMIUM_STARTUP_TIMEOUT_ENV)
            .ok()
            .and_then(|value| value.trim().parse::<u64>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(args.chromium_startup_timeout_ms);
        Ok(Self {
            started_at: Instant::now(),
            auth_token: args
                .auth_token
                .clone()
                .or_else(|| std::env::var("PALYRA_BROWSERD_AUTH_TOKEN").ok())
                .and_then(|value| {
                    let trimmed = value.trim();
                    if trimmed.is_empty() {
                        None
                    } else {
                        Some(trimmed.to_owned())
                    }
                }),
            engine_mode,
            chromium: ChromiumEngineConfig {
                executable_path: chromium_path,
                startup_timeout: Duration::from_millis(chromium_startup_timeout),
            },
            default_idle_ttl: Duration::from_millis(args.session_idle_ttl_ms),
            default_budget: SessionBudget {
                max_navigation_timeout_ms: args.max_navigation_timeout_ms.max(1),
                max_session_lifetime_ms: args.max_session_lifetime_ms.max(1),
                max_screenshot_bytes: args.max_screenshot_bytes.max(1),
                max_response_bytes: args.max_response_bytes.max(1),
                max_title_bytes: args.max_title_bytes.max(1),
                max_action_timeout_ms: DEFAULT_MAX_ACTION_TIMEOUT_MS,
                max_type_input_bytes: DEFAULT_MAX_TYPE_INPUT_BYTES,
                max_actions_per_session: DEFAULT_MAX_ACTIONS_PER_SESSION,
                max_actions_per_window: DEFAULT_MAX_ACTIONS_PER_WINDOW,
                action_rate_window_ms: DEFAULT_ACTION_RATE_WINDOW_MS,
                max_action_log_entries: DEFAULT_MAX_ACTION_LOG_ENTRIES,
                max_observe_snapshot_bytes: DEFAULT_MAX_OBSERVE_SNAPSHOT_BYTES,
                max_visible_text_bytes: DEFAULT_MAX_VISIBLE_TEXT_BYTES,
                max_network_log_entries: DEFAULT_MAX_NETWORK_LOG_ENTRIES,
                max_network_log_bytes: DEFAULT_MAX_NETWORK_LOG_BYTES,
                max_tabs_per_session: DEFAULT_MAX_TABS_PER_SESSION,
            },
            max_sessions: args.max_sessions,
            state_store,
            profile_registry_lock: Mutex::new(()),
            sessions: Mutex::new(HashMap::new()),
            chromium_sessions: Mutex::new(HashMap::new()),
            download_sessions: Mutex::new(HashMap::new()),
        })
    }
}

#[derive(Debug, Clone)]
struct CookieUpdate {
    domain: String,
    name: String,
    value: String,
}

#[derive(Debug, Clone)]
struct NavigateOutcome {
    success: bool,
    final_url: String,
    status_code: u16,
    title: String,
    page_body: String,
    body_bytes: u64,
    latency_ms: u64,
    error: String,
    network_log: Vec<NetworkLogEntryInternal>,
    cookie_updates: Vec<CookieUpdate>,
}

fn extract_html_title(body: &str) -> Option<&str> {
    let lower = body.to_ascii_lowercase();
    let start = lower.find("<title>")?;
    let end = lower[start + 7..].find("</title>")?;
    Some(body[start + 7..start + 7 + end].trim())
}

fn truncate_utf8_bytes_with_flag(raw: &str, max_bytes: usize) -> (String, bool) {
    let truncated = truncate_utf8_bytes(raw, max_bytes);
    let was_truncated = truncated.len() < raw.len();
    (truncated, was_truncated)
}

fn append_network_log_entries(
    tab: &mut BrowserTabRecord,
    entries: &[NetworkLogEntryInternal],
    max_entries: usize,
    max_bytes: u64,
) {
    let mut total_bytes =
        tab.network_log.iter().map(estimate_network_log_entry_internal_bytes).sum::<usize>();
    for entry in entries {
        total_bytes = total_bytes.saturating_add(estimate_network_log_entry_internal_bytes(entry));
        tab.network_log.push_back(entry.clone());
    }
    trim_network_log_to_budget(
        &mut tab.network_log,
        &mut total_bytes,
        max_entries,
        max_bytes as usize,
    );
}

fn clamp_network_log_entries<I>(
    entries: I,
    max_entries: usize,
    max_bytes: u64,
) -> VecDeque<NetworkLogEntryInternal>
where
    I: IntoIterator<Item = NetworkLogEntryInternal>,
{
    let mut network_log = VecDeque::new();
    let mut total_bytes = 0usize;
    for entry in entries.into_iter().take(max_entries) {
        total_bytes = total_bytes.saturating_add(estimate_network_log_entry_internal_bytes(&entry));
        network_log.push_back(entry);
    }
    trim_network_log_to_budget(&mut network_log, &mut total_bytes, max_entries, max_bytes as usize);
    network_log
}

fn trim_network_log_to_budget(
    network_log: &mut VecDeque<NetworkLogEntryInternal>,
    total_bytes: &mut usize,
    max_entries: usize,
    max_bytes: usize,
) {
    while network_log.len() > max_entries {
        if let Some(entry) = network_log.pop_front() {
            *total_bytes =
                total_bytes.saturating_sub(estimate_network_log_entry_internal_bytes(&entry));
        } else {
            break;
        }
    }
    while *total_bytes > max_bytes {
        if let Some(entry) = network_log.pop_front() {
            *total_bytes =
                total_bytes.saturating_sub(estimate_network_log_entry_internal_bytes(&entry));
        } else {
            break;
        }
    }
}

fn estimate_network_log_entry_internal_bytes(entry: &NetworkLogEntryInternal) -> usize {
    let headers_bytes = entry
        .headers
        .iter()
        .map(|header| header.name.len() + header.value.len() + 8)
        .sum::<usize>();
    entry.request_url.len() + entry.timing_bucket.len() + headers_bytes + 64
}

fn network_log_entry_to_proto(
    entry: NetworkLogEntryInternal,
    include_headers: bool,
) -> browser_v1::NetworkLogEntry {
    let headers = if include_headers {
        entry
            .headers
            .into_iter()
            .map(|header| browser_v1::NetworkLogHeader {
                v: CANONICAL_PROTOCOL_MAJOR,
                name: truncate_utf8_bytes(header.name.to_ascii_lowercase().as_str(), 128),
                value: sanitize_single_network_header(
                    header.name.to_ascii_lowercase().as_str(),
                    header.value.as_str(),
                ),
            })
            .collect()
    } else {
        Vec::new()
    };
    browser_v1::NetworkLogEntry {
        v: CANONICAL_PROTOCOL_MAJOR,
        request_url: normalize_url_with_redaction(entry.request_url.as_str()),
        status_code: u32::from(entry.status_code),
        timing_bucket: entry.timing_bucket,
        latency_ms: entry.latency_ms,
        captured_at_unix_ms: entry.captured_at_unix_ms,
        headers,
    }
}

fn estimate_network_log_payload_bytes(entries: &[browser_v1::NetworkLogEntry]) -> usize {
    entries.iter().map(estimate_network_log_proto_entry_bytes).sum::<usize>() + 2
}

fn estimate_network_log_proto_entry_bytes(entry: &browser_v1::NetworkLogEntry) -> usize {
    let headers = entry.headers.iter().map(estimate_network_log_proto_header_bytes).sum::<usize>();
    entry.request_url.len() + entry.timing_bucket.len() + headers + 64
}

fn estimate_network_log_proto_header_bytes(header: &browser_v1::NetworkLogHeader) -> usize {
    header.name.len() + header.value.len() + 8
}

fn truncate_network_log_payload(
    entries: &mut Vec<browser_v1::NetworkLogEntry>,
    max_payload_bytes: usize,
) -> bool {
    let mut truncated = false;
    while !entries.is_empty()
        && estimate_network_log_payload_bytes(entries.as_slice()) > max_payload_bytes
    {
        entries.remove(0);
        truncated = true;
    }
    truncated
}

fn timing_bucket_for_latency(latency_ms: u64) -> &'static str {
    if latency_ms <= 100 {
        "lt_100ms"
    } else if latency_ms <= 500 {
        "100_500ms"
    } else if latency_ms <= 2_000 {
        "500ms_2s"
    } else {
        "gt_2s"
    }
}

fn sanitize_network_headers(headers: &reqwest::header::HeaderMap) -> Vec<NetworkLogHeaderInternal> {
    let mut output = headers
        .iter()
        .take(MAX_NETWORK_LOG_HEADER_COUNT)
        .map(|(name, value)| {
            let header_name = name.as_str().to_ascii_lowercase();
            let raw_value = value.to_str().unwrap_or("<non_utf8>");
            let sanitized = sanitize_single_network_header(header_name.as_str(), raw_value);
            NetworkLogHeaderInternal { name: header_name, value: sanitized }
        })
        .collect::<Vec<_>>();
    output.sort_by(|left, right| left.name.cmp(&right.name));
    output
}

fn sanitize_single_network_header(name: &str, raw_value: &str) -> String {
    if name.eq_ignore_ascii_case("location")
        || raw_value.starts_with("http://")
        || raw_value.starts_with("https://")
    {
        return normalize_url_with_redaction(raw_value);
    }
    if is_sensitive_header_name(name) || contains_sensitive_material(raw_value) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw_value, MAX_NETWORK_LOG_HEADER_VALUE_BYTES)
}

fn is_sensitive_header_name(name: &str) -> bool {
    matches!(
        name,
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "x-api-key"
            | "x-auth-token"
            | "x-csrf-token"
    ) || name.contains("token")
        || name.contains("secret")
        || name.contains("password")
}

fn contains_sensitive_material(raw: &str) -> bool {
    let lower = raw.to_ascii_lowercase();
    [
        "bearer ",
        "token=",
        "access_token=",
        "id_token=",
        "refresh_token=",
        "session=",
        "password=",
        "passwd=",
        "secret=",
        "api_key=",
        "apikey=",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

fn is_sensitive_debug_key(raw_key: &str) -> bool {
    let key = raw_key.trim().to_ascii_lowercase();
    matches!(
        key.as_str(),
        "authorization"
            | "cookie"
            | "csrf"
            | "jwt"
            | "password"
            | "passwd"
            | "secret"
            | "session"
            | "session_id"
            | "set-cookie"
            | "token"
    ) || key.contains("auth")
        || key.contains("cookie")
        || key.contains("password")
        || key.contains("secret")
        || key.contains("session")
        || key.contains("token")
}

fn sanitize_debug_text(raw: &str, max_bytes: usize) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }
    if contains_sensitive_material(raw) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw, max_bytes)
}

fn sanitize_debug_map_value(key: &str, raw_value: &str, max_bytes: usize) -> String {
    if raw_value.trim().is_empty() {
        return String::new();
    }
    if is_sensitive_debug_key(key) || contains_sensitive_material(raw_value) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw_value, max_bytes)
}

fn normalize_url_with_redaction(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if let Ok(parsed) = Url::parse(trimmed) {
        let Some(host) = parsed.host_str() else {
            return truncate_utf8_bytes(
                redact_query_from_raw(trimmed).as_str(),
                MAX_NETWORK_LOG_URL_BYTES,
            );
        };
        let mut output = format!("{}://{host}", parsed.scheme());
        if let Some(port) = parsed.port() {
            if !is_default_port(parsed.scheme(), port) {
                output.push(':');
                output.push_str(port.to_string().as_str());
            }
        }
        if parsed.path().is_empty() {
            output.push('/');
        } else {
            output.push_str(parsed.path());
        }
        if let Some(query) = parsed.query() {
            let redacted = redact_query_pairs(query);
            if !redacted.is_empty() {
                output.push('?');
                output.push_str(redacted.as_str());
            }
        }
        return truncate_utf8_bytes(output.as_str(), MAX_NETWORK_LOG_URL_BYTES);
    }
    truncate_utf8_bytes(redact_query_from_raw(trimmed).as_str(), MAX_NETWORK_LOG_URL_BYTES)
}

fn redact_query_from_raw(raw: &str) -> String {
    let without_fragment = raw.split('#').next().unwrap_or_default();
    let Some((base, query)) = without_fragment.split_once('?') else {
        return without_fragment.to_owned();
    };
    let redacted = redact_query_pairs(query);
    if redacted.is_empty() {
        base.to_owned()
    } else {
        format!("{base}?{redacted}")
    }
}

fn redact_query_pairs(query: &str) -> String {
    query
        .split('&')
        .filter(|pair| !pair.trim().is_empty())
        .map(|pair| {
            let (raw_key, raw_value_opt) = pair
                .split_once('=')
                .map(|(key, value)| (key.trim(), Some(value)))
                .unwrap_or_else(|| (pair.trim(), None));
            if raw_key.is_empty() {
                return String::new();
            }
            let value = raw_value_opt.unwrap_or_default();
            let sanitized = if is_sensitive_query_key(raw_key) || contains_sensitive_material(value)
            {
                "<redacted>".to_owned()
            } else {
                truncate_utf8_bytes(value, 128)
            };
            if raw_value_opt.is_some() {
                format!("{raw_key}={sanitized}")
            } else {
                raw_key.to_owned()
            }
        })
        .filter(|pair| !pair.is_empty())
        .collect::<Vec<_>>()
        .join("&")
}

fn is_sensitive_query_key(raw_key: &str) -> bool {
    let key = raw_key.to_ascii_lowercase();
    matches!(
        key.as_str(),
        "token"
            | "access_token"
            | "id_token"
            | "refresh_token"
            | "code"
            | "state"
            | "auth"
            | "authorization"
            | "api_key"
            | "apikey"
            | "password"
            | "passwd"
            | "secret"
            | "signature"
            | "sig"
            | "session"
            | "session_id"
            | "jwt"
    ) || key.contains("token")
        || key.contains("secret")
        || key.contains("password")
}

fn is_default_port(scheme: &str, port: u16) -> bool {
    matches!((scheme, port), ("http", 80) | ("https", 443))
}

fn build_dom_snapshot(page_body: &str, max_bytes: usize) -> (String, bool) {
    let lines = collect_opening_tags(page_body)
        .iter()
        .enumerate()
        .map(|(index, tag)| build_dom_line(index + 1, tag.as_str()))
        .collect::<Vec<_>>();
    let content = lines.join("\n");
    truncate_utf8_bytes_with_flag(content.as_str(), max_bytes)
}

fn build_dom_line(index: usize, tag: &str) -> String {
    let tag_lower = tag.to_ascii_lowercase();
    let name = html_tag_name(tag_lower.as_str()).unwrap_or("unknown");
    let mut attributes = Vec::new();
    for attr_name in [
        "id",
        "class",
        "name",
        "role",
        "aria-label",
        "type",
        "href",
        "src",
        "action",
        "title",
        "alt",
        "placeholder",
    ] {
        let Some(value) = extract_attr_value(tag_lower.as_str(), attr_name) else {
            continue;
        };
        let sanitized = sanitize_snapshot_attribute(attr_name, value.as_str());
        if sanitized.is_empty() {
            continue;
        }
        attributes.push(format!("{attr_name}=\"{sanitized}\""));
    }
    if attributes.is_empty() {
        format!("{index:04} <{name}>")
    } else {
        format!("{index:04} <{name} {}>", attributes.join(" "))
    }
}

fn sanitize_snapshot_attribute(attr_name: &str, raw_value: &str) -> String {
    if raw_value.trim().is_empty() {
        return String::new();
    }
    let lower = attr_name.to_ascii_lowercase();
    if matches!(lower.as_str(), "value" | "password" | "token") {
        return "<redacted>".to_owned();
    }
    if lower == "href" || lower == "src" || lower == "action" {
        return normalize_url_with_redaction(raw_value);
    }
    if contains_sensitive_material(raw_value) {
        return "<redacted>".to_owned();
    }
    truncate_utf8_bytes(raw_value, 128)
}

fn build_accessibility_tree_snapshot(page_body: &str, max_bytes: usize) -> (String, bool) {
    let mut lines = Vec::new();
    for (index, tag) in collect_opening_tags(page_body).iter().enumerate() {
        if let Some(line) = build_accessibility_line(index + 1, tag.as_str()) {
            lines.push(line);
        }
    }
    let content = lines.join("\n");
    truncate_utf8_bytes_with_flag(content.as_str(), max_bytes)
}

fn build_accessibility_line(index: usize, tag: &str) -> Option<String> {
    let tag_lower = tag.to_ascii_lowercase();
    let role = accessibility_role_for_tag(tag_lower.as_str())?;
    let tag_name = html_tag_name(tag_lower.as_str()).unwrap_or("unknown");
    let name = accessibility_name_for_tag(tag_lower.as_str());
    let selector = accessibility_selector_for_tag(tag_lower.as_str());
    Some(format!("{index:04} role={role}; name={name}; tag={tag_name}; selector={selector}"))
}

fn accessibility_role_for_tag(tag_lower: &str) -> Option<String> {
    if let Some(explicit_role) = extract_attr_value(tag_lower, "role")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return Some(truncate_utf8_bytes(explicit_role.as_str(), 64));
    }
    let tag_name = html_tag_name(tag_lower)?;
    let inferred = match tag_name {
        "a" => "link",
        "button" => "button",
        "textarea" => "textbox",
        "select" => "combobox",
        "img" => "img",
        "form" => "form",
        "nav" => "navigation",
        "main" => "main",
        "header" => "banner",
        "footer" => "contentinfo",
        "ul" | "ol" => "list",
        "li" => "listitem",
        "table" => "table",
        "tr" => "row",
        "td" => "cell",
        "th" => "columnheader",
        "h1" | "h2" | "h3" | "h4" | "h5" | "h6" => "heading",
        "input" => match extract_attr_value(tag_lower, "type")
            .unwrap_or_else(|| "text".to_owned())
            .as_str()
        {
            "checkbox" => "checkbox",
            "radio" => "radio",
            "submit" | "button" | "reset" => "button",
            "search" | "email" | "url" | "tel" | "text" | "password" => "textbox",
            _ => "input",
        },
        _ => return None,
    };
    Some(inferred.to_owned())
}

fn accessibility_name_for_tag(tag_lower: &str) -> String {
    for attr_name in ["aria-label", "title", "alt", "placeholder", "name", "id"] {
        if let Some(value) = extract_attr_value(tag_lower, attr_name)
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
        {
            if contains_sensitive_material(value.as_str()) {
                return "<redacted>".to_owned();
            }
            return truncate_utf8_bytes(value.as_str(), 128);
        }
    }
    if let Some(href) = extract_attr_value(tag_lower, "href")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return normalize_url_with_redaction(href.as_str());
    }
    "-".to_owned()
}

fn accessibility_selector_for_tag(tag_lower: &str) -> String {
    if let Some(id) = extract_attr_value(tag_lower, "id")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return format!("#{}", truncate_utf8_bytes(id.as_str(), 96));
    }
    if let Some(name) = extract_attr_value(tag_lower, "name")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        return format!("[name={}]", truncate_utf8_bytes(name.as_str(), 96));
    }
    if let Some(class) = extract_attr_value(tag_lower, "class")
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
    {
        let first_class = class.split_ascii_whitespace().next().unwrap_or_default();
        if !first_class.is_empty() {
            return format!(".{}", truncate_utf8_bytes(first_class, 96));
        }
    }
    "-".to_owned()
}

fn build_visible_text_snapshot(page_body: &str, max_bytes: usize) -> (String, bool) {
    let without_scripts = strip_tag_block_case_insensitive(page_body, "script");
    let without_styles = strip_tag_block_case_insensitive(without_scripts.as_str(), "style");
    let without_comments = strip_html_comments(without_styles.as_str());
    let mut visible = String::new();
    let mut inside_tag = false;
    for character in without_comments.chars() {
        if character == '<' {
            inside_tag = true;
            visible.push(' ');
            continue;
        }
        if character == '>' {
            inside_tag = false;
            visible.push(' ');
            continue;
        }
        if !inside_tag {
            visible.push(character);
        }
    }
    let collapsed = visible.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_utf8_bytes_with_flag(collapsed.as_str(), max_bytes)
}

fn strip_tag_block_case_insensitive(input: &str, tag_name: &str) -> String {
    let mut output = String::new();
    let lower = input.to_ascii_lowercase();
    let open_pattern = format!("<{tag_name}");
    let close_pattern = format!("</{tag_name}>");
    let mut cursor = 0usize;
    while let Some(rel_open) = lower[cursor..].find(open_pattern.as_str()) {
        let open = cursor + rel_open;
        output.push_str(&input[cursor..open]);
        let Some(rel_close) = lower[open..].find(close_pattern.as_str()) else {
            cursor = input.len();
            break;
        };
        let close_start = open + rel_close;
        cursor = close_start + close_pattern.len();
    }
    if cursor < input.len() {
        output.push_str(&input[cursor..]);
    }
    output
}

fn strip_html_comments(input: &str) -> String {
    let mut output = String::new();
    let mut cursor = 0usize;
    while let Some(rel_start) = input[cursor..].find("<!--") {
        let start = cursor + rel_start;
        output.push_str(&input[cursor..start]);
        let Some(rel_end) = input[start + 4..].find("-->") else {
            cursor = input.len();
            break;
        };
        cursor = start + 4 + rel_end + 3;
    }
    if cursor < input.len() {
        output.push_str(&input[cursor..]);
    }
    output
}

fn collect_opening_tags(html: &str) -> Vec<String> {
    let mut tags = Vec::new();
    let mut cursor = 0usize;
    while let Some(rel_start) = html[cursor..].find('<') {
        let start = cursor + rel_start;
        let Some(rel_end) = html[start..].find('>') else {
            break;
        };
        let end = start + rel_end;
        let tag = &html[start..=end];
        if tag.starts_with("</") || tag.starts_with("<!") || tag.starts_with("<?") {
            cursor = end.saturating_add(1);
            continue;
        }
        let tag_lower = tag.to_ascii_lowercase();
        if matches!(html_tag_name(tag_lower.as_str()), Some("script" | "style")) {
            cursor = end.saturating_add(1);
            continue;
        }
        tags.push(tag.to_owned());
        cursor = end.saturating_add(1);
    }
    tags
}

fn truncate_utf8_bytes(raw: &str, max_bytes: usize) -> String {
    if raw.len() <= max_bytes {
        return raw.to_owned();
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !raw.is_char_boundary(boundary) {
        boundary -= 1;
    }
    raw[..boundary].to_owned()
}

fn parse_session_id(raw: Option<&str>) -> Result<String, String> {
    let value = raw.unwrap_or_default().trim();
    if value.is_empty() {
        return Err("session_id is required".to_owned());
    }
    validate_canonical_id(value).map_err(|error| format!("invalid session_id: {error}"))?;
    Ok(value.to_owned())
}

fn parse_session_id_from_proto(
    raw: Option<proto::palyra::common::v1::CanonicalId>,
) -> Result<String, String> {
    match raw {
        Some(value) => parse_session_id(Some(value.ulid.as_str())),
        None => parse_session_id(None),
    }
}

fn parse_tab_id(raw: Option<&str>) -> Result<String, String> {
    let value = raw.unwrap_or_default().trim();
    if value.is_empty() {
        return Err("tab_id is required".to_owned());
    }
    validate_canonical_id(value).map_err(|error| format!("invalid tab_id: {error}"))?;
    Ok(value.to_owned())
}

fn parse_tab_id_from_proto(
    raw: Option<proto::palyra::common::v1::CanonicalId>,
) -> Result<String, String> {
    match raw {
        Some(value) => parse_tab_id(Some(value.ulid.as_str())),
        None => parse_tab_id(None),
    }
}

fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn normalize_optional_string(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

fn sanitize_persistence_id(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        return None;
    }
    if value.len() > 128 {
        return None;
    }
    if value.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        Some(value.to_owned())
    } else {
        None
    }
}

fn map_to_sorted_map(map: &HashMap<String, String>) -> BTreeMap<String, String> {
    map.iter().map(|(key, value)| (key.clone(), value.clone())).collect()
}

fn nested_map_to_sorted_map(
    map: &HashMap<String, HashMap<String, String>>,
) -> BTreeMap<String, BTreeMap<String, String>> {
    map.iter().map(|(key, value)| (key.clone(), map_to_sorted_map(value))).collect()
}

fn tab_record_for_hash(tab: &BrowserTabRecord) -> BrowserTabRecordForHash {
    BrowserTabRecordForHash {
        tab_id: tab.tab_id.clone(),
        last_title: tab.last_title.clone(),
        last_url: tab.last_url.clone(),
        last_page_body: tab.last_page_body.clone(),
        scroll_x: tab.scroll_x,
        scroll_y: tab.scroll_y,
        typed_inputs: map_to_sorted_map(&tab.typed_inputs),
        network_log: tab.network_log.clone(),
    }
}

fn persisted_snapshot_hash(snapshot: &PersistedSessionSnapshot) -> Result<String> {
    let canonical = PersistedSessionSnapshotForHash {
        v: snapshot.v,
        principal: snapshot.principal.clone(),
        channel: snapshot.channel.clone(),
        tabs: snapshot.tabs.iter().map(tab_record_for_hash).collect(),
        tab_order: snapshot.tab_order.clone(),
        active_tab_id: snapshot.active_tab_id.clone(),
        permissions: snapshot.permissions.clone(),
        cookie_jar: nested_map_to_sorted_map(&snapshot.cookie_jar),
        storage_entries: nested_map_to_sorted_map(&snapshot.storage_entries),
        state_revision: snapshot.state_revision,
        saved_at_unix_ms: snapshot.saved_at_unix_ms,
    };
    let bytes = serde_json::to_vec(&canonical)
        .context("failed to serialize persisted browser state snapshot hash payload")?;
    Ok(sha256_hex(bytes.as_slice()))
}

fn persisted_snapshot_legacy_hash(snapshot: &PersistedSessionSnapshot) -> Result<String> {
    let legacy = PersistedSessionSnapshotLegacyForHash {
        v: snapshot.v,
        principal: snapshot.principal.clone(),
        channel: snapshot.channel.clone(),
        tabs: snapshot.tabs.clone(),
        tab_order: snapshot.tab_order.clone(),
        active_tab_id: snapshot.active_tab_id.clone(),
        permissions: snapshot.permissions.clone(),
        cookie_jar: snapshot.cookie_jar.clone(),
        storage_entries: snapshot.storage_entries.clone(),
        saved_at_unix_ms: snapshot.saved_at_unix_ms,
    };
    let bytes = serde_json::to_vec(&legacy)
        .context("failed to serialize legacy persisted browser state snapshot hash payload")?;
    Ok(sha256_hex(bytes.as_slice()))
}

fn validate_restored_snapshot_against_profile(
    snapshot: &PersistedSessionSnapshot,
    raw_hash_sha256: Option<&str>,
    profile: &BrowserProfileRecord,
) -> Result<()> {
    if snapshot.state_revision < profile.state_revision {
        anyhow::bail!(
            "snapshot revision {} is older than profile revision {}",
            snapshot.state_revision,
            profile.state_revision
        );
    }
    let Some(expected_hash) = profile.state_hash_sha256.as_deref() else {
        return Ok(());
    };
    if raw_hash_sha256.is_some_and(|raw_hash| raw_hash == expected_hash) {
        return Ok(());
    }
    let current_hash = persisted_snapshot_hash(snapshot)?;
    if current_hash == expected_hash {
        return Ok(());
    }
    if snapshot.state_revision == 0 {
        let legacy_hash = persisted_snapshot_legacy_hash(snapshot)?;
        if legacy_hash == expected_hash {
            return Ok(());
        }
    }
    anyhow::bail!("snapshot hash mismatch for profile '{}'", profile.profile_id);
}

fn permission_setting_to_proto(value: PermissionSettingInternal) -> i32 {
    match value {
        PermissionSettingInternal::Deny => 1,
        PermissionSettingInternal::Allow => 2,
    }
}

fn permission_setting_from_proto(value: i32) -> Option<PermissionSettingInternal> {
    match value {
        1 => Some(PermissionSettingInternal::Deny),
        2 => Some(PermissionSettingInternal::Allow),
        _ => None,
    }
}

fn persist_session_snapshot(
    store: &PersistedStateStore,
    session: &BrowserSessionRecord,
) -> Result<()> {
    if !session.persistence.enabled {
        return Ok(());
    }
    let Some(persistence_id) = session.persistence.persistence_id.as_ref() else {
        anyhow::bail!("state persistence is enabled but persistence_id is missing");
    };
    let mut tabs = session
        .tab_order
        .iter()
        .filter_map(|tab_id| session.tabs.get(tab_id.as_str()).cloned())
        .collect::<Vec<_>>();
    for (tab_id, tab) in &session.tabs {
        if !tabs.iter().any(|entry| entry.tab_id == *tab_id) {
            tabs.push(tab.clone());
        }
    }
    let state_revision = next_profile_state_revision(store, session.profile_id.as_deref())?;
    let snapshot = PersistedSessionSnapshot {
        v: CANONICAL_PROTOCOL_MAJOR,
        principal: session.principal.clone(),
        channel: session.channel.clone(),
        tabs,
        tab_order: session.tab_order.clone(),
        active_tab_id: session.active_tab_id.clone(),
        permissions: session.permissions.clone(),
        cookie_jar: session.cookie_jar.clone(),
        storage_entries: session.storage_entries.clone(),
        state_revision,
        saved_at_unix_ms: current_unix_ms(),
    };
    let snapshot_hash = persisted_snapshot_hash(&snapshot)?;
    store.save_snapshot(persistence_id.as_str(), session.profile_id.as_deref(), &snapshot)?;
    if let Some(profile_id) = session.profile_id.as_ref() {
        if let Err(error) = update_profile_state_metadata(
            store,
            profile_id.as_str(),
            PROFILE_RECORD_SCHEMA_VERSION,
            state_revision,
            snapshot_hash.as_str(),
        ) {
            warn!(
                profile_id = profile_id.as_str(),
                error = %error,
                "failed to update browser profile state metadata after snapshot persist"
            );
        }
    }
    Ok(())
}

fn persist_session_after_mutation(
    runtime: &BrowserRuntimeState,
    session_for_persist: Option<BrowserSessionRecord>,
    operation: &str,
) -> Result<()> {
    if let (Some(store), Some(session)) = (runtime.state_store.as_ref(), session_for_persist) {
        if session.persistence.enabled {
            persist_session_snapshot(store, &session)
                .with_context(|| format!("failed to persist state after {operation}"))?;
        }
    }
    Ok(())
}

fn map_persist_error_to_status(error: anyhow::Error) -> Status {
    Status::internal(error.to_string())
}

fn cookie_header_for_url(session: &BrowserSessionRecord, raw_url: &str) -> Option<String> {
    let domain = Url::parse(raw_url).ok()?.host_str()?.to_ascii_lowercase();
    let cookies = session.cookie_jar.get(domain.as_str())?;
    if cookies.is_empty() {
        return None;
    }
    let mut pairs =
        cookies.iter().map(|(name, value)| format!("{name}={value}")).collect::<Vec<_>>();
    pairs.sort();
    Some(pairs.join("; "))
}

fn parse_set_cookie_update(domain: &str, raw_set_cookie: &str) -> Option<CookieUpdate> {
    let normalized_domain = domain.trim().trim_matches('.').to_ascii_lowercase();
    if normalized_domain.is_empty() {
        return None;
    }
    let first_pair = raw_set_cookie.split(';').next()?.trim();
    let (name, value) = first_pair.split_once('=')?;
    let name = name.trim().to_ascii_lowercase();
    if name.is_empty() {
        return None;
    }
    Some(CookieUpdate {
        domain: normalized_domain,
        name,
        value: truncate_utf8_bytes(value.trim(), 1024),
    })
}

fn apply_cookie_updates(session: &mut BrowserSessionRecord, updates: &[CookieUpdate]) {
    for update in updates {
        if update.domain.is_empty() || update.name.is_empty() {
            continue;
        }
        if update.value.is_empty() {
            if let Some(domain_cookies) = session.cookie_jar.get_mut(update.domain.as_str()) {
                domain_cookies.remove(update.name.as_str());
                if domain_cookies.is_empty() {
                    session.cookie_jar.remove(update.domain.as_str());
                }
            }
            continue;
        }
        if !session.cookie_jar.contains_key(update.domain.as_str())
            && session.cookie_jar.len() >= MAX_COOKIE_DOMAINS_PER_SESSION
        {
            continue;
        }
        let domain_cookies = session.cookie_jar.entry(update.domain.clone()).or_default();
        if !domain_cookies.contains_key(update.name.as_str())
            && domain_cookies.len() >= MAX_COOKIES_PER_DOMAIN
        {
            continue;
        }
        domain_cookies.insert(update.name.clone(), update.value.clone());
    }
}

fn apply_storage_entry_update(
    session: &mut BrowserSessionRecord,
    origin: &str,
    key: &str,
    value: &str,
    clear_existing: bool,
) {
    let origin = origin.trim();
    let key = key.trim();
    if origin.is_empty() || key.is_empty() {
        return;
    }
    if !session.storage_entries.contains_key(origin)
        && session.storage_entries.len() >= MAX_STORAGE_ORIGINS_PER_SESSION
    {
        return;
    }
    let storage = session.storage_entries.entry(origin.to_owned()).or_default();
    if !storage.contains_key(key) && storage.len() >= MAX_STORAGE_ENTRIES_PER_ORIGIN {
        return;
    }
    if clear_existing {
        storage.insert(key.to_owned(), truncate_utf8_bytes(value, MAX_STORAGE_ENTRY_VALUE_BYTES));
        return;
    }
    let existing = storage.entry(key.to_owned()).or_default();
    let mut combined = String::with_capacity(existing.len() + value.len());
    combined.push_str(existing.as_str());
    combined.push_str(value);
    *existing = truncate_utf8_bytes(combined.as_str(), MAX_STORAGE_ENTRY_VALUE_BYTES);
}

fn clamp_cookie_jar(
    cookie_jar: HashMap<String, HashMap<String, String>>,
) -> HashMap<String, HashMap<String, String>> {
    let mut clamped = HashMap::new();
    for (domain, cookies) in cookie_jar {
        if domain.trim().is_empty() {
            continue;
        }
        if clamped.len() >= MAX_COOKIE_DOMAINS_PER_SESSION {
            break;
        }
        let mut clamped_cookies = HashMap::new();
        for (name, value) in cookies {
            if name.trim().is_empty() {
                continue;
            }
            if clamped_cookies.len() >= MAX_COOKIES_PER_DOMAIN {
                break;
            }
            clamped_cookies.insert(name, truncate_utf8_bytes(value.as_str(), 1024));
        }
        if !clamped_cookies.is_empty() {
            clamped.insert(domain, clamped_cookies);
        }
    }
    clamped
}

fn clamp_storage_entries(
    storage_entries: HashMap<String, HashMap<String, String>>,
) -> HashMap<String, HashMap<String, String>> {
    let mut clamped = HashMap::new();
    for (origin, entries) in storage_entries {
        if origin.trim().is_empty() {
            continue;
        }
        if clamped.len() >= MAX_STORAGE_ORIGINS_PER_SESSION {
            break;
        }
        let mut clamped_entries = HashMap::new();
        for (key, value) in entries {
            if key.trim().is_empty() {
                continue;
            }
            if clamped_entries.len() >= MAX_STORAGE_ENTRIES_PER_ORIGIN {
                break;
            }
            clamped_entries
                .insert(key, truncate_utf8_bytes(value.as_str(), MAX_STORAGE_ENTRY_VALUE_BYTES));
        }
        if !clamped_entries.is_empty() {
            clamped.insert(origin, clamped_entries);
        }
    }
    clamped
}

fn action_log_entry_to_proto(
    entry: &BrowserActionLogEntryInternal,
) -> browser_v1::BrowserActionLogEntry {
    browser_v1::BrowserActionLogEntry {
        v: CANONICAL_PROTOCOL_MAJOR,
        action_id: entry.action_id.clone(),
        action_name: truncate_utf8_bytes(entry.action_name.as_str(), MAX_INSPECT_ACTION_NAME_BYTES),
        selector: truncate_utf8_bytes(entry.selector.as_str(), MAX_INSPECT_ACTION_SELECTOR_BYTES),
        success: entry.success,
        outcome: sanitize_debug_text(entry.outcome.as_str(), MAX_INSPECT_ACTION_OUTCOME_BYTES),
        error: sanitize_debug_text(entry.error.as_str(), MAX_INSPECT_ACTION_ERROR_BYTES),
        started_at_unix_ms: entry.started_at_unix_ms,
        completed_at_unix_ms: entry.completed_at_unix_ms,
        attempts: entry.attempts,
        page_url: normalize_url_with_redaction(entry.page_url.as_str()),
    }
}

fn cookie_jar_to_proto(
    cookie_jar: &HashMap<String, HashMap<String, String>>,
) -> Vec<browser_v1::SessionCookieDomain> {
    let mut domains = cookie_jar.iter().collect::<Vec<_>>();
    domains.sort_by(|left, right| left.0.cmp(right.0));
    domains
        .into_iter()
        .filter_map(|(domain, cookies)| {
            let mut entries = cookies.iter().collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.cmp(right.0));
            let cookies = entries
                .into_iter()
                .map(|(name, value)| browser_v1::SessionCookieEntry {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    name: truncate_utf8_bytes(name.as_str(), 128),
                    value: sanitize_debug_map_value(
                        name.as_str(),
                        value.as_str(),
                        MAX_INSPECT_COOKIE_VALUE_BYTES,
                    ),
                })
                .collect::<Vec<_>>();
            if cookies.is_empty() {
                None
            } else {
                Some(browser_v1::SessionCookieDomain {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    domain: truncate_utf8_bytes(domain.as_str(), 256),
                    cookies,
                })
            }
        })
        .collect()
}

fn storage_entries_to_proto(
    storage_entries: &HashMap<String, HashMap<String, String>>,
) -> Vec<browser_v1::SessionStorageOrigin> {
    let mut origins = storage_entries.iter().collect::<Vec<_>>();
    origins.sort_by(|left, right| left.0.cmp(right.0));
    origins
        .into_iter()
        .filter_map(|(origin, entries)| {
            let mut values = entries.iter().collect::<Vec<_>>();
            values.sort_by(|left, right| left.0.cmp(right.0));
            let entries = values
                .into_iter()
                .map(|(key, value)| browser_v1::SessionStorageEntry {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    key: truncate_utf8_bytes(key.as_str(), 256),
                    value: sanitize_debug_map_value(
                        key.as_str(),
                        value.as_str(),
                        MAX_INSPECT_STORAGE_VALUE_BYTES,
                    ),
                })
                .collect::<Vec<_>>();
            if entries.is_empty() {
                None
            } else {
                Some(browser_v1::SessionStorageOrigin {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    origin: truncate_utf8_bytes(origin.as_str(), MAX_NETWORK_LOG_URL_BYTES),
                    entries,
                })
            }
        })
        .collect()
}

fn estimate_cookie_payload_bytes(domains: &[browser_v1::SessionCookieDomain]) -> usize {
    domains
        .iter()
        .map(|domain| {
            domain.domain.len()
                + domain
                    .cookies
                    .iter()
                    .map(|cookie| cookie.name.len() + cookie.value.len() + 16)
                    .sum::<usize>()
                + 24
        })
        .sum::<usize>()
        + 2
}

fn truncate_cookie_payload(
    domains: &mut Vec<browser_v1::SessionCookieDomain>,
    max_payload_bytes: usize,
) -> bool {
    let mut truncated = false;
    while !domains.is_empty()
        && estimate_cookie_payload_bytes(domains.as_slice()) > max_payload_bytes
    {
        if let Some(domain) = domains.last_mut() {
            domain.cookies.pop();
            if domain.cookies.is_empty() {
                domains.pop();
            }
        }
        truncated = true;
    }
    truncated
}

fn estimate_storage_payload_bytes(origins: &[browser_v1::SessionStorageOrigin]) -> usize {
    origins
        .iter()
        .map(|origin| {
            origin.origin.len()
                + origin
                    .entries
                    .iter()
                    .map(|entry| entry.key.len() + entry.value.len() + 16)
                    .sum::<usize>()
                + 24
        })
        .sum::<usize>()
        + 2
}

fn truncate_storage_payload(
    origins: &mut Vec<browser_v1::SessionStorageOrigin>,
    max_payload_bytes: usize,
) -> bool {
    let mut truncated = false;
    while !origins.is_empty()
        && estimate_storage_payload_bytes(origins.as_slice()) > max_payload_bytes
    {
        if let Some(origin) = origins.last_mut() {
            origin.entries.pop();
            if origin.entries.is_empty() {
                origins.pop();
            }
        }
        truncated = true;
    }
    truncated
}

fn url_origin_key(raw_url: &str) -> Option<String> {
    let url = Url::parse(raw_url).ok()?;
    let host = url.host_str()?.to_ascii_lowercase();
    let mut origin = format!("{}://{host}", url.scheme());
    if let Some(port) = url.port() {
        if !is_default_port(url.scheme(), port) {
            origin.push(':');
            origin.push_str(port.to_string().as_str());
        }
    }
    Some(origin)
}

#[derive(Debug, Clone)]
struct ActionSessionSnapshot {
    budget: SessionBudget,
    page_body: String,
    allow_downloads: bool,
    current_url: Option<String>,
    allow_private_targets: bool,
    profile_id: Option<String>,
    private_profile: bool,
}

#[derive(Debug, Clone, Copy)]
struct FinalizeActionRequest<'a> {
    action_name: &'a str,
    selector: &'a str,
    success: bool,
    outcome: &'a str,
    error: &'a str,
    started_at_unix_ms: u64,
    attempts: u32,
    capture_failure_screenshot: bool,
    max_failure_screenshot_bytes: u64,
}

async fn consume_action_budget_and_snapshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    require_page_body: bool,
) -> Result<ActionSessionSnapshot, String> {
    if matches!(runtime.engine_mode, BrowserEngineMode::Chromium) {
        let active_tab_id = {
            let sessions = runtime.sessions.lock().await;
            let Some(session) = sessions.get(session_id) else {
                return Err("session_not_found".to_owned());
            };
            session.active_tab_id.clone()
        };
        chromium_refresh_tab_snapshot(runtime, session_id, active_tab_id.as_str()).await?;
    }

    let mut sessions = runtime.sessions.lock().await;
    let Some(session) = sessions.get_mut(session_id) else {
        return Err("session_not_found".to_owned());
    };
    session.last_active = Instant::now();
    enforce_action_domain_allowlist(session)?;
    let page_body = session
        .active_tab()
        .map(|tab| tab.last_page_body.clone())
        .ok_or_else(|| "active_tab_not_found".to_owned())?;
    if require_page_body && page_body.trim().is_empty() {
        return Err("navigate must succeed before performing this browser action".to_owned());
    }

    let now = Instant::now();
    let rate_window = Duration::from_millis(session.budget.action_rate_window_ms.max(1));
    while let Some(front) = session.action_window.front().copied() {
        if now.saturating_duration_since(front) > rate_window {
            session.action_window.pop_front();
        } else {
            break;
        }
    }
    if session.action_count >= session.budget.max_actions_per_session {
        return Err(format!(
            "session action budget exceeded ({} >= {})",
            session.action_count, session.budget.max_actions_per_session
        ));
    }
    if session.action_window.len() as u64 >= session.budget.max_actions_per_window {
        return Err(format!(
            "session action rate limit exceeded ({} per {}ms)",
            session.budget.max_actions_per_window, session.budget.action_rate_window_ms
        ));
    }
    session.action_count = session.action_count.saturating_add(1);
    session.action_window.push_back(now);

    Ok(ActionSessionSnapshot {
        budget: session.budget.clone(),
        page_body,
        allow_downloads: session.allow_downloads,
        current_url: session.active_tab().and_then(|tab| tab.last_url.clone()),
        allow_private_targets: session.allow_private_targets,
        profile_id: session.profile_id.clone(),
        private_profile: session.private_profile,
    })
}

fn enforce_action_domain_allowlist(session: &BrowserSessionRecord) -> Result<(), String> {
    if session.action_allowed_domains.is_empty() {
        return Ok(());
    }
    let Some(current_url) = session.active_tab().and_then(|tab| tab.last_url.as_deref()) else {
        return Err(
            "action domain allowlist is configured but session has no active URL".to_owned()
        );
    };
    let current_host = Url::parse(current_url)
        .ok()
        .and_then(|url| url.host_str().map(|value| value.to_ascii_lowercase()))
        .ok_or_else(|| "failed to resolve host for action domain allowlist check".to_owned())?;
    if session.action_allowed_domains.iter().any(|domain| {
        current_host == *domain || current_host.ends_with(format!(".{domain}").as_str())
    }) {
        return Ok(());
    }
    Err(format!("current page host '{current_host}' is blocked by action domain allowlist"))
}

fn normalize_action_allowed_domains(values: &[String]) -> Vec<String> {
    let mut domains = values
        .iter()
        .filter_map(|value| normalize_single_allowed_domain(value.as_str()))
        .collect::<Vec<_>>();
    domains.sort();
    domains.dedup();
    domains
}

fn normalize_single_allowed_domain(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let from_url = Url::parse(trimmed).ok().and_then(|url| url.host_str().map(str::to_owned));
    let value = from_url.unwrap_or_else(|| {
        trimmed
            .split('/')
            .next()
            .unwrap_or_default()
            .split(':')
            .next()
            .unwrap_or_default()
            .to_owned()
    });
    let normalized = value.trim().trim_matches('.').to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }
    if normalized.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-') {
        Some(normalized)
    } else {
        None
    }
}

async fn finalize_session_action(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    request: FinalizeActionRequest<'_>,
) -> (Option<browser_v1::BrowserActionLogEntry>, Vec<u8>, String) {
    let mut sessions = runtime.sessions.lock().await;
    let Some(session) = sessions.get_mut(session_id) else {
        return (None, Vec::new(), String::new());
    };
    let entry = BrowserActionLogEntryInternal {
        action_id: Ulid::new().to_string(),
        action_name: request.action_name.to_owned(),
        selector: request.selector.to_owned(),
        success: request.success,
        outcome: request.outcome.to_owned(),
        error: request.error.to_owned(),
        started_at_unix_ms: request.started_at_unix_ms,
        completed_at_unix_ms: current_unix_ms(),
        attempts: request.attempts,
        page_url: session.active_tab().and_then(|tab| tab.last_url.clone()).unwrap_or_default(),
    };
    session.last_active = Instant::now();
    session.action_log.push_back(entry.clone());
    while session.action_log.len() > session.budget.max_action_log_entries {
        session.action_log.pop_front();
    }
    let (failure_screenshot_bytes, failure_screenshot_mime_type) =
        if !request.success && request.capture_failure_screenshot {
            let max_bytes = if request.max_failure_screenshot_bytes == 0 {
                session.budget.max_screenshot_bytes
            } else {
                request.max_failure_screenshot_bytes.min(session.budget.max_screenshot_bytes)
            };
            if (ONE_BY_ONE_PNG.len() as u64) <= max_bytes {
                (ONE_BY_ONE_PNG.to_vec(), "image/png".to_owned())
            } else {
                (Vec::new(), String::new())
            }
        } else {
            (Vec::new(), String::new())
        };
    (
        Some(browser_v1::BrowserActionLogEntry {
            v: CANONICAL_PROTOCOL_MAJOR,
            action_id: entry.action_id,
            action_name: entry.action_name,
            selector: entry.selector,
            success: entry.success,
            outcome: entry.outcome,
            error: entry.error,
            started_at_unix_ms: entry.started_at_unix_ms,
            completed_at_unix_ms: entry.completed_at_unix_ms,
            attempts: entry.attempts,
            page_url: entry.page_url,
        }),
        failure_screenshot_bytes,
        failure_screenshot_mime_type,
    )
}

fn find_matching_html_tag(selector: &str, html: &str) -> Option<String> {
    let selector = selector.trim();
    if selector.is_empty() {
        return None;
    }
    let selector_lower = selector.to_ascii_lowercase();
    let mut cursor = 0usize;
    while let Some(rel_start) = html[cursor..].find('<') {
        let start = cursor + rel_start;
        let Some(rel_end) = html[start..].find('>') else {
            break;
        };
        let end = start + rel_end;
        let tag = &html[start..=end];
        if tag.starts_with("</") {
            cursor = end.saturating_add(1);
            continue;
        }
        if html_tag_matches_selector(tag, selector, selector_lower.as_str()) {
            return Some(tag.to_owned());
        }
        cursor = end.saturating_add(1);
    }
    None
}

fn html_tag_matches_selector(tag: &str, selector: &str, selector_lower: &str) -> bool {
    let tag_lower = tag.to_ascii_lowercase();
    if let Some(id) = selector.strip_prefix('#') {
        return has_attr_value(tag_lower.as_str(), "id", id.trim().to_ascii_lowercase().as_str());
    }
    if let Some(class) = selector.strip_prefix('.') {
        let class = class.trim().to_ascii_lowercase();
        let Some(value) = extract_attr_value(tag_lower.as_str(), "class") else {
            return false;
        };
        return value
            .split_ascii_whitespace()
            .any(|token| token.eq_ignore_ascii_case(class.as_str()));
    }
    if selector.starts_with('[') && selector.ends_with(']') {
        let inner = selector[1..selector.len().saturating_sub(1)].trim();
        if let Some(value) = inner.strip_prefix("name=") {
            let value = value.trim().trim_matches('"').trim_matches('\'').to_ascii_lowercase();
            return has_attr_value(tag_lower.as_str(), "name", value.as_str());
        }
        return false;
    }
    html_tag_name(tag_lower.as_str())
        .map(|name| name.eq_ignore_ascii_case(selector_lower))
        .unwrap_or(false)
}

fn has_attr_value(tag_lower: &str, attr_name: &str, expected_value_lower: &str) -> bool {
    extract_attr_value(tag_lower, attr_name)
        .map(|value| value.eq_ignore_ascii_case(expected_value_lower))
        .unwrap_or(false)
}

fn extract_attr_value(tag_lower: &str, attr_name: &str) -> Option<String> {
    let needle = format!("{attr_name}=");
    let start = tag_lower.find(needle.as_str())?;
    parse_attr_value(&tag_lower[start + needle.len()..])
}

fn extract_attr_value_case_insensitive(tag: &str, attr_name: &str) -> Option<String> {
    let tag_lower = tag.to_ascii_lowercase();
    let needle = format!("{}=", attr_name.to_ascii_lowercase());
    let start = tag_lower.find(needle.as_str())?;
    parse_attr_value(&tag[start + needle.len()..])
}

fn parse_attr_value(raw_value: &str) -> Option<String> {
    let value = raw_value.trim_start();
    if let Some(stripped) = value.strip_prefix('"') {
        let end = stripped.find('"')?;
        return Some(stripped[..end].to_owned());
    }
    if let Some(stripped) = value.strip_prefix('\'') {
        let end = stripped.find('\'')?;
        return Some(stripped[..end].to_owned());
    }
    let end = value
        .find(|ch: char| ch.is_ascii_whitespace() || ch == '>' || ch == '/')
        .unwrap_or(value.len());
    Some(value[..end].to_owned())
}

fn html_tag_name(tag_lower: &str) -> Option<&str> {
    let trimmed = tag_lower.trim_start_matches('<').trim_start();
    let end = trimmed
        .find(|ch: char| ch.is_ascii_whitespace() || ch == '>' || ch == '/')
        .unwrap_or(trimmed.len());
    if end == 0 {
        None
    } else {
        Some(&trimmed[..end])
    }
}

fn is_typable_tag(tag: &str) -> bool {
    let tag_lower = tag.to_ascii_lowercase();
    matches!(html_tag_name(tag_lower.as_str()), Some("input" | "textarea"))
}

fn is_download_like_tag(tag: &str) -> bool {
    let tag_lower = tag.to_ascii_lowercase();
    if html_tag_name(tag_lower.as_str()) != Some("a") {
        return false;
    }
    if tag_lower.contains(" download")
        || tag_lower.contains(" download=")
        || tag_lower.ends_with("download>")
    {
        return true;
    }
    let Some(href) = extract_attr_value(tag_lower.as_str(), "href") else {
        return false;
    };
    let href = href.split('?').next().unwrap_or_default();
    let href = href.to_ascii_lowercase();
    [
        ".zip", ".gz", ".tar", ".7z", ".rar", ".pdf", ".csv", ".json", ".txt", ".doc", ".docx",
        ".xls", ".xlsx", ".ppt", ".pptx", ".exe", ".msi",
    ]
    .iter()
    .any(|suffix| href.ends_with(suffix))
}
#[cfg(test)]
#[path = "support/tests.rs"]
mod tests;
