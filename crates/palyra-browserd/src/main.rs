#[cfg(windows)]
use std::process::Command;
use std::{
    collections::{HashMap, VecDeque},
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

use anyhow::{Context, Result};
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use base64::Engine as _;
use clap::Parser;
use headless_chrome::{
    browser::tab::RequestPausedDecision,
    protocol::cdp::{Fetch, Network, Page},
    Browser as HeadlessBrowser, LaunchOptionsBuilder, Tab as HeadlessTab,
};
use palyra_common::{
    build_metadata, health_response, netguard, parse_daemon_bind_socket, validate_canonical_id,
    HealthResponse, CANONICAL_PROTOCOL_MAJOR,
};
use reqwest::{redirect::Policy, Url};
use ring::{
    aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305},
    digest::{Context as DigestContext, SHA256},
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tokio::time::{interval, MissedTickBehavior};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

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

use proto::palyra::browser::v1 as browser_v1;

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
const MAX_NETWORK_LOG_HEADER_COUNT: usize = 24;
const MAX_NETWORK_LOG_HEADER_VALUE_BYTES: usize = 256;
const MAX_NETWORK_LOG_URL_BYTES: usize = 2 * 1024;
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
const CHROMIUM_REMOTE_IP_GUARD_HANDLER_NAME: &str = "palyra.security.remote_ip_guard";
const DNS_VALIDATION_CACHE_MAX_ENTRIES: usize = 512;
const DNS_VALIDATION_CACHE_TTL: Duration = Duration::from_secs(30);
const DNS_VALIDATION_NEGATIVE_TTL: Duration = Duration::from_secs(10);
const DNS_VALIDATION_METRICS_LOG_INTERVAL: u64 = 256;
#[cfg(windows)]
const WINDOWS_SYSTEM_SID: &str = "S-1-5-18";
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserProfileRecord {
    profile_id: String,
    principal: String,
    name: String,
    theme_color: Option<String>,
    created_at_unix_ms: u64,
    updated_at_unix_ms: u64,
    last_used_unix_ms: u64,
    persistence_enabled: bool,
    private_profile: bool,
    state_schema_version: u32,
    #[serde(default)]
    state_revision: u64,
    state_hash_sha256: Option<String>,
    record_hash_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserProfileRegistryDocument {
    v: u32,
    profiles: Vec<BrowserProfileRecord>,
    active_profile_by_principal: HashMap<String, String>,
}

impl Default for BrowserProfileRegistryDocument {
    fn default() -> Self {
        Self {
            v: PROFILE_REGISTRY_SCHEMA_VERSION,
            profiles: Vec::new(),
            active_profile_by_principal: HashMap::new(),
        }
    }
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
        for mut tab in snapshot.tabs {
            if validate_canonical_id(tab.tab_id.as_str()).is_ok() {
                tab.network_log = tab
                    .network_log
                    .into_iter()
                    .take(self.budget.max_network_log_entries)
                    .collect::<VecDeque<_>>();
                while tab
                    .network_log
                    .iter()
                    .map(estimate_network_log_entry_internal_bytes)
                    .sum::<usize>()
                    > self.budget.max_network_log_bytes as usize
                {
                    if tab.network_log.pop_front().is_none() {
                        break;
                    }
                }
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
            for tab_id in tabs.keys() {
                if !tab_order.iter().any(|existing| existing == tab_id) {
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
        self.cookie_jar = snapshot.cookie_jar;
        self.storage_entries = snapshot.storage_entries;
    }
}

#[derive(Debug, Clone)]
struct DownloadArtifactRecord {
    artifact_id: String,
    session_id: String,
    profile_id: Option<String>,
    source_url: String,
    file_name: String,
    mime_type: String,
    size_bytes: u64,
    sha256: String,
    created_at_unix_ms: u64,
    quarantined: bool,
    quarantine_reason: String,
    storage_path: PathBuf,
}

#[derive(Debug)]
struct DownloadSandboxSession {
    root_dir: TempDir,
    used_bytes: u64,
    max_bytes: u64,
    artifacts: VecDeque<DownloadArtifactRecord>,
}

impl DownloadSandboxSession {
    fn new() -> Result<Self, String> {
        let root_dir = tempfile::Builder::new()
            .prefix("palyra-browserd-downloads-")
            .tempdir()
            .map_err(|error| format!("failed to allocate download sandbox: {error}"))?;
        fs::create_dir_all(root_dir.path().join(DOWNLOADS_DIR_ALLOWLIST))
            .map_err(|error| format!("failed to initialize download allowlist dir: {error}"))?;
        fs::create_dir_all(root_dir.path().join(DOWNLOADS_DIR_QUARANTINE))
            .map_err(|error| format!("failed to initialize download quarantine dir: {error}"))?;
        Ok(Self {
            root_dir,
            used_bytes: 0,
            max_bytes: DOWNLOAD_MAX_TOTAL_BYTES_PER_SESSION,
            artifacts: VecDeque::new(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedSessionSnapshot {
    v: u32,
    principal: String,
    channel: Option<String>,
    tabs: Vec<BrowserTabRecord>,
    tab_order: Vec<String>,
    active_tab_id: String,
    permissions: SessionPermissionsInternal,
    cookie_jar: HashMap<String, HashMap<String, String>>,
    storage_entries: HashMap<String, HashMap<String, String>>,
    #[serde(default)]
    state_revision: u64,
    saved_at_unix_ms: u64,
}

#[derive(Debug, Clone, Serialize)]
struct PersistedSessionSnapshotLegacyForHash {
    v: u32,
    principal: String,
    channel: Option<String>,
    tabs: Vec<BrowserTabRecord>,
    tab_order: Vec<String>,
    active_tab_id: String,
    permissions: SessionPermissionsInternal,
    cookie_jar: HashMap<String, HashMap<String, String>>,
    storage_entries: HashMap<String, HashMap<String, String>>,
    saved_at_unix_ms: u64,
}

#[derive(Debug, Clone)]
struct PersistedStateStore {
    root_dir: PathBuf,
    key: [u8; STATE_KEY_LEN],
}

struct ChromiumSessionState {
    browser: Arc<HeadlessBrowser>,
    tabs: HashMap<String, Arc<HeadlessTab>>,
    security_incident: Arc<std::sync::Mutex<Option<String>>>,
    _profile_dir: TempDir,
}

#[derive(Debug, Clone)]
struct ResolvedHostAddresses {
    addresses: Vec<IpAddr>,
    blocked_for_default_policy: bool,
}

impl ResolvedHostAddresses {
    fn from_addresses(addresses: Vec<IpAddr>) -> Result<Self, String> {
        if addresses.is_empty() {
            return Err("DNS resolution returned no addresses".to_owned());
        }
        let blocked_for_default_policy =
            addresses.iter().copied().any(netguard::is_private_or_local_ip);
        Ok(Self { addresses, blocked_for_default_policy })
    }
}

#[derive(Debug, Clone)]
enum DnsCacheResolution {
    Resolved(ResolvedHostAddresses),
    NxDomain,
}

#[derive(Debug, Clone)]
struct DnsValidationCacheEntry {
    resolution: DnsCacheResolution,
    expires_at: Instant,
    last_access_tick: u64,
}

#[derive(Debug)]
struct DnsValidationCache {
    entries: HashMap<String, DnsValidationCacheEntry>,
    max_entries: usize,
    ttl: Duration,
    negative_ttl: Duration,
    next_access_tick: u64,
}

impl DnsValidationCache {
    fn new(max_entries: usize, ttl: Duration, negative_ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries: max_entries.max(1),
            ttl: ttl.max(Duration::from_secs(1)),
            negative_ttl: negative_ttl.max(Duration::from_secs(1)),
            next_access_tick: 0,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn lookup(&mut self, key: &str, now: Instant) -> Option<DnsCacheResolution> {
        let mut should_remove = false;
        let mut output = None;
        let access_tick = self.next_access_tick();
        if let Some(entry) = self.entries.get_mut(key) {
            if now > entry.expires_at {
                should_remove = true;
            } else {
                entry.last_access_tick = access_tick;
                output = Some(entry.resolution.clone());
            }
        }
        if should_remove {
            self.entries.remove(key);
        }
        output
    }

    fn insert_resolved(&mut self, key: String, resolved: ResolvedHostAddresses, now: Instant) {
        self.remove_expired(now);
        let last_access_tick = self.next_access_tick();
        self.entries.insert(
            key,
            DnsValidationCacheEntry {
                resolution: DnsCacheResolution::Resolved(resolved),
                expires_at: now + self.ttl,
                last_access_tick,
            },
        );
        self.prune_lru();
    }

    fn insert_nxdomain(&mut self, key: String, now: Instant) {
        self.remove_expired(now);
        let last_access_tick = self.next_access_tick();
        self.entries.insert(
            key,
            DnsValidationCacheEntry {
                resolution: DnsCacheResolution::NxDomain,
                expires_at: now + self.negative_ttl,
                last_access_tick,
            },
        );
        self.prune_lru();
    }

    fn next_access_tick(&mut self) -> u64 {
        self.next_access_tick = self.next_access_tick.saturating_add(1);
        self.next_access_tick
    }

    fn remove_expired(&mut self, now: Instant) {
        self.entries.retain(|_, entry| now <= entry.expires_at);
    }

    fn prune_lru(&mut self) {
        while self.entries.len() > self.max_entries {
            let Some((candidate, _)) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_access_tick)
                .map(|(key, entry)| (key.clone(), entry.last_access_tick))
            else {
                break;
            };
            self.entries.remove(candidate.as_str());
        }
    }
}

#[derive(Debug, Default)]
struct DnsValidationMetrics {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    dns_lookups: AtomicU64,
    dns_lookup_latency_ms_total: AtomicU64,
    blocked_total: AtomicU64,
    blocked_private_targets: AtomicU64,
    blocked_dns_failures: AtomicU64,
    observations: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
struct DnsValidationMetricsSnapshot {
    cache_hits: u64,
    cache_misses: u64,
    dns_lookups: u64,
    dns_lookup_latency_ms_total: u64,
    blocked_total: u64,
    blocked_private_targets: u64,
    blocked_dns_failures: u64,
    cache_entries: usize,
}

impl DnsValidationMetricsSnapshot {
    fn cache_hit_ratio(self) -> f64 {
        let denominator = self.cache_hits.saturating_add(self.cache_misses);
        if denominator == 0 {
            return 0.0;
        }
        self.cache_hits as f64 / denominator as f64
    }

    fn lookup_avg_latency_ms(self) -> f64 {
        if self.dns_lookups == 0 {
            return 0.0;
        }
        self.dns_lookup_latency_ms_total as f64 / self.dns_lookups as f64
    }
}

impl DnsValidationMetrics {
    fn snapshot(&self, cache_entries: usize) -> DnsValidationMetricsSnapshot {
        DnsValidationMetricsSnapshot {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            dns_lookups: self.dns_lookups.load(Ordering::Relaxed),
            dns_lookup_latency_ms_total: self.dns_lookup_latency_ms_total.load(Ordering::Relaxed),
            blocked_total: self.blocked_total.load(Ordering::Relaxed),
            blocked_private_targets: self.blocked_private_targets.load(Ordering::Relaxed),
            blocked_dns_failures: self.blocked_dns_failures.load(Ordering::Relaxed),
            cache_entries,
        }
    }

    #[cfg(test)]
    fn reset_for_tests(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.dns_lookups.store(0, Ordering::Relaxed);
        self.dns_lookup_latency_ms_total.store(0, Ordering::Relaxed);
        self.blocked_total.store(0, Ordering::Relaxed);
        self.blocked_private_targets.store(0, Ordering::Relaxed);
        self.blocked_dns_failures.store(0, Ordering::Relaxed);
        self.observations.store(0, Ordering::Relaxed);
    }
}

static DNS_VALIDATION_CACHE: LazyLock<std::sync::Mutex<DnsValidationCache>> = LazyLock::new(|| {
    std::sync::Mutex::new(DnsValidationCache::new(
        DNS_VALIDATION_CACHE_MAX_ENTRIES,
        DNS_VALIDATION_CACHE_TTL,
        DNS_VALIDATION_NEGATIVE_TTL,
    ))
});

static DNS_VALIDATION_METRICS: LazyLock<DnsValidationMetrics> =
    LazyLock::new(DnsValidationMetrics::default);

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
            },
            max_sessions: args.max_sessions,
            state_store,
            profile_registry_lock: Mutex::new(()),
            sessions: Mutex::new(HashMap::new()),
            chromium_sessions: Mutex::new(HashMap::new()),
            download_sessions: Mutex::new(HashMap::new()),
        })
    }

    async fn authorize(&self, metadata: &tonic::metadata::MetadataMap) -> Result<(), Status> {
        let Some(expected_token) = self.auth_token.as_ref() else {
            return Ok(());
        };
        let supplied = metadata
            .get(AUTHORIZATION_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        let expected = format!("Bearer {expected_token}");
        if supplied.trim() != expected {
            return Err(Status::unauthenticated("missing or invalid browser service token"));
        }
        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    runtime: Arc<BrowserRuntimeState>,
}

#[derive(Clone)]
struct BrowserServiceImpl {
    runtime: Arc<BrowserRuntimeState>,
}

#[tonic::async_trait]
impl browser_v1::browser_service_server::BrowserService for BrowserServiceImpl {
    async fn health(
        &self,
        request: Request<browser_v1::BrowserHealthRequest>,
    ) -> Result<Response<browser_v1::BrowserHealthResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let active_sessions = self.runtime.sessions.lock().await.len();
        Ok(Response::new(browser_v1::BrowserHealthResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            status: "ok".to_owned(),
            uptime_seconds: self.runtime.started_at.elapsed().as_secs(),
            active_sessions: u32::try_from(active_sessions).unwrap_or(u32::MAX),
        }))
    }

    async fn create_session(
        &self,
        request: Request<browser_v1::CreateSessionRequest>,
    ) -> Result<Response<browser_v1::CreateSessionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let principal = payload.principal.trim();
        if principal.is_empty() {
            return Err(Status::invalid_argument("principal is required"));
        }
        let channel = normalize_optional_string(payload.channel.as_str());
        let requested_profile_id = parse_optional_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let mut profile = resolve_session_profile(
            self.runtime.as_ref(),
            principal,
            requested_profile_id.as_deref(),
        )
        .await
        .map_err(Status::internal)?;

        let mut private_profile = payload.private_profile;
        let mut persistence_enabled = payload.persistence_enabled;
        let mut persistence_id = if payload.persistence_enabled {
            let Some(value) = sanitize_persistence_id(payload.persistence_id.as_str()) else {
                return Err(Status::invalid_argument(
                    "persistence_enabled=true requires non-empty persistence_id",
                ));
            };
            Some(value)
        } else {
            None
        };
        let mut profile_id = None;
        if let Some(resolved_profile) = profile.as_ref() {
            profile_id = Some(resolved_profile.profile_id.clone());
            private_profile = private_profile || resolved_profile.private_profile;
            if resolved_profile.persistence_enabled && !private_profile {
                persistence_enabled = true;
                persistence_id = Some(resolved_profile.profile_id.clone());
            } else {
                persistence_enabled = false;
                persistence_id = None;
            }
        }

        let restored_snapshot = if persistence_enabled {
            let Some(store) = self.runtime.state_store.as_ref() else {
                return Err(Status::failed_precondition(
                    "state persistence requires PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
                ));
            };
            let Some(state_id) = persistence_id.as_ref() else {
                return Err(Status::invalid_argument(
                    "persistence_enabled=true requires non-empty persistence_id",
                ));
            };
            store.load_snapshot(state_id.as_str(), profile_id.as_deref()).map_err(|error| {
                Status::internal(format!("failed to load persisted state: {error}"))
            })?
        } else {
            None
        };

        let session_id = Ulid::new().to_string();
        let now = Instant::now();
        let idle_ttl = if payload.idle_ttl_ms == 0 {
            self.runtime.default_idle_ttl
        } else {
            Duration::from_millis(payload.idle_ttl_ms)
        };
        let budget = SessionBudget {
            max_navigation_timeout_ms: payload
                .budget
                .as_ref()
                .map(|value| value.max_navigation_timeout_ms)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_navigation_timeout_ms),
            max_session_lifetime_ms: payload
                .budget
                .as_ref()
                .map(|value| value.max_session_lifetime_ms)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_session_lifetime_ms),
            max_screenshot_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_screenshot_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_screenshot_bytes),
            max_response_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_response_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_response_bytes),
            max_action_timeout_ms: payload
                .budget
                .as_ref()
                .map(|value| value.max_action_timeout_ms)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_action_timeout_ms),
            max_type_input_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_type_input_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_type_input_bytes),
            max_actions_per_session: payload
                .budget
                .as_ref()
                .map(|value| value.max_actions_per_session)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_actions_per_session),
            max_actions_per_window: payload
                .budget
                .as_ref()
                .map(|value| value.max_actions_per_window)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_actions_per_window),
            action_rate_window_ms: payload
                .budget
                .as_ref()
                .map(|value| value.action_rate_window_ms)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.action_rate_window_ms),
            max_action_log_entries: payload
                .budget
                .as_ref()
                .map(|value| value.max_action_log_entries)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_action_log_entries),
            max_observe_snapshot_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_observe_snapshot_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_observe_snapshot_bytes),
            max_visible_text_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_visible_text_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_visible_text_bytes),
            max_network_log_entries: payload
                .budget
                .as_ref()
                .map(|value| value.max_network_log_entries)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_network_log_entries),
            max_network_log_bytes: payload
                .budget
                .as_ref()
                .map(|value| value.max_network_log_bytes)
                .filter(|value| *value > 0)
                .unwrap_or(self.runtime.default_budget.max_network_log_bytes),
            max_title_bytes: self.runtime.default_budget.max_title_bytes,
        };
        let action_allowed_domains =
            normalize_action_allowed_domains(payload.action_allowed_domains.as_slice());
        let mut session = BrowserSessionRecord::with_defaults(BrowserSessionInit {
            principal: principal.to_owned(),
            channel: channel.clone(),
            now,
            idle_ttl,
            budget: budget.clone(),
            allow_private_targets: payload.allow_private_targets,
            allow_downloads: payload.allow_downloads,
            action_allowed_domains: action_allowed_domains.clone(),
            profile_id: profile_id.clone(),
            private_profile,
            persistence: SessionPersistenceState {
                enabled: persistence_enabled,
                persistence_id: persistence_id.clone(),
                state_restored: false,
            },
        });
        if let Some(snapshot) = restored_snapshot {
            if let Some(profile_record) = profile.as_ref() {
                validate_restored_snapshot_against_profile(&snapshot, profile_record).map_err(
                    |error| {
                        Status::failed_precondition(format!(
                            "persisted state integrity validation failed: {error}"
                        ))
                    },
                )?;
            }
            if snapshot.principal != principal {
                return Err(Status::permission_denied(
                    "persisted state principal does not match session principal",
                ));
            }
            if normalize_optional_string(snapshot.channel.as_deref().unwrap_or_default()) != channel
            {
                return Err(Status::permission_denied(
                    "persisted state channel does not match session channel",
                ));
            }
            session.apply_snapshot(snapshot);
            session.persistence.state_restored = true;
        }
        if let Some(record) = profile.as_mut() {
            record.last_used_unix_ms = current_unix_ms();
            record.updated_at_unix_ms = record.last_used_unix_ms;
            refresh_profile_record_hash(record);
            if let Some(store) = self.runtime.state_store.as_ref() {
                upsert_profile_record(
                    store,
                    &self.runtime.profile_registry_lock,
                    record.clone(),
                    false,
                )
                .await
                .map_err(|error| {
                    Status::internal(format!("failed to update browser profile usage: {error}"))
                })?;
            }
        }
        let state_restored = session.persistence.state_restored;
        let persist_on_create = persistence_enabled;
        let mut session_for_persist = None;
        {
            let mut sessions = self.runtime.sessions.lock().await;
            if sessions.len() >= self.runtime.max_sessions {
                return Err(Status::resource_exhausted("browser session capacity reached"));
            }
            sessions.insert(session_id.clone(), session.clone());
            if persist_on_create {
                session_for_persist = Some(session);
            }
        }
        if let (Some(store), Some(record)) =
            (self.runtime.state_store.as_ref(), session_for_persist)
        {
            persist_session_snapshot(store, &record)
                .map_err(|error| Status::internal(format!("failed to persist state: {error}")))?;
        }
        if payload.allow_downloads {
            let sandbox = DownloadSandboxSession::new().map_err(Status::internal)?;
            self.runtime.download_sessions.lock().await.insert(session_id.clone(), sandbox);
        }
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            let session_snapshot = {
                let sessions = self.runtime.sessions.lock().await;
                sessions.get(session_id.as_str()).cloned()
            }
            .ok_or_else(|| Status::internal("session registration race during engine init"))?;
            if let Err(error) = initialize_chromium_session_runtime(
                self.runtime.as_ref(),
                session_id.as_str(),
                &session_snapshot,
            )
            .await
            {
                self.runtime.sessions.lock().await.remove(session_id.as_str());
                self.runtime.download_sessions.lock().await.remove(session_id.as_str());
                return Err(Status::internal(format!(
                    "failed to initialize chromium session runtime: {error}"
                )));
            }
        }

        Ok(Response::new(browser_v1::CreateSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            created_at_unix_ms: current_unix_ms(),
            effective_budget: Some(browser_v1::SessionBudget {
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
            }),
            downloads_enabled: payload.allow_downloads,
            action_allowed_domains,
            persistence_enabled,
            persistence_id: persistence_id.unwrap_or_default(),
            state_restored,
            profile_id: profile_id
                .clone()
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
            private_profile,
        }))
    }

    async fn close_session(
        &self,
        request: Request<browser_v1::CloseSessionRequest>,
    ) -> Result<Response<browser_v1::CloseSessionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let session_id = parse_session_id_from_proto(request.into_inner().session_id)
            .map_err(Status::invalid_argument)?;
        let removed = self.runtime.sessions.lock().await.remove(session_id.as_str());
        self.runtime.chromium_sessions.lock().await.remove(session_id.as_str());
        self.runtime.download_sessions.lock().await.remove(session_id.as_str());
        if let (Some(store), Some(record)) = (self.runtime.state_store.as_ref(), removed.as_ref()) {
            if record.persistence.enabled {
                persist_session_snapshot(store, record).map_err(|error| {
                    Status::internal(format!(
                        "failed to persist state while closing session: {error}"
                    ))
                })?;
            }
        }
        Ok(Response::new(browser_v1::CloseSessionResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            closed: removed.is_some(),
            reason: if removed.is_some() {
                "closed".to_owned()
            } else {
                "session_not_found".to_owned()
            },
        }))
    }

    async fn list_profiles(
        &self,
        request: Request<browser_v1::ListProfilesRequest>,
    ) -> Result<Response<browser_v1::ListProfilesResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let active_profile_id =
            registry.active_profile_by_principal.get(principal.as_str()).cloned();
        let mut profiles = registry
            .profiles
            .drain(..)
            .filter(|profile| profile.principal == principal)
            .collect::<Vec<_>>();
        profiles.sort_by(|left, right| right.last_used_unix_ms.cmp(&left.last_used_unix_ms));
        Ok(Response::new(browser_v1::ListProfilesResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profiles: profiles
                .iter()
                .map(|profile| {
                    profile_record_to_proto(
                        profile,
                        active_profile_id
                            .as_deref()
                            .map(|value| value == profile.profile_id.as_str())
                            .unwrap_or(false),
                    )
                })
                .collect(),
            active_profile_id: active_profile_id
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        }))
    }

    async fn create_profile(
        &self,
        request: Request<browser_v1::CreateProfileRequest>,
    ) -> Result<Response<browser_v1::CreateProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let name =
            normalize_profile_name(payload.name.as_str()).map_err(Status::invalid_argument)?;
        let theme = normalize_profile_theme(payload.theme_color.as_str())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        prune_profiles_for_principal(&mut registry, principal.as_str());
        let now = current_unix_ms();
        let mut profile = BrowserProfileRecord {
            profile_id: Ulid::new().to_string(),
            principal: principal.clone(),
            name,
            theme_color: theme,
            created_at_unix_ms: now,
            updated_at_unix_ms: now,
            last_used_unix_ms: now,
            persistence_enabled: payload.persistence_enabled && !payload.private_profile,
            private_profile: payload.private_profile,
            state_schema_version: PROFILE_RECORD_SCHEMA_VERSION,
            state_revision: 0,
            state_hash_sha256: None,
            record_hash_sha256: String::new(),
        };
        refresh_profile_record_hash(&mut profile);
        registry.profiles.push(profile.clone());
        registry
            .active_profile_by_principal
            .entry(principal.clone())
            .or_insert_with(|| profile.profile_id.clone());
        prune_profile_registry(&mut registry);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        let active = registry
            .active_profile_by_principal
            .get(principal.as_str())
            .map(|value| value == &profile.profile_id)
            .unwrap_or(false);
        Ok(Response::new(browser_v1::CreateProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(profile_record_to_proto(&profile, active)),
        }))
    }

    async fn rename_profile(
        &self,
        request: Request<browser_v1::RenameProfileRequest>,
    ) -> Result<Response<browser_v1::RenameProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let name =
            normalize_profile_name(payload.name.as_str()).map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let Some(profile) = registry
            .profiles
            .iter_mut()
            .find(|profile| profile.profile_id == profile_id && profile.principal == principal)
        else {
            return Err(Status::not_found("browser profile not found"));
        };
        profile.name = name;
        profile.updated_at_unix_ms = current_unix_ms();
        profile.last_used_unix_ms = profile.updated_at_unix_ms;
        refresh_profile_record_hash(profile);
        let active = registry
            .active_profile_by_principal
            .get(principal.as_str())
            .map(|value| value == &profile_id)
            .unwrap_or(false);
        let output = profile_record_to_proto(profile, active);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        Ok(Response::new(browser_v1::RenameProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(output),
        }))
    }

    async fn delete_profile(
        &self,
        request: Request<browser_v1::DeleteProfileRequest>,
    ) -> Result<Response<browser_v1::DeleteProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let before = registry.profiles.len();
        registry.profiles.retain(|profile| {
            !(profile.profile_id == profile_id && profile.principal == principal)
        });
        let deleted = registry.profiles.len() != before;
        if deleted {
            if registry
                .active_profile_by_principal
                .get(principal.as_str())
                .map(|value| value == &profile_id)
                .unwrap_or(false)
            {
                let replacement = registry
                    .profiles
                    .iter()
                    .filter(|profile| profile.principal == principal)
                    .max_by(|left, right| left.last_used_unix_ms.cmp(&right.last_used_unix_ms))
                    .map(|profile| profile.profile_id.clone());
                if let Some(value) = replacement {
                    registry.active_profile_by_principal.insert(principal.clone(), value);
                } else {
                    registry.active_profile_by_principal.remove(principal.as_str());
                }
            }
            prune_profile_registry(&mut registry);
            store.save_profile_registry(&registry).map_err(|error| {
                Status::internal(format!("failed to save browser profiles after delete: {error}"))
            })?;
            store.delete_snapshot(profile_id.as_str()).map_err(|error| {
                Status::internal(format!("failed to delete browser profile snapshot: {error}"))
            })?;
        }
        Ok(Response::new(browser_v1::DeleteProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            deleted,
            active_profile_id: registry
                .active_profile_by_principal
                .get(principal.as_str())
                .cloned()
                .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        }))
    }

    async fn set_active_profile(
        &self,
        request: Request<browser_v1::SetActiveProfileRequest>,
    ) -> Result<Response<browser_v1::SetActiveProfileResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let principal = normalize_profile_principal(payload.principal.as_str())
            .map_err(Status::invalid_argument)?;
        let profile_id = parse_required_profile_id_from_proto(payload.profile_id.take())
            .map_err(Status::invalid_argument)?;
        let Some(store) = self.runtime.state_store.as_ref() else {
            return Err(Status::failed_precondition(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY",
            ));
        };
        let _guard = self.runtime.profile_registry_lock.lock().await;
        let mut registry = store.load_profile_registry().map_err(|error| {
            Status::internal(format!("failed to load browser profiles: {error}"))
        })?;
        let Some(profile) = registry
            .profiles
            .iter_mut()
            .find(|profile| profile.profile_id == profile_id && profile.principal == principal)
        else {
            return Err(Status::not_found("browser profile not found"));
        };
        profile.last_used_unix_ms = current_unix_ms();
        profile.updated_at_unix_ms = profile.last_used_unix_ms;
        refresh_profile_record_hash(profile);
        let output = profile_record_to_proto(profile, true);
        registry.active_profile_by_principal.insert(principal, profile_id);
        prune_profile_registry(&mut registry);
        store.save_profile_registry(&registry).map_err(|error| {
            Status::internal(format!("failed to save browser profiles: {error}"))
        })?;
        Ok(Response::new(browser_v1::SetActiveProfileResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            profile: Some(output),
        }))
    }

    async fn navigate(
        &self,
        request: Request<browser_v1::NavigateRequest>,
    ) -> Result<Response<browser_v1::NavigateResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let url = payload.url.trim().to_owned();
        if url.is_empty() {
            return Err(Status::invalid_argument("navigate requires non-empty url"));
        }
        let (timeout_ms, max_response_bytes, allow_private_targets, cookie_header) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Err(Status::not_found("browser session not found"));
            };
            session.last_active = Instant::now();
            let timeout_ms =
                payload.timeout_ms.max(1).min(session.budget.max_navigation_timeout_ms);
            let cookie_header = cookie_header_for_url(session, url.as_str());
            (
                timeout_ms,
                session.budget.max_response_bytes,
                payload.allow_private_targets || session.allow_private_targets,
                cookie_header,
            )
        };

        let outcome = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                navigate_with_guards(
                    url.as_str(),
                    timeout_ms,
                    payload.allow_redirects,
                    if payload.max_redirects == 0 { 3 } else { payload.max_redirects },
                    allow_private_targets,
                    max_response_bytes,
                    cookie_header.as_deref(),
                )
                .await
            }
            BrowserEngineMode::Chromium => {
                navigate_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    ChromiumNavigateParams {
                        raw_url: url.clone(),
                        timeout_ms,
                        allow_redirects: payload.allow_redirects,
                        max_redirects: if payload.max_redirects == 0 {
                            3
                        } else {
                            payload.max_redirects
                        },
                        allow_private_targets,
                        max_response_bytes,
                        cookie_header: cookie_header.clone(),
                    },
                )
                .await
            }
        };
        let network_log_entries = outcome.network_log.clone();
        let cookie_updates = outcome.cookie_updates.clone();
        let mut session_for_persist = None;

        let mut sessions = self.runtime.sessions.lock().await;
        if let Some(session) = sessions.get_mut(session_id.as_str()) {
            let max_network_log_entries = session.budget.max_network_log_entries;
            let max_network_log_bytes = session.budget.max_network_log_bytes;
            if let Some(tab) = session.active_tab_mut() {
                if outcome.success {
                    tab.last_title = outcome.title.clone();
                    tab.last_url = Some(outcome.final_url.clone());
                    tab.last_page_body = outcome.page_body.clone();
                    tab.scroll_x = 0;
                    tab.scroll_y = 0;
                    tab.typed_inputs.clear();
                }
                append_network_log_entries(
                    tab,
                    network_log_entries.as_slice(),
                    max_network_log_entries,
                    max_network_log_bytes,
                );
            }
            apply_cookie_updates(session, cookie_updates.as_slice());
            session.last_active = Instant::now();
            if session.persistence.enabled {
                session_for_persist = Some(session.clone());
            }
        }
        drop(sessions);
        if let (Some(store), Some(record)) =
            (self.runtime.state_store.as_ref(), session_for_persist)
        {
            persist_session_snapshot(store, &record).map_err(|error| {
                Status::internal(format!("failed to persist state after navigate: {error}"))
            })?;
        }

        Ok(Response::new(browser_v1::NavigateResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: outcome.success,
            final_url: outcome.final_url,
            status_code: u32::from(outcome.status_code),
            title: truncate_utf8_bytes(
                outcome.title.as_str(),
                self.runtime.default_budget.max_title_bytes as usize,
            ),
            body_bytes: outcome.body_bytes,
            latency_ms: outcome.latency_ms,
            error: outcome.error,
        }))
    }

    async fn click(
        &self,
        request: Request<browser_v1::ClickRequest>,
    ) -> Result<Response<browser_v1::ClickResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let selector = payload.selector.trim();
        if selector.is_empty() {
            return Err(Status::invalid_argument("click requires non-empty selector"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::ClickResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                    artifact: None,
                }));
            }
        };

        let timeout_ms = payload.timeout_ms.max(1).min(context.budget.max_action_timeout_ms);
        let max_attempts = payload.max_retries.clamp(0, 16).saturating_add(1);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let started_at = Instant::now();
                let mut attempts = 0_u32;
                let mut success = false;
                let mut outcome = "selector_not_found".to_owned();
                let mut error = format!("selector '{}' was not found", selector);
                loop {
                    attempts = attempts.saturating_add(1);
                    if let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str())
                    {
                        if is_download_like_tag(tag.as_str()) && !context.allow_downloads {
                            outcome = "download_blocked".to_owned();
                            error =
                                "download-like click is blocked by session policy (allow_downloads=false)"
                                    .to_owned();
                            break;
                        }
                        success = true;
                        outcome = if is_download_like_tag(tag.as_str()) {
                            "download_allowed".to_owned()
                        } else {
                            "clicked".to_owned()
                        };
                        error.clear();
                        break;
                    }
                    if attempts >= max_attempts
                        || started_at.elapsed() >= Duration::from_millis(timeout_ms)
                    {
                        break;
                    }
                    let remaining_ms =
                        timeout_ms.saturating_sub(started_at.elapsed().as_millis() as u64);
                    let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                (success, outcome, error, attempts)
            }
            BrowserEngineMode::Chromium => {
                let result = click_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector,
                    timeout_ms,
                    max_attempts,
                    context.allow_downloads,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };
        let mut success = success;
        let mut outcome = outcome;
        let mut error = error;
        let mut artifact = None;
        if success && outcome == "download_allowed" {
            match capture_download_artifact_for_click(
                self.runtime.as_ref(),
                session_id.as_str(),
                selector,
                &context,
                timeout_ms,
            )
            .await
            {
                Ok(record) => {
                    if record.quarantined {
                        outcome = "download_quarantined".to_owned();
                    }
                    artifact = Some(download_artifact_to_proto(&record));
                }
                Err(download_error) => {
                    success = false;
                    outcome = "download_failed".to_owned();
                    error = download_error;
                }
            }
        }

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "click",
                    selector,
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "click")
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::ClickResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
            artifact,
        }))
    }

    async fn r#type(
        &self,
        request: Request<browser_v1::TypeRequest>,
    ) -> Result<Response<browser_v1::TypeResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let selector = payload.selector.trim();
        if selector.is_empty() {
            return Err(Status::invalid_argument("type requires non-empty selector"));
        }

        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::TypeResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    typed_bytes: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };

        let text = payload.text;
        if (text.len() as u64) > context.budget.max_type_input_bytes {
            let error = format!(
                "type input exceeds max_type_input_bytes ({} > {})",
                text.len(),
                context.budget.max_type_input_bytes
            );
            let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
                finalize_session_action(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    FinalizeActionRequest {
                        action_name: "type",
                        selector,
                        success: false,
                        outcome: "input_too_large",
                        error: error.as_str(),
                        started_at_unix_ms: current_unix_ms(),
                        attempts: 1,
                        capture_failure_screenshot: payload.capture_failure_screenshot,
                        max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                    },
                )
                .await;
            return Ok(Response::new(browser_v1::TypeResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                typed_bytes: 0,
                error,
                action_log,
                failure_screenshot_bytes,
                failure_screenshot_mime_type,
            }));
        }

        let timeout_ms = payload.timeout_ms.max(1).min(context.budget.max_action_timeout_ms);
        let started_at_unix_ms = current_unix_ms();
        let (success, outcome, error, attempts) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let started_at = Instant::now();
                let mut attempts = 0_u32;
                let mut success = false;
                let mut outcome = "selector_not_found".to_owned();
                let mut error = format!("selector '{}' was not found", selector);
                loop {
                    attempts = attempts.saturating_add(1);
                    if let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str())
                    {
                        if !is_typable_tag(tag.as_str()) {
                            outcome = "selector_not_typable".to_owned();
                            error = format!(
                                "selector '{}' does not target an input-like element",
                                selector
                            );
                            break;
                        }
                        success = true;
                        outcome = "typed".to_owned();
                        error.clear();
                        break;
                    }
                    if started_at.elapsed() >= Duration::from_millis(timeout_ms) {
                        break;
                    }
                    let remaining_ms =
                        timeout_ms.saturating_sub(started_at.elapsed().as_millis() as u64);
                    let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
                    tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                }
                (success, outcome, error, attempts)
            }
            BrowserEngineMode::Chromium => {
                let result = type_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    selector,
                    text.as_str(),
                    payload.clear_existing,
                    timeout_ms,
                )
                .await;
                (result.success, result.outcome, result.error, result.attempts)
            }
        };

        if success {
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                let mut origin = None;
                if let Some(tab) = session.active_tab_mut() {
                    let field = tab.typed_inputs.entry(selector.to_owned()).or_default();
                    if payload.clear_existing {
                        *field = text.clone();
                    } else {
                        field.push_str(text.as_str());
                    }
                    origin = tab.last_url.as_deref().and_then(url_origin_key);
                }
                if let Some(origin_key) = origin {
                    let storage = session.storage_entries.entry(origin_key).or_default();
                    if payload.clear_existing {
                        storage.insert(selector.to_owned(), text.clone());
                    } else {
                        let entry = storage.entry(selector.to_owned()).or_default();
                        entry.push_str(text.as_str());
                    }
                }
            }
        }

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "type",
                    selector,
                    success,
                    outcome: outcome.as_str(),
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "type")
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::TypeResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            typed_bytes: if success { text.len() as u64 } else { 0 },
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn scroll(
        &self,
        request: Request<browser_v1::ScrollRequest>,
    ) -> Result<Response<browser_v1::ScrollResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;

        let _context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            false,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::ScrollResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    scroll_x: 0,
                    scroll_y: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                }));
            }
        };

        let (success, scroll_x, scroll_y, error) = match self.runtime.engine_mode {
            BrowserEngineMode::Simulated => {
                let mut scroll_x = 0_i64;
                let mut scroll_y = 0_i64;
                {
                    let mut sessions = self.runtime.sessions.lock().await;
                    if let Some(session) = sessions.get_mut(session_id.as_str()) {
                        if let Some(tab) = session.active_tab_mut() {
                            tab.scroll_x = tab.scroll_x.saturating_add(payload.delta_x);
                            tab.scroll_y = tab.scroll_y.saturating_add(payload.delta_y);
                            scroll_x = tab.scroll_x;
                            scroll_y = tab.scroll_y;
                        }
                    }
                }
                (true, scroll_x, scroll_y, String::new())
            }
            BrowserEngineMode::Chromium => {
                let result = scroll_with_chromium(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    payload.delta_x,
                    payload.delta_y,
                )
                .await;
                (result.success, result.scroll_x, result.scroll_y, result.error)
            }
        };

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "scroll",
                    selector: "",
                    success,
                    outcome: if success { "scrolled" } else { "scroll_failed" },
                    error: error.as_str(),
                    started_at_unix_ms: current_unix_ms(),
                    attempts: 1,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "scroll")
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::ScrollResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            scroll_x,
            scroll_y,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
        }))
    }

    async fn wait_for(
        &self,
        request: Request<browser_v1::WaitForRequest>,
    ) -> Result<Response<browser_v1::WaitForResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let selector = payload.selector.trim().to_owned();
        let text = payload.text;
        if selector.is_empty() && text.trim().is_empty() {
            return Err(Status::invalid_argument(
                "wait_for requires non-empty selector or non-empty text",
            ));
        }
        let context = match consume_action_budget_and_snapshot(
            self.runtime.as_ref(),
            session_id.as_str(),
            true,
        )
        .await
        {
            Ok(value) => value,
            Err(error) => {
                return Ok(Response::new(browser_v1::WaitForResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    waited_ms: 0,
                    error,
                    action_log: None,
                    failure_screenshot_bytes: Vec::new(),
                    failure_screenshot_mime_type: String::new(),
                    matched_selector: String::new(),
                    matched_text: String::new(),
                }));
            }
        };

        let timeout_ms = payload.timeout_ms.max(1).min(context.budget.max_action_timeout_ms);
        let poll_interval_ms = payload.poll_interval_ms.clamp(25, 1_000);
        let started_at_unix_ms = current_unix_ms();
        let (success, matched_selector, matched_text, attempts, waited_ms, error) =
            match self.runtime.engine_mode {
                BrowserEngineMode::Simulated => {
                    let started = Instant::now();
                    let mut attempts = 0_u32;
                    let mut matched_selector = String::new();
                    let mut matched_text = String::new();
                    let mut success = false;
                    loop {
                        attempts = attempts.saturating_add(1);
                        if !selector.is_empty()
                            && find_matching_html_tag(selector.as_str(), context.page_body.as_str())
                                .is_some()
                        {
                            matched_selector = selector.clone();
                            success = true;
                            break;
                        }
                        if !text.trim().is_empty() && context.page_body.contains(text.as_str()) {
                            matched_text = text.clone();
                            success = true;
                            break;
                        }
                        if started.elapsed() >= Duration::from_millis(timeout_ms) {
                            break;
                        }
                        let remaining_ms =
                            timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
                        let sleep_ms = poll_interval_ms.min(remaining_ms.max(1));
                        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
                    }
                    let waited_ms = started.elapsed().as_millis() as u64;
                    let error = if success {
                        String::new()
                    } else {
                        "wait_for condition was not satisfied before timeout".to_owned()
                    };
                    (success, matched_selector, matched_text, attempts, waited_ms, error)
                }
                BrowserEngineMode::Chromium => {
                    let result = wait_for_with_chromium(
                        self.runtime.as_ref(),
                        session_id.as_str(),
                        selector.as_str(),
                        text.as_str(),
                        timeout_ms,
                        poll_interval_ms,
                    )
                    .await;
                    (
                        result.success,
                        result.matched_selector,
                        result.matched_text,
                        result.attempts,
                        result.waited_ms,
                        result.error,
                    )
                }
            };

        let (action_log, failure_screenshot_bytes, failure_screenshot_mime_type) =
            finalize_session_action(
                self.runtime.as_ref(),
                session_id.as_str(),
                FinalizeActionRequest {
                    action_name: "wait_for",
                    selector: selector.as_str(),
                    success,
                    outcome: if success { "condition_matched" } else { "condition_timeout" },
                    error: error.as_str(),
                    started_at_unix_ms,
                    attempts,
                    capture_failure_screenshot: payload.capture_failure_screenshot,
                    max_failure_screenshot_bytes: payload.max_failure_screenshot_bytes,
                },
            )
            .await;
        let session_for_persist = {
            let sessions = self.runtime.sessions.lock().await;
            sessions.get(session_id.as_str()).filter(|session| session.persistence.enabled).cloned()
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "wait_for")
            .map_err(map_persist_error_to_status)?;

        Ok(Response::new(browser_v1::WaitForResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            waited_ms,
            error,
            action_log,
            failure_screenshot_bytes,
            failure_screenshot_mime_type,
            matched_selector,
            matched_text,
        }))
    }

    async fn get_title(
        &self,
        request: Request<browser_v1::GetTitleRequest>,
    ) -> Result<Response<browser_v1::GetTitleResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let max_title_bytes = usize::try_from(payload.max_title_bytes)
            .ok()
            .filter(|value| *value > 0)
            .unwrap_or(self.runtime.default_budget.max_title_bytes as usize);
        let active_tab_id = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::GetTitleResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    title: String::new(),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let Some(tab) = session.active_tab() else {
                return Ok(Response::new(browser_v1::GetTitleResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    title: String::new(),
                    error: "active_tab_not_found".to_owned(),
                }));
            };
            tab.tab_id.clone()
        };
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Ok(title) = chromium_get_title(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await
            {
                let mut sessions = self.runtime.sessions.lock().await;
                if let Some(session) = sessions.get_mut(session_id.as_str()) {
                    if let Some(tab) = session.tabs.get_mut(active_tab_id.as_str()) {
                        tab.last_title = title;
                    }
                }
            }
        }
        let title = {
            let sessions = self.runtime.sessions.lock().await;
            sessions
                .get(session_id.as_str())
                .and_then(|session| session.tabs.get(active_tab_id.as_str()))
                .map(|tab| tab.last_title.clone())
                .unwrap_or_default()
        };
        Ok(Response::new(browser_v1::GetTitleResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            title: truncate_utf8_bytes(title.as_str(), max_title_bytes),
            error: String::new(),
        }))
    }

    async fn screenshot(
        &self,
        request: Request<browser_v1::ScreenshotRequest>,
    ) -> Result<Response<browser_v1::ScreenshotResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        if !payload.format.trim().is_empty() && !payload.format.trim().eq_ignore_ascii_case("png") {
            return Err(Status::invalid_argument("screenshot format must be empty or 'png'"));
        }
        let max_bytes = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ScreenshotResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    image_bytes: Vec::new(),
                    mime_type: "image/png".to_owned(),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            payload.max_bytes.max(1).min(session.budget.max_screenshot_bytes)
        };
        let image_bytes = if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            match chromium_screenshot(self.runtime.as_ref(), session_id.as_str()).await {
                Ok(value) => value,
                Err(error) => {
                    return Ok(Response::new(browser_v1::ScreenshotResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        image_bytes: Vec::new(),
                        mime_type: "image/png".to_owned(),
                        error,
                    }));
                }
            }
        } else {
            ONE_BY_ONE_PNG.to_vec()
        };
        if (image_bytes.len() as u64) > max_bytes {
            return Ok(Response::new(browser_v1::ScreenshotResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                image_bytes: Vec::new(),
                mime_type: "image/png".to_owned(),
                error: format!(
                    "screenshot output exceeds max_bytes ({} > {max_bytes})",
                    image_bytes.len()
                ),
            }));
        }
        Ok(Response::new(browser_v1::ScreenshotResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            image_bytes,
            mime_type: "image/png".to_owned(),
            error: String::new(),
        }))
    }

    async fn observe(
        &self,
        request: Request<browser_v1::ObserveRequest>,
    ) -> Result<Response<browser_v1::ObserveResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let include_dom_snapshot = if payload.include_dom_snapshot
            || payload.include_accessibility_tree
            || payload.include_visible_text
        {
            payload.include_dom_snapshot
        } else {
            true
        };
        let include_accessibility_tree = if payload.include_dom_snapshot
            || payload.include_accessibility_tree
            || payload.include_visible_text
        {
            payload.include_accessibility_tree
        } else {
            true
        };
        let include_visible_text = payload.include_visible_text;

        let (
            active_tab_id,
            max_dom_snapshot_bytes,
            max_accessibility_tree_bytes,
            max_visible_text_bytes,
        ) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let Some(tab) = session.active_tab() else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "active_tab_not_found".to_owned(),
                }));
            };
            (
                tab.tab_id.clone(),
                payload.max_dom_snapshot_bytes.max(1).min(session.budget.max_observe_snapshot_bytes)
                    as usize,
                payload
                    .max_accessibility_tree_bytes
                    .max(1)
                    .min(session.budget.max_observe_snapshot_bytes) as usize,
                payload.max_visible_text_bytes.max(1).min(session.budget.max_visible_text_bytes)
                    as usize,
            )
        };

        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Ok(snapshot) = chromium_observe_snapshot(
                self.runtime.as_ref(),
                session_id.as_str(),
                active_tab_id.as_str(),
            )
            .await
            {
                let mut sessions = self.runtime.sessions.lock().await;
                if let Some(session) = sessions.get_mut(session_id.as_str()) {
                    if let Some(tab) = session.tabs.get_mut(active_tab_id.as_str()) {
                        tab.last_page_body = snapshot.page_body;
                        tab.last_title = snapshot.title;
                        tab.last_url = Some(snapshot.page_url);
                    }
                }
            }
        }

        let (page_body, page_url) = {
            let sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "session_not_found".to_owned(),
                }));
            };
            let Some(tab) = session.tabs.get(active_tab_id.as_str()) else {
                return Ok(Response::new(browser_v1::ObserveResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    dom_snapshot: String::new(),
                    accessibility_tree: String::new(),
                    visible_text: String::new(),
                    dom_truncated: false,
                    accessibility_tree_truncated: false,
                    visible_text_truncated: false,
                    page_url: String::new(),
                    error: "active_tab_not_found".to_owned(),
                }));
            };
            (tab.last_page_body.clone(), tab.last_url.clone().unwrap_or_default())
        };
        if page_body.trim().is_empty() {
            return Ok(Response::new(browser_v1::ObserveResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                dom_snapshot: String::new(),
                accessibility_tree: String::new(),
                visible_text: String::new(),
                dom_truncated: false,
                accessibility_tree_truncated: false,
                visible_text_truncated: false,
                page_url: String::new(),
                error: "navigate must succeed before observe".to_owned(),
            }));
        }

        let (dom_snapshot, dom_truncated) = if include_dom_snapshot {
            build_dom_snapshot(page_body.as_str(), max_dom_snapshot_bytes)
        } else {
            (String::new(), false)
        };
        let (accessibility_tree, accessibility_tree_truncated) = if include_accessibility_tree {
            build_accessibility_tree_snapshot(page_body.as_str(), max_accessibility_tree_bytes)
        } else {
            (String::new(), false)
        };
        let (visible_text, visible_text_truncated) = if include_visible_text {
            build_visible_text_snapshot(page_body.as_str(), max_visible_text_bytes)
        } else {
            (String::new(), false)
        };

        Ok(Response::new(browser_v1::ObserveResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            dom_snapshot,
            accessibility_tree,
            visible_text,
            dom_truncated,
            accessibility_tree_truncated,
            visible_text_truncated,
            page_url: normalize_url_with_redaction(page_url.as_str()),
            error: String::new(),
        }))
    }

    async fn network_log(
        &self,
        request: Request<browser_v1::NetworkLogRequest>,
    ) -> Result<Response<browser_v1::NetworkLogResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::NetworkLogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                entries: Vec::new(),
                truncated: false,
                error: "session_not_found".to_owned(),
            }));
        };
        session.last_active = Instant::now();
        let Some(tab) = session.active_tab() else {
            return Ok(Response::new(browser_v1::NetworkLogResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                entries: Vec::new(),
                truncated: false,
                error: "active_tab_not_found".to_owned(),
            }));
        };
        let limit = if payload.limit == 0 {
            session.budget.max_network_log_entries
        } else {
            usize::try_from(payload.limit).unwrap_or(usize::MAX)
        }
        .min(session.budget.max_network_log_entries)
        .max(1);
        let max_payload_bytes =
            payload.max_payload_bytes.max(1).min(session.budget.max_network_log_bytes) as usize;

        let start = tab.network_log.len().saturating_sub(limit);
        let mut truncated = start > 0;
        let mut entries = tab
            .network_log
            .iter()
            .skip(start)
            .cloned()
            .map(|entry| network_log_entry_to_proto(entry, payload.include_headers))
            .collect::<Vec<_>>();
        truncated = truncate_network_log_payload(&mut entries, max_payload_bytes) || truncated;

        Ok(Response::new(browser_v1::NetworkLogResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            entries,
            truncated,
            error: String::new(),
        }))
    }

    async fn reset_state(
        &self,
        request: Request<browser_v1::ResetStateRequest>,
    ) -> Result<Response<browser_v1::ResetStateResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let default_reset = !payload.clear_cookies
            && !payload.clear_storage
            && !payload.reset_tabs
            && !payload.reset_permissions;
        let clear_cookies = payload.clear_cookies || default_reset;
        let clear_storage = payload.clear_storage || default_reset;
        let mut session_for_persist = None;

        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::ResetStateResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    cookies_cleared: 0,
                    storage_entries_cleared: 0,
                    tabs_closed: 0,
                    permissions: Some(SessionPermissionsInternal::default().to_proto()),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let mut cookies_cleared = 0_u32;
            let mut storage_entries_cleared = 0_u32;
            let mut tabs_closed = 0_u32;
            if clear_cookies {
                cookies_cleared =
                    session.cookie_jar.values().map(|cookies| cookies.len() as u32).sum::<u32>();
                session.cookie_jar.clear();
            }
            if clear_storage {
                storage_entries_cleared = session
                    .storage_entries
                    .values()
                    .map(|entries| entries.len() as u32)
                    .sum::<u32>();
                session.storage_entries.clear();
                if let Some(tab) = session.active_tab_mut() {
                    tab.typed_inputs.clear();
                }
            }
            if payload.reset_tabs && !session.tab_order.is_empty() {
                tabs_closed = session.tab_order.len().saturating_sub(1) as u32;
                let active_tab_id = session.active_tab_id.clone();
                session.tabs.clear();
                session
                    .tabs
                    .insert(active_tab_id.clone(), BrowserTabRecord::new(active_tab_id.clone()));
                session.tab_order = vec![active_tab_id];
            }
            if payload.reset_permissions {
                session.permissions = SessionPermissionsInternal::default();
            }
            if session.persistence.enabled {
                session_for_persist = Some(session.clone());
            }
            browser_v1::ResetStateResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: true,
                cookies_cleared,
                storage_entries_cleared,
                tabs_closed,
                permissions: Some(session.permissions.to_proto()),
                error: String::new(),
            }
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "reset_state")
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn list_tabs(
        &self,
        request: Request<browser_v1::ListTabsRequest>,
    ) -> Result<Response<browser_v1::ListTabsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::ListTabsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                tabs: Vec::new(),
                active_tab_id: None,
                error: "session_not_found".to_owned(),
            }));
        };
        session.last_active = Instant::now();
        Ok(Response::new(browser_v1::ListTabsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            tabs: session.list_tabs(),
            active_tab_id: Some(proto::palyra::common::v1::CanonicalId {
                ulid: session.active_tab_id.clone(),
            }),
            error: String::new(),
        }))
    }

    async fn open_tab(
        &self,
        request: Request<browser_v1::OpenTabRequest>,
    ) -> Result<Response<browser_v1::OpenTabResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let url = payload.url.trim().to_owned();
        let (created_tab_id, timeout_ms, max_response_bytes, allow_private_targets, cookie_header) = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::OpenTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    tab: None,
                    navigated: false,
                    status_code: 0,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let created_tab_id = session.create_tab();
            if payload.activate {
                session.active_tab_id = created_tab_id.clone();
            }
            let timeout_ms =
                payload.timeout_ms.max(1).min(session.budget.max_navigation_timeout_ms);
            let max_response_bytes = session.budget.max_response_bytes;
            let allow_private_targets =
                payload.allow_private_targets || session.allow_private_targets;
            let cookie_header = cookie_header_for_url(session, url.as_str());
            (created_tab_id, timeout_ms, max_response_bytes, allow_private_targets, cookie_header)
        };
        let mut session_for_persist = None;
        if self.runtime.engine_mode == BrowserEngineMode::Chromium {
            if let Err(error) = chromium_open_tab_runtime(
                self.runtime.as_ref(),
                session_id.as_str(),
                created_tab_id.as_str(),
            )
            .await
            {
                let mut sessions = self.runtime.sessions.lock().await;
                if let Some(session) = sessions.get_mut(session_id.as_str()) {
                    if session.tabs.remove(created_tab_id.as_str()).is_some() {
                        session.tab_order.retain(|value| value != created_tab_id.as_str());
                        if session.tab_order.is_empty() {
                            let fallback_id = session.create_tab();
                            session.active_tab_id = fallback_id;
                        } else if session.active_tab_id == created_tab_id {
                            if let Some(first) = session.tab_order.first() {
                                session.active_tab_id = first.clone();
                            }
                        }
                    }
                }
                return Ok(Response::new(browser_v1::OpenTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    tab: None,
                    navigated: false,
                    status_code: 0,
                    error: format!("failed to create chromium tab runtime: {error}"),
                }));
            }
        }

        let mut navigated = false;
        let mut status_code = 0_u32;
        let mut success = true;
        let mut error = String::new();
        if !url.is_empty() {
            navigated = true;
            let outcome = match self.runtime.engine_mode {
                BrowserEngineMode::Simulated => {
                    navigate_with_guards(
                        url.as_str(),
                        timeout_ms,
                        payload.allow_redirects,
                        if payload.max_redirects == 0 { 3 } else { payload.max_redirects },
                        allow_private_targets,
                        max_response_bytes,
                        cookie_header.as_deref(),
                    )
                    .await
                }
                BrowserEngineMode::Chromium => {
                    navigate_tab_with_chromium(
                        self.runtime.as_ref(),
                        session_id.as_str(),
                        created_tab_id.as_str(),
                        &ChromiumNavigateParams {
                            raw_url: url.clone(),
                            timeout_ms,
                            allow_redirects: payload.allow_redirects,
                            max_redirects: if payload.max_redirects == 0 {
                                3
                            } else {
                                payload.max_redirects
                            },
                            allow_private_targets,
                            max_response_bytes,
                            cookie_header: cookie_header.clone(),
                        },
                    )
                    .await
                }
            };
            status_code = outcome.status_code as u32;
            success = outcome.success;
            if !success {
                error = outcome.error.clone();
            }
            let network_log_entries = outcome.network_log.clone();
            let cookie_updates = outcome.cookie_updates.clone();
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                let max_network_log_entries = session.budget.max_network_log_entries;
                let max_network_log_bytes = session.budget.max_network_log_bytes;
                if let Some(tab) = session.tabs.get_mut(created_tab_id.as_str()) {
                    if outcome.success {
                        tab.last_title = outcome.title;
                        tab.last_url = Some(outcome.final_url);
                        tab.last_page_body = outcome.page_body;
                        tab.scroll_x = 0;
                        tab.scroll_y = 0;
                        tab.typed_inputs.clear();
                    }
                    append_network_log_entries(
                        tab,
                        network_log_entries.as_slice(),
                        max_network_log_entries,
                        max_network_log_bytes,
                    );
                }
                apply_cookie_updates(session, cookie_updates.as_slice());
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
            }
        } else {
            let mut sessions = self.runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id.as_str()) {
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
            }
        }
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "open_tab")
            .map_err(map_persist_error_to_status)?;

        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::OpenTabResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                tab: None,
                navigated,
                status_code,
                error: "session_not_found".to_owned(),
            }));
        };
        let tab = session.tab_to_proto(created_tab_id.as_str());
        Ok(Response::new(browser_v1::OpenTabResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success,
            tab,
            navigated,
            status_code,
            error,
        }))
    }

    async fn switch_tab(
        &self,
        request: Request<browser_v1::SwitchTabRequest>,
    ) -> Result<Response<browser_v1::SwitchTabResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let tab_id =
            parse_tab_id_from_proto(payload.tab_id.take()).map_err(Status::invalid_argument)?;
        let mut session_for_persist = None;
        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    active_tab: None,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            if !session.tabs.contains_key(tab_id.as_str()) {
                browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    active_tab: None,
                    error: "tab_not_found".to_owned(),
                }
            } else {
                session.active_tab_id = tab_id;
                if session.persistence.enabled {
                    session_for_persist = Some(session.clone());
                }
                browser_v1::SwitchTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: true,
                    active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                    error: String::new(),
                }
            }
        };
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "switch_tab")
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn close_tab(
        &self,
        request: Request<browser_v1::CloseTabRequest>,
    ) -> Result<Response<browser_v1::CloseTabResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let requested_tab_id = match payload.tab_id.take() {
            Some(value) if !value.ulid.trim().is_empty() => {
                parse_tab_id(Some(value.ulid.trim())).map_err(Status::invalid_argument)?
            }
            _ => String::new(),
        };
        let mut session_for_persist = None;
        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::CloseTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    closed_tab_id: None,
                    active_tab: None,
                    tabs_remaining: 0,
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            let tab_id_to_close = if requested_tab_id.is_empty() {
                session.active_tab_id.clone()
            } else {
                requested_tab_id.clone()
            };
            match session.close_tab(tab_id_to_close.as_str()) {
                Ok((closed_tab_id, _)) => {
                    if session.persistence.enabled {
                        session_for_persist = Some(session.clone());
                    }
                    browser_v1::CloseTabResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: true,
                        closed_tab_id: Some(proto::palyra::common::v1::CanonicalId {
                            ulid: closed_tab_id,
                        }),
                        active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                        tabs_remaining: session.tabs.len() as u32,
                        error: String::new(),
                    }
                }
                Err(error) => browser_v1::CloseTabResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    closed_tab_id: None,
                    active_tab: session.tab_to_proto(session.active_tab_id.as_str()),
                    tabs_remaining: session.tabs.len() as u32,
                    error,
                },
            }
        };
        if self.runtime.engine_mode == BrowserEngineMode::Chromium && response.success {
            if let Some(closed_tab_id) = response.closed_tab_id.as_ref() {
                let _ = chromium_close_tab_runtime(
                    self.runtime.as_ref(),
                    session_id.as_str(),
                    closed_tab_id.ulid.as_str(),
                )
                .await;
            }
        }
        persist_session_after_mutation(self.runtime.as_ref(), session_for_persist, "close_tab")
            .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn get_permissions(
        &self,
        request: Request<browser_v1::GetPermissionsRequest>,
    ) -> Result<Response<browser_v1::GetPermissionsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let mut sessions = self.runtime.sessions.lock().await;
        let Some(session) = sessions.get_mut(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::GetPermissionsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                permissions: Some(SessionPermissionsInternal::default().to_proto()),
                error: "session_not_found".to_owned(),
            }));
        };
        session.last_active = Instant::now();
        Ok(Response::new(browser_v1::GetPermissionsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            success: true,
            permissions: Some(session.permissions.to_proto()),
            error: String::new(),
        }))
    }

    async fn set_permissions(
        &self,
        request: Request<browser_v1::SetPermissionsRequest>,
    ) -> Result<Response<browser_v1::SetPermissionsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let mut session_for_persist = None;
        let response = {
            let mut sessions = self.runtime.sessions.lock().await;
            let Some(session) = sessions.get_mut(session_id.as_str()) else {
                return Ok(Response::new(browser_v1::SetPermissionsResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: false,
                    permissions: Some(SessionPermissionsInternal::default().to_proto()),
                    error: "session_not_found".to_owned(),
                }));
            };
            session.last_active = Instant::now();
            session.permissions.apply_update(
                payload.camera,
                payload.microphone,
                payload.location,
                payload.reset_to_default,
            );
            if session.persistence.enabled {
                session_for_persist = Some(session.clone());
            }
            browser_v1::SetPermissionsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: true,
                permissions: Some(session.permissions.to_proto()),
                error: String::new(),
            }
        };
        persist_session_after_mutation(
            self.runtime.as_ref(),
            session_for_persist,
            "set_permissions",
        )
        .map_err(map_persist_error_to_status)?;
        Ok(Response::new(response))
    }

    async fn relay_action(
        &self,
        request: Request<browser_v1::RelayActionRequest>,
    ) -> Result<Response<browser_v1::RelayActionResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let extension_id = payload.extension_id.trim();
        if extension_id.is_empty() {
            return Err(Status::invalid_argument("extension_id is required"));
        }
        if extension_id.len() > MAX_RELAY_EXTENSION_ID_BYTES {
            return Err(Status::invalid_argument(format!(
                "extension_id exceeds {MAX_RELAY_EXTENSION_ID_BYTES} bytes"
            )));
        }
        if !extension_id
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'.' | b'-' | b'_'))
        {
            return Err(Status::invalid_argument("extension_id contains unsupported characters"));
        }
        if payload.max_payload_bytes > MAX_RELAY_PAYLOAD_BYTES {
            return Err(Status::invalid_argument(format!(
                "relay max_payload_bytes exceeds {} bytes",
                MAX_RELAY_PAYLOAD_BYTES
            )));
        }

        let action = browser_v1::RelayActionKind::try_from(payload.action)
            .unwrap_or(browser_v1::RelayActionKind::Unspecified);
        match action {
            browser_v1::RelayActionKind::OpenTab => {
                let Some(browser_v1::relay_action_request::Payload::OpenTab(open_tab)) =
                    payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::OpenTab as i32,
                        error: "relay open_tab payload is required".to_owned(),
                        result: None,
                    }));
                };
                let open_response = self
                    .open_tab(Request::new(browser_v1::OpenTabRequest {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        session_id: Some(proto::palyra::common::v1::CanonicalId {
                            ulid: session_id.clone(),
                        }),
                        url: open_tab.url,
                        activate: open_tab.activate,
                        timeout_ms: open_tab.timeout_ms,
                        allow_redirects: true,
                        max_redirects: 3,
                        allow_private_targets: false,
                    }))
                    .await?;
                let output = open_response.into_inner();
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: output.success,
                    action: browser_v1::RelayActionKind::OpenTab as i32,
                    error: output.error.clone(),
                    result: output.tab.map(browser_v1::relay_action_response::Result::OpenedTab),
                }))
            }
            browser_v1::RelayActionKind::CaptureSelection => {
                let Some(browser_v1::relay_action_request::Payload::CaptureSelection(
                    selection_payload,
                )) = payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::CaptureSelection as i32,
                        error: "relay capture_selection payload is required".to_owned(),
                        result: None,
                    }));
                };
                let selector = selection_payload.selector.trim();
                if selector.is_empty() {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::CaptureSelection as i32,
                        error: "relay capture_selection selector is required".to_owned(),
                        result: None,
                    }));
                }
                let max_selection_bytes = if selection_payload.max_selection_bytes == 0 {
                    MAX_RELAY_SELECTION_BYTES
                } else {
                    selection_payload.max_selection_bytes.min(MAX_RELAY_SELECTION_BYTES as u64)
                        as usize
                };
                let selected_text = {
                    let mut sessions = self.runtime.sessions.lock().await;
                    let Some(session) = sessions.get_mut(session_id.as_str()) else {
                        return Ok(Response::new(browser_v1::RelayActionResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            success: false,
                            action: browser_v1::RelayActionKind::CaptureSelection as i32,
                            error: "session_not_found".to_owned(),
                            result: None,
                        }));
                    };
                    session.last_active = Instant::now();
                    let Some(tag) = find_matching_html_tag(
                        selector,
                        session
                            .active_tab()
                            .map(|tab| tab.last_page_body.as_str())
                            .unwrap_or_default(),
                    ) else {
                        return Ok(Response::new(browser_v1::RelayActionResponse {
                            v: CANONICAL_PROTOCOL_MAJOR,
                            success: false,
                            action: browser_v1::RelayActionKind::CaptureSelection as i32,
                            error: format!("selector '{selector}' was not found"),
                            result: None,
                        }));
                    };
                    truncate_utf8_bytes(tag.as_str(), max_selection_bytes)
                };
                let truncated = selected_text.len() >= max_selection_bytes;
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: true,
                    action: browser_v1::RelayActionKind::CaptureSelection as i32,
                    error: String::new(),
                    result: Some(browser_v1::relay_action_response::Result::Selection(
                        browser_v1::RelaySelectionResult {
                            selector: selector.to_owned(),
                            selected_text,
                            truncated,
                        },
                    )),
                }))
            }
            browser_v1::RelayActionKind::SendPageSnapshot => {
                let Some(browser_v1::relay_action_request::Payload::PageSnapshot(snapshot_payload)) =
                    payload.payload.take()
                else {
                    return Ok(Response::new(browser_v1::RelayActionResponse {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        success: false,
                        action: browser_v1::RelayActionKind::SendPageSnapshot as i32,
                        error: "relay page_snapshot payload is required".to_owned(),
                        result: None,
                    }));
                };
                let observe = self
                    .observe(Request::new(browser_v1::ObserveRequest {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        session_id: Some(proto::palyra::common::v1::CanonicalId {
                            ulid: session_id.clone(),
                        }),
                        include_dom_snapshot: snapshot_payload.include_dom_snapshot,
                        include_accessibility_tree: false,
                        include_visible_text: snapshot_payload.include_visible_text,
                        max_dom_snapshot_bytes: snapshot_payload.max_dom_snapshot_bytes,
                        max_accessibility_tree_bytes: 0,
                        max_visible_text_bytes: snapshot_payload.max_visible_text_bytes,
                    }))
                    .await?;
                let observe = observe.into_inner();
                Ok(Response::new(browser_v1::RelayActionResponse {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    success: observe.success,
                    action: browser_v1::RelayActionKind::SendPageSnapshot as i32,
                    error: observe.error.clone(),
                    result: if observe.success {
                        Some(browser_v1::relay_action_response::Result::Snapshot(
                            browser_v1::RelayPageSnapshotResult {
                                dom_snapshot: observe.dom_snapshot,
                                visible_text: observe.visible_text,
                                dom_truncated: observe.dom_truncated,
                                visible_text_truncated: observe.visible_text_truncated,
                                page_url: observe.page_url,
                            },
                        ))
                    } else {
                        None
                    },
                }))
            }
            _ => Ok(Response::new(browser_v1::RelayActionResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                success: false,
                action: browser_v1::RelayActionKind::Unspecified as i32,
                error: "unsupported relay action".to_owned(),
                result: None,
            })),
        }
    }

    async fn list_download_artifacts(
        &self,
        request: Request<browser_v1::ListDownloadArtifactsRequest>,
    ) -> Result<Response<browser_v1::ListDownloadArtifactsResponse>, Status> {
        self.runtime.authorize(request.metadata()).await?;
        let mut payload = request.into_inner();
        let session_id = parse_session_id_from_proto(payload.session_id.take())
            .map_err(Status::invalid_argument)?;
        let limit = if payload.limit == 0 {
            MAX_DOWNLOAD_ARTIFACTS_PER_SESSION
        } else {
            usize::try_from(payload.limit).unwrap_or(MAX_DOWNLOAD_ARTIFACTS_PER_SESSION)
        }
        .clamp(1, MAX_DOWNLOAD_ARTIFACTS_PER_SESSION);
        let quarantined_only = payload.quarantined_only;
        let guard = self.runtime.download_sessions.lock().await;
        let Some(download_session) = guard.get(session_id.as_str()) else {
            return Ok(Response::new(browser_v1::ListDownloadArtifactsResponse {
                v: CANONICAL_PROTOCOL_MAJOR,
                artifacts: Vec::new(),
                truncated: false,
                error: "session_not_found".to_owned(),
            }));
        };
        let filtered = download_session
            .artifacts
            .iter()
            .filter(|artifact| !quarantined_only || artifact.quarantined)
            .cloned()
            .collect::<Vec<_>>();
        let truncated = filtered.len() > limit;
        let artifacts = filtered
            .into_iter()
            .rev()
            .take(limit)
            .map(|record| download_artifact_to_proto(&record))
            .collect::<Vec<_>>();
        Ok(Response::new(browser_v1::ListDownloadArtifactsResponse {
            v: CANONICAL_PROTOCOL_MAJOR,
            artifacts,
            truncated,
            error: String::new(),
        }))
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

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();
    let runtime = Arc::new(BrowserRuntimeState::new(&args)?);
    spawn_cleanup_loop(Arc::clone(&runtime));

    let admin_address =
        parse_daemon_bind_socket(&args.bind, args.port).context("invalid bind address or port")?;
    let grpc_address = parse_daemon_bind_socket(&args.grpc_bind, args.grpc_port)
        .context("invalid gRPC bind address or port")?;
    enforce_non_loopback_bind_auth(admin_address, grpc_address, runtime.auth_token.is_some())?;

    let build = build_metadata();
    info!(
        service = "palyra-browserd",
        version = build.version,
        git_hash = build.git_hash,
        build_profile = build.build_profile,
        bind_addr = %args.bind,
        port = args.port,
        grpc_bind_addr = %args.grpc_bind,
        grpc_port = args.grpc_port,
        auth_enabled = runtime.auth_token.is_some(),
        state_persistence_enabled = runtime.state_store.is_some(),
        "browser service startup"
    );

    let app = Router::new()
        .route("/healthz", get(health_handler))
        .with_state(AppState { runtime: Arc::clone(&runtime) });

    let admin_listener = tokio::net::TcpListener::bind(admin_address)
        .await
        .context("failed to bind browserd health listener")?;
    let grpc_listener = tokio::net::TcpListener::bind(grpc_address)
        .await
        .context("failed to bind browserd gRPC listener")?;

    info!(
        listen_addr = %admin_listener.local_addr().context("health local_addr")?,
        "browserd health endpoint ready"
    );
    info!(
        grpc_listen_addr = %grpc_listener.local_addr().context("grpc local_addr")?,
        "browserd gRPC endpoint ready"
    );

    let http_server = axum::serve(admin_listener, app).with_graceful_shutdown(shutdown_signal());
    let grpc_server = Server::builder()
        .add_service(browser_v1::browser_service_server::BrowserServiceServer::new(
            BrowserServiceImpl { runtime: Arc::clone(&runtime) },
        ))
        .serve_with_incoming_shutdown(TcpListenerStream::new(grpc_listener), shutdown_signal());

    let (http_result, grpc_result) = tokio::join!(http_server, grpc_server);
    http_result.context("browserd health server failed")?;
    grpc_result.context("browserd gRPC server failed")?;
    Ok(())
}

fn enforce_non_loopback_bind_auth(
    admin_address: SocketAddr,
    grpc_address: SocketAddr,
    auth_enabled: bool,
) -> Result<()> {
    if auth_enabled {
        return Ok(());
    }

    let admin_non_loopback = !admin_address.ip().is_loopback();
    let grpc_non_loopback = !grpc_address.ip().is_loopback();
    if admin_non_loopback || grpc_non_loopback {
        anyhow::bail!(
            "browser service auth token is required for non-loopback bindings (admin: {admin_address}, grpc: {grpc_address}); set --auth-token or PALYRA_BROWSERD_AUTH_TOKEN"
        );
    }

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().json().with_env_filter(filter).init();
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json::<HealthResponse>(health_response("palyra-browserd", state.runtime.started_at))
}

fn spawn_cleanup_loop(runtime: Arc<BrowserRuntimeState>) {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_millis(CLEANUP_INTERVAL_MS));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let now = Instant::now();
            let expired_ids = {
                let sessions = runtime.sessions.lock().await;
                sessions
                    .iter()
                    .filter_map(|(session_id, session)| {
                        let idle_alive =
                            now.saturating_duration_since(session.last_active) <= session.idle_ttl;
                        let lifetime_alive = now.saturating_duration_since(session.created_at)
                            <= Duration::from_millis(session.budget.max_session_lifetime_ms);
                        if idle_alive && lifetime_alive {
                            None
                        } else {
                            Some(session_id.clone())
                        }
                    })
                    .collect::<Vec<_>>()
            };
            if expired_ids.is_empty() {
                continue;
            }
            let removed_sessions = {
                let mut sessions = runtime.sessions.lock().await;
                expired_ids
                    .iter()
                    .filter_map(|session_id| sessions.remove(session_id.as_str()))
                    .collect::<Vec<_>>()
            };
            {
                let mut chromium_sessions = runtime.chromium_sessions.lock().await;
                for session_id in &expired_ids {
                    chromium_sessions.remove(session_id.as_str());
                }
            }
            {
                let mut download_sessions = runtime.download_sessions.lock().await;
                for session_id in &expired_ids {
                    download_sessions.remove(session_id.as_str());
                }
            }
            if let Some(store) = runtime.state_store.as_ref() {
                for session in removed_sessions {
                    if session.persistence.enabled {
                        if let Err(error) = persist_session_snapshot(store, &session) {
                            warn!(
                                principal = session.principal,
                                channel = ?session.channel,
                                error = %error,
                                "failed to persist state while expiring session"
                            );
                        }
                    }
                }
            }
        }
    });
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(error = %error, "failed to register Ctrl+C handler");
        std::future::pending::<()>().await;
    }
}

#[derive(Debug)]
struct ChromiumActionOutcome {
    success: bool,
    outcome: String,
    error: String,
    attempts: u32,
}

#[derive(Debug)]
struct ChromiumScrollOutcome {
    success: bool,
    scroll_x: i64,
    scroll_y: i64,
    error: String,
}

#[derive(Debug)]
struct ChromiumWaitOutcome {
    success: bool,
    matched_selector: String,
    matched_text: String,
    attempts: u32,
    waited_ms: u64,
    error: String,
}

#[derive(Debug)]
struct ChromiumObserveSnapshot {
    page_body: String,
    title: String,
    page_url: String,
}

#[derive(Debug, Clone)]
struct ChromiumNavigateParams {
    raw_url: String,
    timeout_ms: u64,
    allow_redirects: bool,
    max_redirects: u32,
    allow_private_targets: bool,
    max_response_bytes: u64,
    cookie_header: Option<String>,
}

async fn run_chromium_blocking<T, F>(operation: &str, task: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(task)
        .await
        .map_err(|error| format!("{operation} task join failure: {error}"))?
}

fn build_chromium_launch_options(
    chromium: &ChromiumEngineConfig,
    profile_dir: &TempDir,
) -> Result<headless_chrome::LaunchOptions<'static>, String> {
    let chromium_path = chromium.executable_path.clone();
    let mut builder = LaunchOptionsBuilder::default();
    builder
        .headless(true)
        .sandbox(true)
        .enable_gpu(false)
        .ignore_certificate_errors(false)
        .idle_browser_timeout(chromium.startup_timeout)
        .user_data_dir(Some(profile_dir.path().to_path_buf()))
        .args(vec![
            OsStr::new("--disable-dev-shm-usage"),
            OsStr::new("--disable-gpu"),
            OsStr::new("--no-first-run"),
            OsStr::new("--no-default-browser-check"),
            OsStr::new("--window-size=1280,800"),
            OsStr::new("--disable-blink-features=AutomationControlled"),
        ]);
    if let Some(path) = chromium_path {
        builder.path(Some(path));
    }
    builder.build().map_err(|error| format!("failed to build Chromium launch options: {error}"))
}

fn parse_chromium_remote_ip_literal(raw: &str) -> Option<IpAddr> {
    let trimmed = raw.trim().trim_start_matches('[').trim_end_matches(']');
    trimmed.parse::<IpAddr>().ok()
}

fn record_chromium_remote_ip_incident(
    remote_ip: Option<&str>,
    allow_private_targets: bool,
    security_incident: &Arc<std::sync::Mutex<Option<String>>>,
) {
    if allow_private_targets {
        return;
    }
    let Some(remote_ip_raw) = remote_ip else {
        return;
    };
    let Some(parsed_remote_ip) = parse_chromium_remote_ip_literal(remote_ip_raw) else {
        return;
    };
    if !netguard::is_private_or_local_ip(parsed_remote_ip) {
        return;
    }
    if let Ok(mut guard) = security_incident.lock() {
        if guard.is_none() {
            *guard = Some(format!(
                "remote response IP {} is private/local and violates browser session policy",
                parsed_remote_ip
            ));
        }
    }
}

fn configure_chromium_tab(
    tab: &Arc<HeadlessTab>,
    allow_private_targets: bool,
    timeout: Duration,
    security_incident: Arc<std::sync::Mutex<Option<String>>>,
) -> Result<(), String> {
    tab.set_default_timeout(timeout);
    tab.enable_fetch(None, Some(false))
        .map_err(|error| format!("failed to enable Chromium fetch interception: {error}"))?;
    let request_interceptor =
        Arc::new(move |_transport, _session_id, intercepted: Fetch::events::RequestPausedEvent| {
            let request_url = intercepted.params.request.url.as_str();
            if validate_target_url_blocking(request_url, allow_private_targets).is_ok() {
                RequestPausedDecision::Continue(None)
            } else {
                RequestPausedDecision::Fail(Fetch::FailRequest {
                    request_id: intercepted.params.request_id,
                    error_reason: Network::ErrorReason::BlockedByClient,
                })
            }
        });
    tab.enable_request_interception(request_interceptor).map_err(|error| {
        format!("failed to register Chromium request interception callback: {error}")
    })?;
    let remote_ip_guard = Arc::clone(&security_incident);
    tab.register_response_handling(
        CHROMIUM_REMOTE_IP_GUARD_HANDLER_NAME,
        Box::new(move |response, _fetch_body| {
            record_chromium_remote_ip_incident(
                response.response.remote_ip_address.as_deref(),
                allow_private_targets,
                &remote_ip_guard,
            );
        }),
    )
    .map_err(|error| format!("failed to register Chromium response guard callback: {error}"))?;
    Ok(())
}

async fn initialize_chromium_session_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    session: &BrowserSessionRecord,
) -> Result<(), String> {
    let chromium = runtime.chromium.clone();
    let allow_private_targets = session.allow_private_targets;
    let navigation_timeout = Duration::from_millis(session.budget.max_navigation_timeout_ms.max(1));
    let active_tab_id = session.active_tab_id.clone();
    let mut tab_order = session.tab_order.clone();
    if tab_order.is_empty() {
        tab_order.push(active_tab_id.clone());
    } else if !tab_order.iter().any(|tab_id| tab_id == &active_tab_id) {
        tab_order.insert(0, active_tab_id.clone());
    }
    let security_incident = Arc::new(std::sync::Mutex::new(None::<String>));
    let chromium_session = run_chromium_blocking("chromium session initialization", move || {
        let profile_dir = tempfile::Builder::new()
            .prefix("palyra-browserd-session-")
            .tempdir()
            .map_err(|error| format!("failed to allocate Chromium profile dir: {error}"))?;
        let launch_options = build_chromium_launch_options(&chromium, &profile_dir)?;
        let browser = Arc::new(
            HeadlessBrowser::new(launch_options)
                .map_err(|error| format!("failed to launch Chromium browser process: {error}"))?,
        );
        let mut tabs = HashMap::new();
        for tab_id in tab_order.iter() {
            let tab = browser.new_tab().map_err(|error| {
                format!("failed to create Chromium tab for session restore: {error}")
            })?;
            configure_chromium_tab(
                &tab,
                allow_private_targets,
                navigation_timeout,
                Arc::clone(&security_incident),
            )?;
            tabs.insert(tab_id.clone(), tab);
        }
        Ok(ChromiumSessionState { browser, tabs, security_incident, _profile_dir: profile_dir })
    })
    .await?;
    runtime.chromium_sessions.lock().await.insert(session_id.to_owned(), chromium_session);
    Ok(())
}

async fn chromium_open_tab_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    let (allow_private_targets, timeout_ms) = {
        let sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return Err("session_not_found".to_owned());
        };
        (session.allow_private_targets, session.budget.max_navigation_timeout_ms.max(1))
    };
    let (browser, security_incident) = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        (Arc::clone(&chromium_session.browser), Arc::clone(&chromium_session.security_incident))
    };
    let tab = run_chromium_blocking("chromium open tab", move || {
        let tab = browser
            .new_tab()
            .map_err(|error| format!("failed to allocate Chromium tab: {error}"))?;
        configure_chromium_tab(
            &tab,
            allow_private_targets,
            Duration::from_millis(timeout_ms),
            security_incident,
        )?;
        Ok(tab)
    })
    .await?;
    let mut chromium_sessions = runtime.chromium_sessions.lock().await;
    let Some(chromium_session) = chromium_sessions.get_mut(session_id) else {
        return Err("chromium_session_not_found".to_owned());
    };
    chromium_session.tabs.insert(tab_id.to_owned(), tab);
    Ok(())
}

async fn chromium_close_tab_runtime(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    let tab = {
        let mut chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get_mut(session_id) else {
            return Err("chromium_session_not_found".to_owned());
        };
        chromium_session.tabs.remove(tab_id)
    };
    if let Some(tab) = tab {
        let _ = run_chromium_blocking("chromium close tab", move || {
            tab.close(true).map_err(|error| format!("failed to close Chromium tab: {error}"))?;
            Ok(())
        })
        .await;
    }
    Ok(())
}

async fn enforce_chromium_remote_ip_guard(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<(), String> {
    let incident = {
        let chromium_sessions = runtime.chromium_sessions.lock().await;
        let Some(chromium_session) = chromium_sessions.get(session_id) else {
            return Ok(());
        };
        let mut guard = chromium_session
            .security_incident
            .lock()
            .map_err(|_| "failed to inspect Chromium security incident state".to_owned())?;
        guard.take()
    };
    let Some(reason) = incident else {
        return Ok(());
    };

    runtime.sessions.lock().await.remove(session_id);
    runtime.chromium_sessions.lock().await.remove(session_id);
    runtime.download_sessions.lock().await.remove(session_id);
    warn!(
        session_id = session_id,
        reason = reason.as_str(),
        "terminated browser session after Chromium remote IP guard incident"
    );
    Err(format!("chromium remote IP guard blocked request: {reason}"))
}

async fn chromium_tab_for_session(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<Arc<HeadlessTab>, String> {
    let chromium_sessions = runtime.chromium_sessions.lock().await;
    let Some(chromium_session) = chromium_sessions.get(session_id) else {
        return Err("chromium_session_not_found".to_owned());
    };
    chromium_session.tabs.get(tab_id).cloned().ok_or_else(|| "chromium_tab_not_found".to_owned())
}

async fn chromium_active_tab_for_session(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<(String, Arc<HeadlessTab>), String> {
    let active_tab_id = {
        let sessions = runtime.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return Err("session_not_found".to_owned());
        };
        session.active_tab_id.clone()
    };
    let tab = chromium_tab_for_session(runtime, session_id, active_tab_id.as_str()).await?;
    Ok((active_tab_id, tab))
}

async fn chromium_observe_snapshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<ChromiumObserveSnapshot, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let snapshot = run_chromium_blocking("chromium observe snapshot", move || {
        let page_body = tab
            .get_content()
            .map_err(|error| format!("failed to read Chromium DOM content: {error}"))?;
        let title = tab.get_title().unwrap_or_default();
        let page_url = tab.get_url();
        Ok(ChromiumObserveSnapshot { page_body, title, page_url })
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(snapshot)
}

async fn chromium_refresh_tab_snapshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let snapshot = chromium_observe_snapshot(runtime, session_id, tab_id).await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let mut sessions = runtime.sessions.lock().await;
    let Some(session) = sessions.get_mut(session_id) else {
        return Err("session_not_found".to_owned());
    };
    let Some(tab) = session.tabs.get_mut(tab_id) else {
        return Err("tab_not_found".to_owned());
    };
    tab.last_page_body = snapshot.page_body;
    tab.last_title = snapshot.title;
    tab.last_url = Some(snapshot.page_url);
    Ok(())
}

async fn chromium_get_title(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
) -> Result<String, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let tab = chromium_tab_for_session(runtime, session_id, tab_id).await?;
    let title = run_chromium_blocking("chromium get title", move || {
        tab.get_title().map_err(|error| format!("failed to read Chromium page title: {error}"))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(title)
}

async fn chromium_screenshot(
    runtime: &BrowserRuntimeState,
    session_id: &str,
) -> Result<Vec<u8>, String> {
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    let (_tab_id, tab) = chromium_active_tab_for_session(runtime, session_id).await?;
    let screenshot = run_chromium_blocking("chromium screenshot", move || {
        tab.capture_screenshot(Page::CaptureScreenshotFormatOption::Png, None, None, true)
            .map_err(|error| format!("failed to capture Chromium screenshot: {error}"))
    })
    .await?;
    enforce_chromium_remote_ip_guard(runtime, session_id).await?;
    Ok(screenshot)
}

async fn navigate_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    params: ChromiumNavigateParams,
) -> NavigateOutcome {
    let (tab_id, _tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return NavigateOutcome {
                success: false,
                final_url: String::new(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: 0,
                error: format!("chromium runtime unavailable: {error}"),
                network_log: Vec::new(),
                cookie_updates: Vec::new(),
            }
        }
    };
    navigate_tab_with_chromium(runtime, session_id, tab_id.as_str(), &params).await
}

async fn navigate_tab_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    tab_id: &str,
    params: &ChromiumNavigateParams,
) -> NavigateOutcome {
    let mut outcome = navigate_with_guards(
        params.raw_url.as_str(),
        params.timeout_ms,
        params.allow_redirects,
        params.max_redirects,
        params.allow_private_targets,
        params.max_response_bytes,
        params.cookie_header.as_deref(),
    )
    .await;
    if !outcome.success {
        return outcome;
    }
    let tab = match chromium_tab_for_session(runtime, session_id, tab_id).await {
        Ok(value) => value,
        Err(error) => {
            outcome.success = false;
            outcome.error = format!("chromium tab runtime unavailable: {error}");
            return outcome;
        }
    };
    let target_url = outcome.final_url.clone();
    let chromium_timeout_ms = params.timeout_ms;
    let chromium_snapshot = run_chromium_blocking("chromium navigate", move || {
        tab.set_default_timeout(Duration::from_millis(chromium_timeout_ms.max(1)));
        tab.navigate_to(target_url.as_str())
            .map_err(|error| format!("failed to issue Chromium navigation command: {error}"))?;
        tab.wait_until_navigated()
            .map_err(|error| format!("Chromium navigation timeout or failure: {error}"))?;
        let page_body = tab.get_content().map_err(|error| {
            format!("failed to read Chromium page HTML after navigation: {error}")
        })?;
        let title = tab.get_title().unwrap_or_default();
        let page_url = tab.get_url();
        Ok(ChromiumObserveSnapshot { page_body, title, page_url })
    })
    .await;
    let snapshot = match chromium_snapshot {
        Ok(value) => value,
        Err(error) => {
            outcome.success = false;
            outcome.error = error;
            return outcome;
        }
    };
    if let Err(error) = enforce_chromium_remote_ip_guard(runtime, session_id).await {
        outcome.success = false;
        outcome.error = error;
        return outcome;
    }
    let body_bytes = snapshot.page_body.len() as u64;
    if body_bytes > params.max_response_bytes {
        outcome.success = false;
        outcome.error = format!(
            "response exceeds max_response_bytes ({} > {})",
            body_bytes, params.max_response_bytes
        );
        outcome.body_bytes = body_bytes;
        outcome.page_body.clear();
        outcome.title.clear();
        return outcome;
    }
    outcome.final_url = snapshot.page_url;
    outcome.title = snapshot.title;
    outcome.page_body = snapshot.page_body;
    outcome.body_bytes = body_bytes;
    outcome
}

async fn click_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    timeout_ms: u64,
    max_attempts: u32,
    allow_downloads: bool,
) -> ChromiumActionOutcome {
    enum ClickAttempt {
        Clicked { download_like: bool },
        DownloadBlocked,
        NotFound,
    }

    let (tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumActionOutcome {
                success: false,
                outcome: "chromium_runtime_missing".to_owned(),
                error,
                attempts: 1,
            }
        }
    };
    let started = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        let selector_for_attempt = selector.to_owned();
        let tab_for_attempt = Arc::clone(&tab);
        let attempt = run_chromium_blocking("chromium click", move || {
            let page_body = tab_for_attempt
                .get_content()
                .map_err(|error| format!("failed to read Chromium DOM before click: {error}"))?;
            if let Some(tag) =
                find_matching_html_tag(selector_for_attempt.as_str(), page_body.as_str())
            {
                if is_download_like_tag(tag.as_str()) && !allow_downloads {
                    return Ok(ClickAttempt::DownloadBlocked);
                }
                let element = tab_for_attempt.find_element(selector_for_attempt.as_str()).map_err(
                    |error| {
                        format!(
                            "failed to resolve selector '{}' on Chromium page: {error}",
                            selector_for_attempt
                        )
                    },
                )?;
                element.click().map_err(|error| {
                    format!(
                        "failed to click selector '{}' on Chromium page: {error}",
                        selector_for_attempt
                    )
                })?;
                Ok(ClickAttempt::Clicked { download_like: is_download_like_tag(tag.as_str()) })
            } else {
                Ok(ClickAttempt::NotFound)
            }
        })
        .await;

        match attempt {
            Ok(ClickAttempt::Clicked { download_like }) => {
                let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                return ChromiumActionOutcome {
                    success: true,
                    outcome: if download_like {
                        "download_allowed".to_owned()
                    } else {
                        "clicked".to_owned()
                    },
                    error: String::new(),
                    attempts,
                };
            }
            Ok(ClickAttempt::DownloadBlocked) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "download_blocked".to_owned(),
                    error:
                        "download-like click is blocked by session policy (allow_downloads=false)"
                            .to_owned(),
                    attempts,
                };
            }
            Ok(ClickAttempt::NotFound) => {}
            Err(error) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "click_failed".to_owned(),
                    error,
                    attempts,
                };
            }
        }

        if attempts >= max_attempts || started.elapsed() >= Duration::from_millis(timeout_ms) {
            break;
        }
        let remaining_ms = timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
        let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
    ChromiumActionOutcome {
        success: false,
        outcome: "selector_not_found".to_owned(),
        error: format!("selector '{selector}' was not found"),
        attempts,
    }
}

async fn type_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    text: &str,
    clear_existing: bool,
    timeout_ms: u64,
) -> ChromiumActionOutcome {
    enum TypeAttempt {
        Typed,
        NotFound,
        NotTypable,
    }

    let (tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumActionOutcome {
                success: false,
                outcome: "chromium_runtime_missing".to_owned(),
                error,
                attempts: 1,
            }
        }
    };
    let started = Instant::now();
    let mut attempts = 0_u32;
    loop {
        attempts = attempts.saturating_add(1);
        let selector_for_attempt = selector.to_owned();
        let text_for_attempt = text.to_owned();
        let tab_for_attempt = Arc::clone(&tab);
        let clear_existing_for_attempt = clear_existing;
        let attempt = run_chromium_blocking("chromium type", move || {
            let page_body = tab_for_attempt
                .get_content()
                .map_err(|error| format!("failed to read Chromium DOM before type action: {error}"))?;
            let Some(tag) = find_matching_html_tag(selector_for_attempt.as_str(), page_body.as_str()) else {
                return Ok(TypeAttempt::NotFound);
            };
            if !is_typable_tag(tag.as_str()) {
                return Ok(TypeAttempt::NotTypable);
            }
            let element = tab_for_attempt.find_element(selector_for_attempt.as_str()).map_err(
                |error| format!("failed to resolve selector '{}' on Chromium page: {error}", selector_for_attempt),
            )?;
            if clear_existing_for_attempt {
                let _ = element.call_js_fn(
                    "function () { if (this && this.value !== undefined) { this.value = ''; } if (this && this.textContent !== undefined) { this.textContent = ''; } }",
                    Vec::new(),
                    false,
                );
            }
            element
                .click()
                .map_err(|error| format!("failed to focus selector '{}' for type action: {error}", selector_for_attempt))?;
            element
                .type_into(text_for_attempt.as_str())
                .map_err(|error| format!("failed to type into selector '{}' on Chromium page: {error}", selector_for_attempt))?;
            Ok(TypeAttempt::Typed)
        })
        .await;

        match attempt {
            Ok(TypeAttempt::Typed) => {
                let _ = chromium_refresh_tab_snapshot(runtime, session_id, tab_id.as_str()).await;
                return ChromiumActionOutcome {
                    success: true,
                    outcome: "typed".to_owned(),
                    error: String::new(),
                    attempts,
                };
            }
            Ok(TypeAttempt::NotTypable) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "selector_not_typable".to_owned(),
                    error: format!("selector '{selector}' does not target an input-like element"),
                    attempts,
                };
            }
            Ok(TypeAttempt::NotFound) => {}
            Err(error) => {
                return ChromiumActionOutcome {
                    success: false,
                    outcome: "type_failed".to_owned(),
                    error,
                    attempts,
                };
            }
        }
        if started.elapsed() >= Duration::from_millis(timeout_ms) {
            break;
        }
        let remaining_ms = timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
        let sleep_ms = DEFAULT_ACTION_RETRY_INTERVAL_MS.min(remaining_ms.max(1));
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
    ChromiumActionOutcome {
        success: false,
        outcome: "selector_not_found".to_owned(),
        error: format!("selector '{selector}' was not found"),
        attempts,
    }
}

async fn scroll_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    delta_x: i64,
    delta_y: i64,
) -> ChromiumScrollOutcome {
    let (tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumScrollOutcome { success: false, scroll_x: 0, scroll_y: 0, error }
        }
    };
    let scroll_script = format!(
        "(() => {{ window.scrollBy({delta_x}, {delta_y}); return {{ x: Math.trunc(window.scrollX || window.pageXOffset || 0), y: Math.trunc(window.scrollY || window.pageYOffset || 0) }}; }})()"
    );
    let positions = run_chromium_blocking("chromium scroll", move || {
        let value = tab
            .evaluate(scroll_script.as_str(), false)
            .map_err(|error| format!("failed to execute Chromium scroll script: {error}"))?
            .value
            .unwrap_or(serde_json::Value::Null);
        let x = value.get("x").and_then(serde_json::Value::as_i64).unwrap_or(0);
        let y = value.get("y").and_then(serde_json::Value::as_i64).unwrap_or(0);
        Ok((x, y))
    })
    .await;

    match positions {
        Ok((scroll_x, scroll_y)) => {
            let mut sessions = runtime.sessions.lock().await;
            if let Some(session) = sessions.get_mut(session_id) {
                if let Some(tab_record) = session.tabs.get_mut(tab_id.as_str()) {
                    tab_record.scroll_x = scroll_x;
                    tab_record.scroll_y = scroll_y;
                }
            }
            ChromiumScrollOutcome { success: true, scroll_x, scroll_y, error: String::new() }
        }
        Err(error) => ChromiumScrollOutcome { success: false, scroll_x: 0, scroll_y: 0, error },
    }
}

async fn wait_for_with_chromium(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    text: &str,
    timeout_ms: u64,
    poll_interval_ms: u64,
) -> ChromiumWaitOutcome {
    let (_tab_id, tab) = match chromium_active_tab_for_session(runtime, session_id).await {
        Ok(value) => value,
        Err(error) => {
            return ChromiumWaitOutcome {
                success: false,
                matched_selector: String::new(),
                matched_text: String::new(),
                attempts: 1,
                waited_ms: 0,
                error,
            }
        }
    };
    let started = Instant::now();
    let mut attempts = 0_u32;
    let selector_owned = selector.to_owned();
    let text_owned = text.to_owned();
    loop {
        attempts = attempts.saturating_add(1);
        let tab_for_attempt = Arc::clone(&tab);
        let selector_for_attempt = selector_owned.clone();
        let text_for_attempt = text_owned.clone();
        let check = run_chromium_blocking("chromium wait_for probe", move || {
            let mut matched_selector = false;
            let mut matched_text = false;
            if !selector_for_attempt.is_empty() {
                matched_selector = tab_for_attempt.find_element(selector_for_attempt.as_str()).is_ok();
            }
            if !text_for_attempt.trim().is_empty() {
                let text_json = serde_json::to_string(text_for_attempt.as_str())
                    .map_err(|error| format!("failed to encode wait_for text query: {error}"))?;
                let script = format!(
                    "(() => {{ const text = (document.body && document.body.innerText) ? document.body.innerText : ''; return text.includes({text_json}); }})()"
                );
                matched_text = tab_for_attempt
                    .evaluate(script.as_str(), false)
                    .map_err(|error| format!("failed to evaluate Chromium wait_for text probe: {error}"))?
                    .value
                    .and_then(|value| value.as_bool())
                    .unwrap_or(false);
            }
            Ok((matched_selector, matched_text))
        })
        .await;

        match check {
            Ok((selector_hit, text_hit)) => {
                if selector_hit {
                    return ChromiumWaitOutcome {
                        success: true,
                        matched_selector: selector_owned.clone(),
                        matched_text: String::new(),
                        attempts,
                        waited_ms: started.elapsed().as_millis() as u64,
                        error: String::new(),
                    };
                }
                if text_hit {
                    return ChromiumWaitOutcome {
                        success: true,
                        matched_selector: String::new(),
                        matched_text: text_owned.clone(),
                        attempts,
                        waited_ms: started.elapsed().as_millis() as u64,
                        error: String::new(),
                    };
                }
            }
            Err(error) => {
                return ChromiumWaitOutcome {
                    success: false,
                    matched_selector: String::new(),
                    matched_text: String::new(),
                    attempts,
                    waited_ms: started.elapsed().as_millis() as u64,
                    error,
                };
            }
        }
        if started.elapsed() >= Duration::from_millis(timeout_ms) {
            break;
        }
        let remaining_ms = timeout_ms.saturating_sub(started.elapsed().as_millis() as u64);
        let sleep_ms = poll_interval_ms.min(remaining_ms.max(1));
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }
    ChromiumWaitOutcome {
        success: false,
        matched_selector: String::new(),
        matched_text: String::new(),
        attempts,
        waited_ms: started.elapsed().as_millis() as u64,
        error: "wait_for condition was not satisfied before timeout".to_owned(),
    }
}

fn validate_target_url_blocking(raw_url: &str, allow_private_targets: bool) -> Result<(), String> {
    if raw_url.eq_ignore_ascii_case("about:blank") {
        return Ok(());
    }
    let url = Url::parse(raw_url).map_err(|error| format!("invalid URL: {error}"))?;
    validate_target_url_parts_blocking(&url, allow_private_targets)
}

fn validate_target_url_parts_blocking(
    url: &Url,
    allow_private_targets: bool,
) -> Result<(), String> {
    let result = (|| {
        let (host, port) = extract_target_host_port(url)?;
        let resolved = resolve_host_addresses_blocking(host, port)?;
        enforce_resolved_host_policy(host, resolved, allow_private_targets)
    })();
    maybe_log_dns_validation_metrics();
    result
}

fn lock_dns_validation_cache() -> std::sync::MutexGuard<'static, DnsValidationCache> {
    DNS_VALIDATION_CACHE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn normalize_dns_host_cache_key(host: &str) -> String {
    host.trim().trim_end_matches('.').to_ascii_lowercase()
}

fn is_nxdomain_lookup_error(error: &std::io::Error) -> bool {
    if error.kind() == std::io::ErrorKind::NotFound {
        return true;
    }
    let message = error.to_string().to_ascii_lowercase();
    message.contains("no such host")
        || message.contains("host not found")
        || message.contains("name or service not known")
        || message.contains("nodename nor servname provided")
}

fn dns_resolution_error_for_host(host: &str, error: &std::io::Error) -> String {
    format!("DNS resolution failed for host '{host}': {error}")
}

fn dns_cached_nxdomain_error_for_host(host: &str) -> String {
    format!("DNS resolution failed for host '{host}': cached NXDOMAIN")
}

fn lookup_dns_resolution_cache(host: &str) -> Option<DnsCacheResolution> {
    let key = normalize_dns_host_cache_key(host);
    let now = Instant::now();
    let mut cache = lock_dns_validation_cache();
    let cached = cache.lookup(key.as_str(), now);
    if cached.is_some() {
        DNS_VALIDATION_METRICS.cache_hits.fetch_add(1, Ordering::Relaxed);
    } else {
        DNS_VALIDATION_METRICS.cache_misses.fetch_add(1, Ordering::Relaxed);
    }
    cached
}

fn store_dns_resolution_cache(host: &str, resolved: ResolvedHostAddresses) {
    let key = normalize_dns_host_cache_key(host);
    let now = Instant::now();
    let mut cache = lock_dns_validation_cache();
    cache.insert_resolved(key, resolved, now);
}

fn store_dns_nxdomain_cache(host: &str) {
    let key = normalize_dns_host_cache_key(host);
    let now = Instant::now();
    let mut cache = lock_dns_validation_cache();
    cache.insert_nxdomain(key, now);
}

fn extract_target_host_port(url: &Url) -> Result<(&str, u16), String> {
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!("blocked URL scheme '{}'", url.scheme()));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("URL credentials are not allowed".to_owned());
    }
    let host = url.host_str().ok_or_else(|| "URL host is required".to_owned())?;
    let port =
        url.port_or_known_default().ok_or_else(|| "URL port could not be resolved".to_owned())?;
    Ok((host, port))
}

fn track_dns_lookup_latency(lookup_started: Instant) {
    let lookup_latency_ms = lookup_started.elapsed().as_millis() as u64;
    DNS_VALIDATION_METRICS.dns_lookups.fetch_add(1, Ordering::Relaxed);
    DNS_VALIDATION_METRICS
        .dns_lookup_latency_ms_total
        .fetch_add(lookup_latency_ms, Ordering::Relaxed);
}

fn resolve_host_addresses_blocking(host: &str, port: u16) -> Result<ResolvedHostAddresses, String> {
    if let Some(address) = netguard::parse_host_ip_literal(host)? {
        return ResolvedHostAddresses::from_addresses(vec![address]);
    }

    if let Some(cached) = lookup_dns_resolution_cache(host) {
        return match cached {
            DnsCacheResolution::Resolved(resolved) => Ok(resolved),
            DnsCacheResolution::NxDomain => {
                DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
                DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
                Err(dns_cached_nxdomain_error_for_host(host))
            }
        };
    }

    let lookup_started = Instant::now();
    let addresses = (host, port)
        .to_socket_addrs()
        .map_err(|error| {
            track_dns_lookup_latency(lookup_started);
            if is_nxdomain_lookup_error(&error) {
                store_dns_nxdomain_cache(host);
            }
            DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
            DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
            dns_resolution_error_for_host(host, &error)
        })?
        .map(|socket| socket.ip())
        .collect::<Vec<_>>();
    track_dns_lookup_latency(lookup_started);
    let resolved = ResolvedHostAddresses::from_addresses(addresses).map_err(|error| {
        DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
        DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
        format!("{error} for host '{host}'")
    })?;
    store_dns_resolution_cache(host, resolved.clone());
    Ok(resolved)
}

async fn resolve_host_addresses_async(
    host: &str,
    port: u16,
) -> Result<ResolvedHostAddresses, String> {
    if let Some(address) = netguard::parse_host_ip_literal(host)? {
        return ResolvedHostAddresses::from_addresses(vec![address]);
    }

    if let Some(cached) = lookup_dns_resolution_cache(host) {
        return match cached {
            DnsCacheResolution::Resolved(resolved) => Ok(resolved),
            DnsCacheResolution::NxDomain => {
                DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
                DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
                Err(dns_cached_nxdomain_error_for_host(host))
            }
        };
    }

    let lookup_started = Instant::now();
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|error| {
            track_dns_lookup_latency(lookup_started);
            if is_nxdomain_lookup_error(&error) {
                store_dns_nxdomain_cache(host);
            }
            DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
            DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
            dns_resolution_error_for_host(host, &error)
        })?
        .map(|socket| socket.ip())
        .collect::<Vec<_>>();
    track_dns_lookup_latency(lookup_started);
    let resolved = ResolvedHostAddresses::from_addresses(addresses).map_err(|error| {
        DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
        DNS_VALIDATION_METRICS.blocked_dns_failures.fetch_add(1, Ordering::Relaxed);
        format!("{error} for host '{host}'")
    })?;
    store_dns_resolution_cache(host, resolved.clone());
    Ok(resolved)
}

fn enforce_resolved_host_policy(
    host: &str,
    resolved: ResolvedHostAddresses,
    allow_private_targets: bool,
) -> Result<(), String> {
    if !allow_private_targets && resolved.blocked_for_default_policy {
        let preview = resolved
            .addresses
            .iter()
            .take(4)
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        DNS_VALIDATION_METRICS.blocked_total.fetch_add(1, Ordering::Relaxed);
        DNS_VALIDATION_METRICS.blocked_private_targets.fetch_add(1, Ordering::Relaxed);
        return Err(format!(
            "target resolves to private/local address and is blocked by policy (host '{host}', addresses [{preview}])"
        ));
    }
    Ok(())
}

fn dns_validation_metrics_snapshot() -> DnsValidationMetricsSnapshot {
    let cache_entries = lock_dns_validation_cache().len();
    DNS_VALIDATION_METRICS.snapshot(cache_entries)
}

fn maybe_log_dns_validation_metrics() {
    let observations = DNS_VALIDATION_METRICS.observations.fetch_add(1, Ordering::Relaxed) + 1;
    if !observations.is_multiple_of(DNS_VALIDATION_METRICS_LOG_INTERVAL) {
        return;
    }
    let snapshot = dns_validation_metrics_snapshot();
    info!(
        dns_cache_entries = snapshot.cache_entries,
        dns_cache_hits = snapshot.cache_hits,
        dns_cache_misses = snapshot.cache_misses,
        dns_cache_hit_ratio = snapshot.cache_hit_ratio(),
        dns_lookup_count = snapshot.dns_lookups,
        dns_lookup_avg_latency_ms = snapshot.lookup_avg_latency_ms(),
        dns_blocked_total = snapshot.blocked_total,
        dns_blocked_private_targets = snapshot.blocked_private_targets,
        dns_blocked_dns_failures = snapshot.blocked_dns_failures,
        "browserd DNS validation metrics snapshot"
    );
}

#[cfg(test)]
fn reset_dns_validation_tracking_for_tests() {
    let mut cache = lock_dns_validation_cache();
    cache.entries.clear();
    cache.next_access_tick = 0;
    drop(cache);
    DNS_VALIDATION_METRICS.reset_for_tests();
}

#[cfg(test)]
fn dns_validation_metrics_snapshot_for_tests() -> DnsValidationMetricsSnapshot {
    dns_validation_metrics_snapshot()
}

async fn navigate_with_guards(
    raw_url: &str,
    timeout_ms: u64,
    allow_redirects: bool,
    max_redirects: u32,
    allow_private_targets: bool,
    max_response_bytes: u64,
    cookie_header: Option<&str>,
) -> NavigateOutcome {
    let started_at = Instant::now();
    let mut network_log = Vec::new();
    let mut cookie_updates = Vec::new();
    let mut current_url = match Url::parse(raw_url) {
        Ok(value) => value,
        Err(error) => {
            return NavigateOutcome {
                success: false,
                final_url: String::new(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error: format!("invalid URL: {error}"),
                network_log,
                cookie_updates,
            }
        }
    };
    let client = match reqwest::Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_millis(timeout_ms.max(1)))
        .build()
    {
        Ok(value) => value,
        Err(error) => {
            return NavigateOutcome {
                success: false,
                final_url: current_url.to_string(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error: format!("failed to build HTTP client: {error}"),
                network_log,
                cookie_updates,
            }
        }
    };

    let redirect_limit = max_redirects.clamp(1, 10);
    let mut redirects = 0_u32;
    loop {
        if let Err(error) = validate_target_url(&current_url, allow_private_targets).await {
            return NavigateOutcome {
                success: false,
                final_url: current_url.to_string(),
                status_code: 0,
                title: String::new(),
                page_body: String::new(),
                body_bytes: 0,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error,
                network_log,
                cookie_updates,
            };
        }

        let request_started = Instant::now();
        let mut request_builder = client.get(current_url.clone());
        if let Some(value) = cookie_header.filter(|value| !value.trim().is_empty()) {
            request_builder = request_builder.header(COOKIE_HEADER, value);
        }
        let response = match request_builder.send().await {
            Ok(value) => value,
            Err(error) => {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: 0,
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: format!("request failed: {error}"),
                    network_log,
                    cookie_updates,
                }
            }
        };
        if let Some(domain) = current_url.host_str() {
            for raw_set_cookie in response.headers().get_all(SET_COOKIE_HEADER).iter() {
                if let Ok(value) = raw_set_cookie.to_str() {
                    if let Some(update) = parse_set_cookie_update(domain, value) {
                        cookie_updates.push(update);
                    }
                }
            }
        }
        let request_latency_ms = request_started.elapsed().as_millis() as u64;
        network_log.push(NetworkLogEntryInternal {
            request_url: normalize_url_with_redaction(current_url.as_str()),
            status_code: response.status().as_u16(),
            timing_bucket: timing_bucket_for_latency(request_latency_ms).to_owned(),
            latency_ms: request_latency_ms,
            captured_at_unix_ms: current_unix_ms(),
            headers: sanitize_network_headers(response.headers()),
        });

        if response.status().is_redirection() {
            if !allow_redirects {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: "redirect response blocked by policy".to_owned(),
                    network_log,
                    cookie_updates,
                };
            }
            if redirects >= redirect_limit {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: format!("redirect limit exceeded ({redirect_limit})"),
                    network_log,
                    cookie_updates,
                };
            }
            let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: "redirect missing Location header".to_owned(),
                    network_log,
                    cookie_updates,
                };
            };
            let Ok(location_str) = location.to_str() else {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code: response.status().as_u16(),
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: "redirect location header contains invalid UTF-8".to_owned(),
                    network_log,
                    cookie_updates,
                };
            };
            current_url = match current_url.join(location_str) {
                Ok(value) => value,
                Err(error) => {
                    return NavigateOutcome {
                        success: false,
                        final_url: current_url.to_string(),
                        status_code: response.status().as_u16(),
                        title: String::new(),
                        page_body: String::new(),
                        body_bytes: 0,
                        latency_ms: started_at.elapsed().as_millis() as u64,
                        error: format!("invalid redirect target: {error}"),
                        network_log,
                        cookie_updates,
                    }
                }
            };
            redirects = redirects.saturating_add(1);
            continue;
        }

        let status_code = response.status().as_u16();
        let body = match response.bytes().await {
            Ok(value) => value,
            Err(error) => {
                return NavigateOutcome {
                    success: false,
                    final_url: current_url.to_string(),
                    status_code,
                    title: String::new(),
                    page_body: String::new(),
                    body_bytes: 0,
                    latency_ms: started_at.elapsed().as_millis() as u64,
                    error: format!("failed to read response body: {error}"),
                    network_log,
                    cookie_updates,
                }
            }
        };

        if (body.len() as u64) > max_response_bytes {
            return NavigateOutcome {
                success: false,
                final_url: current_url.to_string(),
                status_code,
                title: String::new(),
                page_body: String::new(),
                body_bytes: body.len() as u64,
                latency_ms: started_at.elapsed().as_millis() as u64,
                error: format!(
                    "response exceeds max_response_bytes ({} > {max_response_bytes})",
                    body.len()
                ),
                network_log,
                cookie_updates,
            };
        }

        let page_body = String::from_utf8_lossy(body.as_ref()).to_string();

        return NavigateOutcome {
            success: (200..400).contains(&status_code),
            final_url: current_url.to_string(),
            status_code,
            title: extract_html_title(page_body.as_str()).unwrap_or_default().to_owned(),
            page_body,
            body_bytes: body.len() as u64,
            latency_ms: started_at.elapsed().as_millis() as u64,
            error: if status_code >= 400 {
                format!("navigation returned HTTP {status_code}")
            } else {
                String::new()
            },
            network_log,
            cookie_updates,
        };
    }
}

async fn validate_target_url(url: &Url, allow_private_targets: bool) -> Result<(), String> {
    let result = async {
        let (host, port) = extract_target_host_port(url)?;
        let resolved = resolve_host_addresses_async(host, port).await?;
        enforce_resolved_host_policy(host, resolved, allow_private_targets)
    }
    .await;
    maybe_log_dns_validation_metrics();
    result
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
    for entry in entries {
        tab.network_log.push_back(entry.clone());
    }
    while tab.network_log.len() > max_entries {
        tab.network_log.pop_front();
    }
    while tab.network_log.iter().map(estimate_network_log_entry_internal_bytes).sum::<usize>()
        > max_bytes as usize
    {
        if tab.network_log.pop_front().is_none() {
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
                name: header.name,
                value: header.value,
            })
            .collect()
    } else {
        Vec::new()
    };
    browser_v1::NetworkLogEntry {
        v: CANONICAL_PROTOCOL_MAJOR,
        request_url: entry.request_url,
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

fn build_state_store_from_env() -> Result<Option<PersistedStateStore>> {
    let key_raw = match std::env::var(STATE_KEY_ENV) {
        Ok(value) => value.trim().to_owned(),
        Err(_) => return Ok(None),
    };
    if key_raw.is_empty() {
        return Ok(None);
    }
    let key = decode_state_key(key_raw.as_str())?;
    let state_dir = std::env::var(STATE_DIR_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(|value| normalize_configured_state_path(value.as_str(), STATE_DIR_ENV))
        .transpose()?
        .unwrap_or(default_browserd_state_dir()?);
    Ok(Some(PersistedStateStore::new(state_dir, key)?))
}

fn normalize_configured_state_path(raw: &str, field: &'static str) -> Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{field} cannot be empty");
    }
    let path = PathBuf::from(trimmed);
    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            anyhow::bail!("{field} cannot contain '..' path segments");
        }
    }
    Ok(path)
}

fn default_browserd_state_dir() -> Result<PathBuf> {
    default_browserd_state_dir_from_env(
        std::env::var_os(STATE_ROOT_ENV),
        std::env::var_os("APPDATA"),
        std::env::var_os("LOCALAPPDATA"),
        std::env::var_os("XDG_STATE_HOME"),
        std::env::var_os("HOME"),
    )
}

fn default_browserd_state_dir_from_env(
    state_root: Option<OsString>,
    appdata: Option<OsString>,
    local_appdata: Option<OsString>,
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf> {
    if let Some(state_root_raw) = state_root {
        let normalized = normalize_configured_state_path(
            state_root_raw.to_string_lossy().as_ref(),
            STATE_ROOT_ENV,
        )?;
        return Ok(normalized.join("browserd"));
    }
    #[cfg(windows)]
    {
        let _ = xdg_state_home;
        let _ = home;
        if let Some(appdata) = appdata {
            return Ok(PathBuf::from(appdata).join("Palyra").join("browserd"));
        }
        if let Some(local_appdata) = local_appdata {
            return Ok(PathBuf::from(local_appdata).join("Palyra").join("browserd"));
        }
        anyhow::bail!(
            "failed to resolve browserd state dir: APPDATA/LOCALAPPDATA are unset and {STATE_ROOT_ENV} is not configured"
        );
    }
    #[cfg(target_os = "macos")]
    {
        let _ = appdata;
        let _ = local_appdata;
        let _ = xdg_state_home;
        let home = home.ok_or_else(|| {
            anyhow::anyhow!(
                "failed to resolve browserd state dir: HOME is unset and {STATE_ROOT_ENV} is not configured"
            )
        })?;
        return Ok(PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("Palyra")
            .join("browserd"));
    }
    #[cfg(all(not(windows), not(target_os = "macos")))]
    {
        let _ = appdata;
        let _ = local_appdata;
        if let Some(xdg_state_home) = xdg_state_home {
            return Ok(PathBuf::from(xdg_state_home).join("palyra").join("browserd"));
        }
        let home = home.ok_or_else(|| {
            anyhow::anyhow!(
                "failed to resolve browserd state dir: XDG_STATE_HOME/HOME are unset and {STATE_ROOT_ENV} is not configured"
            )
        })?;
        Ok(PathBuf::from(home).join(".local").join("state").join("palyra").join("browserd"))
    }
}

fn ensure_owner_only_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)
        .with_context(|| format!("failed to create browserd state dir '{}'", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).with_context(|| {
            format!(
                "failed to enforce owner-only directory permissions on browserd state dir '{}'",
                path.display()
            )
        })?;
    }
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(path, owner_sid.as_str(), true)?;
    }
    Ok(())
}

fn ensure_owner_only_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to enforce owner-only permissions on browserd state file '{}'",
                path.display()
            )
        })?;
    }
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(path, owner_sid.as_str(), false)?;
    }
    Ok(())
}

#[cfg(windows)]
fn current_user_sid() -> Result<String> {
    let output = Command::new("whoami")
        .args(["/user", "/fo", "csv", "/nh"])
        .output()
        .context("failed to execute whoami while resolving browserd state ACL SID")?;
    if !output.status.success() {
        anyhow::bail!(
            "whoami returned non-success status {} while resolving browserd state ACL SID: stdout={} stderr={}",
            output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    parse_whoami_sid_csv(String::from_utf8_lossy(&output.stdout).trim())
        .ok_or_else(|| anyhow::anyhow!("failed to parse user SID from whoami output"))
}

#[cfg(windows)]
fn parse_whoami_sid_csv(raw: &str) -> Option<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in raw.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.trim().to_owned());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current.trim().to_owned());
    if fields.len() < 2 {
        return None;
    }
    let sid = fields[1].trim().trim_matches('"').to_owned();
    if sid.starts_with("S-1-") {
        Some(sid)
    } else {
        None
    }
}

#[cfg(windows)]
fn harden_windows_path_permissions(path: &Path, owner_sid: &str, is_directory: bool) -> Result<()> {
    let grant_mask = if is_directory { "(OI)(CI)F" } else { "F" };
    let owner_grant = format!("*{owner_sid}:{grant_mask}");
    let system_grant = format!("*{WINDOWS_SYSTEM_SID}:{grant_mask}");
    let output = Command::new("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r"])
        .arg(owner_grant)
        .args(["/grant:r"])
        .arg(system_grant)
        .output()
        .with_context(|| {
            format!("failed to execute icacls for browserd state path '{}'", path.display())
        })?;
    if !output.status.success() {
        anyhow::bail!(
            "icacls returned non-success status {} for '{}': stdout={} stderr={}",
            output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
            path.display(),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        );
    }
    Ok(())
}

fn decode_state_key(raw: &str) -> Result<[u8; STATE_KEY_LEN]> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw)
        .context("failed to decode PALYRA_BROWSERD_STATE_ENCRYPTION_KEY as base64")?;
    if decoded.len() != STATE_KEY_LEN {
        anyhow::bail!(
            "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY must decode to exactly {STATE_KEY_LEN} bytes"
        );
    }
    let mut key = [0_u8; STATE_KEY_LEN];
    key.copy_from_slice(decoded.as_slice());
    Ok(key)
}

impl PersistedStateStore {
    fn new(root_dir: PathBuf, key: [u8; STATE_KEY_LEN]) -> Result<Self> {
        ensure_path_is_not_symlink(root_dir.as_path(), "browserd state dir")?;
        ensure_owner_only_dir(root_dir.as_path())?;
        ensure_path_is_secure_directory(root_dir.as_path(), "browserd state dir")?;
        let store = Self { root_dir, key };
        store.cleanup_tmp_files()?;
        Ok(store)
    }

    fn snapshot_path(&self, state_id: &str) -> PathBuf {
        self.root_dir.join(format!("{state_id}.enc"))
    }

    fn tmp_snapshot_path(&self, state_id: &str) -> PathBuf {
        self.root_dir.join(format!("{state_id}.{}.{}", Ulid::new(), STATE_TMP_EXTENSION))
    }

    fn profile_registry_path(&self) -> PathBuf {
        self.root_dir.join(PROFILE_REGISTRY_FILE_NAME)
    }

    fn cleanup_tmp_files(&self) -> Result<()> {
        let entries = match fs::read_dir(self.root_dir.as_path()) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to enumerate browser state dir '{}' for tmp cleanup",
                        self.root_dir.display()
                    )
                })
            }
        };
        for entry in entries {
            let entry = entry.with_context(|| {
                format!("failed to read browser state entry in '{}'", self.root_dir.display())
            })?;
            let path = entry.path();
            let file_type = entry.file_type().with_context(|| {
                format!("failed to inspect browser state entry type for '{}'", path.display())
            })?;
            if file_type.is_symlink() {
                anyhow::bail!(
                    "browser state dir '{}' contains unexpected symlink entry '{}'",
                    self.root_dir.display(),
                    path.display()
                );
            }
            if path
                .extension()
                .and_then(|value| value.to_str())
                .map(|value| value.eq_ignore_ascii_case(STATE_TMP_EXTENSION))
                .unwrap_or(false)
            {
                let _ = fs::remove_file(path.as_path());
            }
        }
        Ok(())
    }

    fn load_snapshot(
        &self,
        state_id: &str,
        profile_id: Option<&str>,
    ) -> Result<Option<PersistedSessionSnapshot>> {
        let path = self.snapshot_path(state_id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = read_hardened_file(path.as_path(), "persisted browser state")?;
        let key = derive_state_encryption_key(&self.key, profile_id);
        let decrypted = decrypt_state_blob(&key, bytes.as_slice()).with_context(|| {
            format!("failed to decrypt persisted browser state '{}'", path.display())
        })?;
        let snapshot: PersistedSessionSnapshot = serde_json::from_slice(decrypted.as_slice())
            .with_context(|| {
                format!("failed to deserialize persisted browser state '{}'", path.display())
            })?;
        Ok(Some(snapshot))
    }

    fn save_snapshot(
        &self,
        state_id: &str,
        profile_id: Option<&str>,
        snapshot: &PersistedSessionSnapshot,
    ) -> Result<()> {
        let serialized =
            serde_json::to_vec(snapshot).context("failed to serialize persisted browser state")?;
        let key = derive_state_encryption_key(&self.key, profile_id);
        let encrypted =
            encrypt_state_blob(&key, serialized.as_slice()).context("failed to encrypt state")?;
        let target_path = self.snapshot_path(state_id);
        let tmp_path = self.tmp_snapshot_path(state_id);
        write_hardened_file_atomic(
            self.root_dir.as_path(),
            target_path.as_path(),
            tmp_path.as_path(),
            encrypted.as_slice(),
            "persisted browser state",
        )?;
        Ok(())
    }

    fn delete_snapshot(&self, state_id: &str) -> Result<()> {
        let path = self.snapshot_path(state_id);
        if !path.exists() {
            return Ok(());
        }
        ensure_path_is_not_symlink(path.as_path(), "persisted browser state")?;
        fs::remove_file(path.as_path()).with_context(|| {
            format!("failed to delete persisted browser state '{}'", path.display())
        })?;
        Ok(())
    }

    fn load_profile_registry(&self) -> Result<BrowserProfileRegistryDocument> {
        let path = self.profile_registry_path();
        if !path.exists() {
            return Ok(BrowserProfileRegistryDocument::default());
        }
        let bytes = read_hardened_file(path.as_path(), "browser profile registry")?;
        let decrypted = decrypt_state_blob(&self.key, bytes.as_slice()).with_context(|| {
            format!("failed to decrypt browser profile registry '{}'", path.display())
        })?;
        let mut registry: BrowserProfileRegistryDocument =
            serde_json::from_slice(decrypted.as_slice()).with_context(|| {
                format!("failed to deserialize browser profile registry '{}'", path.display())
            })?;
        normalize_profile_registry(&mut registry);
        Ok(registry)
    }

    fn save_profile_registry(&self, registry: &BrowserProfileRegistryDocument) -> Result<()> {
        let serialized = serde_json::to_vec(registry)
            .context("failed to serialize browser profile registry document")?;
        if serialized.len() > MAX_PROFILE_REGISTRY_BYTES {
            anyhow::bail!(
                "browser profile registry exceeds max bytes ({} > {})",
                serialized.len(),
                MAX_PROFILE_REGISTRY_BYTES
            );
        }
        let encrypted = encrypt_state_blob(&self.key, serialized.as_slice())
            .context("failed to encrypt browser profile registry")?;
        let target_path = self.profile_registry_path();
        let tmp_path = self.root_dir.join(format!(
            "{}.{}.{}",
            PROFILE_REGISTRY_FILE_NAME,
            Ulid::new(),
            STATE_TMP_EXTENSION
        ));
        write_hardened_file_atomic(
            self.root_dir.as_path(),
            target_path.as_path(),
            tmp_path.as_path(),
            encrypted.as_slice(),
            "browser profile registry",
        )?;
        Ok(())
    }
}

fn ensure_path_is_not_symlink(path: &Path, context: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                anyhow::bail!("{context} '{}' must not be a symlink", path.display());
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!("failed to inspect {context} path '{}' for symlink checks", path.display())
        }),
    }
}

fn ensure_path_is_secure_directory(path: &Path, context: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {context} '{}'", path.display()))?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("{context} '{}' must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        anyhow::bail!("{context} '{}' must be a directory", path.display());
    }
    Ok(())
}

fn read_hardened_file(path: &Path, context: &str) -> Result<Vec<u8>> {
    ensure_path_is_not_symlink(path, context)?;
    #[cfg(unix)]
    {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("failed to open {context} '{}' for read", path.display()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .with_context(|| format!("failed to read {context} '{}'", path.display()))?;
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        fs::read(path).with_context(|| format!("failed to read {context} '{}'", path.display()))
    }
}

fn write_hardened_file_atomic(
    root_dir: &Path,
    target_path: &Path,
    tmp_path: &Path,
    payload: &[u8],
    context: &str,
) -> Result<()> {
    ensure_path_is_secure_directory(root_dir, "browserd state dir")?;
    ensure_path_is_not_symlink(target_path, context)?;
    ensure_path_is_not_symlink(tmp_path, "browserd temporary state file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(tmp_path)
            .with_context(|| format!("failed to create tmp {context} '{}'", tmp_path.display()))?;
        file.write_all(payload)
            .with_context(|| format!("failed to write tmp {context} '{}'", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to fsync tmp {context} '{}'", tmp_path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let mut file =
            fs::OpenOptions::new().create_new(true).write(true).open(tmp_path).with_context(
                || format!("failed to create tmp {context} '{}'", tmp_path.display()),
            )?;
        file.write_all(payload)
            .with_context(|| format!("failed to write tmp {context} '{}'", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to fsync tmp {context} '{}'", tmp_path.display()))?;
    }
    ensure_owner_only_file(tmp_path)?;
    fs::rename(tmp_path, target_path).with_context(|| {
        format!(
            "failed to atomically move tmp {context} '{}' into '{}'",
            tmp_path.display(),
            target_path.display()
        )
    })?;
    ensure_owner_only_file(target_path)?;
    sync_directory(root_dir)?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let directory = fs::File::open(path)
            .with_context(|| format!("failed to open directory '{}' for fsync", path.display()))?;
        directory
            .sync_all()
            .with_context(|| format!("failed to fsync directory '{}'", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn encrypt_state_blob(key: &[u8; STATE_KEY_LEN], plaintext: &[u8]) -> Result<Vec<u8>> {
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key)
        .map_err(|_| anyhow::anyhow!("failed to initialize state cipher key"))?;
    let key = LessSafeKey::new(unbound_key);
    let mut nonce_bytes = [0_u8; STATE_NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| anyhow::anyhow!("failed to generate state encryption nonce"))?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut in_out = plaintext.to_vec();
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to seal state payload"))?;
    let mut output = Vec::with_capacity(STATE_FILE_MAGIC.len() + STATE_NONCE_LEN + in_out.len());
    output.extend_from_slice(STATE_FILE_MAGIC);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(in_out.as_slice());
    Ok(output)
}

fn decrypt_state_blob(key: &[u8; STATE_KEY_LEN], encrypted: &[u8]) -> Result<Vec<u8>> {
    if encrypted.len() < STATE_FILE_MAGIC.len() + STATE_NONCE_LEN {
        anyhow::bail!("state payload is too short");
    }
    if &encrypted[..STATE_FILE_MAGIC.len()] != STATE_FILE_MAGIC {
        anyhow::bail!("state payload magic header is invalid");
    }
    let mut nonce_bytes = [0_u8; STATE_NONCE_LEN];
    nonce_bytes.copy_from_slice(
        &encrypted[STATE_FILE_MAGIC.len()..STATE_FILE_MAGIC.len() + STATE_NONCE_LEN],
    );
    let mut in_out = encrypted[STATE_FILE_MAGIC.len() + STATE_NONCE_LEN..].to_vec();
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key)
        .map_err(|_| anyhow::anyhow!("failed to initialize state cipher key"))?;
    let key = LessSafeKey::new(unbound_key);
    let plaintext = key
        .open_in_place(Nonce::assume_unique_for_key(nonce_bytes), Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to open state payload"))?;
    Ok(plaintext.to_vec())
}

fn derive_state_encryption_key(
    master_key: &[u8; STATE_KEY_LEN],
    profile_id: Option<&str>,
) -> [u8; STATE_KEY_LEN] {
    let Some(profile_id) = profile_id else {
        return *master_key;
    };
    let mut context = DigestContext::new(&SHA256);
    context.update(STATE_PROFILE_DEK_NAMESPACE);
    context.update(master_key);
    context.update(profile_id.as_bytes());
    let digest = context.finish();
    let mut key = [0_u8; STATE_KEY_LEN];
    key.copy_from_slice(digest.as_ref());
    key
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut context = DigestContext::new(&SHA256);
    context.update(bytes);
    encode_hex(context.finish().as_ref())
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|value| format!("{value:02x}")).collect::<String>()
}

fn normalize_profile_registry(registry: &mut BrowserProfileRegistryDocument) {
    registry.v = PROFILE_REGISTRY_SCHEMA_VERSION;
    let mut deduped = HashMap::new();
    for mut profile in registry.profiles.drain(..) {
        if validate_canonical_id(profile.profile_id.as_str()).is_err() {
            continue;
        }
        profile.principal = profile.principal.trim().to_owned();
        if profile.principal.is_empty() {
            continue;
        }
        if profile.name.trim().is_empty() {
            continue;
        }
        if !profile_record_hash_matches(&profile) {
            if profile_record_legacy_hash_matches(&profile) {
                refresh_profile_record_hash(&mut profile);
            } else {
                continue;
            }
        }
        if profile.state_schema_version < PROFILE_RECORD_SCHEMA_VERSION {
            profile.state_schema_version = PROFILE_RECORD_SCHEMA_VERSION;
            refresh_profile_record_hash(&mut profile);
        }
        deduped.insert(profile.profile_id.clone(), profile);
    }
    registry.profiles = deduped.into_values().collect();
    prune_profile_registry(registry);
}

fn prune_profile_registry(registry: &mut BrowserProfileRegistryDocument) {
    let principals =
        registry.profiles.iter().map(|profile| profile.principal.clone()).collect::<Vec<_>>();
    for principal in principals {
        prune_profiles_for_principal(registry, principal.as_str());
    }
    registry.active_profile_by_principal.retain(|principal, profile_id| {
        registry
            .profiles
            .iter()
            .any(|profile| profile.principal == *principal && profile.profile_id == *profile_id)
    });

    loop {
        let serialized_len = serde_json::to_vec(registry).map(|value| value.len()).unwrap_or(0);
        if serialized_len <= MAX_PROFILE_REGISTRY_BYTES {
            break;
        }
        let removable = registry
            .profiles
            .iter()
            .filter(|profile| !is_active_profile(registry, profile.profile_id.as_str()))
            .min_by(|left, right| left.last_used_unix_ms.cmp(&right.last_used_unix_ms))
            .map(|profile| profile.profile_id.clone());
        let Some(profile_id) = removable else {
            break;
        };
        registry.profiles.retain(|profile| profile.profile_id != profile_id);
    }
}

fn prune_profiles_for_principal(registry: &mut BrowserProfileRegistryDocument, principal: &str) {
    loop {
        let principal_count =
            registry.profiles.iter().filter(|profile| profile.principal == principal).count();
        if principal_count <= MAX_PROFILES_PER_PRINCIPAL {
            break;
        }
        let active_profile_id =
            registry.active_profile_by_principal.get(principal).cloned().unwrap_or_default();
        let removable = registry
            .profiles
            .iter()
            .filter(|profile| {
                profile.principal == principal && profile.profile_id != active_profile_id
            })
            .min_by(|left, right| left.last_used_unix_ms.cmp(&right.last_used_unix_ms))
            .map(|profile| profile.profile_id.clone());
        let Some(profile_id) = removable else {
            break;
        };
        registry.profiles.retain(|profile| profile.profile_id != profile_id);
    }
}

fn is_active_profile(registry: &BrowserProfileRegistryDocument, profile_id: &str) -> bool {
    registry.active_profile_by_principal.values().any(|value| value == profile_id)
}

fn profile_record_hash(record: &BrowserProfileRecord) -> String {
    profile_record_hash_with_namespace(record, PROFILE_RECORD_HASH_NAMESPACE, true)
}

fn profile_record_legacy_hash(record: &BrowserProfileRecord) -> String {
    profile_record_hash_with_namespace(record, PROFILE_RECORD_HASH_NAMESPACE_LEGACY, false)
}

fn profile_record_hash_with_namespace(
    record: &BrowserProfileRecord,
    namespace: &[u8],
    include_revision: bool,
) -> String {
    let mut context = DigestContext::new(&SHA256);
    context.update(namespace);
    context.update(record.profile_id.as_bytes());
    context.update(record.principal.as_bytes());
    context.update(record.name.as_bytes());
    context.update(record.theme_color.clone().unwrap_or_default().as_bytes());
    context.update(record.created_at_unix_ms.to_string().as_bytes());
    context.update(record.updated_at_unix_ms.to_string().as_bytes());
    context.update(record.last_used_unix_ms.to_string().as_bytes());
    context.update(if record.persistence_enabled { b"1" } else { b"0" });
    context.update(if record.private_profile { b"1" } else { b"0" });
    context.update(record.state_schema_version.to_string().as_bytes());
    if include_revision {
        context.update(record.state_revision.to_string().as_bytes());
    }
    context.update(record.state_hash_sha256.clone().unwrap_or_default().as_bytes());
    encode_hex(context.finish().as_ref())
}

fn refresh_profile_record_hash(record: &mut BrowserProfileRecord) {
    record.record_hash_sha256 = profile_record_hash(record);
}

fn profile_record_hash_matches(record: &BrowserProfileRecord) -> bool {
    record.record_hash_sha256 == profile_record_hash(record)
}

fn profile_record_legacy_hash_matches(record: &BrowserProfileRecord) -> bool {
    record.record_hash_sha256 == profile_record_legacy_hash(record)
}

fn normalize_profile_principal(raw: &str) -> Result<String, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("principal is required".to_owned());
    }
    if value.len() > 128 {
        return Err("principal exceeds max bytes".to_owned());
    }
    Ok(value.to_owned())
}

fn normalize_profile_name(raw: &str) -> Result<String, String> {
    let value = raw.trim();
    if value.is_empty() {
        return Err("profile name is required".to_owned());
    }
    if value.len() > MAX_PROFILE_NAME_BYTES {
        return Err(format!("profile name exceeds {MAX_PROFILE_NAME_BYTES} bytes"));
    }
    Ok(value.to_owned())
}

fn normalize_profile_theme(raw: &str) -> Result<Option<String>, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if trimmed.len() > MAX_PROFILE_THEME_BYTES {
        return Err(format!("profile theme exceeds {MAX_PROFILE_THEME_BYTES} bytes"));
    }
    if !trimmed
        .bytes()
        .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'#' | b'-' | b'_'))
    {
        return Err("profile theme contains unsupported characters".to_owned());
    }
    Ok(Some(trimmed.to_owned()))
}

fn profile_record_to_proto(
    record: &BrowserProfileRecord,
    active: bool,
) -> browser_v1::BrowserProfile {
    browser_v1::BrowserProfile {
        v: CANONICAL_PROTOCOL_MAJOR,
        profile_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: record.profile_id.clone(),
        }),
        principal: record.principal.clone(),
        name: record.name.clone(),
        theme_color: record.theme_color.clone().unwrap_or_default(),
        created_at_unix_ms: record.created_at_unix_ms,
        updated_at_unix_ms: record.updated_at_unix_ms,
        last_used_unix_ms: record.last_used_unix_ms,
        persistence_enabled: record.persistence_enabled,
        private_profile: record.private_profile,
        active,
    }
}

fn parse_optional_profile_id_from_proto(
    raw: Option<proto::palyra::common::v1::CanonicalId>,
) -> Result<Option<String>, String> {
    let Some(value) = raw else {
        return Ok(None);
    };
    let trimmed = value.ulid.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    validate_canonical_id(trimmed).map_err(|error| format!("invalid profile_id: {error}"))?;
    Ok(Some(trimmed.to_owned()))
}

fn parse_required_profile_id_from_proto(
    raw: Option<proto::palyra::common::v1::CanonicalId>,
) -> Result<String, String> {
    let Some(value) = parse_optional_profile_id_from_proto(raw)? else {
        return Err("profile_id is required".to_owned());
    };
    Ok(value)
}

async fn resolve_session_profile(
    runtime: &BrowserRuntimeState,
    principal: &str,
    requested_profile_id: Option<&str>,
) -> Result<Option<BrowserProfileRecord>, String> {
    let Some(store) = runtime.state_store.as_ref() else {
        if requested_profile_id.is_some() {
            return Err(
                "browser profiles require PALYRA_BROWSERD_STATE_ENCRYPTION_KEY to be configured"
                    .to_owned(),
            );
        }
        return Ok(None);
    };
    let principal = normalize_profile_principal(principal)?;
    let _guard = runtime.profile_registry_lock.lock().await;
    let mut registry = store.load_profile_registry().map_err(|error| error.to_string())?;
    let selected_profile_id = if let Some(profile_id) = requested_profile_id {
        profile_id.to_owned()
    } else if let Some(active) = registry.active_profile_by_principal.get(principal.as_str()) {
        active.clone()
    } else {
        return Ok(None);
    };
    let Some(profile) = registry.profiles.iter_mut().find(|profile| {
        profile.profile_id == selected_profile_id && profile.principal == principal
    }) else {
        return Ok(None);
    };
    profile.last_used_unix_ms = current_unix_ms();
    profile.updated_at_unix_ms = profile.last_used_unix_ms;
    refresh_profile_record_hash(profile);
    let resolved = profile.clone();
    prune_profile_registry(&mut registry);
    store.save_profile_registry(&registry).map_err(|error| error.to_string())?;
    Ok(Some(resolved))
}

async fn upsert_profile_record(
    store: &PersistedStateStore,
    registry_lock: &Mutex<()>,
    mut record: BrowserProfileRecord,
    set_active_if_missing: bool,
) -> Result<(), String> {
    let _guard = registry_lock.lock().await;
    let mut registry = store.load_profile_registry().map_err(|error| error.to_string())?;
    record.updated_at_unix_ms = current_unix_ms();
    record.last_used_unix_ms = record.updated_at_unix_ms;
    refresh_profile_record_hash(&mut record);
    if let Some(existing) =
        registry.profiles.iter_mut().find(|profile| profile.profile_id == record.profile_id)
    {
        *existing = record.clone();
    } else {
        registry.profiles.push(record.clone());
    }
    if set_active_if_missing {
        registry
            .active_profile_by_principal
            .entry(record.principal.clone())
            .or_insert_with(|| record.profile_id.clone());
    }
    prune_profile_registry(&mut registry);
    store.save_profile_registry(&registry).map_err(|error| error.to_string())
}

fn update_profile_state_metadata(
    store: &PersistedStateStore,
    profile_id: &str,
    state_schema_version: u32,
    state_revision: u64,
    state_hash_sha256: &str,
) -> Result<()> {
    let mut registry = store.load_profile_registry()?;
    if let Some(profile) =
        registry.profiles.iter_mut().find(|profile| profile.profile_id == profile_id)
    {
        profile.state_schema_version = state_schema_version;
        profile.state_revision = state_revision;
        profile.state_hash_sha256 = Some(state_hash_sha256.to_owned());
        profile.updated_at_unix_ms = current_unix_ms();
        refresh_profile_record_hash(profile);
        prune_profile_registry(&mut registry);
        store.save_profile_registry(&registry)?;
    }
    Ok(())
}

fn next_profile_state_revision(
    store: &PersistedStateStore,
    profile_id: Option<&str>,
) -> Result<u64> {
    let Some(profile_id) = profile_id else {
        return Ok(0);
    };
    let registry = store.load_profile_registry()?;
    let current = registry
        .profiles
        .iter()
        .find(|profile| profile.profile_id == profile_id)
        .map_or(0, |p| p.state_revision);
    Ok(current.saturating_add(1).max(1))
}

fn persisted_snapshot_hash(snapshot: &PersistedSessionSnapshot) -> Result<String> {
    let bytes = serde_json::to_vec(snapshot)
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
        let domain_cookies = session.cookie_jar.entry(update.domain.clone()).or_default();
        domain_cookies.insert(update.name.clone(), update.value.clone());
    }
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
    let mut value = &tag_lower[start + needle.len()..];
    value = value.trim_start();
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

fn download_artifact_to_proto(record: &DownloadArtifactRecord) -> browser_v1::DownloadArtifact {
    browser_v1::DownloadArtifact {
        v: CANONICAL_PROTOCOL_MAJOR,
        artifact_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: record.artifact_id.clone(),
        }),
        session_id: Some(proto::palyra::common::v1::CanonicalId {
            ulid: record.session_id.clone(),
        }),
        profile_id: record
            .profile_id
            .clone()
            .map(|value| proto::palyra::common::v1::CanonicalId { ulid: value }),
        source_url: normalize_url_with_redaction(record.source_url.as_str()),
        file_name: record.file_name.clone(),
        mime_type: record.mime_type.clone(),
        size_bytes: record.size_bytes,
        sha256: record.sha256.clone(),
        created_at_unix_ms: record.created_at_unix_ms,
        quarantined: record.quarantined,
        quarantine_reason: record.quarantine_reason.clone(),
    }
}

async fn capture_download_artifact_for_click(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    selector: &str,
    context: &ActionSessionSnapshot,
    timeout_ms: u64,
) -> Result<DownloadArtifactRecord, String> {
    let Some(tag) = find_matching_html_tag(selector, context.page_body.as_str()) else {
        return Err("failed to resolve download source tag for click selector".to_owned());
    };
    let (source_url, file_name) =
        resolve_download_target(tag.as_str(), context.current_url.as_deref())?;
    fetch_download_artifact(
        runtime,
        session_id,
        if context.private_profile { None } else { context.profile_id.as_deref() },
        source_url.as_str(),
        file_name.as_str(),
        context.allow_private_targets,
        timeout_ms,
    )
    .await
}

fn resolve_download_target(
    tag: &str,
    current_url: Option<&str>,
) -> Result<(String, String), String> {
    let tag_lower = tag.to_ascii_lowercase();
    let href = extract_attr_value(tag_lower.as_str(), "href")
        .ok_or_else(|| "download-like element is missing href".to_owned())?;
    if href.trim().is_empty() {
        return Err("download-like element has an empty href".to_owned());
    }
    let resolved_url = if let Ok(url) = Url::parse(href.as_str()) {
        url.to_string()
    } else {
        let Some(base) = current_url else {
            return Err("download URL is relative but current page URL is unavailable".to_owned());
        };
        let base_url =
            Url::parse(base).map_err(|error| format!("invalid active page URL: {error}"))?;
        base_url
            .join(href.as_str())
            .map_err(|error| format!("failed to resolve relative download URL: {error}"))?
            .to_string()
    };
    let explicit_name = extract_attr_value(tag_lower.as_str(), "download").unwrap_or_default();
    let file_name = if explicit_name.trim().is_empty() {
        infer_download_file_name(resolved_url.as_str())
    } else {
        sanitize_download_file_name(explicit_name.as_str())
    };
    Ok((resolved_url, file_name))
}

fn infer_download_file_name(raw_url: &str) -> String {
    let Some(url) = Url::parse(raw_url).ok() else {
        return DOWNLOAD_FILE_NAME_FALLBACK.to_owned();
    };
    let Some(value) = url
        .path_segments()
        .and_then(|mut segments| segments.next_back())
        .filter(|segment| !segment.trim().is_empty())
    else {
        return DOWNLOAD_FILE_NAME_FALLBACK.to_owned();
    };
    sanitize_download_file_name(value)
}

fn sanitize_download_file_name(raw: &str) -> String {
    let mut sanitized = raw
        .chars()
        .map(|value| {
            if value.is_ascii_alphanumeric() || matches!(value, '.' | '-' | '_') {
                value
            } else {
                '_'
            }
        })
        .collect::<String>();
    sanitized = sanitized.trim_matches('.').trim_matches('_').to_owned();
    if sanitized.is_empty() {
        return DOWNLOAD_FILE_NAME_FALLBACK.to_owned();
    }
    truncate_utf8_bytes(sanitized.as_str(), 96)
}

fn sniff_download_mime_type(
    header_content_type: Option<&str>,
    file_name: &str,
    bytes: &[u8],
) -> String {
    if let Some(content_type) = header_content_type {
        let normalized =
            content_type.split(';').next().unwrap_or_default().trim().to_ascii_lowercase();
        if !normalized.is_empty() {
            return normalized;
        }
    }
    if bytes.starts_with(b"%PDF-") {
        return "application/pdf".to_owned();
    }
    if bytes.starts_with(&[0x50, 0x4B, 0x03, 0x04]) {
        return "application/zip".to_owned();
    }
    if bytes.starts_with(&[0x1F, 0x8B]) {
        return "application/gzip".to_owned();
    }
    let extension = Path::new(file_name)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    match extension.as_str() {
        "json" => "application/json".to_owned(),
        "csv" => "text/csv".to_owned(),
        "txt" => "text/plain".to_owned(),
        "pdf" => "application/pdf".to_owned(),
        "zip" => "application/zip".to_owned(),
        "gz" => "application/gzip".to_owned(),
        _ => "application/octet-stream".to_owned(),
    }
}

fn extension_is_allowed(file_name: &str) -> bool {
    let extension = Path::new(file_name)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_default();
    DOWNLOAD_ALLOWED_EXTENSIONS.iter().any(|candidate| candidate == &extension)
}

fn mime_type_is_allowed(mime_type: &str) -> bool {
    DOWNLOAD_ALLOWED_MIME_TYPES.contains(&mime_type)
}

async fn fetch_download_artifact(
    runtime: &BrowserRuntimeState,
    session_id: &str,
    profile_id: Option<&str>,
    source_url: &str,
    file_name: &str,
    allow_private_targets: bool,
    timeout_ms: u64,
) -> Result<DownloadArtifactRecord, String> {
    let mut current_url =
        Url::parse(source_url).map_err(|error| format!("invalid download URL: {error}"))?;
    let client = reqwest::Client::builder()
        .redirect(Policy::none())
        .timeout(Duration::from_millis(timeout_ms.max(1)))
        .build()
        .map_err(|error| format!("failed to build download HTTP client: {error}"))?;
    let mut redirects = 0_u32;
    let response = loop {
        validate_target_url(&current_url, allow_private_targets).await?;
        let response = client
            .get(current_url.clone())
            .send()
            .await
            .map_err(|error| format!("download request failed: {error}"))?;
        if response.status().is_redirection() {
            if redirects >= 3 {
                return Err("download redirect limit exceeded (3)".to_owned());
            }
            let Some(location) = response.headers().get(reqwest::header::LOCATION) else {
                return Err("download redirect missing Location header".to_owned());
            };
            let location = location
                .to_str()
                .map_err(|_| "download redirect location header is invalid UTF-8".to_owned())?;
            current_url = current_url
                .join(location)
                .map_err(|error| format!("invalid download redirect target: {error}"))?;
            redirects = redirects.saturating_add(1);
            continue;
        }
        break response;
    };

    if !response.status().is_success() {
        return Err(format!("download request returned HTTP {}", response.status().as_u16()));
    }
    let header_content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response
        .bytes()
        .await
        .map_err(|error| format!("failed to read download response body: {error}"))?;
    if (body.len() as u64) > DOWNLOAD_MAX_FILE_BYTES {
        return Err(format!(
            "download exceeds max file bytes ({} > {})",
            body.len(),
            DOWNLOAD_MAX_FILE_BYTES
        ));
    }
    let mime_type =
        sniff_download_mime_type(header_content_type.as_deref(), file_name, body.as_ref());
    let mut quarantined = false;
    let mut quarantine_reason = String::new();
    if !extension_is_allowed(file_name) {
        quarantined = true;
        quarantine_reason = "extension_not_allowlisted".to_owned();
    }
    if !mime_type_is_allowed(mime_type.as_str()) {
        quarantined = true;
        quarantine_reason = if quarantine_reason.is_empty() {
            "mime_type_not_allowlisted".to_owned()
        } else {
            format!("{quarantine_reason}|mime_type_not_allowlisted")
        };
    }

    let artifact_id = Ulid::new().to_string();
    let sanitized_name = sanitize_download_file_name(file_name);
    let stored_name = format!("{}-{}", artifact_id, sanitized_name);
    let mut guard = runtime.download_sessions.lock().await;
    let Some(sandbox) = guard.get_mut(session_id) else {
        return Err("download sandbox is not active for this session".to_owned());
    };
    if sandbox.used_bytes.saturating_add(body.len() as u64) > sandbox.max_bytes {
        return Err(format!(
            "download sandbox size limit exceeded ({} > {})",
            sandbox.used_bytes.saturating_add(body.len() as u64),
            sandbox.max_bytes
        ));
    }
    while sandbox.artifacts.len() >= MAX_DOWNLOAD_ARTIFACTS_PER_SESSION {
        if let Some(removed) = sandbox.artifacts.pop_front() {
            let _ = fs::remove_file(removed.storage_path.as_path());
            sandbox.used_bytes = sandbox.used_bytes.saturating_sub(removed.size_bytes);
        }
    }
    let target_dir = if quarantined {
        sandbox.root_dir.path().join(DOWNLOADS_DIR_QUARANTINE)
    } else {
        sandbox.root_dir.path().join(DOWNLOADS_DIR_ALLOWLIST)
    };
    let storage_path = target_dir.join(stored_name);
    fs::write(storage_path.as_path(), body.as_ref()).map_err(|error| {
        format!("failed to persist downloaded artifact to '{}' : {error}", storage_path.display())
    })?;
    sandbox.used_bytes = sandbox.used_bytes.saturating_add(body.len() as u64);
    let artifact = DownloadArtifactRecord {
        artifact_id,
        session_id: session_id.to_owned(),
        profile_id: profile_id.map(str::to_owned),
        source_url: current_url.to_string(),
        file_name: sanitized_name,
        mime_type,
        size_bytes: body.len() as u64,
        sha256: sha256_hex(body.as_ref()),
        created_at_unix_ms: current_unix_ms(),
        quarantined,
        quarantine_reason,
        storage_path,
    };
    sandbox.artifacts.push_back(artifact.clone());
    Ok(artifact)
}

#[cfg(test)]
mod tests {
    use super::{
        browser_v1, default_browserd_state_dir_from_env, dns_validation_metrics_snapshot_for_tests,
        enforce_non_loopback_bind_auth, navigate_with_guards, parse_daemon_bind_socket,
        persisted_snapshot_hash, persisted_snapshot_legacy_hash,
        record_chromium_remote_ip_incident, reset_dns_validation_tracking_for_tests,
        store_dns_nxdomain_cache, update_profile_state_metadata,
        validate_restored_snapshot_against_profile, validate_target_url_blocking, Args,
        BrowserEngineMode, BrowserProfileRecord, BrowserRuntimeState, BrowserServiceImpl,
        BrowserTabRecord, PersistedSessionSnapshot, PersistedStateStore,
        SessionPermissionsInternal, CANONICAL_PROTOCOL_MAJOR, CHROMIUM_PATH_ENV,
        DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS, DEFAULT_GRPC_PORT, MAX_RELAY_PAYLOAD_BYTES,
        ONE_BY_ONE_PNG, PROFILE_RECORD_SCHEMA_VERSION, STATE_KEY_LEN,
    };
    use crate::proto;
    use crate::proto::palyra::browser::v1::browser_service_server::BrowserService;
    use std::collections::HashMap;
    use std::ffi::OsString;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::thread;
    use std::time::Duration;
    use tonic::Request;

    const PARITY_DOWNLOAD_TRIGGER_HTML: &str =
        include_str!("../../../fixtures/parity/download-trigger.html");
    const PARITY_NETWORK_LOG_HTML: &str = include_str!("../../../fixtures/parity/network-log.html");
    const PARITY_REDIRECT_TOKEN_URL: &str =
        include_str!("../../../fixtures/parity/redirect-token-url.txt");
    const PARITY_TRICKY_DOM_HTML: &str = include_str!("../../../fixtures/parity/tricky-dom.html");

    fn resolve_chromium_path_for_tests() -> Option<PathBuf> {
        std::env::var(CHROMIUM_PATH_ENV)
            .ok()
            .map(PathBuf::from)
            .or_else(|| headless_chrome::browser::default_executable().ok())
    }

    #[test]
    fn default_browserd_state_dir_prefers_state_root_override() {
        let resolved = default_browserd_state_dir_from_env(
            Some(OsString::from("state-root")),
            None,
            None,
            None,
            None,
        )
        .expect("state root override should resolve");
        assert_eq!(
            resolved,
            PathBuf::from("state-root").join("browserd"),
            "PALYRA_STATE_ROOT should take precedence for browserd defaults"
        );
    }

    #[cfg(windows)]
    #[test]
    fn default_browserd_state_dir_uses_appdata_on_windows() {
        let resolved = default_browserd_state_dir_from_env(
            None,
            Some(OsString::from(r"C:\Users\Test\AppData\Roaming")),
            Some(OsString::from(r"C:\Users\Test\AppData\Local")),
            None,
            None,
        )
        .expect("APPDATA fallback should resolve on windows");
        assert_eq!(
            resolved,
            PathBuf::from(r"C:\Users\Test\AppData\Roaming").join("Palyra").join("browserd")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn default_browserd_state_dir_uses_macos_application_support() {
        let resolved = default_browserd_state_dir_from_env(
            None,
            None,
            None,
            None,
            Some(OsString::from("/Users/tester")),
        )
        .expect("HOME fallback should resolve on macOS");
        assert_eq!(
            resolved,
            PathBuf::from("/Users/tester")
                .join("Library")
                .join("Application Support")
                .join("Palyra")
                .join("browserd")
        );
    }

    #[cfg(all(not(windows), not(target_os = "macos")))]
    #[test]
    fn default_browserd_state_dir_uses_xdg_or_home_on_unix() {
        let xdg = default_browserd_state_dir_from_env(
            None,
            None,
            None,
            Some(OsString::from("/tmp/xdg-state")),
            Some(OsString::from("/home/tester")),
        )
        .expect("XDG_STATE_HOME fallback should resolve");
        assert_eq!(xdg, PathBuf::from("/tmp/xdg-state").join("palyra").join("browserd"));

        let home = default_browserd_state_dir_from_env(
            None,
            None,
            None,
            None,
            Some(OsString::from("/home/tester")),
        )
        .expect("HOME fallback should resolve");
        assert_eq!(
            home,
            PathBuf::from("/home/tester")
                .join(".local")
                .join("state")
                .join("palyra")
                .join("browserd")
        );
    }

    #[cfg(unix)]
    #[test]
    fn persisted_state_store_rejects_symlink_root_dir() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir should be available");
        let actual = temp.path().join("actual-state");
        let symlink_path = temp.path().join("state-link");
        std::fs::create_dir_all(actual.as_path()).expect("actual state dir should be created");
        symlink(actual.as_path(), symlink_path.as_path()).expect("state symlink should be created");

        let error = PersistedStateStore::new(symlink_path, [7_u8; STATE_KEY_LEN])
            .expect_err("symlink root should fail closed");
        let message = error.to_string();
        assert!(
            message.contains("must not be a symlink"),
            "error should explain symlink fail-closed policy: {message}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn persisted_state_store_enforces_owner_only_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir should be available");
        let store = PersistedStateStore::new(temp.path().join("state"), [7_u8; STATE_KEY_LEN])
            .expect("state store should initialize");
        store
            .save_profile_registry(&super::BrowserProfileRegistryDocument::default())
            .expect("registry save should persist encrypted state");

        let root_mode = std::fs::metadata(store.root_dir.as_path())
            .expect("root metadata should load")
            .permissions()
            .mode()
            & 0o777;
        let registry_mode =
            std::fs::metadata(store.root_dir.join(super::PROFILE_REGISTRY_FILE_NAME))
                .expect("registry metadata should load")
                .permissions()
                .mode()
                & 0o777;
        assert_eq!(root_mode, 0o700, "state dir should be owner-only on unix");
        assert_eq!(registry_mode, 0o600, "registry file should be owner-only on unix");
    }

    #[cfg(unix)]
    #[test]
    fn persisted_state_store_rejects_symlink_profile_registry_file() {
        use std::os::unix::fs::symlink;

        let temp = tempfile::tempdir().expect("tempdir should be available");
        let store = PersistedStateStore::new(temp.path().join("state"), [7_u8; STATE_KEY_LEN])
            .expect("state store should initialize");
        let attacker_target = temp.path().join("attacker-profiles.enc");
        std::fs::write(attacker_target.as_path(), b"attacker-controlled")
            .expect("attacker target should be written");
        let registry_path = store.root_dir.join(super::PROFILE_REGISTRY_FILE_NAME);
        symlink(attacker_target.as_path(), registry_path.as_path())
            .expect("registry symlink should be created");

        let error =
            store.load_profile_registry().expect_err("symlinked registry should fail closed");
        let message = error.to_string();
        assert!(
            message.contains("must not be a symlink"),
            "error should explain symlink rejection: {message}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn navigate_with_guards_blocks_file_scheme() {
        let outcome =
            navigate_with_guards("file:///tmp/index.html", 1_000, true, 3, false, 1024, None).await;
        assert!(!outcome.success, "file scheme must be blocked");
        assert!(
            outcome.error.contains("blocked URL scheme"),
            "error should explain blocked scheme: {}",
            outcome.error
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn navigate_with_guards_enforces_response_size_limit() {
        let (url, handle) = spawn_static_http_server(
            200,
            "<html><head><title>Oversized</title></head><body>very large</body></html>",
        );
        let outcome = navigate_with_guards(url.as_str(), 2_000, true, 3, true, 16, None).await;
        assert!(!outcome.success, "oversized payload must fail");
        assert!(
            outcome.error.contains("max_response_bytes"),
            "size limit error should be explicit: {}",
            outcome.error
        );
        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn navigate_with_guards_blocks_private_target_by_default() {
        let outcome =
            navigate_with_guards("http://127.0.0.1:8080/", 1_000, true, 3, false, 1024, None).await;
        assert!(!outcome.success, "private targets should be blocked by default");
        assert!(
            outcome.error.contains("private/local"),
            "error should explain private target block: {}",
            outcome.error
        );
    }

    #[test]
    fn validate_target_url_blocking_rejects_non_canonical_ipv4_literals() {
        for url in
            ["http://2130706433/", "http://0x7f000001/", "http://0177.0.0.1/", "http://127.1/"]
        {
            let error =
                validate_target_url_blocking(url, false).expect_err("non-canonical host must fail");
            assert!(
                error.contains("non-canonical IPv4 literal") || error.contains("private/local"),
                "error should keep fail-closed host guard semantics for {url}: {error}"
            );
        }
    }

    #[test]
    fn dns_validation_cache_reuses_positive_hostname_lookup() {
        reset_dns_validation_tracking_for_tests();
        validate_target_url_blocking("http://localhost:443/", true)
            .expect("first localhost validation should resolve successfully");
        let first = dns_validation_metrics_snapshot_for_tests();
        assert!(first.cache_misses >= 1, "first lookup should register a cache miss: {:?}", first);
        assert!(first.dns_lookups >= 1, "first lookup should invoke DNS: {:?}", first);

        validate_target_url_blocking("http://localhost:443/", true)
            .expect("second localhost validation should reuse cache");
        let second = dns_validation_metrics_snapshot_for_tests();
        assert!(
            second.cache_hits >= first.cache_hits.saturating_add(1),
            "second lookup should register at least one cache hit: first={:?} second={:?}",
            first,
            second
        );
        assert_eq!(
            second.dns_lookups, first.dns_lookups,
            "cache hit should avoid an additional DNS lookup"
        );
    }

    #[test]
    fn dns_validation_cache_short_circuits_cached_nxdomain() {
        reset_dns_validation_tracking_for_tests();
        let host = "cached-nxdomain.invalid";
        let target = format!("http://{host}/");
        store_dns_nxdomain_cache(host);
        let first = dns_validation_metrics_snapshot_for_tests();
        let second_error = validate_target_url_blocking(target.as_str(), false)
            .expect_err("cached NXDOMAIN validation should fail");
        assert!(
            second_error.contains("cached NXDOMAIN"),
            "failure should come from cached NXDOMAIN path: {second_error}"
        );
        let second = dns_validation_metrics_snapshot_for_tests();
        assert!(
            second.cache_hits >= first.cache_hits.saturating_add(1),
            "lookup should use cached NXDOMAIN entry: first={:?} second={:?}",
            first,
            second
        );
        assert_eq!(
            second.dns_lookups, first.dns_lookups,
            "cached NXDOMAIN should avoid repeated DNS lookups"
        );
        assert!(
            second.blocked_dns_failures >= first.blocked_dns_failures.saturating_add(1),
            "dns failure block counter should be recorded: {:?}",
            second
        );
    }

    #[test]
    fn chromium_remote_ip_guard_records_incident_for_private_addresses() {
        let incident = Arc::new(StdMutex::new(None::<String>));
        record_chromium_remote_ip_incident(Some("127.0.0.1"), false, &incident);
        let message = incident
            .lock()
            .expect("guard should lock after IPv4 incident")
            .clone()
            .expect("private IPv4 response IP should record an incident");
        assert!(
            message.contains("127.0.0.1"),
            "incident should include violating IPv4 address: {message}"
        );

        let incident = Arc::new(StdMutex::new(None::<String>));
        record_chromium_remote_ip_incident(Some("[::1]"), false, &incident);
        let message = incident
            .lock()
            .expect("guard should lock after IPv6 incident")
            .clone()
            .expect("private IPv6 response IP should record an incident");
        assert!(
            message.contains("::1"),
            "incident should include violating IPv6 address: {message}"
        );
    }

    #[test]
    fn chromium_remote_ip_guard_ignores_public_and_opted_in_private_targets() {
        let incident = Arc::new(StdMutex::new(None::<String>));
        record_chromium_remote_ip_incident(Some("93.184.216.34"), false, &incident);
        assert!(
            incident.lock().expect("guard should lock after public response IP check").is_none(),
            "public response IP should not produce a remote IP guard incident"
        );

        record_chromium_remote_ip_incident(Some("127.0.0.1"), true, &incident);
        assert!(
            incident.lock().expect("guard should lock after private-target opt-in check").is_none(),
            "private-target opt-in should bypass remote IP guard incidents"
        );
    }

    #[test]
    fn non_loopback_bind_requires_auth_token() {
        let admin = parse_daemon_bind_socket("0.0.0.0", 7143).expect("admin address should parse");
        let grpc = parse_daemon_bind_socket("127.0.0.1", DEFAULT_GRPC_PORT)
            .expect("grpc address should parse");
        let error = enforce_non_loopback_bind_auth(admin, grpc, false)
            .expect_err("non-loopback bind without auth token must fail closed");
        assert!(
            error.to_string().contains("auth token is required"),
            "error should explain startup auth requirement: {error}"
        );
    }

    #[test]
    fn loopback_binds_allow_missing_auth_token() {
        let admin =
            parse_daemon_bind_socket("127.0.0.1", 7143).expect("admin address should parse");
        let grpc =
            parse_daemon_bind_socket("::1", DEFAULT_GRPC_PORT).expect("grpc address should parse");
        enforce_non_loopback_bind_auth(admin, grpc, false)
            .expect("loopback-only binds may run without auth token");
    }

    #[test]
    fn non_loopback_bind_allows_when_auth_is_enabled() {
        let admin = parse_daemon_bind_socket("0.0.0.0", 7143).expect("admin address should parse");
        let grpc = parse_daemon_bind_socket("0.0.0.0", DEFAULT_GRPC_PORT)
            .expect("grpc address should parse");
        enforce_non_loopback_bind_auth(admin, grpc, true)
            .expect("configured auth token should allow non-loopback bind");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_roundtrip_navigate_and_screenshot() {
        let (url, handle) = spawn_static_http_server(
            200,
            "<html><head><title>Integration Title</title></head><body>ok</body></html>",
        );
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };

        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");

        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should succeed")
            .into_inner();
        assert!(navigate.success, "navigation should succeed");
        assert_eq!(navigate.title, "Integration Title");

        let screenshot = service
            .screenshot(Request::new(browser_v1::ScreenshotRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                max_bytes: 1024,
                format: "png".to_owned(),
            }))
            .await
            .expect("screenshot should succeed")
            .into_inner();
        assert!(screenshot.success, "screenshot should succeed");
        assert_eq!(screenshot.image_bytes, ONE_BY_ONE_PNG);

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_chromium_engine_executes_real_dom_actions() {
        let Some(chromium_path) = resolve_chromium_path_for_tests() else {
            return;
        };
        let (url, handle) = spawn_static_http_server_with_request_budget(
            200,
            "<html><head><title>Chromium Fixture</title><script>function markClicked(){document.getElementById('status').textContent='clicked';}</script></head><body><input id='name-input' /><button id='submit-btn' onclick='markClicked()'>Submit</button><div id='status'>idle</div></body></html>",
            8,
        );
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 256 * 1024,
                max_response_bytes: 256 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Chromium,
                chromium_path: Some(chromium_path),
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("chromium runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed for chromium mode")
            .into_inner();
        let session_id = created.session_id.expect("session id should exist");

        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                url,
                timeout_ms: 8_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should execute")
            .into_inner();
        assert!(navigate.success, "chromium navigate should succeed: {}", navigate.error);
        assert_eq!(navigate.title, "Chromium Fixture");

        let typed = service
            .r#type(Request::new(browser_v1::TypeRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                selector: "#name-input".to_owned(),
                text: "hello chromium".to_owned(),
                clear_existing: true,
                timeout_ms: 3_000,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 16 * 1024,
            }))
            .await
            .expect("type should execute")
            .into_inner();
        assert!(typed.success, "chromium type should succeed: {}", typed.error);

        let click = service
            .click(Request::new(browser_v1::ClickRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                selector: "#submit-btn".to_owned(),
                max_retries: 2,
                timeout_ms: 3_000,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 16 * 1024,
            }))
            .await
            .expect("click should execute")
            .into_inner();
        assert!(click.success, "chromium click should succeed: {}", click.error);

        let waited = service
            .wait_for(Request::new(browser_v1::WaitForRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                selector: String::new(),
                text: "clicked".to_owned(),
                timeout_ms: 5_000,
                poll_interval_ms: 50,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 16 * 1024,
            }))
            .await
            .expect("wait_for should execute")
            .into_inner();
        assert!(
            waited.success,
            "chromium wait_for should observe DOM change after click: {}",
            waited.error
        );

        let screenshot = service
            .screenshot(Request::new(browser_v1::ScreenshotRequest {
                v: 1,
                session_id: Some(session_id.clone()),
                max_bytes: 220 * 1024,
                format: "png".to_owned(),
            }))
            .await
            .expect("screenshot should execute")
            .into_inner();
        assert!(screenshot.success, "chromium screenshot should succeed: {}", screenshot.error);
        assert!(
            screenshot.image_bytes.starts_with(&[137, 80, 78, 71]),
            "chromium screenshot must return PNG payload"
        );

        let observed = service
            .observe(Request::new(browser_v1::ObserveRequest {
                v: 1,
                session_id: Some(session_id),
                include_dom_snapshot: true,
                include_accessibility_tree: true,
                include_visible_text: true,
                max_dom_snapshot_bytes: 32 * 1024,
                max_accessibility_tree_bytes: 32 * 1024,
                max_visible_text_bytes: 8 * 1024,
            }))
            .await
            .expect("observe should execute")
            .into_inner();
        assert!(observed.success, "chromium observe should succeed: {}", observed.error);
        assert!(
            observed.visible_text.contains("clicked"),
            "observe visible text should reflect click side-effect from real DOM"
        );

        drop(handle);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_click_type_and_wait_for_on_fixture_page() {
        let (url, handle) = spawn_static_http_server(
            200,
            "<html><head><title>Actions</title></head><body><input id=\"email\" name=\"email\" /><button id=\"submit\">Submit</button></body></html>",
        );
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");

        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should succeed")
            .into_inner();
        assert!(navigate.success, "navigation should succeed");

        let click = service
            .click(Request::new(browser_v1::ClickRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                selector: "#submit".to_owned(),
                max_retries: 2,
                timeout_ms: 500,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 1024,
            }))
            .await
            .expect("click should execute")
            .into_inner();
        assert!(click.success, "click action should succeed");
        assert_eq!(
            click.action_log.as_ref().map(|value| value.action_name.as_str()),
            Some("click")
        );

        let typed = service
            .r#type(Request::new(browser_v1::TypeRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                selector: "#email".to_owned(),
                text: "agent@example.com".to_owned(),
                clear_existing: true,
                timeout_ms: 500,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 1024,
            }))
            .await
            .expect("type should execute")
            .into_inner();
        assert!(typed.success, "type action should succeed");
        assert_eq!(typed.typed_bytes, "agent@example.com".len() as u64);
        assert_eq!(typed.action_log.as_ref().map(|value| value.action_name.as_str()), Some("type"));

        let waited = service
            .wait_for(Request::new(browser_v1::WaitForRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                selector: "#submit".to_owned(),
                text: String::new(),
                timeout_ms: 300,
                poll_interval_ms: 25,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 1024,
            }))
            .await
            .expect("wait_for should execute")
            .into_inner();
        assert!(waited.success, "wait_for should match existing selector");
        assert_eq!(waited.matched_selector, "#submit");

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_rejects_oversized_type_input() {
        let (url, handle) = spawn_static_http_server(
            200,
            "<html><body><input id=\"name\" name=\"name\" /></body></html>",
        );
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: Some(browser_v1::SessionBudget {
                    max_navigation_timeout_ms: 0,
                    max_session_lifetime_ms: 0,
                    max_screenshot_bytes: 0,
                    max_response_bytes: 0,
                    max_action_timeout_ms: 0,
                    max_type_input_bytes: 4,
                    max_actions_per_session: 0,
                    max_actions_per_window: 0,
                    action_rate_window_ms: 0,
                    max_action_log_entries: 0,
                    max_observe_snapshot_bytes: 0,
                    max_visible_text_bytes: 0,
                    max_network_log_entries: 0,
                    max_network_log_bytes: 0,
                }),
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");
        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should succeed")
            .into_inner();
        assert!(navigate.success, "navigation should succeed");

        let typed = service
            .r#type(Request::new(browser_v1::TypeRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                selector: "#name".to_owned(),
                text: "abcdef".to_owned(),
                clear_existing: false,
                timeout_ms: 500,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 1024,
            }))
            .await
            .expect("type request should complete")
            .into_inner();
        assert!(!typed.success, "oversized type payload should fail");
        assert!(
            typed.error.contains("max_type_input_bytes"),
            "error should contain explicit budget context: {}",
            typed.error
        );

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_blocks_download_click_when_disabled() {
        let (url, handle) = spawn_static_http_server(200, PARITY_DOWNLOAD_TRIGGER_HTML);
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");
        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should succeed")
            .into_inner();
        assert!(navigate.success, "navigation should succeed");

        let click = service
            .click(Request::new(browser_v1::ClickRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                selector: "#download-link".to_owned(),
                max_retries: 0,
                timeout_ms: 500,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 1024,
            }))
            .await
            .expect("click request should complete")
            .into_inner();
        assert!(!click.success, "download-like click should be blocked by default");
        assert!(
            click.error.contains("allow_downloads=false"),
            "error should identify explicit download policy: {}",
            click.error
        );
        assert_eq!(
            click.failure_screenshot_bytes, ONE_BY_ONE_PNG,
            "blocked click should include bounded failure screenshot"
        );

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_observe_returns_stable_sanitized_snapshot() {
        let (url, handle) = spawn_static_http_server(200, PARITY_TRICKY_DOM_HTML);
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");
        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url: format!("{url}?access_token=topsecret&lang=en"),
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should succeed")
            .into_inner();
        assert!(navigate.success, "navigation should succeed");

        let observed = service
            .observe(Request::new(browser_v1::ObserveRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                include_dom_snapshot: true,
                include_accessibility_tree: true,
                include_visible_text: true,
                max_dom_snapshot_bytes: 8 * 1024,
                max_accessibility_tree_bytes: 8 * 1024,
                max_visible_text_bytes: 2 * 1024,
            }))
            .await
            .expect("observe should execute")
            .into_inner();
        assert!(observed.success, "observe should succeed");
        assert!(
            observed.dom_snapshot.contains("<form"),
            "dom snapshot should include structural elements"
        );
        assert!(
            observed.dom_snapshot.contains("token=<redacted>")
                || observed.dom_snapshot.contains("access_token=<redacted>"),
            "dom snapshot should redact sensitive URL query params: {}",
            observed.dom_snapshot
        );
        assert!(
            !observed.dom_snapshot.contains("topsecret"),
            "sensitive query values must be redacted from dom snapshot: {}",
            observed.dom_snapshot
        );
        assert!(
            observed.accessibility_tree.contains("role=button"),
            "accessibility tree should include semantic roles: {}",
            observed.accessibility_tree
        );
        assert!(
            observed.visible_text.contains("Portal"),
            "visible text extraction should include visible text content"
        );
        assert!(
            observed.page_url.contains("access_token=<redacted>"),
            "observed page URL should be redacted: {}",
            observed.page_url
        );

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_observe_truncates_deterministically_when_oversized() {
        let large_body = format!(
            "<html><body><main>{}</main></body></html>",
            (0..80)
                .map(|index| format!("<section id=\"section-{index}\"><button id=\"btn-{index}\">Run {index}</button></section>"))
                .collect::<String>()
        );
        let (url, handle) = spawn_static_http_server(200, large_body.as_str());
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 256 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");
        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should succeed")
            .into_inner();
        assert!(navigate.success, "navigation should succeed");

        let request = browser_v1::ObserveRequest {
            v: 1,
            session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            include_dom_snapshot: true,
            include_accessibility_tree: true,
            include_visible_text: true,
            max_dom_snapshot_bytes: 64,
            max_accessibility_tree_bytes: 64,
            max_visible_text_bytes: 48,
        };
        let first = service
            .observe(Request::new(request.clone()))
            .await
            .expect("first observe should execute")
            .into_inner();
        let second = service
            .observe(Request::new(request))
            .await
            .expect("second observe should execute")
            .into_inner();
        assert!(
            first.dom_truncated
                && first.accessibility_tree_truncated
                && first.visible_text_truncated,
            "all observe channels should report truncation for oversized snapshots"
        );
        assert_eq!(first.dom_snapshot, second.dom_snapshot, "dom truncation must be deterministic");
        assert_eq!(
            first.accessibility_tree, second.accessibility_tree,
            "a11y truncation must be deterministic"
        );
        assert_eq!(
            first.visible_text, second.visible_text,
            "visible text truncation must be deterministic"
        );

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_network_log_redacts_sensitive_values() {
        let (url, handle) = spawn_static_http_server_with_headers(
            200,
            PARITY_NETWORK_LOG_HTML,
            &[
                ("Set-Cookie", "session=abc123; HttpOnly"),
                ("X-Api-Key", "secret-key"),
                ("Location", PARITY_REDIRECT_TOKEN_URL.trim()),
            ],
        );
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");
        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url: format!("{url}?access_token=supersecret&safe=1"),
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should succeed")
            .into_inner();
        assert!(navigate.success, "navigation should succeed");

        let without_headers = service
            .network_log(Request::new(browser_v1::NetworkLogRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                limit: 10,
                include_headers: false,
                max_payload_bytes: 8 * 1024,
            }))
            .await
            .expect("network_log without headers should execute")
            .into_inner();
        assert!(without_headers.success, "network log call should succeed");
        assert!(!without_headers.entries.is_empty(), "network log should contain entries");
        assert!(
            without_headers.entries.iter().all(|entry| entry.headers.is_empty()),
            "headers must be excluded unless explicitly requested"
        );

        let with_headers = service
            .network_log(Request::new(browser_v1::NetworkLogRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                limit: 10,
                include_headers: true,
                max_payload_bytes: 8 * 1024,
            }))
            .await
            .expect("network_log with headers should execute")
            .into_inner();
        assert!(with_headers.success, "network log call should succeed");
        let entry =
            with_headers.entries.last().expect("network log should include at least one entry");
        assert!(
            entry.request_url.contains("access_token=<redacted>"),
            "network log URLs should redact sensitive query values: {}",
            entry.request_url
        );
        assert!(
            !entry.request_url.contains("supersecret"),
            "network log must not leak original sensitive URL values: {}",
            entry.request_url
        );
        assert!(
            entry
                .headers
                .iter()
                .any(|header| { header.name == "set-cookie" && header.value == "<redacted>" }),
            "set-cookie header should be redacted"
        );
        assert!(
            entry.headers.iter().any(|header| {
                header.name == "location" && header.value.contains("token=<redacted>")
            }),
            "location header URLs should be normalized and redacted"
        );

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_reset_state_clears_cookie_jar_for_fixture_domain() {
        let (url, handle) = spawn_cookie_state_http_server();
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");

        let first = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url: url.clone(),
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("first navigate should execute")
            .into_inner();
        assert!(first.success, "first navigation should succeed");

        let second = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url: url.clone(),
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("second navigate should execute")
            .into_inner();
        assert!(second.success, "second navigation should replay cookie and succeed");

        let reset = service
            .reset_state(Request::new(browser_v1::ResetStateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                clear_cookies: true,
                clear_storage: false,
                reset_tabs: false,
                reset_permissions: false,
            }))
            .await
            .expect("reset_state should execute")
            .into_inner();
        assert!(reset.success, "reset_state should succeed");
        assert!(reset.cookies_cleared >= 1, "at least one cookie should be removed during reset");

        let third = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("third navigate should execute")
            .into_inner();
        assert!(
            !third.success && third.status_code == 401,
            "third navigation should fail after reset because cookie was cleared"
        );

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_permissions_default_to_deny() {
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");
        let permissions = service
            .get_permissions(Request::new(browser_v1::GetPermissionsRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            }))
            .await
            .expect("get_permissions should execute")
            .into_inner();
        assert!(permissions.success, "permission query should succeed");
        let effective = permissions.permissions.expect("permissions should be returned");
        assert_eq!(effective.camera, 1, "camera permission should default to deny");
        assert_eq!(effective.microphone, 1, "microphone permission should default to deny");
        assert_eq!(effective.location, 1, "location permission should default to deny");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_tabs_keep_independent_state() {
        let (url, handle) = spawn_static_http_server(
            200,
            "<html><head><title>Secondary Tab</title></head><body>tab-two</body></html>",
        );
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");

        let initial_tabs = service
            .list_tabs(Request::new(browser_v1::ListTabsRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
            }))
            .await
            .expect("list_tabs should execute")
            .into_inner();
        assert!(initial_tabs.success, "list_tabs should succeed");
        let first_tab_id = initial_tabs
            .tabs
            .iter()
            .find_map(|tab| tab.tab_id.as_ref().map(|value| value.ulid.clone()))
            .expect("first tab should be present");

        let opened = service
            .open_tab(Request::new(browser_v1::OpenTabRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url: url.clone(),
                activate: true,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("open_tab should execute")
            .into_inner();
        assert!(opened.success, "open_tab should succeed");
        let second_tab_id = opened
            .tab
            .as_ref()
            .and_then(|tab| tab.tab_id.as_ref())
            .map(|value| value.ulid.clone())
            .expect("opened tab id should be present");

        let active_title = service
            .get_title(Request::new(browser_v1::GetTitleRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                max_title_bytes: 1024,
            }))
            .await
            .expect("get_title should execute")
            .into_inner();
        assert_eq!(active_title.title, "Secondary Tab");

        let switched = service
            .switch_tab(Request::new(browser_v1::SwitchTabRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                tab_id: Some(proto::palyra::common::v1::CanonicalId { ulid: first_tab_id }),
            }))
            .await
            .expect("switch_tab should execute")
            .into_inner();
        assert!(switched.success, "switch_tab should succeed");

        let first_tab_title = service
            .get_title(Request::new(browser_v1::GetTitleRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                max_title_bytes: 1024,
            }))
            .await
            .expect("get_title on first tab should execute")
            .into_inner();
        assert!(
            first_tab_title.title.is_empty(),
            "first tab should keep independent state and remain blank"
        );

        let switched_back = service
            .switch_tab(Request::new(browser_v1::SwitchTabRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                tab_id: Some(proto::palyra::common::v1::CanonicalId { ulid: second_tab_id }),
            }))
            .await
            .expect("switch_tab back should execute")
            .into_inner();
        assert!(switched_back.success, "switch back should succeed");
        let second_tab_title = service
            .get_title(Request::new(browser_v1::GetTitleRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                max_title_bytes: 1024,
            }))
            .await
            .expect("get_title on second tab should execute")
            .into_inner();
        assert_eq!(second_tab_title.title, "Secondary Tab");

        handle.join().expect("test server thread should exit");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_profile_persistence_roundtrip_restores_state() {
        let (url, handle) = spawn_static_http_server(
            200,
            "<html><head><title>Persisted Profile</title></head><body><p>persisted</p></body></html>",
        );
        let state_dir = tempfile::tempdir().expect("state temp dir should be available");
        let mut runtime_state = BrowserRuntimeState::new(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize");
        runtime_state.state_store = Some(
            PersistedStateStore::new(state_dir.path().join("state"), [7_u8; STATE_KEY_LEN])
                .expect("state store should initialize"),
        );
        let runtime = std::sync::Arc::new(runtime_state);
        let service = BrowserServiceImpl { runtime };

        let profile = service
            .create_profile(Request::new(browser_v1::CreateProfileRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                name: "Ops".to_owned(),
                theme_color: "#1f2937".to_owned(),
                persistence_enabled: true,
                private_profile: false,
            }))
            .await
            .expect("create_profile should succeed")
            .into_inner()
            .profile
            .expect("profile should be present");
        let profile_id = profile
            .profile_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("profile id should be present");

        let first_session = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: profile_id.clone(),
                }),
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("first create_session should succeed")
            .into_inner();
        let first_session_id = first_session
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("first session id should be present");
        assert!(first_session.persistence_enabled, "profile should enable persistence");
        assert_eq!(
            first_session.profile_id.as_ref().map(|value| value.ulid.as_str()),
            Some(profile_id.as_str())
        );

        let navigate = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: first_session_id.clone(),
                }),
                url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("navigate should execute")
            .into_inner();
        assert!(navigate.success, "navigation should succeed");

        let closed = service
            .close_session(Request::new(browser_v1::CloseSessionRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: first_session_id }),
            }))
            .await
            .expect("close_session should execute")
            .into_inner();
        assert!(closed.closed, "first session should close cleanly");

        let second_session = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: profile_id.clone(),
                }),
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("second create_session should succeed")
            .into_inner();
        let second_session_id = second_session
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("second session id should be present");
        assert!(second_session.state_restored, "second session should restore persisted state");
        assert_eq!(
            second_session.profile_id.as_ref().map(|value| value.ulid.as_str()),
            Some(profile_id.as_str())
        );

        let title = service
            .get_title(Request::new(browser_v1::GetTitleRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: second_session_id,
                }),
                max_title_bytes: 1_024,
            }))
            .await
            .expect("get_title should execute")
            .into_inner();
        assert!(title.success, "title lookup should succeed after restore");
        assert_eq!(title.title, "Persisted Profile");

        handle.join().expect("test server thread should exit");
    }

    #[test]
    fn validate_restored_snapshot_against_profile_accepts_legacy_hash_for_revision_zero() {
        let snapshot = PersistedSessionSnapshot {
            v: CANONICAL_PROTOCOL_MAJOR,
            principal: "user:ops".to_owned(),
            channel: None,
            tabs: vec![BrowserTabRecord::new(ulid::Ulid::new().to_string())],
            tab_order: Vec::new(),
            active_tab_id: String::new(),
            permissions: SessionPermissionsInternal::default(),
            cookie_jar: HashMap::new(),
            storage_entries: HashMap::new(),
            state_revision: 0,
            saved_at_unix_ms: 1_737_000_000_000,
        };
        let legacy_hash = persisted_snapshot_legacy_hash(&snapshot)
            .expect("legacy hash generation should succeed");
        let profile = BrowserProfileRecord {
            profile_id: ulid::Ulid::new().to_string(),
            principal: "user:ops".to_owned(),
            name: "Ops".to_owned(),
            theme_color: None,
            created_at_unix_ms: 1_737_000_000_000,
            updated_at_unix_ms: 1_737_000_000_000,
            last_used_unix_ms: 1_737_000_000_000,
            persistence_enabled: true,
            private_profile: false,
            state_schema_version: PROFILE_RECORD_SCHEMA_VERSION,
            state_revision: 0,
            state_hash_sha256: Some(legacy_hash),
            record_hash_sha256: String::new(),
        };
        validate_restored_snapshot_against_profile(&snapshot, &profile)
            .expect("legacy hash path should stay backward compatible");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_profile_restore_rejects_snapshot_revision_rollback() {
        let state_dir = tempfile::tempdir().expect("state temp dir should be available");
        let mut runtime_state = BrowserRuntimeState::new(&Args {
            bind: "127.0.0.1".to_owned(),
            port: 7143,
            grpc_bind: "127.0.0.1".to_owned(),
            grpc_port: 7543,
            auth_token: None,
            session_idle_ttl_ms: 60_000,
            max_sessions: 16,
            max_navigation_timeout_ms: 10_000,
            max_session_lifetime_ms: 60_000,
            max_screenshot_bytes: 128 * 1024,
            max_response_bytes: 128 * 1024,
            max_title_bytes: 4 * 1024,
            engine_mode: BrowserEngineMode::Simulated,
            chromium_path: None,
            chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
        })
        .expect("runtime should initialize");
        runtime_state.state_store = Some(
            PersistedStateStore::new(state_dir.path().join("state"), [9_u8; STATE_KEY_LEN])
                .expect("state store should initialize"),
        );
        let runtime = std::sync::Arc::new(runtime_state);
        let service = BrowserServiceImpl { runtime: runtime.clone() };

        let profile = service
            .create_profile(Request::new(browser_v1::CreateProfileRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                name: "Ops".to_owned(),
                theme_color: "#1f2937".to_owned(),
                persistence_enabled: true,
                private_profile: false,
            }))
            .await
            .expect("create_profile should succeed")
            .into_inner()
            .profile
            .expect("profile should be present");
        let profile_id = profile.profile_id.expect("profile id should be present").ulid;

        let session = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: profile_id.clone(),
                }),
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = session.session_id.expect("session id should be present").ulid;

        service
            .close_session(Request::new(browser_v1::CloseSessionRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
            }))
            .await
            .expect("close_session should execute");

        let store = runtime
            .state_store
            .as_ref()
            .expect("state store should remain configured for rollback test");
        let snapshot = store
            .load_snapshot(profile_id.as_str(), Some(profile_id.as_str()))
            .expect("snapshot load should succeed")
            .expect("snapshot should be present after persisted profile session");
        assert!(
            snapshot.state_revision >= 1,
            "snapshot revision should advance after first persist"
        );
        let expected_hash =
            persisted_snapshot_hash(&snapshot).expect("snapshot hash should compute");
        let mut rollback_snapshot = snapshot.clone();
        rollback_snapshot.state_revision = snapshot.state_revision.saturating_sub(1);
        store
            .save_snapshot(profile_id.as_str(), Some(profile_id.as_str()), &rollback_snapshot)
            .expect("rollback snapshot write should succeed");
        update_profile_state_metadata(
            store,
            profile_id.as_str(),
            PROFILE_RECORD_SCHEMA_VERSION,
            snapshot.state_revision,
            expected_hash.as_str(),
        )
        .expect("profile metadata update should succeed");

        let rollback_attempt = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: Some(proto::palyra::common::v1::CanonicalId { ulid: profile_id }),
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect_err("rollbacked snapshot should be rejected");
        assert_eq!(
            rollback_attempt.code(),
            tonic::Code::FailedPrecondition,
            "rollback guard should fail with failed_precondition"
        );
        assert!(
            rollback_attempt.message().contains("snapshot revision"),
            "error should explain revision rollback guard: {}",
            rollback_attempt.message()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_relay_rejects_unsupported_action_kind() {
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");

        let relay = service
            .relay_action(Request::new(browser_v1::RelayActionRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                extension_id: "com.palyra.extension".to_owned(),
                action: 999,
                payload: None,
                max_payload_bytes: 4_096,
            }))
            .await
            .expect("relay action should return response")
            .into_inner();
        assert!(!relay.success, "unsupported relay action should fail closed");
        assert!(
            relay.error.contains("unsupported relay action"),
            "error should explain unsupported action: {}",
            relay.error
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_relay_rejects_oversized_payload_budget() {
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 128 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };
        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: false,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");

        let status = service
            .relay_action(Request::new(browser_v1::RelayActionRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                extension_id: "com.palyra.extension".to_owned(),
                action: browser_v1::RelayActionKind::CaptureSelection as i32,
                payload: Some(browser_v1::relay_action_request::Payload::CaptureSelection(
                    browser_v1::RelayCaptureSelectionPayload {
                        selector: "body".to_owned(),
                        max_selection_bytes: 512,
                    },
                )),
                max_payload_bytes: MAX_RELAY_PAYLOAD_BYTES + 1,
            }))
            .await
            .expect_err("oversized relay payload budget must be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("max_payload_bytes exceeds"),
            "error should explain relay payload bound: {}",
            status.message()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn browser_service_download_allowlist_and_quarantine_artifacts() {
        let runtime = std::sync::Arc::new(
            BrowserRuntimeState::new(&Args {
                bind: "127.0.0.1".to_owned(),
                port: 7143,
                grpc_bind: "127.0.0.1".to_owned(),
                grpc_port: 7543,
                auth_token: None,
                session_idle_ttl_ms: 60_000,
                max_sessions: 16,
                max_navigation_timeout_ms: 10_000,
                max_session_lifetime_ms: 60_000,
                max_screenshot_bytes: 128 * 1024,
                max_response_bytes: 256 * 1024,
                max_title_bytes: 4 * 1024,
                engine_mode: BrowserEngineMode::Simulated,
                chromium_path: None,
                chromium_startup_timeout_ms: DEFAULT_CHROMIUM_STARTUP_TIMEOUT_MS,
            })
            .expect("runtime should initialize"),
        );
        let service = BrowserServiceImpl { runtime };

        let created = service
            .create_session(Request::new(browser_v1::CreateSessionRequest {
                v: 1,
                principal: "user:ops".to_owned(),
                idle_ttl_ms: 10_000,
                budget: None,
                allow_private_targets: true,
                allow_downloads: true,
                action_allowed_domains: Vec::new(),
                persistence_enabled: false,
                persistence_id: String::new(),
                profile_id: None,
                private_profile: false,
                channel: String::new(),
            }))
            .await
            .expect("create_session should succeed")
            .into_inner();
        let session_id = created
            .session_id
            .as_ref()
            .map(|value| value.ulid.clone())
            .expect("session id should be present");

        let (allowlist_url, allowlist_handle) =
            spawn_download_fixture_http_server("/report.csv", "text/csv", b"name,score\nalice,9\n");
        let navigate_allowlist = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url: allowlist_url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("allowlist navigate should execute")
            .into_inner();
        assert!(navigate_allowlist.success, "allowlist fixture navigation should succeed");

        let allowlisted_click = service
            .click(Request::new(browser_v1::ClickRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                selector: "#download-link".to_owned(),
                max_retries: 0,
                timeout_ms: 1_500,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 2 * 1024,
            }))
            .await
            .expect("allowlist click should execute")
            .into_inner();
        assert!(allowlisted_click.success, "allowlisted download click should succeed");
        let allowlisted_artifact = allowlisted_click
            .artifact
            .expect("allowlisted download should return artifact metadata");
        assert!(
            !allowlisted_artifact.quarantined,
            "allowlisted artifact should not be quarantined"
        );
        assert_eq!(allowlisted_artifact.file_name, "report.csv");
        allowlist_handle.join().expect("allowlist server thread should exit");

        let (quarantine_url, quarantine_handle) = spawn_download_fixture_http_server(
            "/payload.exe",
            "application/octet-stream",
            b"MZ\x90\x00suspicious",
        );
        let navigate_quarantine = service
            .navigate(Request::new(browser_v1::NavigateRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                url: quarantine_url,
                timeout_ms: 2_000,
                allow_redirects: true,
                max_redirects: 3,
                allow_private_targets: true,
            }))
            .await
            .expect("quarantine navigate should execute")
            .into_inner();
        assert!(navigate_quarantine.success, "quarantine fixture navigation should succeed");

        let quarantined_click = service
            .click(Request::new(browser_v1::ClickRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId {
                    ulid: session_id.clone(),
                }),
                selector: "#download-link".to_owned(),
                max_retries: 0,
                timeout_ms: 1_500,
                capture_failure_screenshot: true,
                max_failure_screenshot_bytes: 2 * 1024,
            }))
            .await
            .expect("quarantine click should execute")
            .into_inner();
        assert!(quarantined_click.success, "quarantined download still records click success");
        assert_eq!(
            quarantined_click.action_log.as_ref().map(|entry| entry.outcome.as_str()),
            Some("download_quarantined")
        );
        let quarantined_artifact = quarantined_click
            .artifact
            .expect("quarantined download should return artifact metadata");
        assert!(quarantined_artifact.quarantined, "suspicious file should be quarantined");
        assert!(
            quarantined_artifact.quarantine_reason.contains("extension_not_allowlisted"),
            "quarantine reason should include extension allowlist signal: {}",
            quarantined_artifact.quarantine_reason
        );
        quarantine_handle.join().expect("quarantine server thread should exit");

        let listed = service
            .list_download_artifacts(Request::new(browser_v1::ListDownloadArtifactsRequest {
                v: 1,
                session_id: Some(proto::palyra::common::v1::CanonicalId { ulid: session_id }),
                limit: 10,
                quarantined_only: false,
            }))
            .await
            .expect("list_download_artifacts should execute")
            .into_inner();
        assert_eq!(listed.artifacts.len(), 2, "both artifacts should be registered");
        assert!(
            listed.artifacts.iter().any(|artifact| artifact.quarantined),
            "download artifact list should include quarantined entries"
        );
    }

    fn spawn_static_http_server(status_code: u16, body: &str) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("listener local address should resolve");
        let body = body.to_owned();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let _ = read_http_request(&mut stream);
            let response = format!(
                "HTTP/1.1 {status_code} OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).expect("server should write response");
            stream.flush().expect("server should flush response");
        });
        (format!("http://{address}/"), handle)
    }

    fn spawn_static_http_server_with_request_budget(
        status_code: u16,
        body: &str,
        max_requests: usize,
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("listener local address should resolve");
        let body = body.to_owned();
        let handle = thread::spawn(move || {
            for _ in 0..max_requests {
                let (mut stream, _) = listener.accept().expect("listener should accept request");
                let _ = read_http_request(&mut stream);
                let response = format!(
                    "HTTP/1.1 {status_code} OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).expect("server should write response");
                stream.flush().expect("server should flush response");
            }
        });
        (format!("http://{address}/"), handle)
    }

    fn spawn_static_http_server_with_headers(
        status_code: u16,
        body: &str,
        headers: &[(&str, &str)],
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("listener local address should resolve");
        let body = body.to_owned();
        let headers = headers
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect::<Vec<_>>();
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("listener should accept request");
            let _ = read_http_request(&mut stream);
            let mut response = format!(
                "HTTP/1.1 {status_code} OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\n",
                body.len()
            );
            for (name, value) in headers {
                response.push_str(format!("{name}: {value}\r\n").as_str());
            }
            response.push_str("Connection: close\r\n\r\n");
            response.push_str(body.as_str());
            stream.write_all(response.as_bytes()).expect("server should write response");
            stream.flush().expect("server should flush response");
        });
        (format!("http://{address}/"), handle)
    }

    fn spawn_cookie_state_http_server() -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("listener local address should resolve");
        let handle = thread::spawn(move || {
            for index in 0..3 {
                let (mut stream, _) = listener.accept().expect("listener should accept request");
                let request = read_http_request(&mut stream);
                let has_cookie = request.to_ascii_lowercase().contains("cookie: session=abc123");
                let (status_code, body, headers) = match index {
                    0 => (200, "seed", vec!["Set-Cookie: session=abc123; Path=/"]),
                    1 => {
                        if has_cookie {
                            (200, "cookie_replayed", Vec::new())
                        } else {
                            (401, "cookie_missing", Vec::new())
                        }
                    }
                    _ => {
                        if has_cookie {
                            (200, "cookie_still_present", Vec::new())
                        } else {
                            (401, "cookie_cleared", Vec::new())
                        }
                    }
                };
                let mut response = format!(
                    "HTTP/1.1 {status_code} OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n",
                    body.len()
                );
                for header in headers {
                    response.push_str(format!("{header}\r\n").as_str());
                }
                response.push_str("Connection: close\r\n\r\n");
                response.push_str(body);
                stream.write_all(response.as_bytes()).expect("server should write response");
                stream.flush().expect("server should flush response");
            }
        });
        (format!("http://{address}/"), handle)
    }

    fn spawn_download_fixture_http_server(
        file_path: &str,
        file_content_type: &str,
        file_body: &[u8],
    ) -> (String, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let address = listener.local_addr().expect("listener local address should resolve");
        let file_path = file_path.to_owned();
        let file_content_type = file_content_type.to_owned();
        let file_body = file_body.to_vec();
        let handle = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("listener should accept request");
                let request = read_http_request(&mut stream);
                let path = http_request_path(request.as_str());
                if path == "/" {
                    let body = format!(
                        "<!doctype html><html><body><a id=\"download-link\" href=\"{file_path}\" download>Download</a></body></html>"
                    );
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    stream
                        .write_all(response.as_bytes())
                        .expect("server should write HTML response");
                    stream.flush().expect("server should flush HTML response");
                    continue;
                }
                if path == file_path {
                    let headers = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: {file_content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        file_body.len()
                    );
                    stream
                        .write_all(headers.as_bytes())
                        .expect("server should write file response headers");
                    stream
                        .write_all(file_body.as_slice())
                        .expect("server should write file response body");
                    stream.flush().expect("server should flush file response");
                    continue;
                }
                let fallback = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: 9\r\nConnection: close\r\n\r\nnot_found";
                stream
                    .write_all(fallback.as_bytes())
                    .expect("server should write fallback response");
                stream.flush().expect("server should flush fallback response");
            }
        });
        (format!("http://{address}/"), handle)
    }

    fn http_request_path(request: &str) -> String {
        request
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "/".to_owned())
    }

    fn read_http_request(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("read timeout should be configured");
        let mut output = Vec::new();
        let mut buffer = [0_u8; 1024];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(read) => {
                    output.extend_from_slice(&buffer[..read]);
                    if output.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        String::from_utf8_lossy(output.as_slice()).to_string()
    }
}
