use crate::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub(crate) enum PermissionSettingInternal {
    #[default]
    Deny,
    Allow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionPermissionsInternal {
    pub(crate) camera: PermissionSettingInternal,
    pub(crate) microphone: PermissionSettingInternal,
    pub(crate) location: PermissionSettingInternal,
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
    pub(crate) fn to_proto(&self) -> browser_v1::SessionPermissions {
        browser_v1::SessionPermissions {
            v: CANONICAL_PROTOCOL_MAJOR,
            camera: permission_setting_to_proto(self.camera),
            microphone: permission_setting_to_proto(self.microphone),
            location: permission_setting_to_proto(self.location),
        }
    }

    pub(crate) fn apply_update(
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

pub(crate) fn instant_to_unix_ms(instant: Instant) -> u64 {
    current_unix_ms()
        .saturating_sub(u64::try_from(instant.elapsed().as_millis()).unwrap_or(u64::MAX))
}

pub(crate) fn session_summary_to_proto(
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

pub(crate) fn session_detail_to_proto(
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
pub(crate) struct BrowserActionLogEntryInternal {
    pub(crate) action_id: String,
    pub(crate) action_name: String,
    pub(crate) selector: String,
    pub(crate) success: bool,
    pub(crate) outcome: String,
    pub(crate) error: String,
    pub(crate) started_at_unix_ms: u64,
    pub(crate) completed_at_unix_ms: u64,
    pub(crate) attempts: u32,
    pub(crate) page_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NetworkLogHeaderInternal {
    pub(crate) name: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct NetworkLogEntryInternal {
    pub(crate) request_url: String,
    pub(crate) status_code: u16,
    pub(crate) timing_bucket: String,
    pub(crate) latency_ms: u64,
    pub(crate) captured_at_unix_ms: u64,
    pub(crate) headers: Vec<NetworkLogHeaderInternal>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(crate) enum BrowserDiagnosticSeverityInternal {
    Debug,
    Info,
    Warn,
    Error,
}

impl BrowserDiagnosticSeverityInternal {
    pub(crate) fn to_proto(self) -> i32 {
        match self {
            Self::Debug => browser_v1::BrowserDiagnosticSeverity::Debug as i32,
            Self::Info => browser_v1::BrowserDiagnosticSeverity::Info as i32,
            Self::Warn => browser_v1::BrowserDiagnosticSeverity::Warn as i32,
            Self::Error => browser_v1::BrowserDiagnosticSeverity::Error as i32,
        }
    }

    pub(crate) fn from_proto(value: i32) -> Self {
        match browser_v1::BrowserDiagnosticSeverity::try_from(value)
            .unwrap_or(browser_v1::BrowserDiagnosticSeverity::Unspecified)
        {
            browser_v1::BrowserDiagnosticSeverity::Debug => Self::Debug,
            browser_v1::BrowserDiagnosticSeverity::Warn => Self::Warn,
            browser_v1::BrowserDiagnosticSeverity::Error => Self::Error,
            _ => Self::Info,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BrowserConsoleEntryInternal {
    pub(crate) severity: BrowserDiagnosticSeverityInternal,
    pub(crate) kind: String,
    pub(crate) message: String,
    pub(crate) captured_at_unix_ms: u64,
    pub(crate) source: String,
    pub(crate) stack_trace: String,
    pub(crate) page_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BrowserTabRecord {
    pub(crate) tab_id: String,
    pub(crate) last_title: String,
    pub(crate) last_url: Option<String>,
    pub(crate) last_page_body: String,
    pub(crate) scroll_x: i64,
    pub(crate) scroll_y: i64,
    pub(crate) typed_inputs: HashMap<String, String>,
    pub(crate) network_log: VecDeque<NetworkLogEntryInternal>,
    #[serde(default)]
    pub(crate) console_log: VecDeque<BrowserConsoleEntryInternal>,
}

impl BrowserTabRecord {
    pub(crate) fn new(tab_id: String) -> Self {
        Self {
            tab_id,
            last_title: String::new(),
            last_url: None,
            last_page_body: String::new(),
            scroll_x: 0,
            scroll_y: 0,
            typed_inputs: HashMap::new(),
            network_log: VecDeque::new(),
            console_log: VecDeque::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SessionPersistenceState {
    pub(crate) enabled: bool,
    pub(crate) persistence_id: Option<String>,
    pub(crate) state_restored: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct BrowserSessionInit {
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) now: Instant,
    pub(crate) idle_ttl: Duration,
    pub(crate) budget: SessionBudget,
    pub(crate) allow_private_targets: bool,
    pub(crate) allow_downloads: bool,
    pub(crate) action_allowed_domains: Vec<String>,
    pub(crate) profile_id: Option<String>,
    pub(crate) private_profile: bool,
    pub(crate) persistence: SessionPersistenceState,
}

#[derive(Debug, Clone)]
pub(crate) struct BrowserSessionRecord {
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) last_active: Instant,
    pub(crate) created_at: Instant,
    pub(crate) idle_ttl: Duration,
    pub(crate) budget: SessionBudget,
    pub(crate) allow_private_targets: bool,
    pub(crate) allow_downloads: bool,
    pub(crate) action_allowed_domains: Vec<String>,
    pub(crate) profile_id: Option<String>,
    pub(crate) private_profile: bool,
    pub(crate) action_count: u64,
    pub(crate) action_window: VecDeque<Instant>,
    pub(crate) action_log: VecDeque<BrowserActionLogEntryInternal>,
    pub(crate) tabs: HashMap<String, BrowserTabRecord>,
    pub(crate) tab_order: Vec<String>,
    pub(crate) active_tab_id: String,
    pub(crate) permissions: SessionPermissionsInternal,
    pub(crate) cookie_jar: HashMap<String, HashMap<String, String>>,
    pub(crate) storage_entries: HashMap<String, HashMap<String, String>>,
    pub(crate) persistence: SessionPersistenceState,
}

impl BrowserSessionRecord {
    pub(crate) fn with_defaults(init: BrowserSessionInit) -> Self {
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

    pub(crate) fn active_tab(&self) -> Option<&BrowserTabRecord> {
        self.tabs.get(self.active_tab_id.as_str())
    }

    pub(crate) fn active_tab_mut(&mut self) -> Option<&mut BrowserTabRecord> {
        self.tabs.get_mut(self.active_tab_id.as_str())
    }

    pub(crate) fn create_tab(&mut self) -> String {
        let tab_id = Ulid::new().to_string();
        self.tabs.insert(tab_id.clone(), BrowserTabRecord::new(tab_id.clone()));
        self.tab_order.push(tab_id.clone());
        tab_id
    }

    pub(crate) fn can_create_tab(&self) -> bool {
        self.tabs.len() < self.budget.max_tabs_per_session
    }

    pub(crate) fn tab_to_proto(&self, tab_id: &str) -> Option<browser_v1::BrowserTab> {
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

    pub(crate) fn list_tabs(&self) -> Vec<browser_v1::BrowserTab> {
        self.tab_order.iter().filter_map(|tab_id| self.tab_to_proto(tab_id)).collect()
    }

    pub(crate) fn close_tab(&mut self, tab_id: &str) -> Result<(String, Option<String>), String> {
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

    pub(crate) fn apply_snapshot(&mut self, snapshot: PersistedSessionSnapshot) {
        let mut tabs = HashMap::new();
        for mut tab in snapshot.tabs.into_iter().take(self.budget.max_tabs_per_session) {
            if validate_canonical_id(tab.tab_id.as_str()).is_ok() {
                tab.network_log = clamp_network_log_entries(
                    tab.network_log,
                    self.budget.max_network_log_entries,
                    self.budget.max_network_log_bytes,
                );
                tab.console_log = clamp_console_log_entries(
                    tab.console_log,
                    DEFAULT_MAX_CONSOLE_LOG_ENTRIES,
                    DEFAULT_MAX_CONSOLE_LOG_BYTES,
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

pub(crate) struct BrowserRuntimeState {
    pub(crate) started_at: Instant,
    pub(crate) auth_token: Option<String>,
    pub(crate) engine_mode: BrowserEngineMode,
    pub(crate) chromium: ChromiumEngineConfig,
    pub(crate) default_idle_ttl: Duration,
    pub(crate) default_budget: SessionBudget,
    pub(crate) max_sessions: usize,
    pub(crate) state_store: Option<PersistedStateStore>,
    pub(crate) profile_registry_lock: Mutex<()>,
    pub(crate) sessions: Mutex<HashMap<String, BrowserSessionRecord>>,
    pub(crate) chromium_sessions: Mutex<HashMap<String, ChromiumSessionState>>,
    pub(crate) download_sessions: Mutex<HashMap<String, DownloadSandboxSession>>,
}

impl BrowserRuntimeState {
    pub(crate) fn new(args: &Args) -> Result<Self> {
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
pub(crate) struct CookieUpdate {
    pub(crate) domain: String,
    pub(crate) name: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone)]
pub(crate) struct NavigateOutcome {
    pub(crate) success: bool,
    pub(crate) final_url: String,
    pub(crate) status_code: u16,
    pub(crate) title: String,
    pub(crate) page_body: String,
    pub(crate) body_bytes: u64,
    pub(crate) latency_ms: u64,
    pub(crate) error: String,
    pub(crate) network_log: Vec<NetworkLogEntryInternal>,
    pub(crate) cookie_updates: Vec<CookieUpdate>,
}

pub(crate) fn truncate_utf8_bytes(raw: &str, max_bytes: usize) -> String {
    if raw.len() <= max_bytes {
        return raw.to_owned();
    }
    let mut boundary = max_bytes;
    while boundary > 0 && !raw.is_char_boundary(boundary) {
        boundary -= 1;
    }
    raw[..boundary].to_owned()
}

pub(crate) fn parse_session_id(raw: Option<&str>) -> Result<String, String> {
    let value = raw.unwrap_or_default().trim();
    if value.is_empty() {
        return Err("session_id is required".to_owned());
    }
    validate_canonical_id(value).map_err(|error| format!("invalid session_id: {error}"))?;
    Ok(value.to_owned())
}

pub(crate) fn parse_session_id_from_proto(
    raw: Option<proto::palyra::common::v1::CanonicalId>,
) -> Result<String, String> {
    match raw {
        Some(value) => parse_session_id(Some(value.ulid.as_str())),
        None => parse_session_id(None),
    }
}

pub(crate) fn parse_tab_id(raw: Option<&str>) -> Result<String, String> {
    let value = raw.unwrap_or_default().trim();
    if value.is_empty() {
        return Err("tab_id is required".to_owned());
    }
    validate_canonical_id(value).map_err(|error| format!("invalid tab_id: {error}"))?;
    Ok(value.to_owned())
}

pub(crate) fn parse_tab_id_from_proto(
    raw: Option<proto::palyra::common::v1::CanonicalId>,
) -> Result<String, String> {
    match raw {
        Some(value) => parse_tab_id(Some(value.ulid.as_str())),
        None => parse_tab_id(None),
    }
}

pub(crate) fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

pub(crate) fn normalize_optional_string(raw: &str) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_owned())
    }
}

pub(crate) fn sanitize_persistence_id(raw: &str) -> Option<String> {
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

pub(crate) fn permission_setting_to_proto(value: PermissionSettingInternal) -> i32 {
    match value {
        PermissionSettingInternal::Deny => 1,
        PermissionSettingInternal::Allow => 2,
    }
}

pub(crate) fn permission_setting_from_proto(value: i32) -> Option<PermissionSettingInternal> {
    match value {
        1 => Some(PermissionSettingInternal::Deny),
        2 => Some(PermissionSettingInternal::Allow),
        _ => None,
    }
}

pub(crate) fn is_default_port(scheme: &str, port: u16) -> bool {
    matches!((scheme, port), ("http", 80) | ("https", 443))
}

pub(crate) fn is_sensitive_query_key(raw_key: &str) -> bool {
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

pub(crate) fn find_matching_html_tag(selector: &str, html: &str) -> Option<String> {
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

pub(crate) fn extract_attr_value(tag_lower: &str, attr_name: &str) -> Option<String> {
    let needle = format!("{attr_name}=");
    let start = tag_lower.find(needle.as_str())?;
    parse_attr_value(&tag_lower[start + needle.len()..])
}

pub(crate) fn extract_attr_value_case_insensitive(tag: &str, attr_name: &str) -> Option<String> {
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

pub(crate) fn html_tag_name(tag_lower: &str) -> Option<&str> {
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

pub(crate) fn is_typable_tag(tag: &str) -> bool {
    let tag_lower = tag.to_ascii_lowercase();
    matches!(html_tag_name(tag_lower.as_str()), Some("input" | "textarea"))
}

pub(crate) fn is_download_like_tag(tag: &str) -> bool {
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
