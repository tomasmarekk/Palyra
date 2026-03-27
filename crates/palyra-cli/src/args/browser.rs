use clap::Subcommand;

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum BrowserCommand {
    #[command(about = "Show browser service health, policy, and local lifecycle state")]
    Status {
        #[arg(long)]
        endpoint: Option<String>,
        #[arg(long)]
        health_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
    },
    #[command(about = "Start a local browser service in the background")]
    Start {
        #[arg(long)]
        bin_path: Option<String>,
        #[arg(long)]
        endpoint: Option<String>,
        #[arg(long)]
        health_url: Option<String>,
        #[arg(long)]
        token: Option<String>,
        #[arg(long, default_value_t = 10_000)]
        wait_ms: u64,
    },
    #[command(about = "Stop the local browser service started by this CLI")]
    Stop,
    #[command(about = "Create a session and immediately navigate it to a URL")]
    Open {
        #[arg(long)]
        url: String,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long, default_value_t = false)]
        allow_private_targets: bool,
        #[arg(long, default_value_t = false)]
        allow_downloads: bool,
        #[arg(long)]
        profile_id: Option<String>,
        #[arg(long, default_value_t = false)]
        private_profile: bool,
        #[arg(long)]
        timeout_ms: Option<u64>,
    },
    #[command(about = "Manage browser sessions")]
    Session {
        #[command(subcommand)]
        command: BrowserSessionCommand,
    },
    #[command(about = "Manage browser profiles")]
    Profiles {
        #[command(subcommand)]
        command: BrowserProfilesCommand,
    },
    #[command(about = "Manage browser tabs for a session")]
    Tabs {
        session_id: String,
        #[command(subcommand)]
        command: BrowserTabsCommand,
    },
    #[command(about = "Navigate an existing browser session")]
    Navigate {
        session_id: String,
        #[arg(long)]
        url: String,
        #[arg(long)]
        timeout_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        allow_redirects: bool,
        #[arg(long)]
        max_redirects: Option<u32>,
        #[arg(long, default_value_t = false)]
        allow_private_targets: bool,
    },
    #[command(about = "Click an element in the active tab")]
    Click {
        session_id: String,
        #[arg(long)]
        selector: String,
        #[arg(long)]
        max_retries: Option<u32>,
        #[arg(long)]
        timeout_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        capture_failure_screenshot: bool,
        #[arg(long)]
        max_failure_screenshot_bytes: Option<u64>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Type text into an element in the active tab")]
    Type {
        session_id: String,
        #[arg(long)]
        selector: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        timeout_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        capture_failure_screenshot: bool,
        #[arg(long)]
        max_failure_screenshot_bytes: Option<u64>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Clear an element and then type text into it")]
    Fill {
        session_id: String,
        #[arg(long)]
        selector: String,
        #[arg(long)]
        text: String,
        #[arg(long)]
        timeout_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        capture_failure_screenshot: bool,
        #[arg(long)]
        max_failure_screenshot_bytes: Option<u64>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Scroll the active tab")]
    Scroll {
        session_id: String,
        #[arg(long, default_value_t = 0)]
        delta_x: i64,
        #[arg(long, default_value_t = 0)]
        delta_y: i64,
        #[arg(long, default_value_t = false)]
        capture_failure_screenshot: bool,
        #[arg(long)]
        max_failure_screenshot_bytes: Option<u64>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Wait for selector or text to appear")]
    Wait {
        session_id: String,
        #[arg(long)]
        selector: Option<String>,
        #[arg(long)]
        text: Option<String>,
        #[arg(long)]
        timeout_ms: Option<u64>,
        #[arg(long)]
        poll_interval_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        capture_failure_screenshot: bool,
        #[arg(long)]
        max_failure_screenshot_bytes: Option<u64>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Observe the active tab and emit a bounded page snapshot")]
    Snapshot {
        session_id: String,
        #[arg(long, default_value_t = false)]
        include_dom_snapshot: bool,
        #[arg(long, default_value_t = false)]
        include_accessibility_tree: bool,
        #[arg(long, default_value_t = false)]
        include_visible_text: bool,
        #[arg(long)]
        max_dom_snapshot_bytes: Option<u64>,
        #[arg(long)]
        max_accessibility_tree_bytes: Option<u64>,
        #[arg(long)]
        max_visible_text_bytes: Option<u64>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Capture a screenshot from the active tab")]
    Screenshot {
        session_id: String,
        #[arg(long)]
        max_bytes: Option<u64>,
        #[arg(long)]
        format: Option<String>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Read the active page title")]
    Title {
        session_id: String,
        #[arg(long)]
        max_title_bytes: Option<u64>,
    },
    #[command(about = "Read the bounded network log for the active tab")]
    Network {
        session_id: String,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        include_headers: bool,
        #[arg(long)]
        max_payload_bytes: Option<u64>,
    },
    #[command(about = "Inspect cookies and storage for a browser session")]
    Storage {
        session_id: String,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Inspect failed browser actions for a session")]
    Errors {
        session_id: String,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Export a trace-like debug artifact for a browser session")]
    Trace {
        session_id: String,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "List download artifacts for a browser session")]
    Downloads {
        session_id: String,
        #[arg(long)]
        limit: Option<u32>,
        #[arg(long, default_value_t = false)]
        quarantined_only: bool,
    },
    #[command(about = "Get or set browser permissions for a session")]
    Permissions {
        session_id: String,
        #[command(subcommand)]
        command: BrowserPermissionsCommand,
    },
    #[command(about = "Reset browser state for a session")]
    ResetState {
        session_id: String,
        #[arg(long, default_value_t = false)]
        clear_cookies: bool,
        #[arg(long, default_value_t = false)]
        clear_storage: bool,
        #[arg(long, default_value_t = false)]
        reset_tabs: bool,
        #[arg(long, default_value_t = false)]
        reset_permissions: bool,
    },
    #[command(about = "Structured unsupported placeholder for console logs")]
    Console {
        session_id: String,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Structured unsupported placeholder for PDF export")]
    Pdf {
        session_id: String,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Structured unsupported placeholder for key presses")]
    Press {
        session_id: String,
        #[arg(long)]
        key: String,
    },
    #[command(about = "Structured unsupported placeholder for select element changes")]
    Select {
        session_id: String,
        #[arg(long)]
        selector: String,
        #[arg(long)]
        value: String,
    },
    #[command(about = "Structured unsupported placeholder for selector highlighting")]
    Highlight {
        session_id: String,
        #[arg(long)]
        selector: String,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum BrowserSessionCommand {
    #[command(about = "Create a browser session")]
    Create {
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        channel: Option<String>,
        #[arg(long)]
        idle_ttl_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        allow_private_targets: bool,
        #[arg(long, default_value_t = false)]
        allow_downloads: bool,
        #[arg(long = "allow-domain")]
        action_allowed_domains: Vec<String>,
        #[arg(long, default_value_t = false)]
        persistence_enabled: bool,
        #[arg(long)]
        persistence_id: Option<String>,
        #[arg(long)]
        profile_id: Option<String>,
        #[arg(long, default_value_t = false)]
        private_profile: bool,
    },
    #[command(about = "List active browser sessions")]
    List {
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        limit: Option<u32>,
    },
    #[command(about = "Show session summary, budget, and tabs")]
    Show { session_id: String },
    #[command(about = "Inspect bounded session debug state")]
    Inspect {
        session_id: String,
        #[arg(long, default_value_t = false)]
        include_cookies: bool,
        #[arg(long, default_value_t = false)]
        include_storage: bool,
        #[arg(long, default_value_t = false)]
        include_action_log: bool,
        #[arg(long, default_value_t = false)]
        include_network_log: bool,
        #[arg(long, default_value_t = false)]
        include_page_snapshot: bool,
        #[arg(long)]
        max_cookie_bytes: Option<u64>,
        #[arg(long)]
        max_storage_bytes: Option<u64>,
        #[arg(long)]
        max_action_log_entries: Option<u32>,
        #[arg(long)]
        max_network_log_entries: Option<u32>,
        #[arg(long)]
        max_network_log_bytes: Option<u64>,
        #[arg(long)]
        max_dom_snapshot_bytes: Option<u64>,
        #[arg(long)]
        max_visible_text_bytes: Option<u64>,
        #[arg(long)]
        output: Option<String>,
    },
    #[command(about = "Close a browser session")]
    Close { session_id: String },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum BrowserProfilesCommand {
    #[command(about = "List browser profiles")]
    List {
        #[arg(long)]
        principal: Option<String>,
    },
    #[command(about = "Create a browser profile")]
    Create {
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        name: String,
        #[arg(long)]
        theme_color: Option<String>,
        #[arg(long, default_value_t = false)]
        persistence_enabled: bool,
        #[arg(long, default_value_t = false)]
        private_profile: bool,
    },
    #[command(about = "Rename a browser profile")]
    Rename {
        profile_id: String,
        #[arg(long)]
        principal: Option<String>,
        #[arg(long)]
        name: String,
    },
    #[command(about = "Delete a browser profile")]
    Delete {
        profile_id: String,
        #[arg(long)]
        principal: Option<String>,
    },
    #[command(about = "Set the active browser profile")]
    Activate {
        profile_id: String,
        #[arg(long)]
        principal: Option<String>,
    },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum BrowserTabsCommand {
    #[command(about = "List tabs in a browser session")]
    List,
    #[command(about = "Open a tab in a browser session")]
    Open {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = true)]
        activate: bool,
        #[arg(long)]
        timeout_ms: Option<u64>,
        #[arg(long, default_value_t = false)]
        allow_redirects: bool,
        #[arg(long)]
        max_redirects: Option<u32>,
        #[arg(long, default_value_t = false)]
        allow_private_targets: bool,
    },
    #[command(about = "Switch the active tab in a browser session")]
    Switch { tab_id: String },
    #[command(about = "Close a tab in a browser session")]
    Close { tab_id: String },
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
pub enum BrowserPermissionsCommand {
    #[command(about = "Show current browser permissions")]
    Get,
    #[command(about = "Set browser permissions")]
    Set {
        #[arg(long)]
        camera: Option<String>,
        #[arg(long)]
        microphone: Option<String>,
        #[arg(long)]
        location: Option<String>,
        #[arg(long, default_value_t = false)]
        reset_to_default: bool,
    },
}
