use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use palyra_common::config_system::get_value_at_path;
use palyra_control_plane as control_plane;
use reqwest::{Client as AsyncClient, Url};
use ring::rand::{SecureRandom, SystemRandom};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::{metadata::MetadataMap, transport::Endpoint, Code, Request};

use crate::args::{
    BrowserPermissionsCommand, BrowserProfilesCommand, BrowserSessionCommand, BrowserTabsCommand,
};
use crate::*;

const DEFAULT_BROWSER_GRPC_URL: &str = "http://127.0.0.1:7543";
const DEFAULT_BROWSER_HEALTH_BASE_URL: &str = DEFAULT_BROWSER_URL;
const DEFAULT_BROWSER_GRPC_PORT: u16 =
    palyra_common::local_runtime_ports::DEFAULT_BROWSER_GRPC_PORT;
const DEFAULT_BROWSER_HEALTH_PORT: u16 =
    palyra_common::local_runtime_ports::DEFAULT_BROWSER_HEALTH_PORT;
const BROWSER_CONTROL_PLANE_TIMEOUT_BUFFER_MS: u64 = 5_000;
const BROWSER_CONTROL_PLANE_MIN_TIMEOUT_MS: u64 = 10_000;
const BROWSER_SERVICE_METADATA_SCHEMA_VERSION: u32 = 1;
const BROWSER_SERVICE_START_POLL_MS: u64 = 250;
const BROWSER_SERVICE_STOP_TIMEOUT_MS: u64 = 5_000;
const BROWSER_SERVICE_STATE_DIR: &str = "browser-cli";
const BROWSER_SERVICE_METADATA_FILE_NAME: &str = "browser-service.json";
const BROWSER_SERVICE_STDOUT_LOG_FILE_NAME: &str = "browserd.stdout.log";
const BROWSER_SERVICE_STDERR_LOG_FILE_NAME: &str = "browserd.stderr.log";
const BROWSERD_STATE_ENCRYPTION_KEY_ENV: &str = "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY";
const BROWSERD_AUTH_TOKEN_ENV: &str = "PALYRA_BROWSERD_AUTH_TOKEN";
const BROWSERD_STATE_ENCRYPTION_KEY_LEN: usize = 32;
const BROWSER_ARTIFACT_DIR: &str = "browser-artifacts";
const BROWSER_CALLER_PRINCIPAL_HEADER: &str = "x-palyra-principal";
const BROWSER_PROBE_PRINCIPAL: &str = "admin:browser-probe";
const BROWSER_UPLOAD_MAX_FILE_BYTES: u64 = 8 * 1024 * 1024;
const BROWSER_GATEWAY_TOOL_NAMES: &[&str] = &[
    "palyra.browser.session.create",
    "palyra.browser.session.close",
    "palyra.browser.navigate",
    "palyra.browser.reload",
    "palyra.browser.click",
    "palyra.browser.type",
    "palyra.browser.fill",
    "palyra.browser.upload",
    "palyra.browser.press",
    "palyra.browser.select",
    "palyra.browser.viewport",
    "palyra.browser.highlight",
    "palyra.browser.scroll",
    "palyra.browser.wait_for",
    "palyra.browser.title",
    "palyra.browser.screenshot",
    "palyra.browser.pdf",
    "palyra.browser.observe",
    "palyra.browser.network_log",
    "palyra.browser.console_log",
    "palyra.browser.reset_state",
    "palyra.browser.tabs.list",
    "palyra.browser.tabs.open",
    "palyra.browser.tabs.switch",
    "palyra.browser.tabs.close",
    "palyra.browser.permissions.get",
    "palyra.browser.permissions.set",
    "palyra.browser.downloads.list",
    "palyra.browser.downloads.get",
];

#[cfg(windows)]
const DETACHED_PROCESS: u32 = 0x0000_0008;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[derive(Debug, Clone)]
struct BrowserServiceConnection {
    grpc_url: String,
    health_base_url: String,
    auth_token: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserPolicySnapshot {
    configured_enabled: bool,
    auth_token_configured: bool,
    endpoint: String,
    connect_timeout_ms: Option<u64>,
    request_timeout_ms: Option<u64>,
    max_screenshot_bytes: Option<u64>,
    max_title_bytes: Option<u64>,
    state_dir: Option<String>,
    browser_tools_allowlisted: bool,
    missing_browser_tools: Vec<String>,
    state_key_vault_ref_configured: bool,
    state_encryption_key_env_configured: bool,
    profiles_ready: bool,
}

#[derive(Debug, Clone)]
struct BrowserResolvedConfig {
    connection: BrowserServiceConnection,
    policy: BrowserPolicySnapshot,
    config_path: Option<String>,
    state_key_vault_ref: Option<String>,
    token_from_cli_only: bool,
    token_conflicts_with_gateway_config: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BrowserServiceMetadata {
    schema_version: u32,
    pid: u32,
    binary: String,
    grpc_url: String,
    health_base_url: String,
    stdout_log_path: String,
    stderr_log_path: String,
    started_at_unix_ms: u64,
    #[serde(default)]
    auth_token_configured: bool,
    #[serde(default)]
    state_encryption_key_configured: bool,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserLifecyclePayload {
    action: String,
    running: bool,
    pid: Option<u32>,
    grpc_url: String,
    health_base_url: String,
    stdout_log_path: Option<String>,
    stderr_log_path: Option<String>,
    detail: String,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserSetupPayload {
    config_path: String,
    browser_service_enabled: bool,
    auth_token_configured: bool,
    auth_token_generated: bool,
    state_key_vault_ref: String,
    state_key_generated: bool,
    allowed_tools_added: Vec<String>,
    gateway_reload_required: bool,
    gateway_next_step: String,
    gateway_restart_command: String,
    gateway_verify_command: String,
    migrated: bool,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserStatusPayload {
    service: &'static str,
    grpc_url: String,
    health_base_url: String,
    port_diagnostics: Vec<BrowserPortDiagnostic>,
    health_ok: bool,
    health_response: Option<Value>,
    grpc_ok: bool,
    grpc_error: Option<String>,
    lifecycle_running: bool,
    lifecycle_metadata: Option<BrowserServiceMetadata>,
    config_path: Option<String>,
    policy: BrowserPolicySnapshot,
    control_plane: BrowserControlPlaneSnapshot,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserPortDiagnostic {
    label: &'static str,
    url: String,
    host: String,
    port: u16,
    bind_available: bool,
    bind_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct BrowserControlPlaneSnapshot {
    reachable: bool,
    browser_enabled: Option<bool>,
    error: Option<String>,
    auth_probe_skipped: bool,
}

struct BrowserOpenArgs {
    url: String,
    principal: Option<String>,
    channel: Option<String>,
    allow_private_targets: bool,
    allow_downloads: bool,
    profile_id: Option<String>,
    private_profile: bool,
    timeout_ms: Option<u64>,
    json: bool,
}

struct BrowserClickArgs {
    session_id: String,
    selector: String,
    max_retries: Option<u32>,
    timeout_ms: Option<u64>,
    capture_failure_screenshot: bool,
    max_failure_screenshot_bytes: Option<u64>,
    output: Option<String>,
    json: bool,
}

struct BrowserTypeArgs {
    session_id: String,
    selector: String,
    text: String,
    clear_existing: bool,
    timeout_ms: Option<u64>,
    capture_failure_screenshot: bool,
    max_failure_screenshot_bytes: Option<u64>,
    output: Option<String>,
    json: bool,
}

struct BrowserUploadArgs {
    session_id: String,
    selector: String,
    file: String,
    timeout_ms: Option<u64>,
    capture_failure_screenshot: bool,
    max_failure_screenshot_bytes: Option<u64>,
    output: Option<String>,
    json: bool,
}

struct BrowserWaitArgs {
    session_id: String,
    selector: Option<String>,
    text: Option<String>,
    timeout_ms: Option<u64>,
    poll_interval_ms: Option<u64>,
    capture_failure_screenshot: bool,
    max_failure_screenshot_bytes: Option<u64>,
    output: Option<String>,
    json: bool,
}

struct BrowserSnapshotArgs {
    session_id: String,
    include_dom_snapshot: bool,
    include_accessibility_tree: bool,
    include_visible_text: bool,
    max_dom_snapshot_bytes: Option<u64>,
    max_accessibility_tree_bytes: Option<u64>,
    max_visible_text_bytes: Option<u64>,
    output: Option<String>,
    json: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BrowserOutputMode {
    Text,
    Json,
    Ndjson,
}

pub(crate) fn run_browser(command: BrowserCommand) -> Result<()> {
    let runtime = build_runtime()?;
    runtime.block_on(run_browser_async(command))
}

async fn run_browser_async(command: BrowserCommand) -> Result<()> {
    if let Some(action) = browser_command_policy_action(&command) {
        ensure_browser_cli_policy_enabled(action)?;
    }
    match command {
        BrowserCommand::Status { endpoint, health_url, token, json } => {
            run_browser_status(endpoint, health_url, token, json).await
        }
        BrowserCommand::Start { bin_path, endpoint, health_url, token, wait_ms, setup, json } => {
            run_browser_start(bin_path, endpoint, health_url, token, wait_ms, setup, json).await
        }
        BrowserCommand::Setup { path, token, force, json } => {
            run_browser_setup(path, token, force, json)
        }
        BrowserCommand::Stop { json } => run_browser_stop(json).await,
        BrowserCommand::Open {
            url,
            principal,
            channel,
            allow_private_targets,
            allow_downloads,
            profile_id,
            private_profile,
            timeout_ms,
            json,
        } => {
            run_browser_open(BrowserOpenArgs {
                url,
                principal,
                channel,
                allow_private_targets,
                allow_downloads,
                profile_id,
                private_profile,
                timeout_ms,
                json,
            })
            .await
        }
        BrowserCommand::Session { command } => run_browser_session_command(command).await,
        BrowserCommand::Profiles { command } => run_browser_profiles_command(command).await,
        BrowserCommand::Tabs { session_id, command } => {
            run_browser_tabs_command(session_id, command).await
        }
        BrowserCommand::Navigate {
            session_id,
            url,
            timeout_ms,
            allow_redirects,
            max_redirects,
            allow_private_targets,
        } => {
            run_browser_navigate(
                session_id,
                url,
                timeout_ms,
                allow_redirects,
                max_redirects,
                allow_private_targets,
            )
            .await
        }
        BrowserCommand::Click {
            session_id,
            selector,
            max_retries,
            timeout_ms,
            capture_failure_screenshot,
            max_failure_screenshot_bytes,
            output,
            json,
        } => {
            run_browser_click(BrowserClickArgs {
                session_id,
                selector,
                max_retries,
                timeout_ms,
                capture_failure_screenshot,
                max_failure_screenshot_bytes,
                output,
                json,
            })
            .await
        }
        BrowserCommand::Type {
            session_id,
            selector,
            text,
            timeout_ms,
            capture_failure_screenshot,
            max_failure_screenshot_bytes,
            output,
            json,
        } => {
            run_browser_type(BrowserTypeArgs {
                session_id,
                selector,
                text,
                clear_existing: false,
                timeout_ms,
                capture_failure_screenshot,
                max_failure_screenshot_bytes,
                output,
                json,
            })
            .await
        }
        BrowserCommand::Upload {
            session_id,
            selector,
            file,
            timeout_ms,
            capture_failure_screenshot,
            max_failure_screenshot_bytes,
            output,
            json,
        } => {
            run_browser_upload(BrowserUploadArgs {
                session_id,
                selector,
                file,
                timeout_ms,
                capture_failure_screenshot,
                max_failure_screenshot_bytes,
                output,
                json,
            })
            .await
        }
        BrowserCommand::Fill {
            session_id,
            selector,
            text,
            timeout_ms,
            capture_failure_screenshot,
            max_failure_screenshot_bytes,
            output,
            json,
        } => {
            run_browser_type(BrowserTypeArgs {
                session_id,
                selector,
                text,
                clear_existing: true,
                timeout_ms,
                capture_failure_screenshot,
                max_failure_screenshot_bytes,
                output,
                json,
            })
            .await
        }
        BrowserCommand::Scroll {
            session_id,
            delta_x,
            delta_y,
            capture_failure_screenshot,
            max_failure_screenshot_bytes,
            output,
        } => {
            run_browser_scroll(
                session_id,
                delta_x,
                delta_y,
                capture_failure_screenshot,
                max_failure_screenshot_bytes,
                output,
            )
            .await
        }
        BrowserCommand::Wait {
            session_id,
            selector,
            text,
            timeout_ms,
            poll_interval_ms,
            capture_failure_screenshot,
            max_failure_screenshot_bytes,
            output,
            json,
        } => {
            run_browser_wait(BrowserWaitArgs {
                session_id,
                selector,
                text,
                timeout_ms,
                poll_interval_ms,
                capture_failure_screenshot,
                max_failure_screenshot_bytes,
                output,
                json,
            })
            .await
        }
        BrowserCommand::Snapshot {
            session_id,
            include_dom_snapshot,
            include_accessibility_tree,
            include_visible_text,
            max_dom_snapshot_bytes,
            max_accessibility_tree_bytes,
            max_visible_text_bytes,
            output,
            json,
        } => {
            run_browser_snapshot(BrowserSnapshotArgs {
                session_id,
                include_dom_snapshot,
                include_accessibility_tree,
                include_visible_text,
                max_dom_snapshot_bytes,
                max_accessibility_tree_bytes,
                max_visible_text_bytes,
                output,
                json,
            })
            .await
        }
        BrowserCommand::Screenshot { session_id, max_bytes, format, output, json } => {
            run_browser_screenshot(session_id, max_bytes, format, output, json).await
        }
        BrowserCommand::Title { session_id, max_title_bytes, json } => {
            run_browser_title(session_id, max_title_bytes, json).await
        }
        BrowserCommand::Network { session_id, limit, include_headers, max_payload_bytes, json } => {
            run_browser_network(session_id, limit, include_headers, max_payload_bytes, json).await
        }
        BrowserCommand::Storage { session_id, output } => {
            run_browser_storage(session_id, output).await
        }
        BrowserCommand::Errors { session_id, limit, output, json } => {
            run_browser_errors(session_id, limit, output, json).await
        }
        BrowserCommand::Trace { session_id, output } => run_browser_trace(session_id, output).await,
        BrowserCommand::Downloads {
            session_id,
            artifact_id,
            output,
            max_bytes,
            limit,
            quarantined_only,
            json,
        } => {
            run_browser_downloads(
                session_id,
                artifact_id,
                output,
                max_bytes,
                limit,
                quarantined_only,
                json,
            )
            .await
        }
        BrowserCommand::Permissions { session_id, command } => {
            run_browser_permissions_command(session_id, command).await
        }
        BrowserCommand::ResetState {
            session_id,
            clear_cookies,
            clear_storage,
            reset_tabs,
            reset_permissions,
        } => {
            run_browser_reset_state(
                session_id,
                clear_cookies,
                clear_storage,
                reset_tabs,
                reset_permissions,
            )
            .await
        }
        BrowserCommand::Console { session_id, output, json } => {
            run_browser_console(session_id, output, json).await
        }
        BrowserCommand::Pdf { session_id, output } => run_browser_pdf(session_id, output).await,
        BrowserCommand::Press { session_id, key } => run_browser_press(session_id, key).await,
        BrowserCommand::Select { session_id, selector, value } => {
            run_browser_select(session_id, selector, value).await
        }
        BrowserCommand::Highlight { session_id, selector } => {
            run_browser_highlight(session_id, selector).await
        }
    }
}

fn browser_command_policy_action(command: &BrowserCommand) -> Option<&'static str> {
    match command {
        BrowserCommand::Status { .. }
        | BrowserCommand::Start { .. }
        | BrowserCommand::Setup { .. }
        | BrowserCommand::Stop { .. } => None,
        BrowserCommand::Open { .. } => Some("open"),
        BrowserCommand::Session { command } => Some(browser_session_policy_action(command)),
        BrowserCommand::Profiles { command } => Some(browser_profiles_policy_action(command)),
        BrowserCommand::Tabs { command, .. } => Some(browser_tabs_policy_action(command)),
        BrowserCommand::Navigate { .. } => Some("navigate"),
        BrowserCommand::Click { .. } => Some("click"),
        BrowserCommand::Type { .. } => Some("type"),
        BrowserCommand::Upload { .. } => Some("upload"),
        BrowserCommand::Fill { .. } => Some("fill"),
        BrowserCommand::Scroll { .. } => Some("scroll"),
        BrowserCommand::Wait { .. } => Some("wait"),
        BrowserCommand::Snapshot { .. } => Some("snapshot"),
        BrowserCommand::Screenshot { .. } => Some("screenshot"),
        BrowserCommand::Title { .. } => Some("title"),
        BrowserCommand::Network { .. } => Some("network"),
        BrowserCommand::Storage { .. } => Some("storage"),
        BrowserCommand::Errors { .. } => Some("errors"),
        BrowserCommand::Trace { .. } => Some("trace"),
        BrowserCommand::Downloads { .. } => Some("downloads"),
        BrowserCommand::Permissions { command, .. } => {
            Some(browser_permissions_policy_action(command))
        }
        BrowserCommand::ResetState { .. } => Some("reset-state"),
        BrowserCommand::Console { .. } => Some("console"),
        BrowserCommand::Pdf { .. } => Some("pdf"),
        BrowserCommand::Press { .. } => Some("press"),
        BrowserCommand::Select { .. } => Some("select"),
        BrowserCommand::Highlight { .. } => Some("highlight"),
    }
}

fn browser_session_policy_action(command: &BrowserSessionCommand) -> &'static str {
    match command {
        BrowserSessionCommand::Create { .. } => "session create",
        BrowserSessionCommand::List { .. } => "session list",
        BrowserSessionCommand::Show { .. } => "session show",
        BrowserSessionCommand::Inspect { .. } => "session inspect",
        BrowserSessionCommand::Close { .. } => "session close",
    }
}

fn browser_profiles_policy_action(command: &BrowserProfilesCommand) -> &'static str {
    match command {
        BrowserProfilesCommand::List { .. } => "profiles list",
        BrowserProfilesCommand::Create { .. } => "profiles create",
        BrowserProfilesCommand::Rename { .. } => "profiles rename",
        BrowserProfilesCommand::Delete { .. } => "profiles delete",
        BrowserProfilesCommand::Activate { .. } => "profiles activate",
    }
}

fn browser_tabs_policy_action(command: &BrowserTabsCommand) -> &'static str {
    match command {
        BrowserTabsCommand::List => "tabs list",
        BrowserTabsCommand::Open { .. } => "tabs open",
        BrowserTabsCommand::Switch { .. } => "tabs switch",
        BrowserTabsCommand::Close { .. } => "tabs close",
    }
}

fn browser_permissions_policy_action(command: &BrowserPermissionsCommand) -> &'static str {
    match command {
        BrowserPermissionsCommand::Get => "permissions get",
        BrowserPermissionsCommand::Set { .. } => "permissions set",
    }
}

fn ensure_browser_cli_policy_enabled(action: &str) -> Result<()> {
    let resolved = resolve_browser_config(None, None, None)?;
    ensure_browser_service_enabled(&resolved.policy, action, resolved.config_path.as_deref())?;
    let metadata = read_browser_service_metadata()?;
    let lifecycle_running = metadata.as_ref().is_some_and(|value| process_is_running(value.pid));
    ensure_browser_profile_prerequisites(
        &resolved.policy,
        metadata.as_ref(),
        lifecycle_running,
        action,
        resolved.config_path.as_deref(),
    )?;
    ensure_browser_gateway_auth_token_alignment(
        &resolved.policy,
        metadata.as_ref(),
        action,
        resolved.config_path.as_deref(),
    )
}

async fn run_browser_status(
    endpoint: Option<String>,
    health_url: Option<String>,
    token: Option<String>,
    json: bool,
) -> Result<()> {
    let resolved = resolve_browser_config(endpoint, health_url, token)?;
    let metadata = read_browser_service_metadata()?;
    let cli_lifecycle_running =
        metadata.as_ref().is_some_and(|value| process_is_running(value.pid));
    let mut policy = browser_policy_with_lifecycle_profile_readiness(
        resolved.policy,
        metadata.as_ref(),
        cli_lifecycle_running,
    );
    let health_response =
        fetch_browser_health(resolved.connection.health_base_url.as_str()).await.ok();
    let grpc_error =
        probe_browser_grpc(&resolved.connection).await.err().map(|error| error.to_string());
    let port_diagnostics = browser_connection_port_diagnostics(&resolved.connection);
    let control_plane = browser_status_control_plane_policy_snapshot();
    let browserd_reachable = health_response.is_some() || grpc_error.is_none();
    let browserd_healthy = health_response.is_some() && grpc_error.is_none();
    if browserd_healthy
        && probe_browser_profile_readiness(&resolved.connection).await.unwrap_or(false)
    {
        policy.profiles_ready = true;
    }
    let lifecycle_running =
        effective_browser_lifecycle_running(cli_lifecycle_running, browserd_reachable);
    let mut warnings = browser_status_warnings(
        &policy,
        &control_plane,
        browserd_reachable,
        browserd_healthy,
        metadata.as_ref(),
        resolved.config_path.as_deref(),
    );
    warnings.extend(browser_profile_prerequisite_warnings(
        &policy,
        metadata.as_ref(),
        cli_lifecycle_running,
        resolved.config_path.as_deref(),
    ));
    warnings.extend(browser_port_diagnostic_warnings(
        port_diagnostics.as_slice(),
        health_response.is_some(),
        grpc_error.is_none(),
    ));
    let payload = BrowserStatusPayload {
        service: "palyra-browserd",
        grpc_url: resolved.connection.grpc_url,
        health_base_url: resolved.connection.health_base_url,
        port_diagnostics,
        health_ok: health_response.is_some(),
        health_response,
        grpc_ok: grpc_error.is_none(),
        grpc_error,
        lifecycle_running,
        lifecycle_metadata: metadata,
        config_path: resolved.config_path,
        policy,
        control_plane,
        warnings,
    };
    let value =
        serde_json::to_value(&payload).context("failed to encode browser status payload")?;
    emit_browser_value_with_json(
        &value,
        format_browser_status_text(&payload),
        "failed to encode browser status output",
        json,
    )
}

fn run_browser_setup(
    path: Option<String>,
    token: Option<String>,
    force: bool,
    json: bool,
) -> Result<()> {
    let payload = configure_browser_setup(path, token.as_deref(), force)?;
    let value = serde_json::to_value(&payload).context("failed to encode browser setup payload")?;
    emit_browser_value_with_json(
        &value,
        format_browser_setup_text(&payload),
        "failed to encode browser setup output",
        json,
    )
}

fn configure_browser_setup(
    path: Option<String>,
    token: Option<&str>,
    force: bool,
) -> Result<BrowserSetupPayload> {
    let token = token.map(str::trim).filter(|value| !value.is_empty()).map(ToOwned::to_owned);
    let config_path = resolve_config_path(path, false)?;
    let path_ref = Path::new(&config_path);
    let (mut document, migration) = load_document_for_mutation(path_ref)
        .with_context(|| format!("failed to parse {}", path_ref.display()))?;

    set_value_at_path(
        &mut document,
        "tool_call.browser_service.enabled",
        toml::Value::Boolean(true),
    )?;
    let existing_endpoint = document_string(Some(&document), "tool_call.browser_service.endpoint");
    let existing_health_base_url =
        document_string(Some(&document), "tool_call.browser_service.health_base_url");
    if existing_endpoint.is_none() {
        let ports = palyra_common::local_runtime_ports::select_available_browser_runtime_ports(
            palyra_common::local_runtime_ports::LOCAL_RUNTIME_LOOPBACK_HOST,
        )
        .map_err(anyhow::Error::msg)?;
        set_value_at_path(
            &mut document,
            "tool_call.browser_service.endpoint",
            toml::Value::String(format!(
                "http://{}:{}",
                palyra_common::local_runtime_ports::LOCAL_RUNTIME_LOOPBACK_HOST,
                ports.grpc
            )),
        )?;
        if existing_health_base_url.is_none() {
            set_value_at_path(
                &mut document,
                "tool_call.browser_service.health_base_url",
                toml::Value::String(format!(
                    "http://{}:{}",
                    palyra_common::local_runtime_ports::LOCAL_RUNTIME_LOOPBACK_HOST,
                    ports.health
                )),
            )?;
        }
    } else if existing_health_base_url.is_none() {
        set_value_at_path(
            &mut document,
            "tool_call.browser_service.health_base_url",
            toml::Value::String(derive_browser_health_base_url(
                existing_endpoint.as_deref().unwrap_or(DEFAULT_BROWSER_GRPC_URL),
            )),
        )?;
    }

    let existing_auth_token =
        document_string(Some(&document), "tool_call.browser_service.auth_token");
    let existing_auth_token_secret_ref =
        document_value_present(Some(&document), "tool_call.browser_service.auth_token_secret_ref");
    let should_write_auth_token = should_write_browser_setup_auth_token(
        force,
        token.is_some(),
        existing_auth_token.as_deref(),
        existing_auth_token_secret_ref,
    );
    let mut auth_token_generated = false;
    if should_write_auth_token {
        let auth_token = match token {
            Some(token) => token,
            None => {
                auth_token_generated = true;
                generate_browser_auth_token()
            }
        };
        unset_value_at_path(&mut document, "tool_call.browser_service.auth_token_secret_ref")?;
        set_value_at_path(
            &mut document,
            "tool_call.browser_service.auth_token",
            toml::Value::String(auth_token),
        )?;
    }

    let existing_state_key_ref =
        document_string(Some(&document), "tool_call.browser_service.state_key_vault_ref");
    let should_write_state_key = force || existing_state_key_ref.is_none();
    let state_key_vault_ref = if should_write_state_key {
        let state_key = generate_browser_state_key()?;
        validate_browserd_state_encryption_key(state_key.as_str(), "generated browser state key")?;
        let scope = parse_vault_scope("global")?;
        let key = "browser_state_key";
        open_cli_vault()
            .context("failed to initialize vault runtime")?
            .put_secret(&scope, key, state_key.as_bytes())
            .context("failed to store generated browser state key")?;
        unset_value_at_path(&mut document, "tool_call.browser_service.state_key_secret_ref")?;
        let vault_ref = format!("{scope}/{key}");
        set_value_at_path(
            &mut document,
            "tool_call.browser_service.state_key_vault_ref",
            toml::Value::String(vault_ref.clone()),
        )?;
        vault_ref
    } else {
        existing_state_key_ref.unwrap_or_else(|| "global/browser_state_key".to_owned())
    };

    let allowed_tools_added = ensure_browser_gateway_tools_allowed(&mut document)?;
    validate_daemon_compatible_document(&document).with_context(|| {
        format!("mutated config {} does not match daemon schema", path_ref.display())
    })?;
    write_document_with_backups(path_ref, &document, 1)
        .with_context(|| format!("failed to persist config {}", path_ref.display()))?;

    Ok(BrowserSetupPayload {
        config_path,
        browser_service_enabled: true,
        auth_token_configured: existing_auth_token.is_some()
            || existing_auth_token_secret_ref
            || should_write_auth_token,
        auth_token_generated,
        state_key_vault_ref,
        state_key_generated: should_write_state_key,
        allowed_tools_added,
        gateway_reload_required: true,
        gateway_next_step: browser_setup_gateway_next_step(),
        gateway_restart_command: browser_setup_gateway_restart_command(),
        gateway_verify_command: browser_setup_gateway_verify_command(),
        migrated: migration.migrated,
    })
}

fn should_write_browser_setup_auth_token(
    force: bool,
    explicit_token: bool,
    existing_auth_token: Option<&str>,
    existing_auth_token_secret_ref: bool,
) -> bool {
    force || explicit_token || (existing_auth_token.is_none() && !existing_auth_token_secret_ref)
}

pub(crate) fn configure_local_browser_prerequisites(path: Option<String>) -> Result<()> {
    configure_browser_setup(path, None, false).map(|_| ())
}

fn ensure_browser_gateway_tools_allowed(document: &mut toml::Value) -> Result<Vec<String>> {
    let mut allowed_tools = document_string_array(Some(document), "tool_call.allowed_tools");
    let mut normalized =
        allowed_tools.iter().map(|tool| tool.trim().to_ascii_lowercase()).collect::<BTreeSet<_>>();
    let mut added = Vec::new();
    for tool in BROWSER_GATEWAY_TOOL_NAMES {
        if normalized.insert((*tool).to_owned()) {
            allowed_tools.push((*tool).to_owned());
            added.push((*tool).to_owned());
        }
    }
    set_value_at_path(
        document,
        "tool_call.allowed_tools",
        toml::Value::Array(allowed_tools.into_iter().map(toml::Value::String).collect()),
    )?;
    Ok(added)
}

fn set_browserd_auth_token(command: &mut Command, auth_token: &str) {
    command.env(BROWSERD_AUTH_TOKEN_ENV, auth_token);
}

fn generate_browser_auth_token() -> String {
    format!("palyra_browser_{}_{}", Ulid::new(), Ulid::new())
}

fn generate_browser_state_key() -> Result<String> {
    let rng = SystemRandom::new();
    let mut key = [0_u8; BROWSERD_STATE_ENCRYPTION_KEY_LEN];
    rng.fill(&mut key).map_err(|_| anyhow::anyhow!("failed to generate browser state key"))?;
    Ok(BASE64_STANDARD.encode(key))
}

async fn run_browser_start(
    bin_path: Option<String>,
    endpoint: Option<String>,
    health_url: Option<String>,
    token: Option<String>,
    wait_ms: u64,
    setup: bool,
    json: bool,
) -> Result<()> {
    let setup_payload =
        if setup { Some(configure_browser_setup(None, token.as_deref(), false)?) } else { None };
    let setup_warning = setup_payload
        .as_ref()
        .map(|payload| browser_setup_gateway_reload_warning(payload.config_path.as_str()));
    let endpoint_overridden = endpoint.as_deref().and_then(normalize_optional_text).is_some();
    let health_url_overridden = health_url.as_deref().and_then(normalize_optional_text).is_some();
    let mut resolved = resolve_browser_config(endpoint, health_url, token)?;
    ensure_browser_start_preflight(&resolved)?;
    let browserd_state_encryption_key = resolve_browserd_state_encryption_key_for_start(&resolved)?;
    let state_encryption_key_configured = browserd_state_encryption_key.is_some()
        || env_optional(BROWSERD_STATE_ENCRYPTION_KEY_ENV).is_some();
    let mut lifecycle_warnings = browser_profile_prerequisite_warnings(
        &resolved.policy,
        None,
        false,
        resolved.config_path.as_deref(),
    );
    if let Some(warning) = setup_warning {
        lifecycle_warnings.insert(0, warning);
    }
    lifecycle_warnings.extend(browser_start_auth_token_warnings(&resolved));
    if fetch_browser_health(resolved.connection.health_base_url.as_str()).await.is_ok() {
        if let Err(error) = probe_browser_grpc(&resolved.connection).await {
            anyhow::bail!(
                "browser health endpoint is reachable at {}, but authenticated gRPC readiness failed at {}: {}. This usually means another browserd is running with a different token; stop that process or restart the desktop supervisor, then rerun `palyra browser start --setup`.",
                resolved.connection.health_base_url,
                resolved.connection.grpc_url,
                error
            );
        }
        let metadata = read_browser_service_metadata()?;
        let payload = BrowserLifecyclePayload {
            action: "start".to_owned(),
            running: true,
            pid: metadata.as_ref().map(|value| value.pid),
            grpc_url: resolved.connection.grpc_url,
            health_base_url: resolved.connection.health_base_url,
            stdout_log_path: metadata.as_ref().map(|value| value.stdout_log_path.clone()),
            stderr_log_path: metadata.as_ref().map(|value| value.stderr_log_path.clone()),
            detail: "browser service is already healthy".to_owned(),
            warnings: lifecycle_warnings,
        };
        let value =
            serde_json::to_value(&payload).context("failed to encode browser lifecycle payload")?;
        return emit_browser_value_with_json(
            &value,
            format_browser_lifecycle_text(&payload),
            "failed to encode browser lifecycle output",
            json,
        );
    }

    let binary = resolve_browser_bin_path(bin_path)?;
    let port_diagnostics = browser_connection_port_diagnostics(&resolved.connection);
    let unavailable =
        port_diagnostics.iter().filter(|diagnostic| !diagnostic.bind_available).collect::<Vec<_>>();
    if !unavailable.is_empty() {
        if endpoint_overridden || health_url_overridden {
            anyhow::bail!(
                "browser service cannot bind the requested endpoint(s): {}. Choose free `--endpoint`/`--health-url` values or remove the overrides and rerun `palyra browser start --setup` so Palyra can select free loopback ports.",
                format_browser_port_diagnostic_summary(unavailable.as_slice())
            );
        }
        let fallback = select_browser_start_fallback_connection(&resolved).with_context(|| {
            format!(
                "configured browser port(s) are unavailable: {}",
                format_browser_port_diagnostic_summary(unavailable.as_slice())
            )
        })?;
        let config_updated =
            persist_browser_service_connection_urls(resolved.config_path.as_deref(), &fallback)?;
        lifecycle_warnings.push(browser_port_fallback_warning(
            &resolved.connection,
            &fallback,
            config_updated,
        ));
        resolved.connection = fallback;
    }
    let (health_host, health_port) =
        parse_http_bind_parts(resolved.connection.health_base_url.as_str(), "browser health URL")?;
    let (grpc_host, grpc_port) =
        parse_http_bind_parts(resolved.connection.grpc_url.as_str(), "browser gRPC URL")?;
    let state_dir = browser_cli_state_dir(true)?;
    let stdout_log_path = state_dir.join(BROWSER_SERVICE_STDOUT_LOG_FILE_NAME);
    let stderr_log_path = state_dir.join(BROWSER_SERVICE_STDERR_LOG_FILE_NAME);
    let stdout = File::create(stdout_log_path.as_path())
        .with_context(|| format!("failed to create {}", stdout_log_path.display()))?;
    let stderr = File::create(stderr_log_path.as_path())
        .with_context(|| format!("failed to create {}", stderr_log_path.display()))?;

    let mut command = Command::new(binary.as_path());
    command
        .arg("--bind")
        .arg(&health_host)
        .arg("--port")
        .arg(health_port.to_string())
        .arg("--grpc-bind")
        .arg(&grpc_host)
        .arg("--grpc-port")
        .arg(grpc_port.to_string())
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    if let Some(auth_token) = resolved.connection.auth_token.as_ref() {
        set_browserd_auth_token(&mut command, auth_token);
    }
    if let Some(state_key) = browserd_state_encryption_key.as_ref() {
        command.env(BROWSERD_STATE_ENCRYPTION_KEY_ENV, state_key);
    }
    #[cfg(windows)]
    command.creation_flags(DETACHED_PROCESS | CREATE_NO_WINDOW);

    let child = command
        .spawn()
        .with_context(|| format!("failed to start browser service binary {}", binary.display()))?;

    let metadata = BrowserServiceMetadata {
        schema_version: BROWSER_SERVICE_METADATA_SCHEMA_VERSION,
        pid: child.id(),
        binary: binary.display().to_string(),
        grpc_url: resolved.connection.grpc_url.clone(),
        health_base_url: resolved.connection.health_base_url.clone(),
        stdout_log_path: stdout_log_path.display().to_string(),
        stderr_log_path: stderr_log_path.display().to_string(),
        started_at_unix_ms: now_unix_ms(),
        auth_token_configured: resolved.connection.auth_token.is_some(),
        state_encryption_key_configured,
    };
    write_browser_service_metadata(&metadata)?;

    let deadline = Duration::from_millis(wait_ms.max(BROWSER_SERVICE_START_POLL_MS));
    let started = SystemTime::now();
    let mut last_health_error: Option<String> = None;
    let mut last_grpc_error: Option<String> = None;
    loop {
        match fetch_browser_health(resolved.connection.health_base_url.as_str()).await {
            Ok(_) => match probe_browser_grpc(&resolved.connection).await {
                Ok(()) => {
                    let payload = BrowserLifecyclePayload {
                        action: "start".to_owned(),
                        running: true,
                        pid: Some(metadata.pid),
                        grpc_url: resolved.connection.grpc_url,
                        health_base_url: resolved.connection.health_base_url,
                        stdout_log_path: Some(metadata.stdout_log_path),
                        stderr_log_path: Some(metadata.stderr_log_path),
                        detail: "browser service started and passed authenticated readiness checks"
                            .to_owned(),
                        warnings: lifecycle_warnings,
                    };
                    let value = serde_json::to_value(&payload)
                        .context("failed to encode browser lifecycle payload")?;
                    return emit_browser_value_with_json(
                        &value,
                        format_browser_lifecycle_text(&payload),
                        "failed to encode browser lifecycle output",
                        json,
                    );
                }
                Err(error) => {
                    last_grpc_error = Some(error.to_string());
                }
            },
            Err(error) => {
                last_health_error = Some(error.to_string());
            }
        }
        if started.elapsed().unwrap_or_default() >= deadline {
            let readiness_detail = browser_start_readiness_timeout_detail(
                last_health_error.as_deref(),
                last_grpc_error.as_deref(),
            );
            anyhow::bail!(
                "browser service did not become ready within {} ms ({readiness_detail}); inspect {} and {}",
                wait_ms.max(BROWSER_SERVICE_START_POLL_MS),
                stdout_log_path.display(),
                stderr_log_path.display()
            );
        }
        sleep(Duration::from_millis(BROWSER_SERVICE_START_POLL_MS)).await;
    }
}

fn browser_start_readiness_timeout_detail(
    last_health_error: Option<&str>,
    last_grpc_error: Option<&str>,
) -> String {
    match (last_health_error, last_grpc_error) {
        (_, Some(grpc_error)) => {
            format!("authenticated gRPC readiness failed: {grpc_error}")
        }
        (Some(health_error), None) => format!("health check failed: {health_error}"),
        (None, None) => "no readiness response was observed".to_owned(),
    }
}

async fn run_browser_stop(json: bool) -> Result<()> {
    let Some(metadata) = read_browser_service_metadata()? else {
        let payload = BrowserLifecyclePayload {
            action: "stop".to_owned(),
            running: false,
            pid: None,
            grpc_url: DEFAULT_BROWSER_GRPC_URL.to_owned(),
            health_base_url: DEFAULT_BROWSER_HEALTH_BASE_URL.to_owned(),
            stdout_log_path: None,
            stderr_log_path: None,
            detail: "no CLI-managed browser service metadata found".to_owned(),
            warnings: Vec::new(),
        };
        let value =
            serde_json::to_value(&payload).context("failed to encode browser lifecycle payload")?;
        return emit_browser_value_with_json(
            &value,
            format_browser_lifecycle_text(&payload),
            "failed to encode browser lifecycle output",
            json,
        );
    };

    if process_is_running(metadata.pid) {
        terminate_process(metadata.pid)
            .with_context(|| format!("failed to stop browser service process {}", metadata.pid))?;
    }
    wait_for_browser_service_stop(
        &metadata,
        Duration::from_millis(BROWSER_SERVICE_STOP_TIMEOUT_MS),
    )
    .await?;
    remove_browser_service_metadata()?;

    let payload = BrowserLifecyclePayload {
        action: "stop".to_owned(),
        running: false,
        pid: Some(metadata.pid),
        grpc_url: metadata.grpc_url,
        health_base_url: metadata.health_base_url,
        stdout_log_path: Some(metadata.stdout_log_path),
        stderr_log_path: Some(metadata.stderr_log_path),
        detail: "browser service stopped and lifecycle metadata removed".to_owned(),
        warnings: Vec::new(),
    };
    let value =
        serde_json::to_value(&payload).context("failed to encode browser lifecycle payload")?;
    emit_browser_value_with_json(
        &value,
        format_browser_lifecycle_text(&payload),
        "failed to encode browser lifecycle output",
        json,
    )
}

async fn wait_for_browser_service_stop(
    metadata: &BrowserServiceMetadata,
    timeout: Duration,
) -> Result<()> {
    let started = SystemTime::now();
    loop {
        let process_running = process_is_running(metadata.pid);
        let health_reachable =
            fetch_browser_health(metadata.health_base_url.as_str()).await.is_ok();
        let ports_released = browser_service_metadata_ports_released(metadata);
        if browser_service_stop_complete(process_running, health_reachable, ports_released) {
            return Ok(());
        }

        if started.elapsed().unwrap_or_default() >= timeout {
            let reasons = browser_service_stop_pending_reasons(
                metadata.pid,
                process_running,
                health_reachable,
                ports_released,
                metadata.health_base_url.as_str(),
                metadata.grpc_url.as_str(),
            );
            anyhow::bail!(
                "browser service did not stop within {} ms; {}; lifecycle metadata was preserved",
                timeout.as_millis(),
                reasons.join("; ")
            );
        }

        sleep(Duration::from_millis(BROWSER_SERVICE_START_POLL_MS)).await;
    }
}

fn browser_service_stop_complete(
    process_running: bool,
    health_reachable: bool,
    ports_released: bool,
) -> bool {
    !process_running && !health_reachable && ports_released
}

fn browser_service_stop_pending_reasons(
    pid: u32,
    process_running: bool,
    health_reachable: bool,
    ports_released: bool,
    health_base_url: &str,
    grpc_url: &str,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if process_running {
        reasons.push(format!("pid {pid} is still running"));
    }
    if health_reachable {
        reasons.push(format!(
            "health endpoint {}/healthz is still reachable",
            health_base_url.trim_end_matches('/')
        ));
    }
    if !ports_released {
        reasons.push(format!(
            "configured browser ports are still occupied for health endpoint {} and gRPC endpoint {}; another process or stale socket is holding the listener",
            health_base_url.trim_end_matches('/'),
            grpc_url.trim_end_matches('/')
        ));
    }
    if reasons.is_empty() {
        reasons.push("stop state could not be confirmed".to_owned());
    }
    reasons
}

fn browser_service_metadata_ports_released(metadata: &BrowserServiceMetadata) -> bool {
    let connection = BrowserServiceConnection {
        grpc_url: metadata.grpc_url.clone(),
        health_base_url: metadata.health_base_url.clone(),
        auth_token: None,
    };
    browser_connection_port_diagnostics(&connection)
        .iter()
        .all(|diagnostic| diagnostic.bind_available)
}

async fn run_browser_open(args: BrowserOpenArgs) -> Result<()> {
    let BrowserOpenArgs {
        url,
        principal,
        channel,
        allow_private_targets,
        allow_downloads,
        profile_id,
        private_profile,
        timeout_ms,
        json,
    } = args;
    let context = client::control_plane::connect_admin_console_with_request_timeout(
        app::ConnectionOverrides::default(),
        browser_control_plane_request_timeout(timeout_ms),
    )
    .await?;
    let create = context
        .client
        .create_browser_session(&control_plane::BrowserSessionCreateRequest {
            principal,
            idle_ttl_ms: None,
            budget: None,
            allow_private_targets: bool_option(allow_private_targets),
            allow_downloads: bool_option(allow_downloads),
            action_allowed_domains: Vec::new(),
            persistence_enabled: None,
            persistence_id: None,
            channel,
            profile_id,
            private_profile: bool_option(private_profile),
        })
        .await
        .context("failed to create browser session")?;
    let session_id =
        create.session_id.clone().context("browser session creation returned no session id")?;
    let navigate = context
        .client
        .navigate_browser_session(
            session_id.as_str(),
            &control_plane::BrowserNavigateRequest {
                url,
                timeout_ms,
                allow_redirects: None,
                max_redirects: None,
                allow_private_targets: bool_option(allow_private_targets),
            },
        )
        .await
        .context("failed to navigate browser session")?;
    let navigate_success = navigate.success;
    let navigate_error = navigate.error.clone();
    let cleanup = if navigate_success {
        None
    } else {
        Some(browser_open_cleanup_session(&context.client, session_id.as_str()).await)
    };
    let payload = browser_open_output_value(session_id.as_str(), &create, &navigate, cleanup);
    emit_browser_value_with_json(
        &payload,
        format!(
            "browser.open session_id={} success={} final_url={} status_code={} cleanup={}",
            browser_session_handle_text(Some(session_id.as_str())),
            payload.pointer("/navigate/success").and_then(Value::as_bool).unwrap_or(false),
            payload.pointer("/navigate/final_url").and_then(Value::as_str).unwrap_or("-"),
            payload.pointer("/navigate/status_code").and_then(Value::as_u64).unwrap_or(0),
            browser_open_cleanup_status_text(payload.get("cleanup"))
        ),
        "failed to encode browser open output",
        json,
    )?;
    ensure_browser_command_success("browser.open", navigate_success, navigate_error.as_str())
}

async fn browser_open_cleanup_session(
    client: &control_plane::ControlPlaneClient,
    session_id: &str,
) -> Value {
    match client.close_browser_session(session_id).await {
        Ok(envelope) => json!({
            "attempted": true,
            "closed": envelope.closed,
            "reason": envelope.reason,
        }),
        Err(error) => json!({
            "attempted": true,
            "closed": false,
            "error": error.to_string(),
        }),
    }
}

fn browser_open_output_value(
    session_id: &str,
    session: &control_plane::BrowserSessionCreateEnvelope,
    navigate: &control_plane::BrowserNavigateEnvelope,
    cleanup: Option<Value>,
) -> Value {
    let mut value = json!({
        "session_id": session_id,
        "session": session,
        "navigate": navigate,
    });
    if let Some(cleanup) = cleanup {
        value["cleanup"] = cleanup;
    }
    value
}

fn browser_open_cleanup_status_text(cleanup: Option<&Value>) -> &'static str {
    let Some(cleanup) = cleanup else {
        return "not_needed";
    };
    if cleanup.get("closed").and_then(Value::as_bool).unwrap_or(false) {
        "closed"
    } else {
        "failed"
    }
}

async fn run_browser_session_command(command: BrowserSessionCommand) -> Result<()> {
    match command {
        BrowserSessionCommand::Create {
            principal,
            channel,
            idle_ttl_ms,
            allow_private_targets,
            allow_downloads,
            action_allowed_domains,
            persistence_enabled,
            persistence_id,
            profile_id,
            private_profile,
            json,
        } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let envelope = context
                .client
                .create_browser_session(&control_plane::BrowserSessionCreateRequest {
                    principal,
                    idle_ttl_ms,
                    budget: None,
                    allow_private_targets: bool_option(allow_private_targets),
                    allow_downloads: bool_option(allow_downloads),
                    action_allowed_domains,
                    persistence_enabled: bool_option(persistence_enabled),
                    persistence_id,
                    channel,
                    profile_id,
                    private_profile: bool_option(private_profile),
                })
                .await
                .context("failed to create browser session")?;
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser session create output")?;
            emit_browser_value_with_json(
                &value,
                format!(
                    "browser.session.create session_id={} principal={} downloads_enabled={} persistence_enabled={} profile_id={}",
                    browser_session_handle_text(envelope.session_id.as_deref()),
                    envelope.principal,
                    envelope.downloads_enabled,
                    envelope.persistence_enabled,
                    redacted_browser_identifier_text(envelope.profile_id.as_deref(), "profile")
                ),
                "failed to encode browser session create output",
                json,
            )
        }
        BrowserSessionCommand::List { limit, json } => {
            let resolved = resolve_browser_config(None, None, None)?;
            let mut client = connect_browser_service(&resolved.connection).await?;
            let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
            let response = client
                .list_sessions(browser_request(
                    browser_v1::ListSessionsRequest {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        principal: caller_principal.clone(),
                        limit: limit.unwrap_or_default(),
                    },
                    resolved.connection.auth_token.as_deref(),
                    caller_principal.as_str(),
                )?)
                .await
                .context("failed to list browser sessions")?
                .into_inner();
            let sessions = response.sessions.iter().map(session_summary_value).collect::<Vec<_>>();
            let value = json!({
                "sessions": sessions,
                "truncated": response.truncated,
                "error": response.error,
            });
            let mut text = format!(
                "browser.session.list count={} truncated={}",
                response.sessions.len(),
                response.truncated
            );
            for session in &response.sessions {
                text.push('\n');
                text.push_str(format_browser_session_summary_text(session).as_str());
            }
            emit_browser_value_with_json(
                &value,
                text,
                "failed to encode browser session list output",
                json,
            )
        }
        BrowserSessionCommand::Show { session_id } => {
            let detail = get_browser_session_detail(session_id.as_str()).await?;
            let value = session_detail_value(&detail);
            let text = format!(
                "browser.session.show session_id={} tabs={} private_targets={} downloads={} profile_id={}",
                browser_session_handle_text(value.pointer("/summary/session_id").and_then(Value::as_str)),
                value.pointer("/summary/tab_count").and_then(Value::as_u64).unwrap_or(0),
                value.pointer("/summary/allow_private_targets").and_then(Value::as_bool).unwrap_or(false),
                value.pointer("/summary/downloads_enabled").and_then(Value::as_bool).unwrap_or(false),
                redacted_browser_identifier_text(
                    value.pointer("/summary/profile_id").and_then(Value::as_str),
                    "profile"
                ),
            );
            emit_browser_value(&value, text, "failed to encode browser session show output")
        }
        BrowserSessionCommand::Inspect {
            session_id,
            include_cookies,
            include_storage,
            include_action_log,
            include_network_log,
            include_page_snapshot,
            max_cookie_bytes,
            max_storage_bytes,
            max_action_log_entries,
            max_network_log_entries,
            max_network_log_bytes,
            max_dom_snapshot_bytes,
            max_visible_text_bytes,
            output,
        } => {
            let mut value = inspect_browser_session(
                session_id.as_str(),
                browser_v1::InspectSessionRequest {
                    v: CANONICAL_PROTOCOL_MAJOR,
                    session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                    include_cookies,
                    include_storage,
                    include_action_log,
                    include_network_log,
                    include_page_snapshot,
                    max_cookie_bytes: max_cookie_bytes.unwrap_or_default(),
                    max_storage_bytes: max_storage_bytes.unwrap_or_default(),
                    max_action_log_entries: max_action_log_entries.unwrap_or_default(),
                    max_network_log_entries: max_network_log_entries.unwrap_or_default(),
                    max_network_log_bytes: max_network_log_bytes.unwrap_or_default(),
                    max_dom_snapshot_bytes: max_dom_snapshot_bytes.unwrap_or_default(),
                    max_visible_text_bytes: max_visible_text_bytes.unwrap_or_default(),
                    include_console_log: false,
                    include_page_diagnostics: false,
                    max_console_log_entries: 0,
                    max_console_log_bytes: 0,
                },
            )
            .await?;
            let written = write_optional_json_output(
                output.as_deref(),
                session_id.as_str(),
                "inspect",
                &value,
            )?;
            maybe_attach_output_path(&mut value, written.as_ref());
            emit_browser_value(
                &value,
                format!(
                    "browser.session.inspect session_id={} cookies={} storage={} action_log={} network_log={} output={}",
                    browser_session_handle_text(Some(session_id.as_str())),
                    value.get("cookies").and_then(Value::as_array).map_or(0, Vec::len),
                    value.get("storage").and_then(Value::as_array).map_or(0, Vec::len),
                    value.get("action_log").and_then(Value::as_array).map_or(0, Vec::len),
                    value.get("network_log").and_then(Value::as_array).map_or(0, Vec::len),
                    written.as_deref().unwrap_or("-"),
                ),
                "failed to encode browser session inspect output",
            )
        }
        BrowserSessionCommand::Close { session_id, json } => {
            let context =
                client::control_plane::connect_admin_console(app::ConnectionOverrides::default())
                    .await?;
            let envelope = context
                .client
                .close_browser_session(session_id.as_str())
                .await
                .context("failed to close browser session")?;
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser session close output")?;
            emit_browser_value_with_json(
                &value,
                format!(
                    "browser.session.close session_id={} closed={} reason={}",
                    browser_session_handle_text(Some(session_id.as_str())),
                    envelope.closed,
                    empty_as_dash(envelope.reason.as_str()),
                ),
                "failed to encode browser session close output",
                json,
            )
        }
    }
}

async fn run_browser_profiles_command(command: BrowserProfilesCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        BrowserProfilesCommand::List { principal, json } => {
            let envelope = context
                .client
                .list_browser_profiles(&control_plane::BrowserProfilesQuery { principal })
                .await
                .context("failed to list browser profiles")?;
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser profiles list output")?;
            let mut text = format!(
                "browser.profiles.list principal={} count={} active_profile_id={}",
                envelope.principal,
                envelope.profiles.len(),
                browser_session_handle_text(envelope.active_profile_id.as_deref()),
            );
            for profile in &envelope.profiles {
                text.push('\n');
                text.push_str(
                    format!(
                        "profile id={} name={} private={} persistence={} active={}",
                        browser_session_handle_text(profile.profile_id.as_deref()),
                        profile.name,
                        profile.private_profile,
                        profile.persistence_enabled,
                        profile.active,
                    )
                    .as_str(),
                );
            }
            emit_browser_value_with_json(
                &value,
                text,
                "failed to encode browser profiles list output",
                json,
            )
        }
        BrowserProfilesCommand::Create {
            principal,
            name,
            theme_color,
            persistence_enabled,
            private_profile,
            json,
        } => {
            let envelope = context
                .client
                .create_browser_profile(&control_plane::BrowserCreateProfileRequest {
                    principal,
                    name,
                    theme_color,
                    persistence_enabled: bool_option(persistence_enabled),
                    private_profile: bool_option(private_profile),
                })
                .await
                .context("failed to create browser profile")?;
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser profile create output")?;
            emit_browser_value_with_json(
                &value,
                format!(
                    "browser.profiles.create profile_id={} name={} private={} active={}",
                    browser_session_handle_text(envelope.profile.profile_id.as_deref()),
                    envelope.profile.name,
                    envelope.profile.private_profile,
                    envelope.profile.active,
                ),
                "failed to encode browser profile create output",
                json,
            )
        }
        BrowserProfilesCommand::Rename { profile_id, principal, name } => {
            let envelope = context
                .client
                .rename_browser_profile(
                    profile_id.as_str(),
                    &control_plane::BrowserRenameProfileRequest { principal, name },
                )
                .await
                .context("failed to rename browser profile")?;
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser profile rename output")?;
            emit_browser_value(
                &value,
                format!(
                    "browser.profiles.rename profile_id={} name={}",
                    browser_session_handle_text(envelope.profile.profile_id.as_deref()),
                    envelope.profile.name,
                ),
                "failed to encode browser profile rename output",
            )
        }
        BrowserProfilesCommand::Delete { profile_id, principal } => {
            let envelope = context
                .client
                .delete_browser_profile(
                    profile_id.as_str(),
                    &control_plane::BrowserProfileScopeRequest { principal },
                )
                .await
                .context("failed to delete browser profile")?;
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser profile delete output")?;
            emit_browser_value(
                &value,
                format!(
                    "browser.profiles.delete profile_id={} deleted={} active_profile_id={}",
                    browser_session_handle_text(Some(envelope.profile_id.as_str())),
                    envelope.deleted,
                    browser_session_handle_text(envelope.active_profile_id.as_deref()),
                ),
                "failed to encode browser profile delete output",
            )
        }
        BrowserProfilesCommand::Activate { profile_id, principal } => {
            let envelope = context
                .client
                .activate_browser_profile(
                    profile_id.as_str(),
                    &control_plane::BrowserProfileScopeRequest { principal },
                )
                .await
                .context("failed to activate browser profile")?;
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser profile activate output")?;
            emit_browser_value(
                &value,
                format!(
                    "browser.profiles.activate profile_id={} name={} active={}",
                    browser_session_handle_text(envelope.profile.profile_id.as_deref()),
                    envelope.profile.name,
                    envelope.profile.active,
                ),
                "failed to encode browser profile activate output",
            )
        }
    }
}

async fn run_browser_tabs_command(session_id: String, command: BrowserTabsCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        BrowserTabsCommand::List => {
            let envelope = context
                .client
                .list_browser_tabs(session_id.as_str())
                .await
                .context("failed to list browser tabs")?;
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser tabs list output")?;
            let mut text = format!(
                "browser.tabs.list session_id={} count={} active_tab_id={}",
                browser_session_handle_text(Some(session_id.as_str())),
                envelope.tabs.len(),
                redacted_browser_identifier_text(envelope.active_tab_id.as_deref(), "tab"),
            );
            for tab in &envelope.tabs {
                text.push('\n');
                text.push_str(
                    format!(
                        "tab id={} active={} title={} url={}",
                        redacted_browser_identifier_text(tab.tab_id.as_deref(), "tab"),
                        tab.active,
                        empty_as_dash(tab.title.as_str()),
                        empty_as_dash(tab.url.as_str()),
                    )
                    .as_str(),
                );
            }
            emit_browser_value(&value, text, "failed to encode browser tabs list output")
        }
        BrowserTabsCommand::Open {
            url,
            activate,
            timeout_ms,
            allow_redirects,
            max_redirects,
            allow_private_targets,
        } => {
            let envelope = context
                .client
                .open_browser_tab(
                    session_id.as_str(),
                    &control_plane::BrowserOpenTabRequest {
                        url,
                        activate: bool_option(activate),
                        timeout_ms,
                        allow_redirects: bool_option(allow_redirects),
                        max_redirects,
                        allow_private_targets: bool_option(allow_private_targets),
                    },
                )
                .await
                .context("failed to open browser tab")?;
            let success = envelope.success;
            let error = envelope.error.clone();
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser tab open output")?;
            let mode = browser_output_mode();
            if browser_command_payload_should_emit(mode, success) {
                emit_browser_value_for_mode(
                    &value,
                    format!(
                        "browser.tabs.open session_id={} tab_id={} success={} status_code={} navigated={}",
                        browser_session_handle_text(Some(session_id.as_str())),
                        envelope
                            .tab
                            .as_ref()
                            .and_then(|tab| tab.tab_id.as_deref())
                            .map(|value| redacted_browser_identifier_text(Some(value), "tab"))
                            .unwrap_or_else(|| "-".to_owned()),
                        envelope.success,
                        envelope.status_code,
                        envelope.navigated,
                    ),
                    "failed to encode browser tab open output",
                    mode,
                )?;
            }
            ensure_browser_command_success("browser.tabs.open", success, error.as_str())
        }
        BrowserTabsCommand::Switch { tab_id } => {
            let envelope = context
                .client
                .switch_browser_tab(
                    session_id.as_str(),
                    &control_plane::BrowserTabMutationRequest { tab_id },
                )
                .await
                .context("failed to switch browser tab")?;
            let success = envelope.success;
            let error = envelope.error.clone();
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser tab switch output")?;
            let mode = browser_output_mode();
            if browser_command_payload_should_emit(mode, success) {
                emit_browser_value_for_mode(
                    &value,
                    format!(
                        "browser.tabs.switch session_id={} active_tab_id={} success={}",
                        browser_session_handle_text(Some(session_id.as_str())),
                        envelope
                            .active_tab
                            .as_ref()
                            .and_then(|tab| tab.tab_id.as_deref())
                            .map(|value| redacted_browser_identifier_text(Some(value), "tab"))
                            .unwrap_or_else(|| "-".to_owned()),
                        envelope.success,
                    ),
                    "failed to encode browser tab switch output",
                    mode,
                )?;
            }
            ensure_browser_command_success("browser.tabs.switch", success, error.as_str())
        }
        BrowserTabsCommand::Close { tab_id } => {
            let envelope = context
                .client
                .close_browser_tab(
                    session_id.as_str(),
                    &control_plane::BrowserTabCloseRequest { tab_id: Some(tab_id) },
                )
                .await
                .context("failed to close browser tab")?;
            let success = envelope.success;
            let error = envelope.error.clone();
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser tab close output")?;
            let mode = browser_output_mode();
            if browser_command_payload_should_emit(mode, success) {
                emit_browser_value_for_mode(
                    &value,
                    format!(
                        "browser.tabs.close session_id={} closed_tab_id={} tabs_remaining={} active_tab_id={}",
                        browser_session_handle_text(Some(session_id.as_str())),
                        redacted_browser_identifier_text(envelope.closed_tab_id.as_deref(), "tab"),
                        envelope.tabs_remaining,
                        envelope
                            .active_tab
                            .as_ref()
                            .and_then(|tab| tab.tab_id.as_deref())
                            .map(|value| redacted_browser_identifier_text(Some(value), "tab"))
                            .unwrap_or_else(|| "-".to_owned()),
                    ),
                    "failed to encode browser tab close output",
                    mode,
                )?;
            }
            ensure_browser_command_success("browser.tabs.close", success, error.as_str())
        }
    }
}

async fn run_browser_navigate(
    session_id: String,
    url: String,
    timeout_ms: Option<u64>,
    allow_redirects: bool,
    max_redirects: Option<u32>,
    allow_private_targets: bool,
) -> Result<()> {
    let context = client::control_plane::connect_admin_console_with_request_timeout(
        app::ConnectionOverrides::default(),
        browser_control_plane_request_timeout(timeout_ms),
    )
    .await?;
    let envelope = context
        .client
        .navigate_browser_session(
            session_id.as_str(),
            &control_plane::BrowserNavigateRequest {
                url,
                timeout_ms,
                allow_redirects: bool_option(allow_redirects),
                max_redirects,
                allow_private_targets: bool_option(allow_private_targets),
            },
        )
        .await
        .context("failed to navigate browser session")?;
    let success = envelope.success;
    let error = envelope.error.clone();
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser navigate output")?;
    normalize_session_scoped_output(&mut value, session_id.as_str());
    let mode = browser_output_mode();
    if browser_command_payload_should_emit(mode, success) {
        emit_browser_value_for_mode(
            &value,
            format!(
                "browser.navigate session_id={} success={} status_code={} final_url={} title={}",
                browser_session_handle_text(Some(session_id.as_str())),
                envelope.success,
                envelope.status_code,
                empty_as_dash(envelope.final_url.as_str()),
                empty_as_dash(envelope.title.as_str()),
            ),
            "failed to encode browser navigate output",
            mode,
        )?;
    }
    ensure_browser_command_success("browser.navigate", success, error.as_str())
}

fn browser_control_plane_request_timeout(action_timeout_ms: Option<u64>) -> Option<Duration> {
    action_timeout_ms.map(|timeout_ms| {
        Duration::from_millis(
            timeout_ms
                .saturating_add(BROWSER_CONTROL_PLANE_TIMEOUT_BUFFER_MS)
                .max(BROWSER_CONTROL_PLANE_MIN_TIMEOUT_MS),
        )
    })
}

async fn run_browser_click(args: BrowserClickArgs) -> Result<()> {
    let BrowserClickArgs {
        session_id,
        selector,
        max_retries,
        timeout_ms,
        capture_failure_screenshot,
        max_failure_screenshot_bytes,
        output,
        json,
    } = args;
    let context = client::control_plane::connect_admin_console_with_request_timeout(
        app::ConnectionOverrides::default(),
        browser_control_plane_request_timeout(timeout_ms),
    )
    .await?;
    let envelope = context
        .client
        .click_browser_session(
            session_id.as_str(),
            &control_plane::BrowserClickRequest {
                selector: selector.clone(),
                max_retries,
                timeout_ms,
                capture_failure_screenshot: bool_option(capture_failure_screenshot),
                max_failure_screenshot_bytes,
            },
        )
        .await
        .context("failed to click browser session")?;
    let success = envelope.success;
    let error = envelope.error.clone();
    let screenshot_path = write_optional_failure_screenshot(
        output.as_deref(),
        session_id.as_str(),
        "click",
        envelope.decode_failure_screenshot().as_deref(),
    )?;
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser click output")?;
    strip_large_binary_fields(
        &mut value,
        screenshot_path.is_some(),
        &["failure_screenshot_base64"],
    );
    maybe_attach_output_path(&mut value, screenshot_path.as_ref());
    normalize_session_scoped_output(&mut value, session_id.as_str());
    emit_browser_value_with_json(
        &value,
        format!(
            "browser.click session_id={} success={} selector={} action_id={} artifact={}",
            browser_session_handle_text(Some(session_id.as_str())),
            envelope.success,
            selector,
            envelope.action_log.as_ref().map(|entry| entry.action_id.as_str()).unwrap_or("-"),
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser click output",
        json,
    )?;
    ensure_browser_command_success("browser.click", success, error.as_str())
}

async fn run_browser_type(args: BrowserTypeArgs) -> Result<()> {
    let BrowserTypeArgs {
        session_id,
        selector,
        text,
        clear_existing,
        timeout_ms,
        capture_failure_screenshot,
        max_failure_screenshot_bytes,
        output,
        json,
    } = args;
    let context = client::control_plane::connect_admin_console_with_request_timeout(
        app::ConnectionOverrides::default(),
        browser_control_plane_request_timeout(timeout_ms),
    )
    .await?;
    let envelope = context
        .client
        .type_browser_session(
            session_id.as_str(),
            &control_plane::BrowserTypeRequest {
                selector: selector.clone(),
                text,
                clear_existing: bool_option(clear_existing),
                timeout_ms,
                capture_failure_screenshot: bool_option(capture_failure_screenshot),
                max_failure_screenshot_bytes,
            },
        )
        .await
        .context("failed to type into browser session")?;
    let success = envelope.success;
    let error = envelope.error.clone();
    let screenshot_path = write_optional_failure_screenshot(
        output.as_deref(),
        session_id.as_str(),
        if clear_existing { "fill" } else { "type" },
        envelope.decode_failure_screenshot().as_deref(),
    )?;
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser type output")?;
    strip_large_binary_fields(
        &mut value,
        screenshot_path.is_some(),
        &["failure_screenshot_base64"],
    );
    maybe_attach_output_path(&mut value, screenshot_path.as_ref());
    normalize_session_scoped_output(&mut value, session_id.as_str());
    emit_browser_value_with_json(
        &value,
        format!(
            "browser.{} session_id={} success={} selector={} typed_bytes={} artifact={}",
            if clear_existing { "fill" } else { "type" },
            browser_session_handle_text(Some(session_id.as_str())),
            envelope.success,
            selector,
            envelope.typed_bytes,
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser type output",
        json,
    )?;
    ensure_browser_command_success(
        if clear_existing { "browser.fill" } else { "browser.type" },
        success,
        error.as_str(),
    )
}

async fn run_browser_upload(args: BrowserUploadArgs) -> Result<()> {
    let BrowserUploadArgs {
        session_id,
        selector,
        file,
        timeout_ms,
        capture_failure_screenshot,
        max_failure_screenshot_bytes,
        output,
        json,
    } = args;
    let file_path = PathBuf::from(file.as_str());
    let metadata = fs::metadata(file_path.as_path())
        .with_context(|| format!("failed to inspect upload file {}", file_path.display()))?;
    if !metadata.is_file() {
        anyhow::bail!("browser upload file is not a regular file: {}", file_path.display());
    }
    if metadata.len() > BROWSER_UPLOAD_MAX_FILE_BYTES {
        anyhow::bail!(
            "browser upload file exceeds max bytes ({} > {})",
            metadata.len(),
            BROWSER_UPLOAD_MAX_FILE_BYTES
        );
    }
    let file_name = file_path
        .file_name()
        .and_then(OsStr::to_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow::anyhow!("browser upload file path has no file name"))?
        .to_owned();
    let file_bytes = fs::read(file_path.as_path())
        .with_context(|| format!("failed to read upload file {}", file_path.display()))?;

    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .set_file_input(browser_request(
            browser_v1::SetFileInputRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                selector: selector.clone(),
                file_name: file_name.clone(),
                file_bytes,
                timeout_ms: timeout_ms.unwrap_or(0),
                capture_failure_screenshot,
                max_failure_screenshot_bytes: max_failure_screenshot_bytes.unwrap_or(0),
            },
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to upload file through browser session")?
        .into_inner();
    let success = response.success;
    let error = response.error.clone();
    let screenshot_path = write_optional_failure_screenshot(
        output.as_deref(),
        session_id.as_str(),
        "upload",
        (!response.failure_screenshot_bytes.is_empty())
            .then_some(response.failure_screenshot_bytes.as_slice()),
    )?;
    let mut value = json!({
        "session_id": browser_identifier_json_value(Some(session_id.as_str())),
        "success": response.success,
        "selector": selector,
        "file_name": response.uploaded_file_name,
        "uploaded_file_bytes": response.uploaded_file_bytes,
        "error": response.error,
        "action_log": response.action_log.as_ref().map(action_log_entry_value).unwrap_or(Value::Null),
        "failure_screenshot_mime_type": response.failure_screenshot_mime_type,
    });
    maybe_attach_output_path(&mut value, screenshot_path.as_ref());
    emit_browser_value_with_json(
        &value,
        format!(
            "browser.upload session_id={} success={} selector={} file={} bytes={} artifact={}",
            browser_session_handle_text(Some(session_id.as_str())),
            success,
            value.get("selector").and_then(Value::as_str).unwrap_or("-"),
            value.get("file_name").and_then(Value::as_str).unwrap_or("-"),
            value.get("uploaded_file_bytes").and_then(Value::as_u64).unwrap_or(0),
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser upload output",
        json,
    )?;
    ensure_browser_command_success("browser.upload", success, error.as_str())
}

async fn run_browser_scroll(
    session_id: String,
    delta_x: i64,
    delta_y: i64,
    capture_failure_screenshot: bool,
    max_failure_screenshot_bytes: Option<u64>,
    output: Option<String>,
) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let envelope = context
        .client
        .scroll_browser_session(
            session_id.as_str(),
            &control_plane::BrowserScrollRequest {
                delta_x: Some(delta_x),
                delta_y: Some(delta_y),
                capture_failure_screenshot: bool_option(capture_failure_screenshot),
                max_failure_screenshot_bytes,
            },
        )
        .await
        .context("failed to scroll browser session")?;
    let success = envelope.success;
    let error = envelope.error.clone();
    let screenshot_path = write_optional_failure_screenshot(
        output.as_deref(),
        session_id.as_str(),
        "scroll",
        envelope.decode_failure_screenshot().as_deref(),
    )?;
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser scroll output")?;
    strip_large_binary_fields(
        &mut value,
        screenshot_path.is_some(),
        &["failure_screenshot_base64"],
    );
    maybe_attach_output_path(&mut value, screenshot_path.as_ref());
    normalize_session_scoped_output(&mut value, session_id.as_str());
    emit_browser_value(
        &value,
        format!(
            "browser.scroll session_id={} success={} scroll_x={} scroll_y={} artifact={}",
            browser_session_handle_text(Some(session_id.as_str())),
            envelope.success,
            envelope.scroll_x,
            envelope.scroll_y,
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser scroll output",
    )?;
    ensure_browser_command_success("browser.scroll", success, error.as_str())
}

async fn run_browser_wait(args: BrowserWaitArgs) -> Result<()> {
    let BrowserWaitArgs {
        session_id,
        selector,
        text,
        timeout_ms,
        poll_interval_ms,
        capture_failure_screenshot,
        max_failure_screenshot_bytes,
        output,
        json,
    } = args;
    let context = client::control_plane::connect_admin_console_with_request_timeout(
        app::ConnectionOverrides::default(),
        browser_control_plane_request_timeout(timeout_ms),
    )
    .await?;
    let envelope = context
        .client
        .wait_for_browser_session(
            session_id.as_str(),
            &control_plane::BrowserWaitForRequest {
                selector,
                text,
                timeout_ms,
                poll_interval_ms,
                capture_failure_screenshot: bool_option(capture_failure_screenshot),
                max_failure_screenshot_bytes,
            },
        )
        .await
        .context("failed to wait for browser session state")?;
    let success = envelope.success;
    let error = envelope.error.clone();
    let screenshot_path = write_optional_failure_screenshot(
        output.as_deref(),
        session_id.as_str(),
        "wait",
        envelope.decode_failure_screenshot().as_deref(),
    )?;
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser wait output")?;
    strip_large_binary_fields(
        &mut value,
        screenshot_path.is_some(),
        &["failure_screenshot_base64"],
    );
    maybe_attach_output_path(&mut value, screenshot_path.as_ref());
    normalize_session_scoped_output(&mut value, session_id.as_str());
    emit_browser_value_with_json(
        &value,
        format!(
            "browser.wait session_id={} success={} waited_ms={} matched_selector={} matched_text={} artifact={}",
            browser_session_handle_text(Some(session_id.as_str())),
            envelope.success,
            envelope.waited_ms,
            empty_as_dash(envelope.matched_selector.as_str()),
            empty_as_dash(envelope.matched_text.as_str()),
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser wait output",
        json,
    )?;
    ensure_browser_command_success("browser.wait", success, error.as_str())
}

async fn run_browser_snapshot(args: BrowserSnapshotArgs) -> Result<()> {
    let BrowserSnapshotArgs {
        session_id,
        include_dom_snapshot,
        include_accessibility_tree,
        include_visible_text,
        max_dom_snapshot_bytes,
        max_accessibility_tree_bytes,
        max_visible_text_bytes,
        output,
        json,
    } = args;
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let mut value = serde_json::to_value(
        &context
            .client
            .observe_browser_session(
                session_id.as_str(),
                &control_plane::BrowserObserveQuery {
                    include_dom_snapshot: bool_option(include_dom_snapshot),
                    include_accessibility_tree: bool_option(include_accessibility_tree),
                    include_visible_text: bool_option(include_visible_text),
                    max_dom_snapshot_bytes,
                    max_accessibility_tree_bytes,
                    max_visible_text_bytes,
                },
            )
            .await
            .context("failed to observe browser session")?,
    )
    .context("failed to encode browser snapshot output")?;
    normalize_session_scoped_output(&mut value, session_id.as_str());
    let written =
        write_optional_json_output(output.as_deref(), session_id.as_str(), "snapshot", &value)?;
    maybe_attach_output_path(&mut value, written.as_ref());
    emit_browser_snapshot_value(
        &value,
        format!(
            "browser.snapshot session_id={} page_url={} dom_truncated={} text_truncated={} output={}",
            browser_session_handle_text(Some(session_id.as_str())),
            value.get("page_url").and_then(Value::as_str).unwrap_or("-"),
            value.get("dom_truncated").and_then(Value::as_bool).unwrap_or(false),
            value.get("visible_text_truncated").and_then(Value::as_bool).unwrap_or(false),
            written.as_deref().unwrap_or("-"),
        ),
        written.is_some(),
        "failed to encode browser snapshot output",
        json,
    )?;
    ensure_browser_value_success("browser.snapshot", &value)
}

async fn run_browser_screenshot(
    session_id: String,
    max_bytes: Option<u64>,
    format: Option<String>,
    output: Option<String>,
    json: bool,
) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let envelope = context
        .client
        .get_browser_screenshot(
            session_id.as_str(),
            &control_plane::BrowserScreenshotQuery { max_bytes, format: format.clone() },
        )
        .await
        .context("failed to capture browser screenshot")?;
    let success = envelope.success;
    let error = envelope.error.clone();
    let suggested_ext = format
        .as_deref()
        .map(sanitize_screenshot_format)
        .unwrap_or_else(|| mime_extension(envelope.mime_type.as_deref()).to_owned());
    let mode = if json { BrowserOutputMode::Json } else { browser_output_mode() };
    let output_path = write_optional_binary_output_for_mode(
        output.as_deref(),
        session_id.as_str(),
        "screenshot",
        suggested_ext.as_str(),
        envelope.decode_image().as_deref(),
        mode,
    )?;
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser screenshot output")?;
    strip_large_binary_fields(&mut value, output_path.is_some(), &["image_base64"]);
    maybe_attach_output_path(&mut value, output_path.as_ref());
    normalize_session_scoped_output(&mut value, session_id.as_str());
    if browser_command_payload_should_emit(mode, success) {
        emit_browser_value_for_mode(
            &value,
            format!(
                "browser.screenshot session_id={} success={} mime_type={} output={}",
                browser_session_handle_text(Some(session_id.as_str())),
                envelope.success,
                envelope.mime_type.as_deref().unwrap_or("-"),
                output_path.as_deref().unwrap_or("-"),
            ),
            "failed to encode browser screenshot output",
            mode,
        )?;
    }
    ensure_browser_command_success("browser.screenshot", success, error.as_str())
}

async fn run_browser_title(
    session_id: String,
    max_title_bytes: Option<u64>,
    json: bool,
) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let envelope = context
        .client
        .get_browser_title(
            session_id.as_str(),
            &control_plane::BrowserTitleQuery { max_title_bytes },
        )
        .await
        .context("failed to read browser title")?;
    let success = envelope.success;
    let error = envelope.error.clone();
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser title output")?;
    normalize_session_scoped_output(&mut value, session_id.as_str());
    emit_browser_value_with_json(
        &value,
        format!(
            "browser.title session_id={} success={} title={}",
            browser_session_handle_text(Some(session_id.as_str())),
            envelope.success,
            empty_as_dash(envelope.title.as_str()),
        ),
        "failed to encode browser title output",
        json,
    )?;
    ensure_browser_command_success("browser.title", success, error.as_str())
}

async fn run_browser_network(
    session_id: String,
    limit: Option<u32>,
    include_headers: bool,
    max_payload_bytes: Option<u64>,
    json: bool,
) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let envelope = context
        .client
        .get_browser_network_log(
            session_id.as_str(),
            &control_plane::BrowserNetworkLogQuery {
                limit,
                include_headers: bool_option(include_headers),
                max_payload_bytes,
            },
        )
        .await
        .context("failed to fetch browser network log")?;
    let success = envelope.success;
    let error = envelope.error.clone();
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser network output")?;
    normalize_session_scoped_output(&mut value, session_id.as_str());
    let mut text = format!(
        "browser.network session_id={} success={} entries={} truncated={}",
        browser_session_handle_text(Some(session_id.as_str())),
        envelope.success,
        envelope.entries.len(),
        envelope.truncated,
    );
    for entry in &envelope.entries {
        text.push('\n');
        text.push_str(
            format!(
                "request url={} status_code={} latency_ms={} timing={}",
                empty_as_dash(entry.request_url.as_str()),
                entry.status_code,
                entry.latency_ms,
                empty_as_dash(entry.timing_bucket.as_str()),
            )
            .as_str(),
        );
    }
    emit_browser_value_with_json(&value, text, "failed to encode browser network output", json)?;
    ensure_browser_command_success("browser.network", success, error.as_str())
}

async fn run_browser_storage(session_id: String, output: Option<String>) -> Result<()> {
    let mut value = inspect_browser_session(
        session_id.as_str(),
        browser_v1::InspectSessionRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(resolve_required_canonical_id(session_id.clone())?),
            include_cookies: true,
            include_storage: true,
            include_action_log: false,
            include_network_log: false,
            include_page_snapshot: false,
            max_cookie_bytes: 0,
            max_storage_bytes: 0,
            max_action_log_entries: 0,
            max_network_log_entries: 0,
            max_network_log_bytes: 0,
            max_dom_snapshot_bytes: 0,
            max_visible_text_bytes: 0,
            include_console_log: false,
            include_page_diagnostics: false,
            max_console_log_entries: 0,
            max_console_log_bytes: 0,
        },
    )
    .await?;
    let written =
        write_optional_json_output(output.as_deref(), session_id.as_str(), "storage", &value)?;
    maybe_attach_output_path(&mut value, written.as_ref());
    emit_browser_value(
        &value,
        format!(
            "browser.storage session_id={} cookie_domains={} origins={} output={}",
            browser_session_handle_text(Some(session_id.as_str())),
            value.get("cookies").and_then(Value::as_array).map_or(0, Vec::len),
            value.get("storage").and_then(Value::as_array).map_or(0, Vec::len),
            written.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser storage output",
    )
}

async fn run_browser_errors(
    session_id: String,
    limit: Option<u32>,
    output: Option<String>,
    json: bool,
) -> Result<()> {
    let mut value = inspect_browser_session(
        session_id.as_str(),
        browser_v1::InspectSessionRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(resolve_required_canonical_id(session_id.clone())?),
            include_cookies: false,
            include_storage: false,
            include_action_log: true,
            include_network_log: false,
            include_page_snapshot: false,
            max_cookie_bytes: 0,
            max_storage_bytes: 0,
            max_action_log_entries: limit.unwrap_or_default(),
            max_network_log_entries: 0,
            max_network_log_bytes: 0,
            max_dom_snapshot_bytes: 0,
            max_visible_text_bytes: 0,
            include_console_log: true,
            include_page_diagnostics: true,
            max_console_log_entries: limit.unwrap_or_default(),
            max_console_log_bytes: 0,
        },
    )
    .await?;
    let filtered = value
        .get("action_log")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|entry| {
            !entry.get("success").and_then(Value::as_bool).unwrap_or(false)
                || entry
                    .get("error")
                    .and_then(Value::as_str)
                    .is_some_and(|error| !error.trim().is_empty())
        })
        .collect::<Vec<_>>();
    value["errors"] = Value::Array(filtered);
    let written =
        write_optional_json_output(output.as_deref(), session_id.as_str(), "errors", &value)?;
    maybe_attach_output_path(&mut value, written.as_ref());
    emit_browser_value_with_json(
        &value,
        format!(
            "browser.errors session_id={} count={} output={}",
            browser_session_handle_text(Some(session_id.as_str())),
            value.get("errors").and_then(Value::as_array).map_or(0, Vec::len),
            written.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser errors output",
        json,
    )
}

async fn run_browser_trace(session_id: String, output: Option<String>) -> Result<()> {
    let mut value = inspect_browser_session(
        session_id.as_str(),
        browser_v1::InspectSessionRequest {
            v: CANONICAL_PROTOCOL_MAJOR,
            session_id: Some(resolve_required_canonical_id(session_id.clone())?),
            include_cookies: true,
            include_storage: true,
            include_action_log: true,
            include_network_log: true,
            include_page_snapshot: true,
            max_cookie_bytes: 0,
            max_storage_bytes: 0,
            max_action_log_entries: 0,
            max_network_log_entries: 0,
            max_network_log_bytes: 0,
            max_dom_snapshot_bytes: 0,
            max_visible_text_bytes: 0,
            include_console_log: true,
            include_page_diagnostics: true,
            max_console_log_entries: 0,
            max_console_log_bytes: 0,
        },
    )
    .await?;
    value["trace_generated_at_unix_ms"] = json!(now_unix_ms());
    let written =
        write_optional_json_output(output.as_deref(), session_id.as_str(), "trace", &value)?;
    maybe_attach_output_path(&mut value, written.as_ref());
    emit_browser_value(
        &value,
        format!(
            "browser.trace session_id={} output={} action_log={} network_log={}",
            browser_session_handle_text(Some(session_id.as_str())),
            written.as_deref().unwrap_or("-"),
            value.get("action_log").and_then(Value::as_array).map_or(0, Vec::len),
            value.get("network_log").and_then(Value::as_array).map_or(0, Vec::len),
        ),
        "failed to encode browser trace output",
    )
}

async fn run_browser_console(session_id: String, output: Option<String>, json: bool) -> Result<()> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .console_log(browser_request(
            browser_v1::ConsoleLogRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                limit: 50,
                minimum_severity: browser_v1::BrowserDiagnosticSeverity::Info as i32,
                include_page_diagnostics: true,
                max_payload_bytes: 0,
            },
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to fetch browser console log")?
        .into_inner();
    if !response.success {
        anyhow::bail!("browser console lookup failed: {}", empty_as_dash(response.error.as_str()));
    }
    let mut value = json!({
        "session_id": browser_identifier_json_value(Some(session_id.as_str())),
        "entries": response.entries.iter().map(console_entry_value).collect::<Vec<_>>(),
        "truncated": response.truncated,
        "page_diagnostics": response.page_diagnostics.as_ref().map(page_diagnostics_value).unwrap_or(Value::Null),
    });
    let written =
        write_optional_json_output(output.as_deref(), session_id.as_str(), "console", &value)?;
    maybe_attach_output_path(&mut value, written.as_ref());
    let text = format_browser_console_text(
        session_id.as_str(),
        response.entries.as_slice(),
        response.truncated,
        written.as_deref(),
    );
    emit_browser_value_with_json(&value, text, "failed to encode browser console output", json)
}

async fn run_browser_pdf(session_id: String, output: Option<String>) -> Result<()> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .export_pdf(browser_request(
            browser_v1::ExportPdfRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                max_bytes: 0,
            },
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to export browser session PDF")?
        .into_inner();
    if !response.success {
        anyhow::bail!("browser pdf export failed: {}", empty_as_dash(response.error.as_str()));
    }
    let output_path = write_optional_binary_output(
        output.as_deref(),
        session_id.as_str(),
        "session",
        "pdf",
        Some(response.pdf_bytes.as_slice()),
    )?;
    let mut value = json!({
        "session_id": browser_identifier_json_value(Some(session_id.as_str())),
        "success": response.success,
        "mime_type": response.mime_type,
        "size_bytes": response.size_bytes,
        "sha256": response.sha256,
        "artifact": response.artifact.as_ref().map(download_artifact_proto_value).unwrap_or(Value::Null),
    });
    maybe_attach_output_path(&mut value, output_path.as_ref());
    emit_browser_value(
        &value,
        format!(
            "browser.pdf session_id={} size_bytes={} output={}",
            browser_session_handle_text(Some(session_id.as_str())),
            value.get("size_bytes").and_then(Value::as_u64).unwrap_or(0),
            output_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser pdf output",
    )
}

async fn run_browser_press(session_id: String, key: String) -> Result<()> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .press(browser_request(
            browser_v1::PressRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                key: key.clone(),
                timeout_ms: 0,
                capture_failure_screenshot: false,
                max_failure_screenshot_bytes: 0,
            },
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to press browser key")?
        .into_inner();
    let success = response.success;
    let error = response.error.clone();
    let value = json!({
        "session_id": browser_identifier_json_value(Some(session_id.as_str())),
        "success": response.success,
        "key": response.key,
        "error": response.error,
        "action_log": response.action_log.as_ref().map(action_log_entry_value).unwrap_or(Value::Null),
    });
    emit_browser_value(
        &value,
        format!(
            "browser.press session_id={} success={} key={}",
            browser_session_handle_text(Some(session_id.as_str())),
            value.get("success").and_then(Value::as_bool).unwrap_or(false),
            key,
        ),
        "failed to encode browser press output",
    )?;
    ensure_browser_command_success("browser.press", success, error.as_str())
}

async fn run_browser_select(session_id: String, selector: String, value: String) -> Result<()> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .select(browser_request(
            browser_v1::SelectRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                selector: selector.clone(),
                value: value.clone(),
                timeout_ms: 0,
                capture_failure_screenshot: false,
                max_failure_screenshot_bytes: 0,
            },
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to select browser option")?
        .into_inner();
    let success = response.success;
    let error = response.error.clone();
    let payload = json!({
        "session_id": browser_identifier_json_value(Some(session_id.as_str())),
        "success": response.success,
        "selector": selector,
        "selected_value": response.selected_value,
        "error": response.error,
        "action_log": response.action_log.as_ref().map(action_log_entry_value).unwrap_or(Value::Null),
    });
    emit_browser_value(
        &payload,
        format!(
            "browser.select session_id={} success={} value={}",
            browser_session_handle_text(Some(session_id.as_str())),
            payload.get("success").and_then(Value::as_bool).unwrap_or(false),
            value,
        ),
        "failed to encode browser select output",
    )?;
    ensure_browser_command_success("browser.select", success, error.as_str())
}

async fn run_browser_highlight(session_id: String, selector: String) -> Result<()> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .highlight(browser_request(
            browser_v1::HighlightRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                selector: selector.clone(),
                timeout_ms: 0,
                duration_ms: 1_500,
                capture_failure_screenshot: false,
                max_failure_screenshot_bytes: 0,
            },
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to highlight browser selector")?
        .into_inner();
    let success = response.success;
    let error = response.error.clone();
    let payload = json!({
        "session_id": browser_identifier_json_value(Some(session_id.as_str())),
        "success": response.success,
        "selector": response.selector,
        "error": response.error,
        "action_log": response.action_log.as_ref().map(action_log_entry_value).unwrap_or(Value::Null),
    });
    emit_browser_value(
        &payload,
        format!(
            "browser.highlight session_id={} success={} selector={}",
            browser_session_handle_text(Some(session_id.as_str())),
            payload.get("success").and_then(Value::as_bool).unwrap_or(false),
            selector,
        ),
        "failed to encode browser highlight output",
    )?;
    ensure_browser_command_success("browser.highlight", success, error.as_str())
}

async fn run_browser_downloads(
    session_id: String,
    artifact_id: Option<String>,
    output: Option<String>,
    max_bytes: Option<u64>,
    limit: Option<u32>,
    quarantined_only: bool,
    json: bool,
) -> Result<()> {
    if output.is_some() {
        return run_browser_download_save(session_id, artifact_id, output, max_bytes, json).await;
    }
    if artifact_id.is_some() {
        anyhow::bail!("--artifact-id requires --output to save a download artifact");
    }

    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let envelope = context
        .client
        .list_browser_download_artifacts(&control_plane::BrowserDownloadArtifactsQuery {
            session_id: session_id.clone(),
            limit,
            quarantined_only,
        })
        .await
        .context("failed to list browser download artifacts")?;
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser downloads output")?;
    normalize_session_scoped_output(&mut value, session_id.as_str());
    let mut text = format!(
        "browser.downloads session_id={} count={} truncated={} quarantined_only={}",
        browser_session_handle_text(Some(session_id.as_str())),
        envelope.artifacts.len(),
        envelope.truncated,
        quarantined_only,
    );
    for artifact in &envelope.artifacts {
        text.push('\n');
        text.push_str(
            format!(
                "artifact id={} file={} size_bytes={} quarantined={} sha256={}",
                redacted_browser_identifier_text(artifact.artifact_id.as_deref(), "artifact"),
                artifact.file_name,
                artifact.size_bytes,
                artifact.quarantined,
                artifact.sha256,
            )
            .as_str(),
        );
    }
    emit_browser_value_with_json(&value, text, "failed to encode browser downloads output", json)
}

async fn run_browser_download_save(
    session_id: String,
    artifact_id: Option<String>,
    output: Option<String>,
    max_bytes: Option<u64>,
    json: bool,
) -> Result<()> {
    let artifact_id = match artifact_id {
        Some(value) => value,
        None => latest_browser_download_artifact_id(session_id.as_str()).await?,
    };
    let output =
        output.ok_or_else(|| anyhow::anyhow!("--output is required to save a download"))?;
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .get_download_artifact(browser_request(
            browser_v1::GetDownloadArtifactRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(resolve_required_canonical_id(session_id.clone())?),
                artifact_id: Some(resolve_required_canonical_id(artifact_id.clone())?),
                max_bytes: max_bytes.unwrap_or(0),
            },
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to fetch browser download artifact")?
        .into_inner();
    if !response.success {
        anyhow::bail!("browser download save failed: {}", empty_as_dash(response.error.as_str()));
    }
    let output_path = PathBuf::from(output.as_str());
    write_artifact_bytes(output_path.as_path(), response.content.as_slice())?;
    let artifact = response.artifact.as_ref().map(download_artifact_proto_value);
    let value = json!({
        "session_id": browser_identifier_json_value(Some(session_id.as_str())),
        "success": response.success,
        "artifact_id": browser_identifier_json_value(Some(artifact_id.as_str())),
        "artifact": artifact.unwrap_or(Value::Null),
        "size_bytes": response.content.len(),
        "output_path": output_path.display().to_string(),
    });
    emit_browser_value_with_json(
        &value,
        format!(
            "browser.downloads.save session_id={} artifact_id={} size_bytes={} output={}",
            browser_session_handle_text(Some(session_id.as_str())),
            redacted_browser_identifier_text(Some(artifact_id.as_str()), "artifact"),
            response.content.len(),
            output_path.display(),
        ),
        "failed to encode browser download save output",
        json,
    )
}

async fn latest_browser_download_artifact_id(session_id: &str) -> Result<String> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let envelope = context
        .client
        .list_browser_download_artifacts(&control_plane::BrowserDownloadArtifactsQuery {
            session_id: session_id.to_owned(),
            limit: Some(1),
            quarantined_only: false,
        })
        .await
        .context("failed to list browser download artifacts before save")?;
    envelope
        .artifacts
        .first()
        .and_then(|artifact| artifact.artifact_id.clone())
        .ok_or_else(|| anyhow::anyhow!("browser session has no download artifacts to save"))
}

async fn run_browser_permissions_command(
    session_id: String,
    command: BrowserPermissionsCommand,
) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        BrowserPermissionsCommand::Get => {
            let envelope = context
                .client
                .get_browser_permissions(session_id.as_str())
                .await
                .context("failed to get browser permissions")?;
            let mut value = serde_json::to_value(&envelope)
                .context("failed to encode browser permissions output")?;
            normalize_session_scoped_output(&mut value, session_id.as_str());
            emit_browser_value(
                &value,
                format!(
                    "browser.permissions.get session_id={} success={} camera={} microphone={} location={}",
                    browser_session_handle_text(Some(session_id.as_str())),
                    envelope.success,
                    permission_setting_text(
                        envelope.permissions.as_ref().map(|value| value.camera)
                    ),
                    permission_setting_text(
                        envelope.permissions.as_ref().map(|value| value.microphone)
                    ),
                    permission_setting_text(
                        envelope.permissions.as_ref().map(|value| value.location)
                    ),
                ),
                "failed to encode browser permissions output",
            )
        }
        BrowserPermissionsCommand::Set { camera, microphone, location, reset_to_default } => {
            let envelope = context
                .client
                .set_browser_permissions(
                    session_id.as_str(),
                    &control_plane::BrowserSetPermissionsRequest {
                        camera: parse_permission_setting(camera.as_deref())?,
                        microphone: parse_permission_setting(microphone.as_deref())?,
                        location: parse_permission_setting(location.as_deref())?,
                        reset_to_default: bool_option(reset_to_default),
                    },
                )
                .await
                .context("failed to set browser permissions")?;
            let mut value = serde_json::to_value(&envelope)
                .context("failed to encode browser permissions mutation output")?;
            normalize_session_scoped_output(&mut value, session_id.as_str());
            emit_browser_value(
                &value,
                format!(
                    "browser.permissions.set session_id={} success={} camera={} microphone={} location={}",
                    browser_session_handle_text(Some(session_id.as_str())),
                    envelope.success,
                    permission_setting_text(
                        envelope.permissions.as_ref().map(|value| value.camera)
                    ),
                    permission_setting_text(
                        envelope.permissions.as_ref().map(|value| value.microphone)
                    ),
                    permission_setting_text(
                        envelope.permissions.as_ref().map(|value| value.location)
                    ),
                ),
                "failed to encode browser permissions mutation output",
            )
        }
    }
}

async fn run_browser_reset_state(
    session_id: String,
    clear_cookies: bool,
    clear_storage: bool,
    reset_tabs: bool,
    reset_permissions: bool,
) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    let envelope = context
        .client
        .reset_browser_state(
            session_id.as_str(),
            &control_plane::BrowserResetStateRequest {
                clear_cookies: bool_option(clear_cookies),
                clear_storage: bool_option(clear_storage),
                reset_tabs: bool_option(reset_tabs),
                reset_permissions: bool_option(reset_permissions),
            },
        )
        .await
        .context("failed to reset browser state")?;
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser reset-state output")?;
    normalize_session_scoped_output(&mut value, session_id.as_str());
    emit_browser_value(
        &value,
        format!(
            "browser.reset-state session_id={} success={} cookies_cleared={} storage_entries_cleared={} tabs_closed={}",
            browser_session_handle_text(Some(session_id.as_str())),
            envelope.success,
            envelope.cookies_cleared,
            envelope.storage_entries_cleared,
            envelope.tabs_closed,
        ),
        "failed to encode browser reset-state output",
    )
}

async fn connect_browser_service(
    connection: &BrowserServiceConnection,
) -> Result<browser_v1::browser_service_client::BrowserServiceClient<tonic::transport::Channel>> {
    let endpoint = Endpoint::from_shared(connection.grpc_url.clone())
        .with_context(|| format!("invalid browser gRPC URL {}", connection.grpc_url))?;
    let channel = endpoint
        .connect()
        .await
        .with_context(|| format!("failed to connect browser service {}", connection.grpc_url))?;
    Ok(browser_v1::browser_service_client::BrowserServiceClient::new(channel))
}

fn browser_request<T>(
    payload: T,
    auth_token: Option<&str>,
    caller_principal: &str,
) -> Result<Request<T>> {
    let mut request = Request::new(payload);
    apply_browser_service_auth(request.metadata_mut(), auth_token)?;
    apply_browser_service_caller_principal(request.metadata_mut(), caller_principal)?;
    Ok(request)
}

fn apply_browser_service_auth(metadata: &mut MetadataMap, auth_token: Option<&str>) -> Result<()> {
    if let Some(token) = auth_token.filter(|value| !value.trim().is_empty()) {
        metadata.insert(
            "authorization",
            format!("Bearer {token}")
                .parse()
                .context("invalid browser service authorization metadata")?,
        );
    }
    Ok(())
}

fn apply_browser_service_caller_principal(
    metadata: &mut MetadataMap,
    caller_principal: &str,
) -> Result<()> {
    let caller_principal = caller_principal.trim();
    if caller_principal.is_empty() {
        anyhow::bail!("browser caller principal must not be empty");
    }
    metadata.insert(
        BROWSER_CALLER_PRINCIPAL_HEADER,
        caller_principal.parse().context("invalid browser caller principal metadata")?,
    );
    Ok(())
}

fn resolve_browser_caller_principal(defaults: app::ConnectionDefaults) -> Result<String> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for browser command"))?;
    let connection =
        root_context.resolve_grpc_connection(app::ConnectionOverrides::default(), defaults)?;
    Ok(connection.principal)
}

async fn probe_browser_grpc(connection: &BrowserServiceConnection) -> Result<()> {
    let mut client = connect_browser_service(connection).await?;
    client
        .list_sessions(browser_request(
            browser_v1::ListSessionsRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                principal: String::new(),
                limit: 1,
            },
            connection.auth_token.as_deref(),
            BROWSER_PROBE_PRINCIPAL,
        )?)
        .await
        .context("failed to call browser ListSessions")?;
    Ok(())
}

async fn probe_browser_profile_readiness(connection: &BrowserServiceConnection) -> Result<bool> {
    let mut client = connect_browser_service(connection).await?;
    let response = client
        .list_profiles(browser_request(
            browser_v1::ListProfilesRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                principal: BROWSER_PROBE_PRINCIPAL.to_owned(),
            },
            connection.auth_token.as_deref(),
            BROWSER_PROBE_PRINCIPAL,
        )?)
        .await;
    match response {
        Ok(_) => Ok(true),
        Err(status)
            if status.code() == Code::FailedPrecondition
                && status.message().contains(BROWSERD_STATE_ENCRYPTION_KEY_ENV) =>
        {
            Ok(false)
        }
        Err(status) => Err(anyhow!("failed to call browser ListProfiles: {status}")),
    }
}

async fn get_browser_session_detail(session_id: &str) -> Result<browser_v1::BrowserSessionDetail> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .get_session(browser_request(
            browser_v1::GetSessionRequest {
                v: CANONICAL_PROTOCOL_MAJOR,
                session_id: Some(resolve_required_canonical_id(session_id.to_owned())?),
            },
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to fetch browser session")?
        .into_inner();
    if !response.success {
        anyhow::bail!("browser session lookup failed: {}", empty_as_dash(response.error.as_str()));
    }
    response.session.context("browser session lookup returned empty session payload")
}

async fn inspect_browser_session(
    session_id: &str,
    request: browser_v1::InspectSessionRequest,
) -> Result<Value> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(app::ConnectionDefaults::USER)?;
    let mut client = connect_browser_service(&resolved.connection).await?;
    let response = client
        .inspect_session(browser_request(
            request,
            resolved.connection.auth_token.as_deref(),
            caller_principal.as_str(),
        )?)
        .await
        .context("failed to inspect browser session")?
        .into_inner();
    if !response.success {
        anyhow::bail!(
            "browser session inspection failed: {}",
            empty_as_dash(response.error.as_str())
        );
    }
    Ok(json!({
        "session": response.session.as_ref().map(session_detail_value).unwrap_or(Value::Null),
        "cookies": response.cookies.iter().map(cookie_domain_value).collect::<Vec<_>>(),
        "storage": response.storage.iter().map(storage_origin_value).collect::<Vec<_>>(),
        "action_log": response.action_log.iter().map(action_log_entry_value).collect::<Vec<_>>(),
        "network_log": response.network_log.iter().map(network_log_entry_value).collect::<Vec<_>>(),
        "console_log": response.console_log.iter().map(console_entry_value).collect::<Vec<_>>(),
        "dom_snapshot": response.dom_snapshot,
        "visible_text": response.visible_text,
        "page_url": response.page_url,
        "cookies_truncated": response.cookies_truncated,
        "storage_truncated": response.storage_truncated,
        "action_log_truncated": response.action_log_truncated,
        "network_log_truncated": response.network_log_truncated,
        "console_log_truncated": response.console_log_truncated,
        "dom_truncated": response.dom_truncated,
        "visible_text_truncated": response.visible_text_truncated,
        "page_diagnostics": response.page_diagnostics.as_ref().map(page_diagnostics_value).unwrap_or(Value::Null),
        "error": response.error,
        "session_id": browser_identifier_json_value(Some(session_id)),
    }))
}

async fn fetch_browser_health(health_base_url: &str) -> Result<Value> {
    let client = AsyncClient::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to initialize browser health client")?;
    let url = format!("{}/healthz", health_base_url.trim_end_matches('/'));
    let response = client
        .get(url.as_str())
        .send()
        .await
        .with_context(|| format!("failed to reach browser health endpoint {url}"))?
        .error_for_status()
        .with_context(|| format!("browser health endpoint returned error {url}"))?;
    response.json::<Value>().await.context("failed to decode browser health response")
}

fn resolve_browser_config(
    endpoint: Option<String>,
    health_url: Option<String>,
    token: Option<String>,
) -> Result<BrowserResolvedConfig> {
    let config_path = current_config_path();
    let document = load_optional_config_document(config_path.as_deref())?;
    let file_endpoint = document_string(document.as_ref(), "tool_call.browser_service.endpoint");
    let file_health_base_url =
        document_string(document.as_ref(), "tool_call.browser_service.health_base_url");
    let file_enabled = document_bool(document.as_ref(), "tool_call.browser_service.enabled");
    let file_auth_token =
        document_string(document.as_ref(), "tool_call.browser_service.auth_token");
    let file_connect_timeout_ms =
        document_u64(document.as_ref(), "tool_call.browser_service.connect_timeout_ms");
    let file_request_timeout_ms =
        document_u64(document.as_ref(), "tool_call.browser_service.request_timeout_ms");
    let file_max_screenshot_bytes =
        document_u64(document.as_ref(), "tool_call.browser_service.max_screenshot_bytes");
    let file_max_title_bytes =
        document_u64(document.as_ref(), "tool_call.browser_service.max_title_bytes");
    let file_state_dir = document_string(document.as_ref(), "tool_call.browser_service.state_dir");
    let file_state_key_vault_ref =
        document_string(document.as_ref(), "tool_call.browser_service.state_key_vault_ref");
    let file_allowed_tools = document_string_array(document.as_ref(), "tool_call.allowed_tools");

    let env_endpoint = env_optional("PALYRA_BROWSER_SERVICE_ENDPOINT");
    let env_token = env_optional("PALYRA_BROWSER_SERVICE_AUTH_TOKEN");
    let env_enabled = env_bool("PALYRA_BROWSER_SERVICE_ENABLED");
    let env_connect_timeout_ms = env_u64("PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS");
    let env_request_timeout_ms = env_u64("PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS");
    let env_max_screenshot_bytes = env_u64("PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES");
    let env_max_title_bytes = env_u64("PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES");
    let env_state_dir = env_optional("PALYRA_BROWSERD_STATE_DIR");
    let env_state_key_vault_ref = env_optional("PALYRA_BROWSERD_STATE_ENCRYPTION_KEY_VAULT_REF");
    let env_state_encryption_key = env_optional(BROWSERD_STATE_ENCRYPTION_KEY_ENV);
    let configured_allowed_tools = env_tool_allowlist().unwrap_or(file_allowed_tools);
    let missing_browser_tools = missing_browser_gateway_tools(configured_allowed_tools.as_slice());
    let state_key_vault_ref = env_state_key_vault_ref.clone().or(file_state_key_vault_ref.clone());
    let state_key_vault_ref_configured = state_key_vault_ref.is_some();

    let grpc_url = normalize_browser_base_url(
        endpoint
            .or(env_endpoint.clone())
            .or(file_endpoint.clone())
            .unwrap_or_else(|| DEFAULT_BROWSER_GRPC_URL.to_owned()),
        "browser gRPC URL",
    )?;
    let health_base_url = normalize_browser_base_url(
        health_url
            .or(file_health_base_url)
            .unwrap_or_else(|| derive_browser_health_base_url(grpc_url.as_str())),
        "browser health URL",
    )?;
    let cli_token = token.as_deref().and_then(normalize_optional_text).map(ToOwned::to_owned);
    let gateway_configured_token = env_token.clone().or(file_auth_token.clone());
    let resolved_token = cli_token.clone().or(gateway_configured_token.clone());
    let token_from_cli_only = cli_token.is_some() && gateway_configured_token.is_none();
    let token_conflicts_with_gateway_config = cli_token
        .as_deref()
        .zip(gateway_configured_token.as_deref())
        .is_some_and(|(cli_token, gateway_token)| cli_token != gateway_token);

    Ok(BrowserResolvedConfig {
        connection: BrowserServiceConnection {
            grpc_url: grpc_url.clone(),
            health_base_url,
            auth_token: resolved_token.clone(),
        },
        policy: BrowserPolicySnapshot {
            configured_enabled: env_enabled.or(file_enabled).unwrap_or(false),
            auth_token_configured: resolved_token.is_some()
                || file_auth_token.is_some()
                || env_token.is_some(),
            endpoint: grpc_url,
            connect_timeout_ms: env_connect_timeout_ms.or(file_connect_timeout_ms),
            request_timeout_ms: env_request_timeout_ms.or(file_request_timeout_ms),
            max_screenshot_bytes: env_max_screenshot_bytes.or(file_max_screenshot_bytes),
            max_title_bytes: env_max_title_bytes.or(file_max_title_bytes),
            state_dir: env_state_dir.or(file_state_dir),
            browser_tools_allowlisted: missing_browser_tools.is_empty(),
            missing_browser_tools,
            state_key_vault_ref_configured,
            state_encryption_key_env_configured: env_state_encryption_key.is_some(),
            profiles_ready: env_state_encryption_key.is_some(),
        },
        config_path: config_path.map(|value| value.display().to_string()),
        state_key_vault_ref,
        token_from_cli_only,
        token_conflicts_with_gateway_config,
    })
}

fn ensure_browser_start_preflight(resolved: &BrowserResolvedConfig) -> Result<()> {
    let config_path = resolved.config_path.as_deref();
    let mut blockers = Vec::new();
    let mut advisories = Vec::new();

    if !resolved.policy.configured_enabled {
        let enable_command = browser_service_enable_command(config_path);
        blockers.push(format!(
            "`tool_call.browser_service.enabled` is false. Enable the gateway browser service with `{enable_command}`."
        ));
    }

    let configure_token_command = browser_service_auth_token_command(config_path);
    if resolved.token_conflicts_with_gateway_config {
        blockers.push(
            "the supplied `--token` differs from the browser service token already configured for the gateway. Use the configured token, update `tool_call.browser_service.auth_token`, or restart the gateway with matching `PALYRA_BROWSER_SERVICE_AUTH_TOKEN`."
                .to_owned(),
        );
    } else if resolved.token_from_cli_only {
        blockers.push(format!(
            "`--token` only configures browserd for this launch; gateway-mediated browser commands require the same token in `tool_call.browser_service.auth_token`. Configure it with `{configure_token_command}`."
        ));
    } else if resolved.connection.auth_token.is_none() {
        blockers.push(format!(
            "`tool_call.browser_service.auth_token` is missing. Configure it with `{configure_token_command}`."
        ));
    }

    if !resolved.policy.browser_tools_allowlisted {
        advisories.push(format!(
            "`tool_call.allowed_tools` is missing gateway browser tools ({}). Add the missing `palyra.browser.*` tools before expecting agents to use browser actions.",
            browser_missing_tools_summary(resolved.policy.missing_browser_tools.as_slice())
        ));
    }

    if !browser_profile_state_key_available_for_start(&resolved.policy) {
        advisories.push(browser_profile_state_key_guidance(config_path));
    }

    if blockers.is_empty() {
        return Ok(());
    }

    let mut message =
        "browser start cannot launch until gateway browser prerequisites are configured:"
            .to_owned();
    for (index, blocker) in blockers.iter().enumerate() {
        message.push_str(format!("\n{}. {blocker}", index + 1).as_str());
    }
    if !advisories.is_empty() {
        message.push_str("\n\nAdditional readiness checks:");
        for advisory in advisories {
            message.push_str("\n- ");
            message.push_str(advisory.as_str());
        }
    }
    message.push_str(
        "\n\nRun `palyra browser setup` or `palyra browser start --setup` to create local browser prerequisites, then restart the gateway with `palyra gateway run` or restart the running gateway service and rerun `palyra browser start`.",
    );

    Err(anyhow::anyhow!(message))
}

fn browser_start_auth_token_warnings(resolved: &BrowserResolvedConfig) -> Vec<String> {
    if !resolved.token_from_cli_only {
        return Vec::new();
    }
    vec![browser_gateway_auth_token_setup_warning(resolved.config_path.as_deref())]
}

fn ensure_browser_gateway_auth_token_alignment(
    policy: &BrowserPolicySnapshot,
    metadata: Option<&BrowserServiceMetadata>,
    action: &str,
    config_path: Option<&str>,
) -> Result<()> {
    if metadata.is_some_and(|entry| entry.auth_token_configured) && !policy.auth_token_configured {
        anyhow::bail!(
            "palyra browser {action} cannot use the CLI-managed browser service because browserd was started with an auth token, but the gateway has no browser service token configured. {}",
            browser_gateway_auth_token_setup_warning(config_path)
        );
    }
    Ok(())
}

fn browser_gateway_auth_token_setup_warning(config_path: Option<&str>) -> String {
    let configure_command = browser_service_auth_token_command(config_path);
    format!(
        "Set `tool_call.browser_service.auth_token` to the same token with `{configure_command}`. Replace `<shared-browser-token>` with the token used by browserd; do not paste the placeholder literally. Restart the gateway with `palyra gateway run` or restart the running gateway service, then rerun gateway-mediated browser commands such as `palyra browser open`."
    )
}

fn browser_setup_gateway_next_step() -> String {
    format!(
        "Gateway reload required before agent/browser tools can use this config. If no gateway is running, run `{}`. If a gateway is already running, stop and restart that gateway process or service. Then verify with `{}` and rerun gateway-mediated browser commands such as `palyra browser open`.",
        browser_setup_gateway_restart_command(),
        browser_setup_gateway_verify_command()
    )
}

fn browser_setup_gateway_restart_command() -> String {
    "palyra gateway run".to_owned()
}

fn browser_setup_gateway_verify_command() -> String {
    "palyra browser status --json".to_owned()
}

fn browser_setup_gateway_reload_warning(config_path: &str) -> String {
    format!(
        "browser setup wrote gateway browser prerequisites to {config_path}. {}",
        browser_setup_gateway_next_step()
    )
}

fn browser_service_auth_token_command(config_path: Option<&str>) -> String {
    let mut command = "palyra config set".to_owned();
    if let Some(path) = config_path.and_then(normalize_optional_text) {
        command.push_str(" --path ");
        command.push_str(&quote_cli_argument(path));
    }
    command.push_str(
        " --key tool_call.browser_service.auth_token --value '\"<shared-browser-token>\"'",
    );
    command
}

fn ensure_browser_service_enabled(
    policy: &BrowserPolicySnapshot,
    action: &str,
    config_path: Option<&str>,
) -> Result<()> {
    if policy.configured_enabled {
        return Ok(());
    }
    let enable_command = browser_service_enable_command(config_path);
    anyhow::bail!(
        "browser service is disabled (tool_call.browser_service.enabled=false).\nNext steps:\n1. Run `palyra browser setup` to configure local browser prerequisites, or enable the gateway browser service with `{enable_command}`.\n2. Ensure `tool_call.allowed_tools` includes the `palyra.browser.*` tools for gateway-mediated agent browsing.\n3. Restart the gateway with `palyra gateway run` or restart the running gateway service.\n4. Rerun `palyra browser {action}`."
    );
}

fn browser_status_control_plane_policy_snapshot() -> BrowserControlPlaneSnapshot {
    BrowserControlPlaneSnapshot {
        reachable: false,
        browser_enabled: None,
        error: Some(
            "authenticated gateway diagnostics skipped by `palyra browser status` to avoid sending admin credentials; use `palyra status --json` for authenticated runtime diagnostics"
                .to_owned(),
        ),
        auth_probe_skipped: true,
    }
}

fn browser_status_warnings(
    policy: &BrowserPolicySnapshot,
    control_plane: &BrowserControlPlaneSnapshot,
    browserd_reachable: bool,
    browserd_healthy: bool,
    metadata: Option<&BrowserServiceMetadata>,
    config_path: Option<&str>,
) -> Vec<String> {
    let mut warnings = Vec::new();
    if metadata.is_some_and(|entry| entry.auth_token_configured) && !policy.auth_token_configured {
        warnings.push(format!(
            "CLI-managed browserd was started with an auth token, but no gateway browser service token is configured. {}",
            browser_gateway_auth_token_setup_warning(config_path)
        ));
    }
    if browserd_reachable && !browserd_healthy && metadata.is_none() {
        warnings.push(
            "browserd is reachable, but no CLI lifecycle metadata exists; the service may have been started outside `palyra browser start` or a previous stop failed before cleanup"
                .to_owned(),
        );
    }
    if browserd_reachable
        && !browserd_healthy
        && metadata.is_some_and(|entry| !process_is_running(entry.pid))
    {
        warnings.push(
            "browserd is reachable, but the CLI-managed metadata pid is not running; the endpoint may be owned by an unmanaged or stale browser service"
                .to_owned(),
        );
    }
    if policy.configured_enabled
        && control_plane.reachable
        && control_plane.browser_enabled == Some(false)
    {
        warnings.push(
            "browser service is enabled in the local config, but the running gateway still reports tool_call.browser_service.enabled=false; restart the gateway with `palyra gateway run` or restart the running gateway service before using browser action commands"
                .to_owned(),
        );
    }
    if policy.configured_enabled
        && browserd_reachable
        && !control_plane.reachable
        && !control_plane.auth_probe_skipped
    {
        warnings.push(
            "browserd is reachable, but the gateway runtime policy could not be verified; if browser service was enabled while the gateway was already running, restart the gateway before using browser action commands"
                .to_owned(),
        );
    }
    if policy.configured_enabled && !policy.browser_tools_allowlisted {
        warnings.push(format!(
            "browser service is enabled, but gateway-mediated agent browser tools are not fully allowlisted. Add the missing tools to `tool_call.allowed_tools` ({}) and restart the gateway before expecting agents to use browser actions.",
            browser_missing_tools_summary(policy.missing_browser_tools.as_slice())
        ));
    }
    warnings
}

fn browser_port_diagnostic_warnings(
    diagnostics: &[BrowserPortDiagnostic],
    health_ok: bool,
    grpc_ok: bool,
) -> Vec<String> {
    diagnostics
        .iter()
        .filter(|diagnostic| {
            !diagnostic.bind_available
                && ((diagnostic.label == "health" && !health_ok)
                    || (diagnostic.label == "grpc" && !grpc_ok))
        })
        .map(|diagnostic| {
            format!(
                "browser {} port {} on {} is occupied while {} is not reachable; `palyra browser start --setup` can select and persist free loopback ports. Bind error: {}",
                diagnostic.label,
                diagnostic.port,
                diagnostic.host,
                diagnostic.url,
                diagnostic.bind_error.as_deref().unwrap_or("unknown bind failure")
            )
        })
        .collect()
}

fn effective_browser_lifecycle_running(
    cli_lifecycle_running: bool,
    browserd_reachable: bool,
) -> bool {
    cli_lifecycle_running || browserd_reachable
}

fn browser_profile_prerequisite_warnings(
    policy: &BrowserPolicySnapshot,
    metadata: Option<&BrowserServiceMetadata>,
    lifecycle_running: bool,
    config_path: Option<&str>,
) -> Vec<String> {
    if !policy.configured_enabled
        || browser_profile_state_key_configured(policy, metadata, lifecycle_running)
    {
        return Vec::new();
    }
    vec![browser_profile_state_key_guidance(config_path)]
}

fn ensure_browser_profile_prerequisites(
    policy: &BrowserPolicySnapshot,
    metadata: Option<&BrowserServiceMetadata>,
    lifecycle_running: bool,
    action: &str,
    config_path: Option<&str>,
) -> Result<()> {
    if !action.starts_with("profiles ")
        || browser_profile_state_key_available_for_command(policy, metadata, lifecycle_running)
    {
        return Ok(());
    }
    anyhow::bail!(
        "palyra browser {action} requires browser profile state encryption setup before contacting browserd. {}",
        browser_profile_state_key_guidance(config_path)
    );
}

fn browser_profile_state_key_configured(
    policy: &BrowserPolicySnapshot,
    metadata: Option<&BrowserServiceMetadata>,
    lifecycle_running: bool,
) -> bool {
    policy.profiles_ready
        || (lifecycle_running
            && metadata.is_some_and(|entry| entry.state_encryption_key_configured))
}

fn browser_profile_state_key_available_for_start(policy: &BrowserPolicySnapshot) -> bool {
    policy.profiles_ready
        || policy.state_encryption_key_env_configured
        || policy.state_key_vault_ref_configured
}

fn browser_profile_state_key_available_for_command(
    policy: &BrowserPolicySnapshot,
    metadata: Option<&BrowserServiceMetadata>,
    lifecycle_running: bool,
) -> bool {
    browser_profile_state_key_configured(policy, metadata, lifecycle_running)
        || policy.state_key_vault_ref_configured
}

fn browser_policy_with_lifecycle_profile_readiness(
    mut policy: BrowserPolicySnapshot,
    metadata: Option<&BrowserServiceMetadata>,
    lifecycle_running: bool,
) -> BrowserPolicySnapshot {
    if browser_profile_state_key_configured(&policy, metadata, lifecycle_running) {
        policy.profiles_ready = true;
    }
    policy
}

fn browser_profile_state_key_guidance(config_path: Option<&str>) -> String {
    let configure_command = browser_state_key_configure_command(config_path);
    format!(
        "Browser profiles require {BROWSERD_STATE_ENCRYPTION_KEY_ENV} in the running browserd process environment. Store a stable base64-encoded 32-byte key with `{configure_command}` or set it in the browserd environment, then restart browserd through `palyra browser start`, the desktop supervisor, or the test harness launcher. Run `palyra browser status` and confirm profiles_ready=true before using `palyra browser profiles ...`."
    )
}

fn browser_state_key_configure_command(config_path: Option<&str>) -> String {
    let mut command =
        "palyra secrets configure browser-state-key global browser_state_key --value-stdin"
            .to_owned();
    if let Some(path) = config_path.and_then(normalize_optional_text) {
        command.push_str(" --path ");
        command.push_str(&quote_cli_argument(path));
    }
    command
}

fn resolve_browserd_state_encryption_key_for_start(
    resolved: &BrowserResolvedConfig,
) -> Result<Option<String>> {
    if let Some(env_key) = env_optional(BROWSERD_STATE_ENCRYPTION_KEY_ENV) {
        validate_browserd_state_encryption_key(
            env_key.as_str(),
            BROWSERD_STATE_ENCRYPTION_KEY_ENV,
        )?;
        return Ok(None);
    }

    let Some(vault_ref) = resolved.state_key_vault_ref.as_deref() else {
        return Ok(None);
    };
    let parsed = VaultRef::parse(vault_ref).with_context(|| {
        format!("failed to parse tool_call.browser_service.state_key_vault_ref `{vault_ref}`")
    })?;
    let secret = open_cli_vault()
        .context("failed to initialize vault runtime for browser state key")?
        .get_secret(&parsed.scope, parsed.key.as_str())
        .with_context(|| format!("failed to read browser state key vault ref `{vault_ref}`"))?;
    let secret = String::from_utf8(secret)
        .with_context(|| format!("browser state key vault ref `{vault_ref}` must be UTF-8 text"))?;
    let trimmed = secret.trim();
    if trimmed.is_empty() {
        anyhow::bail!("browser state key vault ref `{vault_ref}` resolved to an empty value");
    }
    validate_browserd_state_encryption_key(
        trimmed,
        "tool_call.browser_service.state_key_vault_ref",
    )?;
    Ok(Some(trimmed.to_owned()))
}

pub(crate) fn validate_browserd_state_encryption_key(value: &str, source: &str) -> Result<()> {
    let decoded = BASE64_STANDARD.decode(value.trim()).with_context(|| {
        format!("{source} must contain a base64-encoded 32-byte browser state key")
    })?;
    if decoded.len() != BROWSERD_STATE_ENCRYPTION_KEY_LEN {
        anyhow::bail!(
            "{source} must decode to exactly {BROWSERD_STATE_ENCRYPTION_KEY_LEN} bytes for browser profile state encryption"
        );
    }
    Ok(())
}

fn browser_service_enable_command(config_path: Option<&str>) -> String {
    let mut command = "palyra config set".to_owned();
    if let Some(path) = config_path.and_then(normalize_optional_text) {
        command.push_str(" --path ");
        command.push_str(&quote_cli_argument(path));
    }
    command.push_str(" --key tool_call.browser_service.enabled --value true");
    command
}

fn missing_browser_gateway_tools(allowed_tools: &[String]) -> Vec<String> {
    let normalized = allowed_tools
        .iter()
        .map(|tool| tool.trim().to_ascii_lowercase())
        .filter(|tool| !tool.is_empty())
        .collect::<BTreeSet<_>>();
    BROWSER_GATEWAY_TOOL_NAMES
        .iter()
        .filter(|tool| !normalized.contains(**tool))
        .map(|tool| (*tool).to_owned())
        .collect()
}

fn browser_missing_tools_summary(missing_tools: &[String]) -> String {
    const MAX_EXAMPLES: usize = 5;
    let examples =
        missing_tools.iter().take(MAX_EXAMPLES).map(String::as_str).collect::<Vec<_>>().join(", ");
    if missing_tools.len() <= MAX_EXAMPLES {
        return examples;
    }
    format!(
        "{} and {} more; full list is in browser status JSON",
        examples,
        missing_tools.len() - MAX_EXAMPLES
    )
}

fn quote_cli_argument(value: &str) -> String {
    if value.chars().all(|character| {
        character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.' | ':' | '/' | '\\')
    }) {
        return value.to_owned();
    }
    shell_single_quote(value)
}

#[cfg(windows)]
fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(not(windows))]
fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn current_config_path() -> Option<PathBuf> {
    app::current_root_context().and_then(|context| context.config_path().map(Path::to_path_buf))
}

fn load_optional_config_document(path: Option<&Path>) -> Result<Option<toml::Value>> {
    let Some(path) = path else {
        return Ok(None);
    };
    if !path.exists() {
        return Ok(None);
    }
    let (document, _) = load_document_from_existing_path(path)
        .with_context(|| format!("failed to load {}", path.display()))?;
    Ok(Some(document))
}

fn document_string(document: Option<&toml::Value>, path: &str) -> Option<String> {
    document
        .and_then(|document| get_value_at_path(document, path).ok().flatten())
        .and_then(|value| value.as_str().map(str::trim).map(ToOwned::to_owned))
        .filter(|value| !value.is_empty())
}

fn document_value_present(document: Option<&toml::Value>, path: &str) -> bool {
    document.and_then(|document| get_value_at_path(document, path).ok().flatten()).is_some()
}

fn document_bool(document: Option<&toml::Value>, path: &str) -> Option<bool> {
    document
        .and_then(|document| get_value_at_path(document, path).ok().flatten())
        .and_then(|value| value.as_bool())
}

fn document_u64(document: Option<&toml::Value>, path: &str) -> Option<u64> {
    document
        .and_then(|document| get_value_at_path(document, path).ok().flatten())
        .and_then(|value| value.as_integer())
        .and_then(|value| u64::try_from(value).ok())
}

fn document_string_array(document: Option<&toml::Value>, path: &str) -> Vec<String> {
    document
        .and_then(|document| get_value_at_path(document, path).ok().flatten())
        .and_then(toml::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::trim).map(ToOwned::to_owned))
                .filter(|value| !value.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

fn env_optional(name: &str) -> Option<String> {
    env::var(name).ok().map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name).ok().and_then(|value| value.trim().parse::<bool>().ok())
}

fn env_u64(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|value| value.trim().parse::<u64>().ok())
}

fn env_tool_allowlist() -> Option<Vec<String>> {
    env_optional("PALYRA_TOOL_CALL_ALLOWED_TOOLS").map(|value| {
        value
            .split(',')
            .map(str::trim)
            .filter(|tool| !tool.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    })
}

fn normalize_browser_base_url(raw: String, label: &str) -> Result<String> {
    let url = Url::parse(raw.trim()).with_context(|| format!("invalid {label}: {}", raw.trim()))?;
    if !matches!(url.scheme(), "http" | "https") {
        anyhow::bail!("{label} must use http or https");
    }
    if url.host_str().is_none() {
        anyhow::bail!("{label} must include a host");
    }
    if url.path() != "/" && !url.path().is_empty() {
        anyhow::bail!("{label} must not include a path");
    }
    if url.query().is_some() || url.fragment().is_some() {
        anyhow::bail!("{label} must not include a query string or fragment");
    }
    let mut normalized = url;
    normalized.set_path("");
    Ok(normalized.to_string().trim_end_matches('/').to_owned())
}

fn derive_browser_health_base_url(grpc_url: &str) -> String {
    Url::parse(grpc_url)
        .ok()
        .and_then(|mut url| {
            let grpc_port = url.port_or_known_default().unwrap_or(DEFAULT_BROWSER_GRPC_PORT);
            let health_port = if grpc_port == DEFAULT_BROWSER_GRPC_PORT {
                DEFAULT_BROWSER_HEALTH_PORT
            } else {
                grpc_port.saturating_sub(1).max(1)
            };
            url.set_port(Some(health_port)).ok()?;
            url.set_path("");
            Some(url.to_string().trim_end_matches('/').to_owned())
        })
        .unwrap_or_else(|| DEFAULT_BROWSER_HEALTH_BASE_URL.to_owned())
}

fn parse_http_bind_parts(url: &str, label: &str) -> Result<(String, u16)> {
    let parsed = Url::parse(url).with_context(|| format!("invalid {label}: {url}"))?;
    let host = parsed.host_str().context(format!("{label} missing host"))?.to_owned();
    let port = parsed.port_or_known_default().context(format!("{label} missing port"))?;
    Ok((host, port))
}

fn browser_connection_port_diagnostics(
    connection: &BrowserServiceConnection,
) -> Vec<BrowserPortDiagnostic> {
    [("health", connection.health_base_url.as_str()), ("grpc", connection.grpc_url.as_str())]
        .into_iter()
        .filter_map(|(label, url)| browser_port_diagnostic(label, url))
        .collect()
}

fn browser_port_diagnostic(label: &'static str, url: &str) -> Option<BrowserPortDiagnostic> {
    let (host, port) = parse_http_bind_parts(url, "browser diagnostic URL").ok()?;
    let availability = palyra_common::local_runtime_ports::port_availability(host.as_str(), port);
    Some(BrowserPortDiagnostic {
        label,
        url: url.to_owned(),
        host,
        port,
        bind_available: availability.available,
        bind_error: availability.error,
    })
}

fn select_browser_start_fallback_connection(
    resolved: &BrowserResolvedConfig,
) -> Result<BrowserServiceConnection> {
    let (health_host, _) =
        parse_http_bind_parts(resolved.connection.health_base_url.as_str(), "browser health URL")?;
    let (grpc_host, _) =
        parse_http_bind_parts(resolved.connection.grpc_url.as_str(), "browser gRPC URL")?;
    if !palyra_common::local_runtime_ports::is_loopback_host(health_host.as_str())
        || !palyra_common::local_runtime_ports::is_loopback_host(grpc_host.as_str())
    {
        anyhow::bail!(
            "browser port recovery only auto-selects loopback ports, got health host `{health_host}` and gRPC host `{grpc_host}`"
        );
    }
    let ports = palyra_common::local_runtime_ports::select_available_browser_runtime_ports(
        palyra_common::local_runtime_ports::LOCAL_RUNTIME_LOOPBACK_HOST,
    )
    .map_err(anyhow::Error::msg)?;
    Ok(BrowserServiceConnection {
        grpc_url: format!(
            "http://{}:{}",
            palyra_common::local_runtime_ports::LOCAL_RUNTIME_LOOPBACK_HOST,
            ports.grpc
        ),
        health_base_url: format!(
            "http://{}:{}",
            palyra_common::local_runtime_ports::LOCAL_RUNTIME_LOOPBACK_HOST,
            ports.health
        ),
        auth_token: resolved.connection.auth_token.clone(),
    })
}

fn persist_browser_service_connection_urls(
    config_path: Option<&str>,
    connection: &BrowserServiceConnection,
) -> Result<bool> {
    let Some(config_path) = config_path.and_then(normalize_optional_text) else {
        return Ok(false);
    };
    let path = Path::new(config_path);
    if !path.exists() {
        return Ok(false);
    }
    let (mut document, _) = load_document_for_mutation(path)
        .with_context(|| format!("failed to parse {}", path.display()))?;
    set_value_at_path(
        &mut document,
        "tool_call.browser_service.endpoint",
        toml::Value::String(connection.grpc_url.clone()),
    )?;
    set_value_at_path(
        &mut document,
        "tool_call.browser_service.health_base_url",
        toml::Value::String(connection.health_base_url.clone()),
    )?;
    validate_daemon_compatible_document(&document).with_context(|| {
        format!("mutated config {} does not match daemon schema", path.display())
    })?;
    write_document_with_backups(path, &document, 1)
        .with_context(|| format!("failed to persist config {}", path.display()))?;
    Ok(true)
}

fn browser_port_fallback_warning(
    previous: &BrowserServiceConnection,
    fallback: &BrowserServiceConnection,
    config_updated: bool,
) -> String {
    let persistence = if config_updated {
        "updated the active config so gateway hot reload can use the same endpoint"
    } else {
        "could not update an active config path; pass --setup with PALYRA_CONFIG set so gateway-mediated browser tools use the same endpoint"
    };
    format!(
        "configured browser ports were unavailable (health={}, grpc={}); selected free loopback ports (health={}, grpc={}) and {persistence}",
        previous.health_base_url,
        previous.grpc_url,
        fallback.health_base_url,
        fallback.grpc_url
    )
}

fn format_browser_port_diagnostic_summary(diagnostics: &[&BrowserPortDiagnostic]) -> String {
    diagnostics
        .iter()
        .map(|diagnostic| {
            format!(
                "{} {} is unavailable ({})",
                diagnostic.label,
                diagnostic.url,
                diagnostic.bind_error.as_deref().unwrap_or("port is not bindable")
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn resolve_browser_bin_path(bin_path: Option<String>) -> Result<PathBuf> {
    if let Some(path) = bin_path.as_deref().and_then(normalize_optional_text) {
        return Ok(PathBuf::from(path));
    }
    if let Some(path) = env_optional("PALYRA_DESKTOP_BROWSERD_BIN") {
        return Ok(PathBuf::from(path));
    }
    let current_exe =
        env::current_exe().context("failed to resolve current CLI executable path")?;
    let sibling = current_exe.with_file_name(if cfg!(windows) {
        "palyra-browserd.exe"
    } else {
        "palyra-browserd"
    });
    if sibling.exists() {
        return Ok(sibling);
    }
    Ok(PathBuf::from(if cfg!(windows) { "palyra-browserd.exe" } else { "palyra-browserd" }))
}

fn browser_cli_state_dir(create: bool) -> Result<PathBuf> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for browser command"))?;
    let path = root_context.state_root().join(BROWSER_SERVICE_STATE_DIR);
    if create {
        fs::create_dir_all(path.as_path()).with_context(|| {
            format!("failed to create browser CLI state dir {}", path.display())
        })?;
    }
    Ok(path)
}

fn browser_service_metadata_path() -> Result<PathBuf> {
    Ok(browser_cli_state_dir(false)?.join(BROWSER_SERVICE_METADATA_FILE_NAME))
}

fn read_browser_service_metadata() -> Result<Option<BrowserServiceMetadata>> {
    let path = browser_service_metadata_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let payload = fs::read(path.as_path())
        .with_context(|| format!("failed to read browser metadata {}", path.display()))?;
    serde_json::from_slice::<BrowserServiceMetadata>(payload.as_slice())
        .with_context(|| format!("failed to parse browser metadata {}", path.display()))
        .map(Some)
}

fn write_browser_service_metadata(metadata: &BrowserServiceMetadata) -> Result<()> {
    let path = browser_service_metadata_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let payload =
        serde_json::to_vec_pretty(metadata).context("failed to encode browser metadata")?;
    write_file_atomically(path.as_path(), payload.as_slice())
        .with_context(|| format!("failed to write browser metadata {}", path.display()))
}

fn remove_browser_service_metadata() -> Result<()> {
    let path = browser_service_metadata_path()?;
    if path.exists() {
        fs::remove_file(path.as_path())
            .with_context(|| format!("failed to remove browser metadata {}", path.display()))?;
    }
    Ok(())
}

fn process_is_running(pid: u32) -> bool {
    #[cfg(windows)]
    {
        Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .output()
            .ok()
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .is_some_and(|output| output.contains(&format!("\"{pid}\"")))
    }
    #[cfg(not(windows))]
    {
        Command::new("kill")
            .args(["-0", &pid.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .is_ok_and(|status| status.success())
    }
}

fn terminate_process(pid: u32) -> Result<()> {
    #[cfg(windows)]
    {
        let status = Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .status()
            .context("failed to execute taskkill")?;
        if !status.success() {
            anyhow::bail!("taskkill returned non-zero exit status for pid {pid}");
        }
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let status = Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .context("failed to execute kill")?;
        if !status.success() {
            anyhow::bail!("kill returned non-zero exit status for pid {pid}");
        }
        Ok(())
    }
}

fn browser_output_mode() -> BrowserOutputMode {
    if app::current_root_context().is_some_and(|context| context.prefers_json()) {
        BrowserOutputMode::Json
    } else if app::current_root_context().is_some_and(|context| context.prefers_ndjson()) {
        BrowserOutputMode::Ndjson
    } else {
        BrowserOutputMode::Text
    }
}

fn emit_browser_value(value: &Value, text: String, error_context: &'static str) -> Result<()> {
    emit_browser_value_with_json(value, text, error_context, false)
}

fn browser_failure_detail(error: &str) -> String {
    let trimmed = error.trim();
    if trimmed.is_empty() {
        "browser service returned success=false".to_owned()
    } else if let Some(class) = browser_failure_class(trimmed) {
        format!("{class}: {trimmed}")
    } else {
        trimmed.to_owned()
    }
}

fn browser_failure_class(error: &str) -> Option<&'static str> {
    let lower = error.to_ascii_lowercase();
    if lower.contains("private/local") || lower.contains("blocked url scheme") {
        Some("policy_blocked")
    } else if lower.contains("socks5") || lower.contains("proxy") {
        Some("browser_proxy_failed")
    } else if lower.contains("chromium") || lower.contains("tab runtime") {
        Some("browser_runtime_failed")
    } else if lower.contains("navigation returned http") {
        Some("navigation_failed")
    } else if lower.contains("request failed") || lower.contains("error sending request") {
        Some("network_request_failed")
    } else {
        None
    }
}

fn ensure_browser_command_success(command: &str, success: bool, error: &str) -> Result<()> {
    if success {
        return Ok(());
    }
    anyhow::bail!("{command} failed: {}", browser_failure_detail(error))
}

fn ensure_browser_value_success(command: &str, value: &Value) -> Result<()> {
    let success = value.get("success").and_then(Value::as_bool).unwrap_or(true);
    let error = value.get("error").and_then(Value::as_str).unwrap_or("");
    ensure_browser_command_success(command, success, error)
}

fn browser_command_payload_should_emit(mode: BrowserOutputMode, success: bool) -> bool {
    success || matches!(mode, BrowserOutputMode::Text)
}

fn emit_browser_value_with_json(
    value: &Value,
    text: String,
    error_context: &'static str,
    json: bool,
) -> Result<()> {
    let mode = if json { BrowserOutputMode::Json } else { browser_output_mode() };
    emit_browser_value_for_mode(value, text, error_context, mode)
}

fn emit_browser_value_for_mode(
    value: &Value,
    text: String,
    error_context: &'static str,
    mode: BrowserOutputMode,
) -> Result<()> {
    match mode {
        BrowserOutputMode::Json => output::print_json_pretty(value, error_context),
        BrowserOutputMode::Ndjson => output::print_json_line(value, error_context),
        BrowserOutputMode::Text => {
            print!("{text}");
            if !text.ends_with('\n') {
                println!();
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
}

fn emit_browser_snapshot_value(
    value: &Value,
    text: String,
    output_written: bool,
    error_context: &'static str,
    json: bool,
) -> Result<()> {
    let mode = if json { BrowserOutputMode::Json } else { browser_output_mode() };
    if browser_snapshot_emits_json_to_stdout(mode, output_written) {
        let mut redacted = value.clone();
        redact_browser_output_value(&mut redacted, None);
        return output::print_json_pretty(&redacted, error_context);
    }
    emit_browser_value_for_mode(value, text, error_context, mode)
}

fn browser_snapshot_emits_json_to_stdout(mode: BrowserOutputMode, output_written: bool) -> bool {
    matches!(mode, BrowserOutputMode::Text) && !output_written
}

fn format_browser_status_text(payload: &BrowserStatusPayload) -> String {
    let mut lines = vec![format!(
        "browser.status service={} health_ok={} grpc_ok={} lifecycle_running={} grpc_url={} health_url={}",
        payload.service,
        payload.health_ok,
        payload.grpc_ok,
        payload.lifecycle_running,
        payload.grpc_url,
        payload.health_base_url,
    )];
    lines.push(format!(
        "browser.policy enabled={} auth_token_configured={} browser_tools_allowlisted={} missing_browser_tools={} endpoint={} connect_timeout_ms={} request_timeout_ms={} max_screenshot_bytes={} max_title_bytes={}",
        payload.policy.configured_enabled,
        payload.policy.auth_token_configured,
        payload.policy.browser_tools_allowlisted,
        payload.policy.missing_browser_tools.len(),
        payload.policy.endpoint,
        payload
            .policy
            .connect_timeout_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_owned()),
        payload
            .policy
            .request_timeout_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_owned()),
        payload
            .policy
            .max_screenshot_bytes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_owned()),
        payload
            .policy
            .max_title_bytes
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_owned()),
    ));
    lines.push(format!(
        "browser.profile_policy profiles_ready={} state_key_env_configured={} state_key_vault_ref_configured={}",
        payload.policy.profiles_ready,
        payload.policy.state_encryption_key_env_configured,
        payload.policy.state_key_vault_ref_configured,
    ));
    if let Some(metadata) = payload.lifecycle_metadata.as_ref() {
        lines.push(format!(
            "browser.lifecycle pid={} binary={} stdout_log={} stderr_log={}",
            metadata.pid, metadata.binary, metadata.stdout_log_path, metadata.stderr_log_path,
        ));
    }
    lines.push(format!(
        "browser.control_plane reachable={} browser_enabled={} auth_probe_skipped={} error={}",
        payload.control_plane.reachable,
        payload
            .control_plane
            .browser_enabled
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_owned()),
        payload.control_plane.auth_probe_skipped,
        payload.control_plane.error.as_deref().unwrap_or("-"),
    ));
    for warning in &payload.warnings {
        lines.push(format!("browser.warning {warning}"));
    }
    if let Some(error) = payload.grpc_error.as_deref() {
        lines.push(format!("browser.grpc_error {}", sanitize_diagnostic_error(error)));
    }
    if let Some(response) = payload.health_response.as_ref() {
        lines.push(format!(
            "browser.health_response {}",
            serde_json::to_string(response).unwrap_or_else(|_| "{}".to_owned())
        ));
    }
    lines.join("\n")
}

fn format_browser_setup_text(payload: &BrowserSetupPayload) -> String {
    format!(
        "browser.setup config_path={} browser_service_enabled={} auth_token_configured={} auth_token_generated={} state_key_vault_ref={} state_key_generated={} allowed_tools_added={} gateway_reload_required={} gateway_restart_command=\"{}\" gateway_verify_command=\"{}\" gateway_next_step=\"{}\" migrated={}",
        payload.config_path,
        payload.browser_service_enabled,
        payload.auth_token_configured,
        payload.auth_token_generated,
        payload.state_key_vault_ref,
        payload.state_key_generated,
        payload.allowed_tools_added.len(),
        payload.gateway_reload_required,
        payload.gateway_restart_command,
        payload.gateway_verify_command,
        payload.gateway_next_step,
        payload.migrated,
    )
}

fn format_browser_lifecycle_text(payload: &BrowserLifecyclePayload) -> String {
    let mut text = format!(
        "browser.{} running={} pid={} grpc_url={} health_url={} stdout_log={} stderr_log={} detail={}",
        payload.action,
        payload.running,
        payload.pid.map(|value| value.to_string()).unwrap_or_else(|| "-".to_owned()),
        payload.grpc_url,
        payload.health_base_url,
        payload.stdout_log_path.as_deref().unwrap_or("-"),
        payload.stderr_log_path.as_deref().unwrap_or("-"),
        payload.detail,
    );
    for warning in &payload.warnings {
        text.push('\n');
        text.push_str(format!("browser.warning {warning}").as_str());
    }
    text
}

fn write_optional_json_output(
    output: Option<&str>,
    session_id: &str,
    stem: &str,
    value: &Value,
) -> Result<Option<String>> {
    let Some(path) = resolve_output_path(output, session_id, stem, "json", false)? else {
        return Ok(None);
    };
    let mut redacted = value.clone();
    redact_browser_output_value(&mut redacted, None);
    let payload =
        serde_json::to_vec_pretty(&redacted).context("failed to encode browser artifact")?;
    write_artifact_bytes(path.as_path(), payload.as_slice())?;
    Ok(Some(path.display().to_string()))
}

fn write_optional_binary_output(
    output: Option<&str>,
    session_id: &str,
    stem: &str,
    extension: &str,
    payload: Option<&[u8]>,
) -> Result<Option<String>> {
    write_optional_binary_output_for_mode(
        output,
        session_id,
        stem,
        extension,
        payload,
        browser_output_mode(),
    )
}

fn write_optional_binary_output_for_mode(
    output: Option<&str>,
    session_id: &str,
    stem: &str,
    extension: &str,
    payload: Option<&[u8]>,
    mode: BrowserOutputMode,
) -> Result<Option<String>> {
    let Some(payload) = payload else {
        return Ok(None);
    };
    let Some(path) = resolve_output_path(
        output,
        session_id,
        stem,
        extension,
        matches!(mode, BrowserOutputMode::Text),
    )?
    else {
        return Ok(None);
    };
    write_artifact_bytes(path.as_path(), payload)?;
    Ok(Some(path.display().to_string()))
}

fn write_optional_failure_screenshot(
    output: Option<&str>,
    session_id: &str,
    stem: &str,
    payload: Option<&[u8]>,
) -> Result<Option<String>> {
    write_optional_binary_output(output, session_id, stem, "png", payload)
}

fn resolve_output_path(
    output: Option<&str>,
    session_id: &str,
    stem: &str,
    extension: &str,
    allow_default: bool,
) -> Result<Option<PathBuf>> {
    if let Some(output) = output.and_then(normalize_optional_text) {
        return Ok(Some(PathBuf::from(output)));
    }
    if !allow_default {
        return Ok(None);
    }
    let artifact_root = browser_cli_state_dir(true)?
        .join(BROWSER_ARTIFACT_DIR)
        .join(browser_identifier_scope("session", session_id));
    fs::create_dir_all(artifact_root.as_path())
        .with_context(|| format!("failed to create {}", artifact_root.display()))?;
    Ok(Some(artifact_root.join(format!("{stem}.{extension}"))))
}

fn write_artifact_bytes(path: &Path, payload: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent().filter(|value| !value.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    write_file_atomically(path, payload)
        .with_context(|| format!("failed to write browser artifact {}", path.display()))
}

fn maybe_attach_output_path(value: &mut Value, output_path: Option<&String>) {
    if let (Some(map), Some(output_path)) = (value.as_object_mut(), output_path) {
        map.insert("output_path".to_owned(), Value::String(output_path.clone()));
    }
}

fn strip_large_binary_fields(value: &mut Value, wrote_artifact: bool, fields: &[&str]) {
    if !wrote_artifact {
        return;
    }
    let Some(map) = value.as_object_mut() else {
        return;
    };
    for field in fields {
        map.remove(*field);
    }
}

fn bool_option(value: bool) -> Option<bool> {
    value.then_some(true)
}

fn browser_identifier_scope(kind: &'static str, value: &str) -> String {
    if value.trim().is_empty() {
        return format!("{kind}-none");
    }
    let digest = Sha256::digest(value.as_bytes());
    let mut suffix = String::with_capacity(12);
    for byte in &digest[..6] {
        suffix.push_str(format!("{byte:02x}").as_str());
    }
    format!("{kind}-{suffix}")
}

fn redacted_browser_identifier_text(value: Option<&str>, kind: &'static str) -> String {
    value
        .filter(|candidate| !candidate.trim().is_empty())
        .map(|candidate| browser_identifier_scope(kind, candidate))
        .unwrap_or_else(|| "-".to_owned())
}

fn browser_identifier_json_value(value: Option<&str>) -> Value {
    value
        .filter(|candidate| !candidate.trim().is_empty())
        .map(|candidate| Value::String(candidate.to_owned()))
        .unwrap_or(Value::Null)
}

fn browser_session_handle_text(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|candidate| !candidate.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| "-".to_owned())
}

fn normalize_session_scoped_output(value: &mut Value, requested_session_id: &str) {
    let requested = requested_session_id.trim();
    if requested.is_empty() {
        return;
    }
    let Some(map) = value.as_object_mut() else {
        return;
    };
    let Some(returned) = map
        .get("session_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    if returned == requested {
        return;
    }
    map.insert("runtime_session_id".to_owned(), Value::String(returned.to_owned()));
    map.insert("session_id".to_owned(), Value::String(requested.to_owned()));
}

fn browser_canonical_session_handle_text(value: Option<&common_v1::CanonicalId>) -> String {
    browser_session_handle_text(value.map(|entry| entry.ulid.as_str()))
}

fn format_browser_session_summary_text(session: &browser_v1::BrowserSessionSummary) -> String {
    format!(
        "session session_id={} principal={} channel={} tabs={} has_active_tab={} private_targets={} downloads={} has_profile={}",
        browser_canonical_session_handle_text(session.session_id.as_ref()),
        empty_as_dash(session.principal.as_str()),
        empty_as_dash(session.channel.as_str()),
        session.tab_count,
        session.active_tab_id.is_some(),
        session.allow_private_targets,
        session.downloads_enabled,
        session.profile_id.is_some(),
    )
}

fn format_browser_console_text(
    session_id: &str,
    entries: &[browser_v1::BrowserConsoleEntry],
    truncated: bool,
    output_path: Option<&str>,
) -> String {
    let mut text = format!(
        "browser.console session_id={} entries={} truncated={} output={}",
        browser_session_handle_text(Some(session_id)),
        entries.len(),
        truncated,
        output_path.unwrap_or("-"),
    );
    for (index, entry) in entries.iter().enumerate() {
        text.push('\n');
        text.push_str(
            format!(
                "browser.console.entry index={} severity={} kind={} source={} message={} page_url={}",
                index + 1,
                proto_console_severity_text(entry.severity),
                quoted_browser_text_field(entry.kind.as_str()),
                quoted_browser_text_field(entry.source.as_str()),
                quoted_browser_text_field(entry.message.as_str()),
                quoted_browser_text_field(entry.page_url.as_str()),
            )
            .as_str(),
        );
    }
    text
}

fn quoted_browser_text_field(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        "-".to_owned()
    } else {
        serde_json::to_string(trimmed).unwrap_or_else(|_| "\"<invalid>\"".to_owned())
    }
}

fn browser_identifier_kind_for_key(key: &str) -> Option<&'static str> {
    match key {
        "runtime_session_id" => Some("session"),
        "active_tab_id" | "tab_id" | "closed_tab_id" => Some("tab"),
        "profile_id" | "active_profile_id" => Some("profile"),
        "artifact_id" => Some("artifact"),
        "action_id" => Some("action"),
        _ => None,
    }
}

fn redact_browser_output_value(value: &mut Value, key_context: Option<&str>) {
    match value {
        Value::Object(map) => {
            for (key, entry) in map.iter_mut() {
                redact_browser_output_value(entry, Some(key.as_str()));
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_browser_output_value(item, key_context);
            }
        }
        Value::String(text) => {
            if let Some(kind) = key_context.and_then(browser_identifier_kind_for_key) {
                if !text.trim().is_empty() {
                    *text = browser_identifier_scope(kind, text.as_str());
                }
            }
        }
        _ => {}
    }
}

fn canonical_id_json_value(value: Option<&common_v1::CanonicalId>) -> Value {
    value.map(|entry| Value::String(entry.ulid.clone())).unwrap_or(Value::Null)
}

fn empty_as_dash(value: &str) -> &str {
    if value.trim().is_empty() {
        "-"
    } else {
        value
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn sanitize_screenshot_format(format: &str) -> String {
    let trimmed = format.trim().to_ascii_lowercase();
    match trimmed.as_str() {
        "jpg" | "jpeg" => "jpg".to_owned(),
        "webp" => "webp".to_owned(),
        _ => "png".to_owned(),
    }
}

fn mime_extension(mime_type: Option<&str>) -> &'static str {
    match mime_type.unwrap_or_default() {
        "image/jpeg" => "jpg",
        "image/webp" => "webp",
        _ => "png",
    }
}

fn parse_permission_setting(
    value: Option<&str>,
) -> Result<Option<control_plane::BrowserPermissionSetting>> {
    let Some(value) = value.and_then(normalize_optional_text) else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().as_str() {
        "allow" => Ok(Some(control_plane::BrowserPermissionSetting::Allow)),
        "deny" => Ok(Some(control_plane::BrowserPermissionSetting::Deny)),
        "unspecified" | "default" => {
            Ok(Some(control_plane::BrowserPermissionSetting::Unspecified))
        }
        other => anyhow::bail!(
            "invalid browser permission setting '{other}'; expected allow, deny, unspecified, or default"
        ),
    }
}

fn permission_setting_text(value: Option<control_plane::BrowserPermissionSetting>) -> &'static str {
    match value.unwrap_or(control_plane::BrowserPermissionSetting::Unspecified) {
        control_plane::BrowserPermissionSetting::Allow => "allow",
        control_plane::BrowserPermissionSetting::Deny => "deny",
        control_plane::BrowserPermissionSetting::Unspecified => "unspecified",
    }
}

fn session_summary_value(summary: &browser_v1::BrowserSessionSummary) -> Value {
    json!({
        "session_id": canonical_id_json_value(summary.session_id.as_ref()),
        "principal": summary.principal,
        "channel": summary.channel,
        "created_at_unix_ms": summary.created_at_unix_ms,
        "last_active_unix_ms": summary.last_active_unix_ms,
        "idle_ttl_ms": summary.idle_ttl_ms,
        "age_ms": summary.age_ms,
        "idle_for_ms": summary.idle_for_ms,
        "action_count": summary.action_count,
        "action_log_entries": summary.action_log_entries,
        "tab_count": summary.tab_count,
        "active_tab_id": canonical_id_json_value(summary.active_tab_id.as_ref()),
        "active_tab_url": summary.active_tab_url,
        "active_tab_title": summary.active_tab_title,
        "allow_private_targets": summary.allow_private_targets,
        "downloads_enabled": summary.downloads_enabled,
        "persistence_enabled": summary.persistence_enabled,
        "persistence_id": summary.persistence_id,
        "state_restored": summary.state_restored,
        "profile_id": canonical_id_json_value(summary.profile_id.as_ref()),
        "private_profile": summary.private_profile,
        "action_allowed_domains": summary.action_allowed_domains,
        "permissions": summary.permissions.as_ref().map(session_permissions_value).unwrap_or(Value::Null),
    })
}

fn session_detail_value(detail: &browser_v1::BrowserSessionDetail) -> Value {
    json!({
        "summary": detail.summary.as_ref().map(session_summary_value).unwrap_or(Value::Null),
        "effective_budget": detail.effective_budget.as_ref().map(session_budget_value).unwrap_or(Value::Null),
        "tabs": detail.tabs.iter().map(browser_tab_value).collect::<Vec<_>>(),
    })
}

fn session_budget_value(budget: &browser_v1::SessionBudget) -> Value {
    json!({
        "max_navigation_timeout_ms": budget.max_navigation_timeout_ms,
        "max_session_lifetime_ms": budget.max_session_lifetime_ms,
        "max_screenshot_bytes": budget.max_screenshot_bytes,
        "max_response_bytes": budget.max_response_bytes,
        "max_action_timeout_ms": budget.max_action_timeout_ms,
        "max_type_input_bytes": budget.max_type_input_bytes,
        "max_actions_per_session": budget.max_actions_per_session,
        "max_actions_per_window": budget.max_actions_per_window,
        "action_rate_window_ms": budget.action_rate_window_ms,
        "max_action_log_entries": budget.max_action_log_entries,
        "max_observe_snapshot_bytes": budget.max_observe_snapshot_bytes,
        "max_visible_text_bytes": budget.max_visible_text_bytes,
        "max_network_log_entries": budget.max_network_log_entries,
        "max_network_log_bytes": budget.max_network_log_bytes,
    })
}

fn session_permissions_value(permissions: &browser_v1::SessionPermissions) -> Value {
    json!({
        "camera": proto_permission_setting_text(permissions.camera),
        "microphone": proto_permission_setting_text(permissions.microphone),
        "location": proto_permission_setting_text(permissions.location),
    })
}

fn proto_permission_setting_text(value: i32) -> &'static str {
    match browser_v1::PermissionSetting::try_from(value)
        .unwrap_or(browser_v1::PermissionSetting::Unspecified)
    {
        browser_v1::PermissionSetting::Allow => "allow",
        browser_v1::PermissionSetting::Deny => "deny",
        browser_v1::PermissionSetting::Unspecified => "unspecified",
    }
}

fn browser_tab_value(tab: &browser_v1::BrowserTab) -> Value {
    json!({
        "tab_id": canonical_id_json_value(tab.tab_id.as_ref()),
        "url": tab.url,
        "title": tab.title,
        "active": tab.active,
    })
}

fn cookie_domain_value(cookie_domain: &browser_v1::SessionCookieDomain) -> Value {
    json!({
        "domain": cookie_domain.domain,
        "cookies": cookie_domain
            .cookies
            .iter()
            .map(|cookie| {
                json!({
                    "name": cookie.name,
                    "value": cookie.value,
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn storage_origin_value(storage_origin: &browser_v1::SessionStorageOrigin) -> Value {
    json!({
        "origin": storage_origin.origin,
        "entries": storage_origin
            .entries
            .iter()
            .map(|entry| {
                json!({
                    "key": entry.key,
                    "value": entry.value,
                })
            })
            .collect::<Vec<_>>(),
    })
}

fn action_log_entry_value(entry: &browser_v1::BrowserActionLogEntry) -> Value {
    json!({
        "action_id": entry.action_id,
        "action_name": entry.action_name,
        "selector": entry.selector,
        "success": entry.success,
        "outcome": entry.outcome,
        "error": entry.error,
        "started_at_unix_ms": entry.started_at_unix_ms,
        "completed_at_unix_ms": entry.completed_at_unix_ms,
        "attempts": entry.attempts,
        "page_url": entry.page_url,
    })
}

fn network_log_entry_value(entry: &browser_v1::NetworkLogEntry) -> Value {
    json!({
        "request_url": entry.request_url,
        "status_code": entry.status_code,
        "timing_bucket": entry.timing_bucket,
        "latency_ms": entry.latency_ms,
        "captured_at_unix_ms": entry.captured_at_unix_ms,
        "headers": entry
            .headers
            .iter()
            .map(|header| json!({"name": header.name, "value": header.value}))
            .collect::<Vec<_>>(),
    })
}

fn console_entry_value(entry: &browser_v1::BrowserConsoleEntry) -> Value {
    json!({
        "severity": proto_console_severity_text(entry.severity),
        "kind": entry.kind,
        "message": entry.message,
        "captured_at_unix_ms": entry.captured_at_unix_ms,
        "source": entry.source,
        "stack_trace": entry.stack_trace,
        "page_url": entry.page_url,
    })
}

fn page_diagnostics_value(value: &browser_v1::BrowserPageDiagnostics) -> Value {
    json!({
        "page_url": value.page_url,
        "page_title": value.page_title,
        "console_entry_count": value.console_entry_count,
        "warning_count": value.warning_count,
        "error_count": value.error_count,
        "last_event_unix_ms": value.last_event_unix_ms,
    })
}

fn download_artifact_proto_value(value: &browser_v1::DownloadArtifact) -> Value {
    json!({
        "artifact_id": value.artifact_id.as_ref().map(|entry| entry.ulid.clone()),
        "session_id": value
            .session_id
            .as_ref()
            .map(|entry| browser_identifier_json_value(Some(entry.ulid.as_str())))
            .unwrap_or(Value::Null),
        "profile_id": value.profile_id.as_ref().map(|entry| entry.ulid.clone()),
        "source_url": value.source_url,
        "file_name": value.file_name,
        "mime_type": value.mime_type,
        "size_bytes": value.size_bytes,
        "sha256": value.sha256,
        "created_at_unix_ms": value.created_at_unix_ms,
        "quarantined": value.quarantined,
        "quarantine_reason": value.quarantine_reason,
    })
}

fn proto_console_severity_text(value: i32) -> &'static str {
    match browser_v1::BrowserDiagnosticSeverity::try_from(value)
        .unwrap_or(browser_v1::BrowserDiagnosticSeverity::Unspecified)
    {
        browser_v1::BrowserDiagnosticSeverity::Debug => "debug",
        browser_v1::BrowserDiagnosticSeverity::Warn => "warn",
        browser_v1::BrowserDiagnosticSeverity::Error => "error",
        _ => "info",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        browser_command_payload_should_emit, browser_command_policy_action,
        browser_control_plane_request_timeout, browser_failure_detail,
        browser_identifier_json_value, browser_open_cleanup_status_text, browser_open_output_value,
        browser_service_auth_token_command, browser_service_enable_command,
        browser_service_stop_complete, browser_service_stop_pending_reasons,
        browser_session_handle_text, browser_setup_gateway_reload_warning,
        browser_snapshot_emits_json_to_stdout, browser_start_auth_token_warnings,
        browser_start_readiness_timeout_detail, browser_status_control_plane_policy_snapshot,
        browser_status_warnings, effective_browser_lifecycle_running,
        ensure_browser_command_success, ensure_browser_gateway_auth_token_alignment,
        ensure_browser_service_enabled, ensure_browser_start_preflight,
        ensure_browser_value_success, format_browser_console_text,
        format_browser_session_summary_text, normalize_session_scoped_output,
        redact_browser_output_value, session_summary_value, BrowserControlPlaneSnapshot,
        BrowserOutputMode, BrowserPolicySnapshot, BrowserResolvedConfig, BrowserServiceConnection,
        BrowserServiceMetadata,
    };
    use crate::{args::BrowserCommand, browser_v1, common_v1};
    use palyra_control_plane as control_plane;
    use serde_json::{json, Value};
    use std::{process::Command, time::Duration};

    fn disabled_policy() -> BrowserPolicySnapshot {
        BrowserPolicySnapshot {
            configured_enabled: false,
            auth_token_configured: false,
            endpoint: "http://127.0.0.1:7543".to_owned(),
            connect_timeout_ms: None,
            request_timeout_ms: None,
            max_screenshot_bytes: None,
            max_title_bytes: None,
            state_dir: None,
            browser_tools_allowlisted: false,
            missing_browser_tools: super::BROWSER_GATEWAY_TOOL_NAMES
                .iter()
                .map(|tool| (*tool).to_owned())
                .collect(),
            state_key_vault_ref_configured: false,
            state_encryption_key_env_configured: false,
            profiles_ready: false,
        }
    }

    fn resolved_browser_config_for_test() -> BrowserResolvedConfig {
        BrowserResolvedConfig {
            connection: BrowserServiceConnection {
                grpc_url: "http://127.0.0.1:7543".to_owned(),
                health_base_url: "http://127.0.0.1:7143".to_owned(),
                auth_token: Some("token-a".to_owned()),
            },
            policy: BrowserPolicySnapshot {
                configured_enabled: true,
                auth_token_configured: true,
                endpoint: "http://127.0.0.1:7543".to_owned(),
                connect_timeout_ms: None,
                request_timeout_ms: None,
                max_screenshot_bytes: None,
                max_title_bytes: None,
                state_dir: None,
                browser_tools_allowlisted: true,
                missing_browser_tools: Vec::new(),
                state_key_vault_ref_configured: false,
                state_encryption_key_env_configured: false,
                profiles_ready: false,
            },
            config_path: Some(r"C:\Palyra\palyra.toml".to_owned()),
            state_key_vault_ref: None,
            token_from_cli_only: false,
            token_conflicts_with_gateway_config: false,
        }
    }

    fn browser_metadata_with_token() -> BrowserServiceMetadata {
        BrowserServiceMetadata {
            schema_version: super::BROWSER_SERVICE_METADATA_SCHEMA_VERSION,
            pid: 123,
            binary: "palyra-browserd".to_owned(),
            grpc_url: "http://127.0.0.1:7543".to_owned(),
            health_base_url: "http://127.0.0.1:7143".to_owned(),
            stdout_log_path: "browserd.stdout.log".to_owned(),
            stderr_log_path: "browserd.stderr.log".to_owned(),
            started_at_unix_ms: 1,
            auth_token_configured: true,
            state_encryption_key_configured: false,
        }
    }

    #[test]
    fn browserd_auth_token_is_passed_via_environment_not_argv() {
        let mut command = Command::new("palyra-browserd");

        super::set_browserd_auth_token(&mut command, "browser-token-for-test");

        let args =
            command.get_args().map(|arg| arg.to_string_lossy().into_owned()).collect::<Vec<_>>();
        assert!(
            !args.iter().any(|arg| arg == "--auth-token" || arg == "browser-token-for-test"),
            "browserd token must not be exposed through process arguments: {args:?}"
        );
        assert_eq!(
            command
                .get_envs()
                .find_map(|(key, value)| {
                    (key == super::BROWSERD_AUTH_TOKEN_ENV)
                        .then(|| value.map(|entry| entry.to_string_lossy().into_owned()))
                })
                .flatten()
                .as_deref(),
            Some("browser-token-for-test")
        );
    }

    #[test]
    fn browser_gateway_allowlist_covers_agent_browser_tools() {
        for expected in [
            "palyra.browser.reload",
            "palyra.browser.fill",
            "palyra.browser.upload",
            "palyra.browser.downloads.list",
            "palyra.browser.downloads.get",
        ] {
            assert!(
                super::BROWSER_GATEWAY_TOOL_NAMES.contains(&expected),
                "browser setup/status allowlist should include registered agent tool {expected}"
            );
        }
    }

    #[test]
    fn browser_start_fails_closed_when_service_is_disabled() {
        let error = ensure_browser_service_enabled(&disabled_policy(), "start", None)
            .expect_err("disabled browser service should block start");
        assert!(
            error.to_string().contains("tool_call.browser_service.enabled=false"),
            "disabled-service error should explain the policy gate: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("palyra config set --key tool_call.browser_service.enabled --value true"),
            "disabled-service error should include an exact enable command: {error}"
        );
        assert!(
            error.to_string().contains("Restart the gateway"),
            "disabled-service error should explain that the running gateway must reload the config: {error}"
        );
        assert!(
            error.to_string().contains("Next steps:")
                && !error.to_string().contains("<same-token>")
                && !error.to_string().contains("<shared-browser-token>")
                && !error.to_string().contains("tool_call.browser_service.auth_token"),
            "disabled-service remediation should stay focused on the disabled feature gate: {error}"
        );
    }

    #[test]
    fn browser_enable_command_includes_config_path_when_available() {
        let command = browser_service_enable_command(Some(r"C:\Palyra\palyra.toml"));

        assert_eq!(
            command,
            r"palyra config set --path C:\Palyra\palyra.toml --key tool_call.browser_service.enabled --value true"
        );
    }

    #[test]
    fn browser_enable_command_single_quotes_shell_substitutions() {
        let command = browser_service_enable_command(Some("/tmp/palyra$(touch pwn).toml"));

        assert_eq!(
            command,
            "palyra config set --path '/tmp/palyra$(touch pwn).toml' --key tool_call.browser_service.enabled --value true"
        );
        assert!(
            !command.contains("\"/tmp/palyra$("),
            "browser enable command must not use double quotes around shell-substitution paths: {command}"
        );
    }

    #[test]
    fn browser_enable_command_escapes_single_quotes_for_current_shell() {
        let command = browser_service_enable_command(Some("/tmp/palyra'$(touch pwn).toml"));

        #[cfg(windows)]
        assert_eq!(
            command,
            "palyra config set --path '/tmp/palyra''$(touch pwn).toml' --key tool_call.browser_service.enabled --value true"
        );
        #[cfg(not(windows))]
        assert_eq!(
            command,
            r"palyra config set --path '/tmp/palyra'\''$(touch pwn).toml' --key tool_call.browser_service.enabled --value true"
        );
    }

    #[test]
    fn browser_success_false_is_a_command_failure() {
        let error = ensure_browser_command_success("browser.screenshot", false, "tab crashed")
            .expect_err("success=false browser envelopes must fail the CLI command");

        assert!(
            error.to_string().contains("browser.screenshot failed: tab crashed"),
            "failure should include command and browser service error: {error}"
        );
    }

    #[test]
    fn browser_snapshot_success_false_is_a_command_failure() {
        let payload = json!({
            "success": false,
            "error": "session_not_found",
        });
        let error = ensure_browser_value_success("browser.snapshot", &payload)
            .expect_err("success=false snapshot envelopes must fail the CLI command");

        assert!(
            error.to_string().contains("browser.snapshot failed: session_not_found"),
            "snapshot failure should include command and browser service error: {error}"
        );
    }

    #[test]
    fn browser_open_cleanup_status_summarizes_best_effort_close() {
        assert_eq!(browser_open_cleanup_status_text(None), "not_needed");
        assert_eq!(
            browser_open_cleanup_status_text(Some(&json!({"attempted": true, "closed": true}))),
            "closed"
        );
        assert_eq!(
            browser_open_cleanup_status_text(Some(&json!({
                "attempted": true,
                "closed": false,
                "error": "close failed"
            }))),
            "failed"
        );
    }

    #[test]
    fn browser_failure_detail_classifies_network_proxy_and_policy_errors() {
        let network = ensure_browser_command_success(
            "browser.navigate",
            false,
            "request failed: error sending request for url (https://example.com/)",
        )
        .expect_err("network navigation failure should fail");
        assert!(
            network.to_string().contains("network_request_failed:"),
            "network failure should include an actionable class: {network}"
        );
        assert!(browser_failure_detail("blocked URL scheme file").starts_with("policy_blocked:"));
        assert!(browser_failure_detail("Chromium session SOCKS5 proxy request failed")
            .starts_with("browser_proxy_failed:"));
        assert!(browser_failure_detail("navigation returned HTTP 403")
            .starts_with("navigation_failed:"));
    }

    #[test]
    fn browser_navigate_control_plane_timeout_exceeds_action_timeout() {
        assert_eq!(browser_control_plane_request_timeout(None), None);
        assert_eq!(
            browser_control_plane_request_timeout(Some(10_000)),
            Some(Duration::from_millis(15_000))
        );
        assert_eq!(
            browser_control_plane_request_timeout(Some(1_000)),
            Some(Duration::from_millis(10_000))
        );
    }

    #[test]
    fn browser_actions_fail_closed_when_service_is_disabled() {
        let error = ensure_browser_service_enabled(
            &disabled_policy(),
            "navigate",
            Some(r"C:\Palyra\palyra.toml"),
        )
        .expect_err("disabled browser service should block navigation");
        assert!(
            error.to_string().contains("palyra browser navigate"),
            "disabled-service error should name the blocked browser action: {error}"
        );
        assert!(
            error.to_string().contains(
                r"palyra config set --path C:\Palyra\palyra.toml --key tool_call.browser_service.enabled --value true"
            ),
            "disabled-service error should include a config-specific enable command: {error}"
        );
        assert!(
            !error.to_string().contains("auth_token"),
            "disabled-service error should not front-load auth-token setup: {error}"
        );
        assert!(
            error.to_string().contains("palyra gateway run"),
            "disabled-service error should include a gateway restart command: {error}"
        );
    }

    #[test]
    fn browser_command_policy_action_covers_navigate() {
        let command = BrowserCommand::Navigate {
            session_id: "session".to_owned(),
            url: "https://example.com".to_owned(),
            timeout_ms: None,
            allow_redirects: false,
            max_redirects: None,
            allow_private_targets: false,
        };

        assert_eq!(browser_command_policy_action(&command), Some("navigate"));
    }

    #[test]
    fn browser_start_allows_enabled_policy() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        ensure_browser_service_enabled(&policy, "start", None)
            .expect("enabled browser service should allow start");
    }

    #[test]
    fn browser_start_requires_gateway_auth_token() {
        let mut resolved = resolved_browser_config_for_test();
        resolved.connection.auth_token = None;
        resolved.policy.auth_token_configured = false;

        let error = ensure_browser_start_preflight(&resolved)
            .expect_err("browser start should fail closed without a gateway token");

        assert!(
            error.to_string().contains("tool_call.browser_service.auth_token"),
            "missing-token error should name the required config key: {error}"
        );
        assert!(
            error.to_string().contains(
                r"palyra config set --path C:\Palyra\palyra.toml --key tool_call.browser_service.auth_token"
            ),
            "missing-token error should include the exact config command: {error}"
        );
    }

    #[test]
    fn browser_start_rejects_cli_only_auth_token() {
        let mut resolved = resolved_browser_config_for_test();
        resolved.token_from_cli_only = true;
        resolved.policy.auth_token_configured = true;
        resolved.connection.auth_token = Some("token-from-cli".to_owned());

        let error = ensure_browser_start_preflight(&resolved)
            .expect_err("CLI-only token should not satisfy gateway auth-token setup");

        assert!(
            error.to_string().contains("--token")
                && error.to_string().contains("tool_call.browser_service.auth_token"),
            "CLI-only token error should explain why config is still required: {error}"
        );
    }

    #[test]
    fn browser_start_accepts_configured_gateway_auth_token() {
        let resolved = resolved_browser_config_for_test();

        ensure_browser_start_preflight(&resolved)
            .expect("configured gateway browser token should satisfy browser start preflight");
    }

    #[test]
    fn browser_start_preflight_reports_all_missing_prerequisites() {
        let mut resolved = resolved_browser_config_for_test();
        resolved.connection.auth_token = None;
        resolved.policy.configured_enabled = false;
        resolved.policy.auth_token_configured = false;
        resolved.policy.browser_tools_allowlisted = false;
        resolved.policy.missing_browser_tools =
            super::BROWSER_GATEWAY_TOOL_NAMES.iter().map(|tool| (*tool).to_owned()).collect();

        let error = ensure_browser_start_preflight(&resolved)
            .expect_err("browser start should report every missing setup requirement at once");
        let message = error.to_string();

        assert!(
            message.contains("tool_call.browser_service.enabled"),
            "preflight should mention the disabled browser service gate: {message}"
        );
        assert!(
            message.contains("tool_call.browser_service.auth_token"),
            "preflight should mention the missing gateway auth token: {message}"
        );
        assert!(
            message.contains("tool_call.allowed_tools"),
            "preflight should include the browser tool allowlist readiness check: {message}"
        );
        assert!(
            message.contains("PALYRA_BROWSERD_STATE_ENCRYPTION_KEY")
                && message.contains("palyra secrets configure browser-state-key"),
            "preflight should surface profile encryption setup before a second run: {message}"
        );
        assert!(
            message.contains("palyra gateway run") && message.contains("palyra browser start"),
            "preflight should include the gateway restart and retry workflow: {message}"
        );
    }

    #[test]
    fn browser_start_preflight_allows_non_profile_start_when_core_setup_is_ready() {
        let resolved = resolved_browser_config_for_test();

        ensure_browser_start_preflight(&resolved)
            .expect("profile state-key readiness should warn later, not block browserd startup");
    }

    #[test]
    fn browser_identifier_json_values_remain_reusable() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";

        assert_eq!(browser_identifier_json_value(Some(session_id)).as_str(), Some(session_id));
    }

    #[test]
    fn browser_session_summary_preserves_canonical_session_id() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let summary = browser_v1::BrowserSessionSummary {
            session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
            ..Default::default()
        };

        let value = session_summary_value(&summary);

        assert_eq!(value.get("session_id").and_then(Value::as_str), Some(session_id));
    }

    #[test]
    fn browser_open_output_exposes_reusable_session_id_at_root() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let contract = control_plane::ContractDescriptor {
            contract_version: control_plane::CONTROL_PLANE_CONTRACT_VERSION.to_owned(),
        };
        let session = control_plane::BrowserSessionCreateEnvelope {
            contract: contract.clone(),
            principal: "user:ops".to_owned(),
            channel: Some("cli".to_owned()),
            session_id: Some(session_id.to_owned()),
            created_at_unix_ms: 1,
            effective_budget: None,
            downloads_enabled: false,
            action_allowed_domains: Vec::new(),
            persistence_enabled: false,
            persistence_id: String::new(),
            state_restored: false,
            profile_id: None,
            private_profile: false,
        };
        let navigate = control_plane::BrowserNavigateEnvelope {
            contract,
            session_id: session_id.to_owned(),
            success: true,
            final_url: "https://example.com/".to_owned(),
            status_code: 200,
            title: "Example".to_owned(),
            body_bytes: 256,
            latency_ms: 10,
            error: String::new(),
        };

        let value = browser_open_output_value(session_id, &session, &navigate, None);

        assert_eq!(value.get("session_id").and_then(Value::as_str), Some(session_id));
        assert_eq!(value.pointer("/session/session_id").and_then(Value::as_str), Some(session_id));
    }

    #[test]
    fn browser_session_list_text_preserves_reusable_session_id() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let line = format_browser_session_summary_text(&browser_v1::BrowserSessionSummary {
            session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
            principal: "user:ops".to_owned(),
            channel: "cli".to_owned(),
            tab_count: 1,
            ..Default::default()
        });

        assert!(
            line.contains("session_id=01ARZ3NDEKTSV4RRFFQ69G5FAV"),
            "session list text should preserve the canonical reusable session handle: {line}"
        );
    }

    #[test]
    fn browser_console_text_includes_entry_messages() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let text = format_browser_console_text(
            session_id,
            &[
                browser_v1::BrowserConsoleEntry {
                    severity: browser_v1::BrowserDiagnosticSeverity::Info as i32,
                    kind: "console".to_owned(),
                    source: "console.log".to_owned(),
                    message: "local page loaded".to_owned(),
                    page_url: "http://127.0.0.1:5177/".to_owned(),
                    ..Default::default()
                },
                browser_v1::BrowserConsoleEntry {
                    severity: browser_v1::BrowserDiagnosticSeverity::Warn as i32,
                    kind: "console".to_owned(),
                    source: "console.warn".to_owned(),
                    message: "clicked Palyra".to_owned(),
                    page_url: "http://127.0.0.1:5177/".to_owned(),
                    ..Default::default()
                },
            ],
            false,
            None,
        );

        assert!(text.contains("browser.console session_id=01ARZ3NDEKTSV4RRFFQ69G5FAV"));
        assert!(text.contains("entries=2"));
        assert!(text.contains("severity=info"));
        assert!(text.contains("source=\"console.log\""));
        assert!(text.contains("message=\"local page loaded\""));
        assert!(text.contains("severity=warn"));
        assert!(text.contains("message=\"clicked Palyra\""));
    }

    #[test]
    fn browser_output_redaction_preserves_reusable_session_id() {
        let session_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let runtime_session_id = "session-b66347f61acd";
        let mut value = json!({
            "session_id": session_id,
            "runtime_session_id": runtime_session_id,
            "active_tab_id": "tab-secret-value",
        });

        redact_browser_output_value(&mut value, None);

        assert_eq!(value.get("session_id").and_then(Value::as_str), Some(session_id));
        assert!(
            value
                .get("runtime_session_id")
                .and_then(Value::as_str)
                .is_some_and(|value| value.starts_with("session-")),
            "runtime session id should stay redacted: {value}"
        );
        assert!(
            value
                .get("active_tab_id")
                .and_then(Value::as_str)
                .is_some_and(|value| value.starts_with("tab-")),
            "tab id should stay redacted: {value}"
        );
    }

    #[test]
    fn session_scoped_text_preserves_requested_session_id() {
        let requested = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let text = format!(
            "browser.screenshot session_id={}",
            browser_session_handle_text(Some(requested))
        );

        assert!(text.contains("session_id=01ARZ3NDEKTSV4RRFFQ69G5FAV"), "{text}");
        assert!(!text.contains("runtime_session_id="), "{text}");
    }

    #[test]
    fn browser_snapshot_text_mode_uses_stdout_json_without_output_file() {
        assert!(browser_snapshot_emits_json_to_stdout(BrowserOutputMode::Text, false));
        assert!(!browser_snapshot_emits_json_to_stdout(BrowserOutputMode::Text, true));
        assert!(!browser_snapshot_emits_json_to_stdout(BrowserOutputMode::Json, false));
        assert!(!browser_snapshot_emits_json_to_stdout(BrowserOutputMode::Ndjson, false));
    }

    #[test]
    fn local_json_binary_output_skips_default_artifact() -> anyhow::Result<()> {
        let skipped = super::write_optional_binary_output_for_mode(
            None,
            "session-01",
            "screenshot",
            "png",
            Some(b"sensitive image bytes"),
            BrowserOutputMode::Json,
        )?;
        assert_eq!(skipped, None);

        let temp = tempfile::tempdir()?;
        let explicit_path = temp.path().join("screenshot.png");
        let written = super::write_optional_binary_output_for_mode(
            Some(explicit_path.to_string_lossy().as_ref()),
            "session-01",
            "screenshot",
            "png",
            Some(b"sensitive image bytes"),
            BrowserOutputMode::Json,
        )?;

        assert_eq!(written.as_deref(), Some(explicit_path.to_string_lossy().as_ref()));
        assert_eq!(std::fs::read(explicit_path)?, b"sensitive image bytes");
        Ok(())
    }

    #[test]
    fn structured_browser_failures_skip_domain_payload_before_root_error() {
        assert!(browser_command_payload_should_emit(BrowserOutputMode::Text, false));
        assert!(browser_command_payload_should_emit(BrowserOutputMode::Json, true));
        assert!(!browser_command_payload_should_emit(BrowserOutputMode::Json, false));
        assert!(!browser_command_payload_should_emit(BrowserOutputMode::Ndjson, false));
    }

    #[test]
    fn session_scoped_output_preserves_requested_session_id() {
        let requested = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let runtime = "session-b66347f61acd";
        let mut value = json!({
            "session_id": runtime,
            "success": true,
        });

        normalize_session_scoped_output(&mut value, requested);

        assert_eq!(value.get("session_id").and_then(Value::as_str), Some(requested));
        assert_eq!(value.get("runtime_session_id").and_then(Value::as_str), Some(runtime));
    }

    #[test]
    fn browser_profile_prerequisite_warning_mentions_state_key() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        let warnings = super::browser_profile_prerequisite_warnings(
            &policy,
            None,
            false,
            Some(r"C:\Palyra\palyra.toml"),
        );

        assert!(
            warnings.iter().any(|warning| {
                warning.contains("PALYRA_BROWSERD_STATE_ENCRYPTION_KEY")
                    && warning.contains("palyra secrets configure browser-state-key")
                    && warning.contains("palyra browser status")
            }),
            "missing browser profile key should produce an actionable warning: {warnings:?}"
        );
    }

    #[test]
    fn browser_profiles_fail_before_gateway_when_state_key_is_missing() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;

        let error = super::ensure_browser_profile_prerequisites(
            &policy,
            None,
            false,
            "profiles list",
            Some(r"C:\Palyra\palyra.toml"),
        )
        .expect_err("profile commands should fail before browserd without a state key");

        assert!(
            error.to_string().contains("PALYRA_BROWSERD_STATE_ENCRYPTION_KEY")
                && error.to_string().contains("palyra secrets configure browser-state-key")
                && error.to_string().contains("palyra browser status"),
            "profile prerequisite error should include state key setup guidance: {error}"
        );
    }

    #[test]
    fn browser_profile_prerequisites_allow_vault_ref_to_reach_runtime() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        policy.state_key_vault_ref_configured = true;

        super::ensure_browser_profile_prerequisites(
            &policy,
            None,
            false,
            "profiles list",
            Some(r"C:\Palyra\palyra.toml"),
        )
        .expect(
            "profile commands should contact browserd when a vault-backed state key is configured",
        );
    }

    #[test]
    fn browser_profile_status_does_not_mark_vault_ref_alone_ready() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        policy.state_key_vault_ref_configured = true;

        let hydrated = super::browser_policy_with_lifecycle_profile_readiness(policy, None, false);
        assert!(
            !hydrated.profiles_ready,
            "status policy must not treat a configured vault ref as a running browserd key"
        );

        let warnings = super::browser_profile_prerequisite_warnings(
            &hydrated,
            None,
            false,
            Some(r"C:\Palyra\palyra.toml"),
        );
        assert!(
            warnings.iter().any(|warning| {
                warning.contains("profiles_ready=true")
                    && warning.contains("test harness launcher")
                    && !warning.contains("state_key_vault_ref_configured=true")
            }),
            "vault-only profile config should produce runtime readiness guidance: {warnings:?}"
        );
    }

    #[test]
    fn browser_start_preflight_accepts_vault_ref_configuration() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        policy.state_key_vault_ref_configured = true;

        assert!(
            super::browser_profile_state_key_available_for_start(&policy),
            "start preflight should accept a configured vault ref because start can inject it"
        );
    }

    #[test]
    fn browser_profile_prerequisites_accept_running_lifecycle_state_key() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        let mut metadata = browser_metadata_with_token();
        metadata.state_encryption_key_configured = true;

        super::ensure_browser_profile_prerequisites(
            &policy,
            Some(&metadata),
            true,
            "profiles list",
            Some(r"C:\Palyra\palyra.toml"),
        )
        .expect("running CLI-managed browserd state key should satisfy profile preflight");

        let hydrated =
            super::browser_policy_with_lifecycle_profile_readiness(policy, Some(&metadata), true);
        assert!(
            hydrated.profiles_ready,
            "status policy should expose profile readiness from lifecycle metadata"
        );
    }

    #[test]
    fn browser_profile_prerequisite_warning_is_clear_when_ready() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        policy.state_encryption_key_env_configured = true;
        policy.profiles_ready = true;

        assert!(super::browser_profile_prerequisite_warnings(&policy, None, false, None).is_empty());
    }

    #[test]
    fn browser_status_control_plane_probe_skips_admin_credentials() {
        let snapshot = browser_status_control_plane_policy_snapshot();

        assert!(!snapshot.reachable);
        assert!(snapshot.auth_probe_skipped);
        assert!(snapshot.browser_enabled.is_none());
        assert!(
            snapshot
                .error
                .as_deref()
                .is_some_and(|error| error.contains("avoid sending admin credentials")),
            "skipped browser status probe should explain why authenticated diagnostics are absent: {snapshot:?}"
        );
    }

    #[test]
    fn browser_status_skipped_control_plane_probe_does_not_emit_restart_warning() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        policy.browser_tools_allowlisted = true;
        policy.missing_browser_tools = Vec::new();
        let warnings = browser_status_warnings(
            &policy,
            &browser_status_control_plane_policy_snapshot(),
            true,
            true,
            None,
            None,
        );

        assert!(
            !warnings
                .iter()
                .any(|warning| warning.contains("gateway runtime policy could not be verified")),
            "skipping authenticated diagnostics should not emit the stale-gateway warning by itself: {warnings:?}"
        );
    }

    #[test]
    fn browserd_state_key_validation_requires_base64_32_bytes() {
        super::validate_browserd_state_encryption_key(
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
            "test",
        )
        .expect("32-byte base64 key should be accepted");

        let error = super::validate_browserd_state_encryption_key("dG9vLXNob3J0", "test")
            .expect_err("short key should be rejected");

        assert!(
            error.to_string().contains("exactly 32 bytes"),
            "invalid browser state key should fail before browserd start: {error}"
        );
    }

    #[test]
    fn browser_status_warns_when_gateway_policy_is_stale() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        policy.browser_tools_allowlisted = true;
        policy.missing_browser_tools = Vec::new();
        let warnings = browser_status_warnings(
            &policy,
            &BrowserControlPlaneSnapshot {
                reachable: true,
                browser_enabled: Some(false),
                error: None,
                auth_probe_skipped: false,
            },
            true,
            true,
            None,
            None,
        );

        assert!(
            warnings.iter().any(|warning| {
                warning.contains("running gateway")
                    && warning.contains("tool_call.browser_service.enabled=false")
                    && warning.contains("restart the gateway")
            }),
            "stale gateway policy should produce a restart warning: {warnings:?}"
        );
    }

    #[test]
    fn browser_status_treats_reachable_unmanaged_browserd_as_running() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        policy.browser_tools_allowlisted = true;
        policy.missing_browser_tools = Vec::new();

        assert!(
            effective_browser_lifecycle_running(false, true),
            "reachable browserd should not be rendered as lifecycle_running=false"
        );

        let warnings = browser_status_warnings(
            &policy,
            &BrowserControlPlaneSnapshot {
                reachable: true,
                browser_enabled: Some(true),
                error: None,
                auth_probe_skipped: false,
            },
            true,
            true,
            None,
            None,
        );

        assert!(
            warnings.iter().all(|warning| !warning.contains("no CLI lifecycle metadata")),
            "healthy desktop-managed browserd should not warn about missing CLI lifecycle metadata: {warnings:?}"
        );

        let degraded_warnings = browser_status_warnings(
            &policy,
            &BrowserControlPlaneSnapshot {
                reachable: true,
                browser_enabled: Some(true),
                error: None,
                auth_probe_skipped: false,
            },
            true,
            false,
            None,
            None,
        );

        assert!(
            degraded_warnings
                .iter()
                .any(|warning| warning.contains("no CLI lifecycle metadata")),
            "partially reachable unmanaged browserd should still produce a lifecycle warning: {degraded_warnings:?}"
        );
    }

    #[test]
    fn browser_start_timeout_detail_prefers_authenticated_grpc_failure() {
        let detail = browser_start_readiness_timeout_detail(
            Some("health endpoint refused connection"),
            Some("failed to call browser ListSessions: unauthenticated"),
        );

        assert!(detail.contains("authenticated gRPC readiness failed"));
        assert!(detail.contains("failed to call browser ListSessions"));
        assert!(!detail.contains("health endpoint refused connection"));
    }

    #[test]
    fn browser_start_timeout_detail_reports_health_failure_without_grpc_probe() {
        let detail = browser_start_readiness_timeout_detail(
            Some("failed to reach browser health endpoint"),
            None,
        );

        assert_eq!(detail, "health check failed: failed to reach browser health endpoint");
    }

    #[test]
    fn browser_stop_wait_requires_process_exit_and_unreachable_health() {
        assert!(browser_service_stop_complete(false, false, true));
        assert!(!browser_service_stop_complete(true, false, true));
        assert!(!browser_service_stop_complete(false, true, true));
        assert!(!browser_service_stop_complete(false, false, false));

        let reasons = browser_service_stop_pending_reasons(
            42,
            true,
            true,
            false,
            "http://127.0.0.1:7143/",
            "http://127.0.0.1:7543/",
        );
        assert!(reasons.iter().any(|reason| reason.contains("pid 42")));
        assert!(reasons.iter().any(|reason| { reason.contains("http://127.0.0.1:7143/healthz") }));
        assert!(reasons.iter().any(|reason| { reason.contains("configured browser ports") }));
    }

    #[test]
    fn browser_status_warns_when_agent_browser_tools_are_not_allowlisted() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        policy.missing_browser_tools =
            vec!["palyra.browser.navigate".to_owned(), "palyra.browser.screenshot".to_owned()];

        let warnings = browser_status_warnings(
            &policy,
            &BrowserControlPlaneSnapshot {
                reachable: true,
                browser_enabled: Some(true),
                error: None,
                auth_probe_skipped: false,
            },
            true,
            true,
            None,
            Some(r"C:\Palyra\palyra.toml"),
        );

        assert!(
            warnings.iter().any(|warning| {
                warning.contains("tool_call.allowed_tools")
                    && warning.contains("palyra.browser.navigate")
                    && warning.contains("restart the gateway")
            }),
            "missing browser tool allowlist should produce actionable guidance: {warnings:?}"
        );
    }

    #[test]
    fn browser_auth_token_command_includes_config_path_and_placeholder() {
        let command = browser_service_auth_token_command(Some(r"C:\Palyra\palyra.toml"));

        assert_eq!(
            command,
            r#"palyra config set --path C:\Palyra\palyra.toml --key tool_call.browser_service.auth_token --value '"<shared-browser-token>"'"#
        );
    }

    #[test]
    fn browser_setup_reload_warning_names_gateway_restart_and_retry() {
        let warning = browser_setup_gateway_reload_warning(r"C:\Palyra\palyra.toml");

        assert!(warning.contains("C:\\Palyra\\palyra.toml"), "{warning}");
        assert!(warning.contains("palyra gateway run"), "{warning}");
        assert!(warning.contains("palyra browser open"), "{warning}");
    }

    #[test]
    fn browser_start_warns_for_cli_only_token() {
        let mut resolved = resolved_browser_config_for_test();
        resolved.token_from_cli_only = true;

        let warnings = browser_start_auth_token_warnings(&resolved);

        assert!(
            warnings.iter().any(|warning| {
                warning.contains("tool_call.browser_service.auth_token")
                    && warning.contains("Replace `<shared-browser-token>`")
                    && warning.contains("Restart the gateway")
                    && warning.contains("palyra browser open")
            }),
            "CLI-only browser start token should warn about gateway setup: {warnings:?}"
        );
    }

    #[test]
    fn browser_start_rejects_token_that_conflicts_with_gateway_config() {
        let mut resolved = resolved_browser_config_for_test();
        resolved.token_conflicts_with_gateway_config = true;

        let error = ensure_browser_start_preflight(&resolved)
            .expect_err("conflicting browser start token should be rejected");

        assert!(
            error.to_string().contains("differs from the browser service token"),
            "conflict error should explain the token mismatch: {error}"
        );
    }

    #[test]
    fn browser_status_warns_when_cli_managed_browserd_token_is_not_in_gateway_config() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        let metadata = browser_metadata_with_token();
        let warnings = browser_status_warnings(
            &policy,
            &BrowserControlPlaneSnapshot {
                reachable: true,
                browser_enabled: Some(true),
                error: None,
                auth_probe_skipped: false,
            },
            true,
            true,
            Some(&metadata),
            Some(r"C:\Palyra\palyra.toml"),
        );

        assert!(
            warnings.iter().any(|warning| {
                warning.contains("CLI-managed browserd")
                    && warning.contains("tool_call.browser_service.auth_token")
                    && warning.contains("Replace `<shared-browser-token>`")
                    && warning.contains("palyra browser open")
            }),
            "missing gateway token should produce a setup blocker warning: {warnings:?}"
        );
    }

    #[test]
    fn browser_actions_fail_before_gateway_401_when_cli_managed_token_is_missing_from_config() {
        let mut policy = disabled_policy();
        policy.configured_enabled = true;
        let metadata = browser_metadata_with_token();

        let error = ensure_browser_gateway_auth_token_alignment(
            &policy,
            Some(&metadata),
            "open",
            Some(r"C:\Palyra\palyra.toml"),
        )
        .expect_err("gateway-mediated browser actions should fail before a browserd 401");

        assert!(
            error.to_string().contains("gateway has no browser service token configured")
                && error.to_string().contains("palyra config set"),
            "preflight error should include actionable gateway token setup: {error}"
        );
    }
}
