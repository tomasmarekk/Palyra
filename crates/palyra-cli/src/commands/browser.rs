use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(windows)]
use std::os::windows::process::CommandExt;

use palyra_common::config_system::get_value_at_path;
use palyra_control_plane as control_plane;
use reqwest::{Client as AsyncClient, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tonic::{metadata::MetadataMap, transport::Endpoint, Request};

use crate::args::{
    BrowserPermissionsCommand, BrowserProfilesCommand, BrowserSessionCommand, BrowserTabsCommand,
};
use crate::*;

const DEFAULT_BROWSER_GRPC_URL: &str = "http://127.0.0.1:7543";
const DEFAULT_BROWSER_HEALTH_BASE_URL: &str = DEFAULT_BROWSER_URL;
const DEFAULT_BROWSER_HEALTH_PORT: u16 = 7143;
const BROWSER_SERVICE_METADATA_SCHEMA_VERSION: u32 = 1;
const BROWSER_SERVICE_START_POLL_MS: u64 = 250;
const BROWSER_SERVICE_STATE_DIR: &str = "browser-cli";
const BROWSER_SERVICE_METADATA_FILE_NAME: &str = "browser-service.json";
const BROWSER_SERVICE_STDOUT_LOG_FILE_NAME: &str = "browserd.stdout.log";
const BROWSER_SERVICE_STDERR_LOG_FILE_NAME: &str = "browserd.stderr.log";
const BROWSER_ARTIFACT_DIR: &str = "browser-artifacts";
const BROWSER_CALLER_PRINCIPAL_HEADER: &str = "x-palyra-principal";
const BROWSER_PROBE_PRINCIPAL: &str = "admin:browser-probe";

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
    state_key_vault_ref_configured: bool,
}

#[derive(Debug, Clone)]
struct BrowserResolvedConfig {
    connection: BrowserServiceConnection,
    policy: BrowserPolicySnapshot,
    config_path: Option<String>,
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
}

#[derive(Debug, Clone, Serialize)]
struct BrowserStatusPayload {
    service: &'static str,
    grpc_url: String,
    health_base_url: String,
    health_ok: bool,
    health_response: Option<Value>,
    grpc_ok: bool,
    grpc_error: Option<String>,
    lifecycle_running: bool,
    lifecycle_metadata: Option<BrowserServiceMetadata>,
    config_path: Option<String>,
    policy: BrowserPolicySnapshot,
}

#[derive(Debug, Clone, Serialize)]
struct UnsupportedBrowserCapabilityPayload {
    capability: String,
    session_id: String,
    supported: bool,
    detail: String,
    suggestions: Vec<String>,
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
    match command {
        BrowserCommand::Status { endpoint, health_url, token } => {
            run_browser_status(endpoint, health_url, token).await
        }
        BrowserCommand::Start { bin_path, endpoint, health_url, token, wait_ms } => {
            run_browser_start(bin_path, endpoint, health_url, token, wait_ms).await
        }
        BrowserCommand::Stop => run_browser_stop(),
        BrowserCommand::Open {
            url,
            principal,
            channel,
            allow_private_targets,
            allow_downloads,
            profile_id,
            private_profile,
            timeout_ms,
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
        } => run_browser_navigate(
            session_id,
            url,
            timeout_ms,
            allow_redirects,
            max_redirects,
            allow_private_targets,
        )
        .await,
        BrowserCommand::Click {
            session_id,
            selector,
            max_retries,
            timeout_ms,
            capture_failure_screenshot,
            max_failure_screenshot_bytes,
            output,
        } => {
            run_browser_click(
                session_id,
                selector,
                max_retries,
                timeout_ms,
                capture_failure_screenshot,
                max_failure_screenshot_bytes,
                output,
            )
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
            })
            .await
        }
        BrowserCommand::Screenshot { session_id, max_bytes, format, output } => {
            run_browser_screenshot(session_id, max_bytes, format, output).await
        }
        BrowserCommand::Title { session_id, max_title_bytes } => {
            run_browser_title(session_id, max_title_bytes).await
        }
        BrowserCommand::Network { session_id, limit, include_headers, max_payload_bytes } => {
            run_browser_network(session_id, limit, include_headers, max_payload_bytes).await
        }
        BrowserCommand::Storage { session_id, principal, output } => {
            run_browser_storage(session_id, principal, output).await
        }
        BrowserCommand::Errors { session_id, principal, limit, output } => {
            run_browser_errors(session_id, principal, limit, output).await
        }
        BrowserCommand::Trace { session_id, principal, output } => {
            run_browser_trace(session_id, principal, output).await
        }
        BrowserCommand::Downloads { session_id, limit, quarantined_only } => {
            run_browser_downloads(session_id, limit, quarantined_only).await
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
        } => run_browser_reset_state(
            session_id,
            clear_cookies,
            clear_storage,
            reset_tabs,
            reset_permissions,
        )
        .await,
        BrowserCommand::Console { session_id, output } => emit_unsupported_browser_capability(
            "console",
            session_id.as_str(),
            "Console log export is not available in the current browser backend.",
            &["browser trace", "browser errors", "browser snapshot"],
            output.as_deref(),
        ),
        BrowserCommand::Pdf { session_id, output } => emit_unsupported_browser_capability(
            "pdf",
            session_id.as_str(),
            "PDF export is not available in the current browser backend.",
            &["browser screenshot", "browser snapshot", "browser trace"],
            output.as_deref(),
        ),
        BrowserCommand::Press { session_id, key } => emit_unsupported_browser_capability(
            "press",
            session_id.as_str(),
            format!("Key press '{key}' is not available as a dedicated browser primitive.").as_str(),
            &["browser type", "browser fill"],
            None,
        ),
        BrowserCommand::Select { session_id, selector, value } => emit_unsupported_browser_capability(
            "select",
            session_id.as_str(),
            format!(
                "Select mutation for selector '{selector}' and value '{value}' is not available as a dedicated browser primitive."
            )
            .as_str(),
            &["browser fill", "browser type"],
            None,
        ),
        BrowserCommand::Highlight { session_id, selector } => emit_unsupported_browser_capability(
            "highlight",
            session_id.as_str(),
            format!(
                "Highlight for selector '{selector}' is not available in the current browser backend."
            )
            .as_str(),
            &["browser snapshot", "browser screenshot"],
            None,
        ),
    }
}

async fn run_browser_status(
    endpoint: Option<String>,
    health_url: Option<String>,
    token: Option<String>,
) -> Result<()> {
    let resolved = resolve_browser_config(endpoint, health_url, token)?;
    let metadata = read_browser_service_metadata()?;
    let lifecycle_running = metadata.as_ref().is_some_and(|value| process_is_running(value.pid));
    let health_response =
        fetch_browser_health(resolved.connection.health_base_url.as_str()).await.ok();
    let grpc_error =
        probe_browser_grpc(&resolved.connection).await.err().map(|error| error.to_string());
    let payload = BrowserStatusPayload {
        service: "palyra-browserd",
        grpc_url: resolved.connection.grpc_url,
        health_base_url: resolved.connection.health_base_url,
        health_ok: health_response.is_some(),
        health_response,
        grpc_ok: grpc_error.is_none(),
        grpc_error,
        lifecycle_running,
        lifecycle_metadata: metadata,
        config_path: resolved.config_path,
        policy: resolved.policy,
    };
    let value =
        serde_json::to_value(&payload).context("failed to encode browser status payload")?;
    emit_browser_value(
        &value,
        format_browser_status_text(&payload),
        "failed to encode browser status output",
    )
}

async fn run_browser_start(
    bin_path: Option<String>,
    endpoint: Option<String>,
    health_url: Option<String>,
    token: Option<String>,
    wait_ms: u64,
) -> Result<()> {
    let resolved = resolve_browser_config(endpoint, health_url, token)?;
    if fetch_browser_health(resolved.connection.health_base_url.as_str()).await.is_ok() {
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
        };
        let value =
            serde_json::to_value(&payload).context("failed to encode browser lifecycle payload")?;
        return emit_browser_value(
            &value,
            format_browser_lifecycle_text(&payload),
            "failed to encode browser lifecycle output",
        );
    }

    let binary = resolve_browser_bin_path(bin_path)?;
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
        command.arg("--auth-token").arg(auth_token);
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
    };
    write_browser_service_metadata(&metadata)?;

    let deadline = Duration::from_millis(wait_ms.max(BROWSER_SERVICE_START_POLL_MS));
    let started = SystemTime::now();
    loop {
        if fetch_browser_health(resolved.connection.health_base_url.as_str()).await.is_ok() {
            let payload = BrowserLifecyclePayload {
                action: "start".to_owned(),
                running: true,
                pid: Some(metadata.pid),
                grpc_url: resolved.connection.grpc_url,
                health_base_url: resolved.connection.health_base_url,
                stdout_log_path: Some(metadata.stdout_log_path),
                stderr_log_path: Some(metadata.stderr_log_path),
                detail: "browser service started and passed health check".to_owned(),
            };
            let value = serde_json::to_value(&payload)
                .context("failed to encode browser lifecycle payload")?;
            return emit_browser_value(
                &value,
                format_browser_lifecycle_text(&payload),
                "failed to encode browser lifecycle output",
            );
        }
        if started.elapsed().unwrap_or_default() >= deadline {
            anyhow::bail!(
                "browser service did not become healthy within {} ms; inspect {} and {}",
                wait_ms.max(BROWSER_SERVICE_START_POLL_MS),
                stdout_log_path.display(),
                stderr_log_path.display()
            );
        }
        sleep(Duration::from_millis(BROWSER_SERVICE_START_POLL_MS)).await;
    }
}

fn run_browser_stop() -> Result<()> {
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
        };
        let value =
            serde_json::to_value(&payload).context("failed to encode browser lifecycle payload")?;
        return emit_browser_value(
            &value,
            format_browser_lifecycle_text(&payload),
            "failed to encode browser lifecycle output",
        );
    };

    if process_is_running(metadata.pid) {
        terminate_process(metadata.pid)
            .with_context(|| format!("failed to stop browser service process {}", metadata.pid))?;
    }
    remove_browser_service_metadata()?;

    let payload = BrowserLifecyclePayload {
        action: "stop".to_owned(),
        running: false,
        pid: Some(metadata.pid),
        grpc_url: metadata.grpc_url,
        health_base_url: metadata.health_base_url,
        stdout_log_path: Some(metadata.stdout_log_path),
        stderr_log_path: Some(metadata.stderr_log_path),
        detail: "browser service stop requested".to_owned(),
    };
    let value =
        serde_json::to_value(&payload).context("failed to encode browser lifecycle payload")?;
    emit_browser_value(
        &value,
        format_browser_lifecycle_text(&payload),
        "failed to encode browser lifecycle output",
    )
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
    } = args;
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
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
    let payload = json!({
        "session": create,
        "navigate": navigate,
    });
    emit_browser_value(
        &payload,
        format!(
            "browser.open session_id={} success={} final_url={} status_code={}",
            redacted_browser_identifier_text(Some(session_id.as_str()), "session"),
            payload.pointer("/navigate/success").and_then(Value::as_bool).unwrap_or(false),
            payload.pointer("/navigate/final_url").and_then(Value::as_str).unwrap_or("-"),
            payload.pointer("/navigate/status_code").and_then(Value::as_u64).unwrap_or(0)
        ),
        "failed to encode browser open output",
    )
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
            emit_browser_value(
                &value,
                format!(
                    "browser.session.create session_id={} principal={} downloads_enabled={} persistence_enabled={} profile_id={}",
                    redacted_browser_identifier_text(envelope.session_id.as_deref(), "session"),
                    envelope.principal,
                    envelope.downloads_enabled,
                    envelope.persistence_enabled,
                    redacted_browser_identifier_text(envelope.profile_id.as_deref(), "profile")
                ),
                "failed to encode browser session create output",
            )
        }
        BrowserSessionCommand::List { principal, limit } => {
            let resolved = resolve_browser_config(None, None, None)?;
            let mut client = connect_browser_service(&resolved.connection).await?;
            let caller_principal =
                resolve_browser_caller_principal(principal.clone(), app::ConnectionDefaults::USER)?;
            let response = client
                .list_sessions(browser_request(
                    browser_v1::ListSessionsRequest {
                        v: CANONICAL_PROTOCOL_MAJOR,
                        principal: principal.unwrap_or_default(),
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
                text.push_str(
                    format!(
                        "session principal={} channel={} tabs={} has_active_tab={} private_targets={} downloads={} has_profile={}",
                        empty_as_dash(session.principal.as_str()),
                        empty_as_dash(session.channel.as_str()),
                        session.tab_count,
                        session.active_tab_id.is_some(),
                        session.allow_private_targets,
                        session.downloads_enabled,
                        session.profile_id.is_some(),
                    )
                    .as_str(),
                );
            }
            emit_browser_value(&value, text, "failed to encode browser session list output")
        }
        BrowserSessionCommand::Show { session_id, principal } => {
            let detail =
                get_browser_session_detail(session_id.as_str(), principal.as_deref()).await?;
            let value = session_detail_value(&detail);
            let text = format!(
                "browser.session.show session_id={} tabs={} private_targets={} downloads={} profile_id={}",
                value.pointer("/summary/session_id").and_then(Value::as_str).unwrap_or("-"),
                value.pointer("/summary/tab_count").and_then(Value::as_u64).unwrap_or(0),
                value.pointer("/summary/allow_private_targets").and_then(Value::as_bool).unwrap_or(false),
                value.pointer("/summary/downloads_enabled").and_then(Value::as_bool).unwrap_or(false),
                value.pointer("/summary/profile_id").and_then(Value::as_str).unwrap_or("-"),
            );
            emit_browser_value(&value, text, "failed to encode browser session show output")
        }
        BrowserSessionCommand::Inspect {
            session_id,
            principal,
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
                principal.as_deref(),
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
                    redacted_browser_identifier_text(Some(session_id.as_str()), "session"),
                    value.get("cookies").and_then(Value::as_array).map_or(0, Vec::len),
                    value.get("storage").and_then(Value::as_array).map_or(0, Vec::len),
                    value.get("action_log").and_then(Value::as_array).map_or(0, Vec::len),
                    value.get("network_log").and_then(Value::as_array).map_or(0, Vec::len),
                    written.as_deref().unwrap_or("-"),
                ),
                "failed to encode browser session inspect output",
            )
        }
        BrowserSessionCommand::Close { session_id } => {
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
            emit_browser_value(
                &value,
                format!(
                    "browser.session.close session_id={} closed={} reason={}",
                    redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
                    envelope.closed,
                    empty_as_dash(envelope.reason.as_str()),
                ),
                "failed to encode browser session close output",
            )
        }
    }
}

async fn run_browser_profiles_command(command: BrowserProfilesCommand) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
    match command {
        BrowserProfilesCommand::List { principal } => {
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
                redacted_browser_identifier_text(envelope.active_profile_id.as_deref(), "profile",),
            );
            for profile in &envelope.profiles {
                text.push('\n');
                text.push_str(
                    format!(
                        "profile id={} name={} private={} persistence={} active={}",
                        redacted_browser_identifier_text(profile.profile_id.as_deref(), "profile",),
                        profile.name,
                        profile.private_profile,
                        profile.persistence_enabled,
                        profile.active,
                    )
                    .as_str(),
                );
            }
            emit_browser_value(&value, text, "failed to encode browser profiles list output")
        }
        BrowserProfilesCommand::Create {
            principal,
            name,
            theme_color,
            persistence_enabled,
            private_profile,
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
            emit_browser_value(
                &value,
                format!(
                    "browser.profiles.create profile_id={} name={} private={} active={}",
                    redacted_browser_identifier_text(
                        envelope.profile.profile_id.as_deref(),
                        "profile",
                    ),
                    envelope.profile.name,
                    envelope.profile.private_profile,
                    envelope.profile.active,
                ),
                "failed to encode browser profile create output",
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
                    redacted_browser_identifier_text(
                        envelope.profile.profile_id.as_deref(),
                        "profile",
                    ),
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
                    redacted_browser_identifier_text(Some(envelope.profile_id.as_str()), "profile"),
                    envelope.deleted,
                    redacted_browser_identifier_text(
                        envelope.active_profile_id.as_deref(),
                        "profile",
                    ),
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
                    redacted_browser_identifier_text(
                        envelope.profile.profile_id.as_deref(),
                        "profile",
                    ),
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
                redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
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
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser tab open output")?;
            emit_browser_value(
                &value,
                format!(
                    "browser.tabs.open session_id={} tab_id={} success={} status_code={} navigated={}",
                    redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
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
            )
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
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser tab switch output")?;
            emit_browser_value(
                &value,
                format!(
                    "browser.tabs.switch session_id={} active_tab_id={} success={}",
                    redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
                    envelope
                        .active_tab
                        .as_ref()
                        .and_then(|tab| tab.tab_id.as_deref())
                        .map(|value| redacted_browser_identifier_text(Some(value), "tab"))
                        .unwrap_or_else(|| "-".to_owned()),
                    envelope.success,
                ),
                "failed to encode browser tab switch output",
            )
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
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser tab close output")?;
            emit_browser_value(
                &value,
                format!(
                    "browser.tabs.close session_id={} closed_tab_id={} tabs_remaining={} active_tab_id={}",
                    redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
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
            )
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
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
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
    let value =
        serde_json::to_value(&envelope).context("failed to encode browser navigate output")?;
    emit_browser_value(
        &value,
        format!(
            "browser.navigate session_id={} success={} status_code={} final_url={} title={}",
            redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
            envelope.success,
            envelope.status_code,
            empty_as_dash(envelope.final_url.as_str()),
            empty_as_dash(envelope.title.as_str()),
        ),
        "failed to encode browser navigate output",
    )
}

async fn run_browser_click(
    session_id: String,
    selector: String,
    max_retries: Option<u32>,
    timeout_ms: Option<u64>,
    capture_failure_screenshot: bool,
    max_failure_screenshot_bytes: Option<u64>,
    output: Option<String>,
) -> Result<()> {
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
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
    emit_browser_value(
        &value,
        format!(
            "browser.click session_id={} success={} selector={} action_id={} artifact={}",
            redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
            envelope.success,
            selector,
            envelope.action_log.as_ref().map(|entry| entry.action_id.as_str()).unwrap_or("-"),
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser click output",
    )
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
    } = args;
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
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
    emit_browser_value(
        &value,
        format!(
            "browser.{} session_id={} success={} selector={} typed_bytes={} artifact={}",
            if clear_existing { "fill" } else { "type" },
            redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
            envelope.success,
            selector,
            envelope.typed_bytes,
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser type output",
    )
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
    emit_browser_value(
        &value,
        format!(
            "browser.scroll session_id={} success={} scroll_x={} scroll_y={} artifact={}",
            redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
            envelope.success,
            envelope.scroll_x,
            envelope.scroll_y,
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser scroll output",
    )
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
    } = args;
    let context =
        client::control_plane::connect_admin_console(app::ConnectionOverrides::default()).await?;
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
    emit_browser_value(
        &value,
        format!(
            "browser.wait session_id={} success={} waited_ms={} matched_selector={} matched_text={} artifact={}",
            redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
            envelope.success,
            envelope.waited_ms,
            empty_as_dash(envelope.matched_selector.as_str()),
            empty_as_dash(envelope.matched_text.as_str()),
            screenshot_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser wait output",
    )
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
    let written =
        write_optional_json_output(output.as_deref(), session_id.as_str(), "snapshot", &value)?;
    maybe_attach_output_path(&mut value, written.as_ref());
    emit_browser_value(
        &value,
        format!(
            "browser.snapshot session_id={} page_url={} dom_truncated={} text_truncated={} output={}",
            redacted_browser_identifier_text(Some(session_id.as_str()), "session"),
            value.get("page_url").and_then(Value::as_str).unwrap_or("-"),
            value.get("dom_truncated").and_then(Value::as_bool).unwrap_or(false),
            value.get("visible_text_truncated").and_then(Value::as_bool).unwrap_or(false),
            written.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser snapshot output",
    )
}

async fn run_browser_screenshot(
    session_id: String,
    max_bytes: Option<u64>,
    format: Option<String>,
    output: Option<String>,
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
    let suggested_ext = format
        .as_deref()
        .map(sanitize_screenshot_format)
        .unwrap_or_else(|| mime_extension(envelope.mime_type.as_deref()).to_owned());
    let output_path = write_optional_binary_output(
        output.as_deref(),
        session_id.as_str(),
        "screenshot",
        suggested_ext.as_str(),
        envelope.decode_image().as_deref(),
    )?;
    let mut value =
        serde_json::to_value(&envelope).context("failed to encode browser screenshot output")?;
    strip_large_binary_fields(&mut value, output_path.is_some(), &["image_base64"]);
    maybe_attach_output_path(&mut value, output_path.as_ref());
    emit_browser_value(
        &value,
        format!(
            "browser.screenshot session_id={} success={} mime_type={} output={}",
            redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
            envelope.success,
            envelope.mime_type.as_deref().unwrap_or("-"),
            output_path.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser screenshot output",
    )
}

async fn run_browser_title(session_id: String, max_title_bytes: Option<u64>) -> Result<()> {
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
    let value = serde_json::to_value(&envelope).context("failed to encode browser title output")?;
    emit_browser_value(
        &value,
        format!(
            "browser.title session_id={} success={} title={}",
            redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
            envelope.success,
            empty_as_dash(envelope.title.as_str()),
        ),
        "failed to encode browser title output",
    )
}

async fn run_browser_network(
    session_id: String,
    limit: Option<u32>,
    include_headers: bool,
    max_payload_bytes: Option<u64>,
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
    let value =
        serde_json::to_value(&envelope).context("failed to encode browser network output")?;
    let mut text = format!(
        "browser.network session_id={} success={} entries={} truncated={}",
        redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
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
    emit_browser_value(&value, text, "failed to encode browser network output")
}

async fn run_browser_storage(
    session_id: String,
    principal: Option<String>,
    output: Option<String>,
) -> Result<()> {
    let mut value = inspect_browser_session(
        session_id.as_str(),
        principal.as_deref(),
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
            redacted_browser_identifier_text(Some(session_id.as_str()), "session"),
            value.get("cookies").and_then(Value::as_array).map_or(0, Vec::len),
            value.get("storage").and_then(Value::as_array).map_or(0, Vec::len),
            written.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser storage output",
    )
}

async fn run_browser_errors(
    session_id: String,
    principal: Option<String>,
    limit: Option<u32>,
    output: Option<String>,
) -> Result<()> {
    let mut value = inspect_browser_session(
        session_id.as_str(),
        principal.as_deref(),
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
    emit_browser_value(
        &value,
        format!(
            "browser.errors session_id={} count={} output={}",
            redacted_browser_identifier_text(Some(session_id.as_str()), "session"),
            value.get("errors").and_then(Value::as_array).map_or(0, Vec::len),
            written.as_deref().unwrap_or("-"),
        ),
        "failed to encode browser errors output",
    )
}

async fn run_browser_trace(
    session_id: String,
    principal: Option<String>,
    output: Option<String>,
) -> Result<()> {
    let mut value = inspect_browser_session(
        session_id.as_str(),
        principal.as_deref(),
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
            redacted_browser_identifier_text(Some(session_id.as_str()), "session"),
            written.as_deref().unwrap_or("-"),
            value.get("action_log").and_then(Value::as_array).map_or(0, Vec::len),
            value.get("network_log").and_then(Value::as_array).map_or(0, Vec::len),
        ),
        "failed to encode browser trace output",
    )
}

async fn run_browser_downloads(
    session_id: String,
    limit: Option<u32>,
    quarantined_only: bool,
) -> Result<()> {
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
    let value =
        serde_json::to_value(&envelope).context("failed to encode browser downloads output")?;
    let mut text = format!(
        "browser.downloads session_id={} count={} truncated={} quarantined_only={}",
        redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
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
    emit_browser_value(&value, text, "failed to encode browser downloads output")
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
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser permissions output")?;
            emit_browser_value(
                &value,
                format!(
                    "browser.permissions.get session_id={} success={} camera={} microphone={} location={}",
                    redacted_browser_identifier_text(
                        Some(envelope.session_id.as_str()),
                        "session",
                    ),
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
            let value = serde_json::to_value(&envelope)
                .context("failed to encode browser permissions mutation output")?;
            emit_browser_value(
                &value,
                format!(
                    "browser.permissions.set session_id={} success={} camera={} microphone={} location={}",
                    redacted_browser_identifier_text(
                        Some(envelope.session_id.as_str()),
                        "session",
                    ),
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
    let value =
        serde_json::to_value(&envelope).context("failed to encode browser reset-state output")?;
    emit_browser_value(
        &value,
        format!(
            "browser.reset-state session_id={} success={} cookies_cleared={} storage_entries_cleared={} tabs_closed={}",
            redacted_browser_identifier_text(Some(envelope.session_id.as_str()), "session"),
            envelope.success,
            envelope.cookies_cleared,
            envelope.storage_entries_cleared,
            envelope.tabs_closed,
        ),
        "failed to encode browser reset-state output",
    )
}

fn emit_unsupported_browser_capability(
    capability: &str,
    session_id: &str,
    detail: &str,
    suggestions: &[&str],
    output: Option<&str>,
) -> Result<()> {
    let payload = UnsupportedBrowserCapabilityPayload {
        capability: capability.to_owned(),
        session_id: session_id.to_owned(),
        supported: false,
        detail: detail.to_owned(),
        suggestions: suggestions.iter().map(|value| (*value).to_owned()).collect(),
    };
    let mut value = serde_json::to_value(&payload)
        .context("failed to encode unsupported browser capability")?;
    let written = write_optional_json_output(output, session_id, capability, &value)?;
    maybe_attach_output_path(&mut value, written.as_ref());
    emit_browser_value(
        &value,
        format!(
            "browser.{} supported=false session_id={} detail={} output={}",
            capability,
            redacted_browser_identifier_text(Some(session_id), "session"),
            detail,
            written.as_deref().unwrap_or("-"),
        ),
        "failed to encode unsupported browser capability output",
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

fn resolve_browser_caller_principal(
    override_principal: Option<String>,
    defaults: app::ConnectionDefaults,
) -> Result<String> {
    let root_context = app::current_root_context()
        .ok_or_else(|| anyhow!("CLI root context is unavailable for browser command"))?;
    let connection = root_context.resolve_grpc_connection(
        app::ConnectionOverrides { principal: override_principal, ..Default::default() },
        defaults,
    )?;
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

async fn get_browser_session_detail(
    session_id: &str,
    principal: Option<&str>,
) -> Result<browser_v1::BrowserSessionDetail> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(
        principal.map(str::to_owned),
        app::ConnectionDefaults::USER,
    )?;
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
    principal: Option<&str>,
    request: browser_v1::InspectSessionRequest,
) -> Result<Value> {
    let resolved = resolve_browser_config(None, None, None)?;
    let caller_principal = resolve_browser_caller_principal(
        principal.map(str::to_owned),
        app::ConnectionDefaults::USER,
    )?;
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
        "dom_snapshot": response.dom_snapshot,
        "visible_text": response.visible_text,
        "page_url": response.page_url,
        "cookies_truncated": response.cookies_truncated,
        "storage_truncated": response.storage_truncated,
        "action_log_truncated": response.action_log_truncated,
        "network_log_truncated": response.network_log_truncated,
        "dom_truncated": response.dom_truncated,
        "visible_text_truncated": response.visible_text_truncated,
        "error": response.error,
        "session_id": redacted_browser_identifier_json_value(Some(session_id), "session"),
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

    let env_endpoint = env_optional("PALYRA_BROWSER_SERVICE_ENDPOINT");
    let env_token = env_optional("PALYRA_BROWSER_SERVICE_AUTH_TOKEN");
    let env_enabled = env_bool("PALYRA_BROWSER_SERVICE_ENABLED");
    let env_connect_timeout_ms = env_u64("PALYRA_BROWSER_SERVICE_CONNECT_TIMEOUT_MS");
    let env_request_timeout_ms = env_u64("PALYRA_BROWSER_SERVICE_REQUEST_TIMEOUT_MS");
    let env_max_screenshot_bytes = env_u64("PALYRA_BROWSER_SERVICE_MAX_SCREENSHOT_BYTES");
    let env_max_title_bytes = env_u64("PALYRA_BROWSER_SERVICE_MAX_TITLE_BYTES");
    let env_state_dir = env_optional("PALYRA_BROWSERD_STATE_DIR");
    let env_state_key_vault_ref = env_optional("PALYRA_BROWSERD_STATE_ENCRYPTION_KEY_VAULT_REF");

    let grpc_url = normalize_browser_base_url(
        endpoint
            .or(env_endpoint.clone())
            .or(file_endpoint.clone())
            .unwrap_or_else(|| DEFAULT_BROWSER_GRPC_URL.to_owned()),
        "browser gRPC URL",
    )?;
    let health_base_url = normalize_browser_base_url(
        health_url.unwrap_or_else(|| derive_browser_health_base_url(grpc_url.as_str())),
        "browser health URL",
    )?;
    let resolved_token = token
        .as_deref()
        .and_then(normalize_optional_text)
        .map(ToOwned::to_owned)
        .or(env_token.clone())
        .or(file_auth_token.clone());

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
            state_key_vault_ref_configured: env_state_key_vault_ref.is_some()
                || file_state_key_vault_ref.is_some(),
        },
        config_path: config_path.map(|value| value.display().to_string()),
    })
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

fn env_optional(name: &str) -> Option<String> {
    env::var(name).ok().map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name).ok().and_then(|value| value.trim().parse::<bool>().ok())
}

fn env_u64(name: &str) -> Option<u64> {
    env::var(name).ok().and_then(|value| value.trim().parse::<u64>().ok())
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
            url.set_port(Some(DEFAULT_BROWSER_HEALTH_PORT)).ok()?;
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
    match browser_output_mode() {
        BrowserOutputMode::Json => {
            let mut redacted = value.clone();
            redact_browser_output_value(&mut redacted, None);
            output::print_json_pretty(&redacted, error_context)
        }
        BrowserOutputMode::Ndjson => {
            let mut redacted = value.clone();
            redact_browser_output_value(&mut redacted, None);
            output::print_json_line(&redacted, error_context)
        }
        BrowserOutputMode::Text => {
            print!("{text}");
            if !text.ends_with('\n') {
                println!();
            }
            std::io::stdout().flush().context("stdout flush failed")
        }
    }
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
        "browser.policy enabled={} auth_token_configured={} endpoint={} connect_timeout_ms={} request_timeout_ms={} max_screenshot_bytes={} max_title_bytes={}",
        payload.policy.configured_enabled,
        payload.policy.auth_token_configured,
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
    if let Some(metadata) = payload.lifecycle_metadata.as_ref() {
        lines.push(format!(
            "browser.lifecycle pid={} binary={} stdout_log={} stderr_log={}",
            metadata.pid, metadata.binary, metadata.stdout_log_path, metadata.stderr_log_path,
        ));
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

fn format_browser_lifecycle_text(payload: &BrowserLifecyclePayload) -> String {
    format!(
        "browser.{} running={} pid={} grpc_url={} health_url={} stdout_log={} stderr_log={} detail={}",
        payload.action,
        payload.running,
        payload.pid.map(|value| value.to_string()).unwrap_or_else(|| "-".to_owned()),
        payload.grpc_url,
        payload.health_base_url,
        payload.stdout_log_path.as_deref().unwrap_or("-"),
        payload.stderr_log_path.as_deref().unwrap_or("-"),
        payload.detail,
    )
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
    let Some(payload) = payload else {
        return Ok(None);
    };
    let mode = browser_output_mode();
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

fn redacted_browser_identifier_json_value(value: Option<&str>, kind: &'static str) -> Value {
    value
        .filter(|candidate| !candidate.trim().is_empty())
        .map(|candidate| Value::String(browser_identifier_scope(kind, candidate)))
        .unwrap_or(Value::Null)
}

fn browser_identifier_kind_for_key(key: &str) -> Option<&'static str> {
    match key {
        "session_id" => Some("session"),
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

fn canonical_id_text(value: Option<&common_v1::CanonicalId>, kind: &'static str) -> String {
    if value.is_some() {
        format!("{kind}:redacted")
    } else {
        "-".to_owned()
    }
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
        "session_id": canonical_id_text(summary.session_id.as_ref(), "session"),
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
        "active_tab_id": canonical_id_text(summary.active_tab_id.as_ref(), "tab"),
        "active_tab_url": summary.active_tab_url,
        "active_tab_title": summary.active_tab_title,
        "allow_private_targets": summary.allow_private_targets,
        "downloads_enabled": summary.downloads_enabled,
        "persistence_enabled": summary.persistence_enabled,
        "persistence_id": summary.persistence_id,
        "state_restored": summary.state_restored,
        "profile_id": if summary.profile_id.is_some() {
            Value::String(canonical_id_text(summary.profile_id.as_ref(), "profile"))
        } else {
            Value::Null
        },
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
        "tab_id": canonical_id_text(tab.tab_id.as_ref(), "tab"),
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
