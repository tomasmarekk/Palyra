//! Background gateway service management (`palyra gateway install/start/...`).
//!
//! Wraps the per-platform user-level service manager: Windows Scheduled Tasks,
//! macOS launchd agents, and systemd user units. The installed definition is
//! tracked in `gateway-service.json` under the state root so later lifecycle
//! commands operate only on services this CLI installed.

#[cfg(windows)]
use std::process::Output;
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
#[cfg(windows)]
use windows_sys::Win32::Globalization::{GetACP, GetOEMCP, MultiByteToWideChar};

const SERVICE_METADATA_SCHEMA_VERSION: u32 = 1;

/// Persisted record of an installed gateway service definition; the source of
/// truth for which service the lifecycle commands are allowed to manage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct GatewayServiceMetadata {
    pub(crate) schema_version: u32,
    pub(crate) platform: String,
    pub(crate) manager: String,
    pub(crate) service_name: String,
    pub(crate) state_root: String,
    pub(crate) config_path: Option<String>,
    pub(crate) daemon_bin: String,
    pub(crate) service_root: String,
    pub(crate) wrapper_path: String,
    pub(crate) definition_path: String,
    pub(crate) stdout_log_path: String,
    pub(crate) stderr_log_path: String,
}

/// Point-in-time service state as reported by the platform service manager.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct GatewayServiceStatus {
    pub(crate) installed: bool,
    pub(crate) running: bool,
    pub(crate) enabled: bool,
    pub(crate) manager: String,
    pub(crate) service_name: String,
    pub(crate) definition_path: Option<String>,
    pub(crate) stdout_log_path: Option<String>,
    pub(crate) stderr_log_path: Option<String>,
    pub(crate) detail: Option<String>,
}

/// Inputs for installing the gateway service definition.
#[derive(Debug, Clone)]
pub(crate) struct GatewayServiceInstallRequest {
    pub(crate) service_name: Option<String>,
    pub(crate) daemon_bin: PathBuf,
    pub(crate) state_root: PathBuf,
    pub(crate) config_path: Option<PathBuf>,
    pub(crate) log_dir: Option<PathBuf>,
    pub(crate) start_now: bool,
}

// Resolved, borrowed view of the install request shared by the per-platform
// installers.
struct GatewayServiceInstallContext<'a> {
    service_root: &'a Path,
    service_name: &'a str,
    daemon_bin: &'a Path,
    state_root: &'a Path,
    config_path: Option<&'a Path>,
    working_directory: &'a Path,
    stdout_log_path: &'a Path,
    stderr_log_path: &'a Path,
    start_now: bool,
}

/// Returns the `gateway-service.json` metadata path for a state root.
pub(crate) fn service_metadata_path(state_root: &Path) -> PathBuf {
    state_root.join("service").join("gateway-service.json")
}

/// Returns the platform-conventional default service name.
pub(crate) fn default_service_name() -> String {
    if cfg!(windows) {
        "PalyraGateway".to_owned()
    } else if cfg!(target_os = "macos") {
        "cz.marektomas.palyra.gateway".to_owned()
    } else {
        "palyra-gateway".to_owned()
    }
}

/// Loads the managed service metadata; `Ok(None)` when none was installed.
///
/// # Errors
/// Returns an error when the metadata file exists but cannot be read or parsed.
pub(crate) fn load_service_metadata(state_root: &Path) -> Result<Option<GatewayServiceMetadata>> {
    let metadata_path = service_metadata_path(state_root);
    if !metadata_path.is_file() {
        return Ok(None);
    }
    let raw = fs::read_to_string(metadata_path.as_path())
        .with_context(|| format!("failed to read service metadata {}", metadata_path.display()))?;
    let metadata = serde_json::from_str::<GatewayServiceMetadata>(raw.as_str())
        .with_context(|| format!("failed to parse service metadata {}", metadata_path.display()))?;
    Ok(Some(metadata))
}

/// Installs (or reinstalls) the gateway service definition for the current
/// platform, persists its metadata, and returns the resulting status.
///
/// # Errors
/// Returns an error when directories or wrapper/definition files cannot be
/// written, the daemon binary cannot be canonicalized, or the platform service
/// manager rejects the installation.
pub(crate) fn install_gateway_service(
    request: &GatewayServiceInstallRequest,
) -> Result<GatewayServiceStatus> {
    let service_name = request.service_name.clone().unwrap_or_else(default_service_name);
    let service_root = request.state_root.join("service");
    let log_dir = request.log_dir.clone().unwrap_or_else(|| request.state_root.join("logs"));
    fs::create_dir_all(service_root.as_path()).with_context(|| {
        format!("failed to create gateway service root {}", service_root.display())
    })?;
    fs::create_dir_all(log_dir.as_path())
        .with_context(|| format!("failed to create log directory {}", log_dir.display()))?;

    let stdout_log_path = log_dir.join("palyrad.service.stdout.log");
    let stderr_log_path = log_dir.join("palyrad.service.stderr.log");
    let daemon_bin = request.daemon_bin.canonicalize().with_context(|| {
        format!("failed to canonicalize palyrad binary {}", request.daemon_bin.display())
    })?;
    // Best-effort canonicalization: the config file may be created after the
    // service is installed, so a non-resolvable path is kept as provided.
    let config_path = request
        .config_path
        .as_ref()
        .map(|value| value.canonicalize().unwrap_or_else(|_| value.to_path_buf()));
    let working_directory =
        daemon_bin.parent().map(Path::to_path_buf).unwrap_or_else(|| request.state_root.clone());
    let install_context = GatewayServiceInstallContext {
        service_root: service_root.as_path(),
        service_name: service_name.as_str(),
        daemon_bin: daemon_bin.as_path(),
        state_root: request.state_root.as_path(),
        config_path: config_path.as_deref(),
        working_directory: working_directory.as_path(),
        stdout_log_path: stdout_log_path.as_path(),
        stderr_log_path: stderr_log_path.as_path(),
        start_now: request.start_now,
    };

    #[cfg(windows)]
    let (wrapper_path, definition_path, manager) = install_windows_task(&install_context)?;
    #[cfg(target_os = "macos")]
    let (wrapper_path, definition_path, manager) = install_launch_agent(&install_context)?;
    #[cfg(all(unix, not(target_os = "macos")))]
    let (wrapper_path, definition_path, manager) = install_systemd_user_unit(&install_context)?;

    let metadata = GatewayServiceMetadata {
        schema_version: SERVICE_METADATA_SCHEMA_VERSION,
        platform: env::consts::OS.to_owned(),
        manager,
        service_name: service_name.clone(),
        state_root: request.state_root.display().to_string(),
        config_path: config_path.as_ref().map(|value| value.display().to_string()),
        daemon_bin: daemon_bin.display().to_string(),
        service_root: service_root.display().to_string(),
        wrapper_path: wrapper_path.display().to_string(),
        definition_path: definition_path.display().to_string(),
        stdout_log_path: stdout_log_path.display().to_string(),
        stderr_log_path: stderr_log_path.display().to_string(),
    };
    write_service_metadata(request.state_root.as_path(), &metadata)?;
    query_gateway_service_status(request.state_root.as_path())
}

/// Starts the managed gateway service and reports the resulting status.
///
/// # Errors
/// Returns an error when no managed service metadata exists or the platform
/// service manager fails to start the service.
pub(crate) fn start_gateway_service(state_root: &Path) -> Result<GatewayServiceStatus> {
    let metadata = require_service_metadata(state_root)?;
    service_manager_start(&metadata)?;
    query_gateway_service_status(state_root)
}

/// Stops the managed gateway service and reports the resulting status.
///
/// # Errors
/// Returns an error when no managed service metadata exists or the platform
/// service manager fails to stop the service.
pub(crate) fn stop_gateway_service(state_root: &Path) -> Result<GatewayServiceStatus> {
    let metadata = require_service_metadata(state_root)?;
    service_manager_stop(&metadata)?;
    query_gateway_service_status(state_root)
}

/// Restarts the managed gateway service and reports the resulting status.
///
/// # Errors
/// Returns an error when no managed service metadata exists or the platform
/// service manager fails to restart the service.
pub(crate) fn restart_gateway_service(state_root: &Path) -> Result<GatewayServiceStatus> {
    let metadata = require_service_metadata(state_root)?;
    service_manager_restart(&metadata)?;
    query_gateway_service_status(state_root)
}

/// Uninstalls the managed gateway service definition and removes its metadata.
/// Reports a not-installed status when no metadata exists.
///
/// # Errors
/// Returns an error when the service definition or metadata cannot be removed.
pub(crate) fn uninstall_gateway_service(state_root: &Path) -> Result<GatewayServiceStatus> {
    let Some(metadata) = load_service_metadata(state_root)? else {
        return Ok(GatewayServiceStatus {
            installed: false,
            running: false,
            enabled: false,
            manager: current_service_manager().to_owned(),
            service_name: default_service_name(),
            definition_path: None,
            stdout_log_path: None,
            stderr_log_path: None,
            detail: Some("gateway service metadata was not found".to_owned()),
        });
    };

    // Best-effort stop: the service may already be stopped or never started,
    // and uninstall must proceed either way.
    let _ = service_manager_stop(&metadata);
    service_manager_uninstall(&metadata)?;
    let metadata_path = service_metadata_path(state_root);
    if metadata_path.exists() {
        fs::remove_file(metadata_path.as_path()).with_context(|| {
            format!("failed to remove service metadata {}", metadata_path.display())
        })?;
    }
    Ok(GatewayServiceStatus {
        installed: false,
        running: false,
        enabled: false,
        manager: metadata.manager,
        service_name: metadata.service_name,
        definition_path: Some(metadata.definition_path),
        stdout_log_path: Some(metadata.stdout_log_path),
        stderr_log_path: Some(metadata.stderr_log_path),
        detail: Some("gateway service definition was removed".to_owned()),
    })
}

/// Queries the platform service manager for the managed service's status;
/// reports a not-installed status when no metadata exists.
///
/// # Errors
/// Returns an error when the metadata cannot be loaded or the platform query
/// command cannot be launched.
pub(crate) fn query_gateway_service_status(state_root: &Path) -> Result<GatewayServiceStatus> {
    let Some(metadata) = load_service_metadata(state_root)? else {
        return Ok(GatewayServiceStatus {
            installed: false,
            running: false,
            enabled: false,
            manager: current_service_manager().to_owned(),
            service_name: default_service_name(),
            definition_path: None,
            stdout_log_path: None,
            stderr_log_path: None,
            detail: Some("gateway service is not installed for this state root".to_owned()),
        });
    };
    query_service_status_from_metadata(&metadata)
}

fn write_service_metadata(state_root: &Path, metadata: &GatewayServiceMetadata) -> Result<()> {
    let metadata_path = service_metadata_path(state_root);
    if let Some(parent) = metadata_path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!("failed to create service metadata directory {}", parent.display())
        })?;
    }
    let encoded = serde_json::to_vec_pretty(metadata)
        .context("failed to serialize gateway service metadata")?;
    fs::write(metadata_path.as_path(), encoded.as_slice())
        .with_context(|| format!("failed to write service metadata {}", metadata_path.display()))
}

fn require_service_metadata(state_root: &Path) -> Result<GatewayServiceMetadata> {
    let metadata_path = service_metadata_path(state_root);
    load_service_metadata(state_root)?.ok_or_else(|| {
        anyhow!(
            "precondition failed: no managed gateway service metadata exists at {}; \
             `palyra gateway start`, `palyra gateway stop`, and `palyra gateway restart` \
             control only a background service installed with `palyra gateway install`; \
             a foreground `palyra gateway run` process is attached to its terminal and must be \
             stopped with Ctrl+C in that terminal",
            metadata_path.display()
        )
    })
}

fn current_service_manager() -> &'static str {
    if cfg!(windows) {
        "schtasks"
    } else if cfg!(target_os = "macos") {
        "launchctl"
    } else {
        "systemd-user"
    }
}

fn query_service_status_from_metadata(
    metadata: &GatewayServiceMetadata,
) -> Result<GatewayServiceStatus> {
    #[cfg(windows)]
    {
        query_windows_task_status(metadata)
    }
    #[cfg(target_os = "macos")]
    {
        query_launch_agent_status(metadata)
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        query_systemd_user_status(metadata)
    }
}

#[cfg(windows)]
fn install_windows_task(
    context: &GatewayServiceInstallContext<'_>,
) -> Result<(PathBuf, PathBuf, String)> {
    let wrapper_path = context.service_root.join("gateway-service.cmd");
    let mut body = String::from("@echo off\r\nsetlocal\r\n");
    if let Some(config_path) = context.config_path {
        body.push_str(format!("set PALYRA_CONFIG={}\r\n", config_path.display()).as_str());
    }
    body.push_str(format!("set PALYRA_STATE_ROOT={}\r\n", context.state_root.display()).as_str());
    body.push_str(format!("cd /d \"{}\"\r\n", context.working_directory.display()).as_str());
    body.push_str(
        format!(
            "\"{}\" >> \"{}\" 2>> \"{}\"\r\n",
            context.daemon_bin.display(),
            context.stdout_log_path.display(),
            context.stderr_log_path.display()
        )
        .as_str(),
    );
    fs::write(wrapper_path.as_path(), body.as_bytes()).with_context(|| {
        format!("failed to write Windows gateway wrapper {}", wrapper_path.display())
    })?;
    let task_name = format!("\\{}", context.service_name);
    // Reinstall must be idempotent: drop any existing task with the same name
    // before creating the new definition.
    let query = Command::new("schtasks")
        .args(["/Query", "/TN", task_name.as_str()])
        .output()
        .context("failed to query existing scheduled task")?;
    if query.status.success() {
        let delete = Command::new("schtasks")
            .args(["/Delete", "/TN", task_name.as_str(), "/F"])
            .output()
            .context("failed to remove existing scheduled task before reinstall")?;
        if !delete.status.success() {
            return Err(build_windows_task_install_error(
                "remove existing",
                task_name.as_str(),
                wrapper_path.as_path(),
                &delete,
            ));
        }
    }
    let create = Command::new("schtasks")
        .args([
            "/Create",
            "/TN",
            task_name.as_str(),
            "/SC",
            "ONLOGON",
            "/RL",
            "LIMITED",
            "/TR",
            wrapper_path.display().to_string().as_str(),
            "/F",
        ])
        .output()
        .context("failed to install scheduled task for gateway service")?;
    if !create.status.success() {
        return Err(build_windows_task_install_error(
            "create",
            task_name.as_str(),
            wrapper_path.as_path(),
            &create,
        ));
    }
    if context.start_now {
        // Best-effort start: installation already succeeded, and the follow-up
        // status query reports whether the task is actually running.
        let _ = Command::new("schtasks").args(["/Run", "/TN", task_name.as_str()]).status();
    }
    // Scheduled tasks have no on-disk unit file, so the wrapper script serves
    // as both the wrapper and the definition path in the metadata.
    Ok((wrapper_path.clone(), wrapper_path, "schtasks".to_owned()))
}

#[cfg(windows)]
fn build_windows_task_install_error(
    operation: &str,
    task_name: &str,
    wrapper_path: &Path,
    output: &Output,
) -> anyhow::Error {
    let status = output.status.code().map_or_else(|| "unknown".into(), |value| value.to_string());
    let detail = summarize_command_output(output).unwrap_or_else(|| "none".to_owned());
    anyhow!(
        "failed to {operation} Windows scheduled task {task_name} (wrapper: {}): schtasks exited with status {status}; {detail}. Use `palyra gateway run` for a foreground runtime, or remove the conflicting scheduled task / fix the current user-task permissions and retry `palyra gateway install --start`.",
        wrapper_path.display()
    )
}

#[cfg(windows)]
fn summarize_command_output(output: &Output) -> Option<String> {
    let stdout = decode_windows_process_output(output.stdout.as_slice());
    let stderr = decode_windows_process_output(output.stderr.as_slice());
    match (stdout.is_empty(), stderr.is_empty()) {
        (false, false) => Some(format!("stdout: {stdout}; stderr: {stderr}")),
        (false, true) => Some(format!("stdout: {stdout}")),
        (true, false) => Some(format!("stderr: {stderr}")),
        (true, true) => None,
    }
}

// Localized console tools (schtasks) emit legacy code-page bytes, not UTF-8.
// Try UTF-8 first, then decode with several likely code pages and keep the
// candidate that scores lowest on the mojibake penalty heuristic below.
#[cfg(windows)]
fn decode_windows_process_output(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }
    if let Ok(value) = std::str::from_utf8(bytes) {
        return value.trim().to_owned();
    }

    windows_process_output_candidates(bytes)
        .into_iter()
        .min_by_key(|candidate| windows_output_decode_penalty(candidate))
        .unwrap_or_else(|| format!("non-UTF-8 localized output ({} bytes)", bytes.len()))
        .trim()
        .to_owned()
}

#[cfg(windows)]
fn windows_process_output_candidates(bytes: &[u8]) -> Vec<String> {
    // Console output usually uses the OEM code page, the ANSI code page is the
    // next best guess, and 852/1250 cover Central European systems where the
    // active pages may not match the text. SAFETY: GetOEMCP/GetACP take no
    // arguments and only read process-global locale state.
    let mut code_pages = Vec::from([unsafe { GetOEMCP() }, unsafe { GetACP() }, 852, 1250]);
    code_pages.dedup();
    code_pages
        .into_iter()
        .filter_map(|code_page| windows_code_page_to_string(bytes, code_page))
        .collect()
}

// Scores how "wrong" a decoded candidate looks: replacement chars and control
// bytes dominate, followed by box-drawing/symbol chars that typically appear
// when text is decoded with the wrong Central European code page.
#[cfg(windows)]
fn windows_output_decode_penalty(value: &str) -> usize {
    value
        .chars()
        .map(|ch| match ch {
            '\u{fffd}' => 100,
            ch if ch.is_control() && !ch.is_whitespace() => 50,
            '²' | '°' | '±' | '¤' | '¦' | '§' | '¨' | '¸' | '¬' | 'ˇ' | '˙' | 'ø' | 'Ø' => {
                10
            }
            ch if ch.is_alphabetic() || ch.is_ascii_punctuation() || ch.is_whitespace() => 0,
            ch if ch.is_numeric() => 0,
            _ => 1,
        })
        .sum()
}

#[cfg(windows)]
fn windows_code_page_to_string(bytes: &[u8], code_page: u32) -> Option<String> {
    let input_len = i32::try_from(bytes.len()).ok()?;
    // SAFETY: standard two-call MultiByteToWideChar protocol. The first call
    // only sizes the output (null buffer, length 0); the second writes at most
    // `required` UTF-16 units into a buffer allocated with exactly that length,
    // and `input_len` matches the live `bytes` slice in both calls.
    let required = unsafe {
        MultiByteToWideChar(code_page, 0, bytes.as_ptr(), input_len, std::ptr::null_mut(), 0)
    };
    if required <= 0 {
        return None;
    }
    let mut buffer = vec![0_u16; usize::try_from(required).ok()?];
    let written = unsafe {
        MultiByteToWideChar(code_page, 0, bytes.as_ptr(), input_len, buffer.as_mut_ptr(), required)
    };
    if written <= 0 {
        return None;
    }
    Some(String::from_utf16_lossy(&buffer[..usize::try_from(written).ok()?]))
}

#[cfg(windows)]
fn query_windows_task_status(metadata: &GatewayServiceMetadata) -> Result<GatewayServiceStatus> {
    let task_name = format!("\\{}", metadata.service_name);
    let output = Command::new("schtasks")
        .args(["/Query", "/TN", task_name.as_str(), "/FO", "CSV", "/NH"])
        .output()
        .context("failed to query gateway scheduled task status")?;
    if !output.status.success() {
        return Ok(GatewayServiceStatus {
            installed: false,
            running: false,
            enabled: false,
            manager: metadata.manager.clone(),
            service_name: metadata.service_name.clone(),
            definition_path: Some(metadata.definition_path.clone()),
            stdout_log_path: Some(metadata.stdout_log_path.clone()),
            stderr_log_path: Some(metadata.stderr_log_path.clone()),
            detail: Some(decode_windows_process_output(&output.stderr).trim().to_owned()),
        });
    }
    let text = decode_windows_process_output(&output.stdout);
    let fields = parse_schtasks_csv_row(text.as_str());
    let running = fields
        .as_ref()
        .and_then(|fields| fields.get(2))
        .is_some_and(|status| is_schtasks_running_status(status));
    let detail = fields
        .as_ref()
        .and_then(|fields| fields.get(1))
        .filter(|next_run_time| !next_run_time.trim().is_empty())
        .map(|next_run_time| format!("next_run_time={}", next_run_time.trim()))
        .or_else(|| {
            text.lines()
                .find(|line| !line.trim().is_empty())
                .map(|line| format!("unexpected schtasks CSV output: {}", line.trim()))
        });
    Ok(GatewayServiceStatus {
        installed: true,
        running,
        enabled: true,
        manager: metadata.manager.clone(),
        service_name: metadata.service_name.clone(),
        definition_path: Some(metadata.definition_path.clone()),
        stdout_log_path: Some(metadata.stdout_log_path.clone()),
        stderr_log_path: Some(metadata.stderr_log_path.clone()),
        detail,
    })
}

#[cfg(windows)]
fn parse_schtasks_csv_row(text: &str) -> Option<Vec<String>> {
    text.lines().map(str::trim).find(|line| !line.is_empty()).and_then(parse_csv_record)
}

#[cfg(windows)]
fn parse_csv_record(line: &str) -> Option<Vec<String>> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut chars = line.chars().peekable();
    let mut in_quotes = false;
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                let _ = chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(field.trim().to_owned());
                field.clear();
            }
            _ => field.push(ch),
        }
    }
    if in_quotes {
        return None;
    }
    fields.push(field.trim().to_owned());
    Some(fields)
}

#[cfg(windows)]
fn is_schtasks_running_status(status: &str) -> bool {
    status.trim().eq_ignore_ascii_case("running")
}

#[cfg(windows)]
fn service_manager_start(metadata: &GatewayServiceMetadata) -> Result<()> {
    let task_name = format!("\\{}", metadata.service_name);
    let status = Command::new("schtasks")
        .args(["/Run", "/TN", task_name.as_str()])
        .status()
        .context("failed to start gateway scheduled task")?;
    if !status.success() {
        anyhow::bail!("failed to start scheduled task {}", metadata.service_name);
    }
    Ok(())
}

#[cfg(windows)]
fn service_manager_stop(metadata: &GatewayServiceMetadata) -> Result<()> {
    let task_name = format!("\\{}", metadata.service_name);
    let status = Command::new("schtasks")
        .args(["/End", "/TN", task_name.as_str()])
        .status()
        .context("failed to stop gateway scheduled task")?;
    if !status.success() {
        anyhow::bail!("failed to stop scheduled task {}", metadata.service_name);
    }
    Ok(())
}

#[cfg(windows)]
fn service_manager_restart(metadata: &GatewayServiceMetadata) -> Result<()> {
    // Best-effort stop: restarting a task that is not running is fine.
    let _ = service_manager_stop(metadata);
    service_manager_start(metadata)
}

#[cfg(windows)]
fn service_manager_uninstall(metadata: &GatewayServiceMetadata) -> Result<()> {
    let task_name = format!("\\{}", metadata.service_name);
    let status = Command::new("schtasks")
        .args(["/Delete", "/TN", task_name.as_str(), "/F"])
        .status()
        .context("failed to remove gateway scheduled task")?;
    if !status.success() {
        anyhow::bail!("failed to delete scheduled task {}", metadata.service_name);
    }
    cleanup_service_files(metadata)
}

#[cfg(target_os = "macos")]
fn install_launch_agent(
    context: &GatewayServiceInstallContext<'_>,
) -> Result<(PathBuf, PathBuf, String)> {
    let wrapper_path = context.service_root.join("gateway-service.sh");
    write_unix_wrapper(
        wrapper_path.as_path(),
        context.daemon_bin,
        context.state_root,
        context.config_path,
        context.working_directory,
        context.stdout_log_path,
        context.stderr_log_path,
    )?;
    let agent_dir = home_dir()?.join("Library").join("LaunchAgents");
    fs::create_dir_all(agent_dir.as_path())
        .with_context(|| format!("failed to create {}", agent_dir.display()))?;
    let definition_path = agent_dir.join(format!("{}.plist", context.service_name));
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{service_name}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{wrapper}</string>
  </array>
  <key>RunAtLoad</key>
  <false/>
  <key>KeepAlive</key>
  <true/>
  <key>WorkingDirectory</key>
  <string>{working_directory}</string>
  <key>StandardOutPath</key>
  <string>{stdout_log_path}</string>
  <key>StandardErrorPath</key>
  <string>{stderr_log_path}</string>
</dict>
</plist>
"#,
        service_name = context.service_name,
        wrapper = wrapper_path.display(),
        working_directory = context.working_directory.display(),
        stdout_log_path = context.stdout_log_path.display(),
        stderr_log_path = context.stderr_log_path.display(),
    );
    fs::write(definition_path.as_path(), plist.as_bytes())
        .with_context(|| format!("failed to write {}", definition_path.display()))?;
    let domain = launchctl_domain()?;
    // Best-effort bootout first: bootstrap fails if a previous definition with
    // the same label is still loaded, and on a fresh install there is nothing
    // to unload.
    let _ = Command::new("launchctl")
        .args(["bootout", domain.as_str(), definition_path.display().to_string().as_str()])
        .status();
    run_command(
        "launchctl",
        &["bootstrap", domain.as_str(), definition_path.display().to_string().as_str()],
        "failed to bootstrap launch agent",
    )?;
    if context.start_now {
        run_command(
            "launchctl",
            &["kickstart", "-k", format!("{domain}/{}", context.service_name).as_str()],
            "failed to start launch agent",
        )?;
    }
    Ok((wrapper_path, definition_path, "launchctl".to_owned()))
}

#[cfg(target_os = "macos")]
fn query_launch_agent_status(metadata: &GatewayServiceMetadata) -> Result<GatewayServiceStatus> {
    let domain = launchctl_domain()?;
    let label = format!("{domain}/{}", metadata.service_name);
    let output = Command::new("launchctl")
        .args(["print", label.as_str()])
        .output()
        .context("failed to query launch agent status")?;
    if !output.status.success() {
        return Ok(GatewayServiceStatus {
            installed: Path::new(metadata.definition_path.as_str()).exists(),
            running: false,
            enabled: false,
            manager: metadata.manager.clone(),
            service_name: metadata.service_name.clone(),
            definition_path: Some(metadata.definition_path.clone()),
            stdout_log_path: Some(metadata.stdout_log_path.clone()),
            stderr_log_path: Some(metadata.stderr_log_path.clone()),
            detail: Some(String::from_utf8_lossy(&output.stderr).trim().to_owned()),
        });
    }
    let text = String::from_utf8_lossy(&output.stdout);
    Ok(GatewayServiceStatus {
        installed: true,
        running: text.contains("state = running"),
        enabled: true,
        manager: metadata.manager.clone(),
        service_name: metadata.service_name.clone(),
        definition_path: Some(metadata.definition_path.clone()),
        stdout_log_path: Some(metadata.stdout_log_path.clone()),
        stderr_log_path: Some(metadata.stderr_log_path.clone()),
        detail: Some(
            text.lines()
                .find(|line| line.trim_start().starts_with("state ="))
                .unwrap_or_default()
                .trim()
                .to_owned(),
        ),
    })
}

#[cfg(target_os = "macos")]
fn service_manager_start(metadata: &GatewayServiceMetadata) -> Result<()> {
    let domain = launchctl_domain()?;
    run_command(
        "launchctl",
        &["kickstart", "-k", format!("{domain}/{}", metadata.service_name).as_str()],
        "failed to start launch agent",
    )
}

#[cfg(target_os = "macos")]
fn service_manager_stop(metadata: &GatewayServiceMetadata) -> Result<()> {
    let domain = launchctl_domain()?;
    run_command(
        "launchctl",
        &["bootout", domain.as_str(), metadata.definition_path.as_str()],
        "failed to stop launch agent",
    )
}

#[cfg(target_os = "macos")]
fn service_manager_restart(metadata: &GatewayServiceMetadata) -> Result<()> {
    // Best-effort stop: the agent may not be loaded; bootstrap + kickstart
    // below re-establish the running state either way.
    let _ = service_manager_stop(metadata);
    let domain = launchctl_domain()?;
    run_command(
        "launchctl",
        &["bootstrap", domain.as_str(), metadata.definition_path.as_str()],
        "failed to restart launch agent",
    )?;
    service_manager_start(metadata)
}

#[cfg(target_os = "macos")]
fn service_manager_uninstall(metadata: &GatewayServiceMetadata) -> Result<()> {
    // Best-effort stop: uninstall proceeds whether or not the agent is loaded.
    let _ = service_manager_stop(metadata);
    if Path::new(metadata.definition_path.as_str()).exists() {
        fs::remove_file(metadata.definition_path.as_str())
            .with_context(|| format!("failed to remove {}", metadata.definition_path))?;
    }
    cleanup_service_files(metadata)
}

#[cfg(all(unix, not(target_os = "macos")))]
fn install_systemd_user_unit(
    context: &GatewayServiceInstallContext<'_>,
) -> Result<(PathBuf, PathBuf, String)> {
    let wrapper_path = context.service_root.join("gateway-service.sh");
    write_unix_wrapper(
        wrapper_path.as_path(),
        context.daemon_bin,
        context.state_root,
        context.config_path,
        context.working_directory,
        context.stdout_log_path,
        context.stderr_log_path,
    )?;
    let unit_dir = home_dir()?.join(".config").join("systemd").join("user");
    fs::create_dir_all(unit_dir.as_path())
        .with_context(|| format!("failed to create {}", unit_dir.display()))?;
    let definition_path = unit_dir.join(format!("{}.service", context.service_name));
    let unit = format!(
        r#"[Unit]
Description=Palyra gateway daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory={}
ExecStart={}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
"#,
        context.working_directory.display(),
        wrapper_path.display(),
    );
    fs::write(definition_path.as_path(), unit.as_bytes())
        .with_context(|| format!("failed to write {}", definition_path.display()))?;
    run_command("systemctl", &["--user", "daemon-reload"], "failed to reload systemd user units")?;
    run_command(
        "systemctl",
        &["--user", "enable", context.service_name],
        "failed to enable gateway service",
    )?;
    if context.start_now {
        run_command(
            "systemctl",
            &["--user", "restart", context.service_name],
            "failed to start gateway service",
        )?;
    }
    Ok((wrapper_path, definition_path, "systemd-user".to_owned()))
}

#[cfg(all(unix, not(target_os = "macos")))]
fn query_systemd_user_status(metadata: &GatewayServiceMetadata) -> Result<GatewayServiceStatus> {
    let active = Command::new("systemctl")
        .args(["--user", "is-active", metadata.service_name.as_str()])
        .output()
        .context("failed to query systemd active state")?;
    let enabled = Command::new("systemctl")
        .args(["--user", "is-enabled", metadata.service_name.as_str()])
        .output()
        .context("failed to query systemd enabled state")?;
    let installed = Path::new(metadata.definition_path.as_str()).exists();
    let running =
        active.status.success() && String::from_utf8_lossy(&active.stdout).trim() == "active";
    let enabled_flag =
        enabled.status.success() && String::from_utf8_lossy(&enabled.stdout).trim() == "enabled";
    Ok(GatewayServiceStatus {
        installed,
        running,
        enabled: enabled_flag,
        manager: metadata.manager.clone(),
        service_name: metadata.service_name.clone(),
        definition_path: Some(metadata.definition_path.clone()),
        stdout_log_path: Some(metadata.stdout_log_path.clone()),
        stderr_log_path: Some(metadata.stderr_log_path.clone()),
        detail: Some(format!(
            "active={} enabled={}",
            String::from_utf8_lossy(&active.stdout).trim(),
            String::from_utf8_lossy(&enabled.stdout).trim()
        )),
    })
}

#[cfg(all(unix, not(target_os = "macos")))]
fn service_manager_start(metadata: &GatewayServiceMetadata) -> Result<()> {
    run_command(
        "systemctl",
        &["--user", "start", metadata.service_name.as_str()],
        "failed to start gateway service",
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn service_manager_stop(metadata: &GatewayServiceMetadata) -> Result<()> {
    run_command(
        "systemctl",
        &["--user", "stop", metadata.service_name.as_str()],
        "failed to stop gateway service",
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn service_manager_restart(metadata: &GatewayServiceMetadata) -> Result<()> {
    run_command(
        "systemctl",
        &["--user", "restart", metadata.service_name.as_str()],
        "failed to restart gateway service",
    )
}

#[cfg(all(unix, not(target_os = "macos")))]
fn service_manager_uninstall(metadata: &GatewayServiceMetadata) -> Result<()> {
    // Best-effort disable/stop: the unit may already be disabled or absent,
    // and removing the unit file below is what actually uninstalls it.
    let _ = Command::new("systemctl")
        .args(["--user", "disable", "--now", metadata.service_name.as_str()])
        .status();
    if Path::new(metadata.definition_path.as_str()).exists() {
        fs::remove_file(metadata.definition_path.as_str())
            .with_context(|| format!("failed to remove {}", metadata.definition_path))?;
    }
    let _ = Command::new("systemctl").args(["--user", "daemon-reload"]).status();
    cleanup_service_files(metadata)
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn write_unix_wrapper(
    wrapper_path: &Path,
    daemon_bin: &Path,
    state_root: &Path,
    config_path: Option<&Path>,
    working_directory: &Path,
    stdout_log_path: &Path,
    stderr_log_path: &Path,
) -> Result<()> {
    let mut body = String::from("#!/usr/bin/env bash\nset -euo pipefail\n");
    if let Some(config_path) = config_path {
        body.push_str(format!("export PALYRA_CONFIG=\"{}\"\n", config_path.display()).as_str());
    }
    body.push_str(format!("export PALYRA_STATE_ROOT=\"{}\"\n", state_root.display()).as_str());
    body.push_str(format!("cd \"{}\"\n", working_directory.display()).as_str());
    body.push_str(
        format!(
            "exec \"{}\" >> \"{}\" 2>> \"{}\"\n",
            daemon_bin.display(),
            stdout_log_path.display(),
            stderr_log_path.display()
        )
        .as_str(),
    );
    fs::write(wrapper_path, body.as_bytes())
        .with_context(|| format!("failed to write {}", wrapper_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = fs::metadata(wrapper_path)
            .with_context(|| format!("failed to read {}", wrapper_path.display()))?
            .permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(wrapper_path, permissions).with_context(|| {
            format!("failed to set executable bit on {}", wrapper_path.display())
        })?;
    }
    Ok(())
}

fn cleanup_service_files(metadata: &GatewayServiceMetadata) -> Result<()> {
    for path in [&metadata.wrapper_path, &metadata.definition_path] {
        let candidate = Path::new(path.as_str());
        if candidate.exists() && candidate.is_file() {
            fs::remove_file(candidate)
                .with_context(|| format!("failed to remove {}", candidate.display()))?;
        }
    }
    Ok(())
}

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn run_command(command: &str, args: &[&str], context: &str) -> Result<()> {
    let status = Command::new(command)
        .args(args)
        .status()
        .with_context(|| format!("{context}: failed to launch `{command}`"))?;
    if !status.success() {
        anyhow::bail!(
            "{context}: {} exited with status {}",
            command,
            status.code().map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned())
        );
    }
    Ok(())
}

// Only compiled on Unix targets (Windows task installation does not need the
// home directory), so the HOME lookup is sufficient.
#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn home_dir() -> Result<PathBuf> {
    env::var_os("HOME").map(PathBuf::from).ok_or_else(|| anyhow!("HOME is not set"))
}

#[cfg(target_os = "macos")]
fn launchctl_domain() -> Result<String> {
    Ok(format!("gui/{}", current_uid()?))
}

#[cfg(target_os = "macos")]
fn current_uid() -> Result<u32> {
    let output = Command::new("id").arg("-u").output().context("failed to resolve current UID")?;
    if !output.status.success() {
        anyhow::bail!("`id -u` failed while resolving current UID");
    }
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse::<u32>()
        .context("failed to parse current UID")
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use super::{
        decode_windows_process_output, is_schtasks_running_status, parse_schtasks_csv_row,
        summarize_command_output,
    };
    use super::{
        default_service_name, load_service_metadata, query_gateway_service_status,
        require_service_metadata, service_metadata_path, GatewayServiceMetadata,
        SERVICE_METADATA_SCHEMA_VERSION,
    };
    use crate::output::{classify_error, CliExitCode};
    use std::fs;
    #[cfg(windows)]
    use std::os::windows::process::ExitStatusExt;
    #[cfg(windows)]
    use std::process::Output;
    use tempfile::tempdir;

    #[test]
    fn query_gateway_service_status_without_metadata_reports_not_installed() {
        let tempdir = tempdir().expect("tempdir");
        let status = query_gateway_service_status(tempdir.path())
            .expect("status without metadata should resolve");
        assert!(!status.installed, "service should be absent without metadata");
        assert_eq!(status.service_name, default_service_name());
        assert!(
            status.detail.as_deref().is_some_and(|value| value.contains("not installed")),
            "status detail should explain missing installation"
        );
    }

    #[test]
    fn require_service_metadata_without_metadata_explains_service_mode() {
        let tempdir = tempdir().expect("tempdir");
        let error = require_service_metadata(tempdir.path())
            .expect_err("missing metadata should fail with guidance");
        let message = error.to_string();

        assert_eq!(classify_error(&error), CliExitCode::Precondition);
        assert!(message.contains("precondition failed"));
        assert!(message.contains("gateway-service.json"));
        assert!(message.contains("palyra gateway install"));
        assert!(message.contains("palyra gateway start"));
        assert!(message.contains("palyra gateway stop"));
        assert!(message.contains("palyra gateway run"));
        assert!(message.contains("Ctrl+C"));
        assert!(
            !message.contains("not found"),
            "service-mode guidance should not look like a missing-path bug: {message}"
        );
    }

    #[test]
    fn load_service_metadata_round_trips_existing_file() {
        let tempdir = tempdir().expect("tempdir");
        let metadata = GatewayServiceMetadata {
            schema_version: SERVICE_METADATA_SCHEMA_VERSION,
            platform: "windows".to_owned(),
            manager: "schtasks".to_owned(),
            service_name: "PalyraGateway".to_owned(),
            state_root: tempdir.path().display().to_string(),
            config_path: Some("C:/palyra/palyra.toml".to_owned()),
            daemon_bin: "C:/palyra/palyrad.exe".to_owned(),
            service_root: tempdir.path().join("service").display().to_string(),
            wrapper_path: tempdir.path().join("service/wrapper.cmd").display().to_string(),
            definition_path: tempdir.path().join("service/wrapper.cmd").display().to_string(),
            stdout_log_path: tempdir.path().join("logs/stdout.log").display().to_string(),
            stderr_log_path: tempdir.path().join("logs/stderr.log").display().to_string(),
        };
        let metadata_path = service_metadata_path(tempdir.path());
        fs::create_dir_all(metadata_path.parent().expect("metadata dir")).expect("mkdirs");
        fs::write(
            metadata_path.as_path(),
            serde_json::to_vec_pretty(&metadata).expect("serialize"),
        )
        .expect("write metadata");

        let loaded = load_service_metadata(tempdir.path())
            .expect("load metadata")
            .expect("metadata should exist");
        assert_eq!(loaded.service_name, metadata.service_name);
        assert_eq!(loaded.manager, metadata.manager);
        assert_eq!(loaded.stdout_log_path, metadata.stdout_log_path);
    }

    #[cfg(windows)]
    #[test]
    fn summarize_command_output_combines_stdout_and_stderr() {
        let output = Output {
            status: std::process::ExitStatus::from_raw(1),
            stdout: b"ERROR: Missing file.".to_vec(),
            stderr: b"ERROR: Access denied.".to_vec(),
        };

        let summary = summarize_command_output(&output).expect("summary should include output");
        assert!(summary.contains("stdout: ERROR:"));
        assert!(summary.contains("stderr: ERROR:"));
    }

    #[cfg(windows)]
    #[test]
    fn decode_windows_process_output_preserves_utf8_symbolic_text() {
        let decoded = decode_windows_process_output(b"ERROR: marker \xe2\x98\x85.");

        assert!(
            decoded.contains('\u{2605}'),
            "UTF-8 process output should not lose symbolic text: {decoded}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn decode_windows_process_output_keeps_non_utf8_schtasks_text_readable() {
        let decoded = decode_windows_process_output(b"ERROR: path\xffsegment.");

        assert!(decoded.starts_with("ERROR: path"), "decoded output should preserve ASCII prefix");
        assert!(!decoded.contains('\u{fffd}'), "decoded output should avoid replacement chars");
        assert!(
            !decoded.contains("non-UTF-8"),
            "Windows code-page fallback should decode non-UTF-8 process output: {decoded}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn schtasks_csv_row_parser_reads_status_by_position() {
        let fields = parse_schtasks_csv_row("\"\\PalyraGateway\",\"N/A\",\"Running\"\r\n")
            .expect("schtasks CSV row should parse");

        assert_eq!(fields.get(1).map(String::as_str), Some("N/A"));
        assert!(fields.get(2).is_some_and(|value| is_schtasks_running_status(value)));
    }

    #[cfg(windows)]
    #[test]
    fn schtasks_csv_row_parser_handles_quoted_commas_and_escaped_quotes() {
        let fields = parse_schtasks_csv_row(
            "\"\\Palyra, Gateway\",\"6/12/2026 1:00:00 PM\",\"Ready\",\"author \"\"ops\"\"\"\r\n",
        )
        .expect("quoted schtasks CSV row should parse");

        assert_eq!(fields[0], "\\Palyra, Gateway");
        assert_eq!(fields[3], "author \"ops\"");
        assert!(!is_schtasks_running_status(&fields[2]));
    }
}
