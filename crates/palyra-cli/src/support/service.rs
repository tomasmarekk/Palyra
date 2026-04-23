use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

const SERVICE_METADATA_SCHEMA_VERSION: u32 = 1;

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

#[derive(Debug, Clone)]
pub(crate) struct GatewayServiceInstallRequest {
    pub(crate) service_name: Option<String>,
    pub(crate) daemon_bin: PathBuf,
    pub(crate) state_root: PathBuf,
    pub(crate) config_path: Option<PathBuf>,
    pub(crate) log_dir: Option<PathBuf>,
    pub(crate) start_now: bool,
}

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

pub(crate) fn service_metadata_path(state_root: &Path) -> PathBuf {
    state_root.join("service").join("gateway-service.json")
}

pub(crate) fn default_service_name() -> String {
    if cfg!(windows) {
        "PalyraGateway".to_owned()
    } else if cfg!(target_os = "macos") {
        "cz.marektomas.palyra.gateway".to_owned()
    } else {
        "palyra-gateway".to_owned()
    }
}

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

pub(crate) fn start_gateway_service(state_root: &Path) -> Result<GatewayServiceStatus> {
    let metadata = require_service_metadata(state_root)?;
    service_manager_start(&metadata)?;
    query_gateway_service_status(state_root)
}

pub(crate) fn stop_gateway_service(state_root: &Path) -> Result<GatewayServiceStatus> {
    let metadata = require_service_metadata(state_root)?;
    service_manager_stop(&metadata)?;
    query_gateway_service_status(state_root)
}

pub(crate) fn restart_gateway_service(state_root: &Path) -> Result<GatewayServiceStatus> {
    let metadata = require_service_metadata(state_root)?;
    service_manager_restart(&metadata)?;
    query_gateway_service_status(state_root)
}

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
    load_service_metadata(state_root)?
        .ok_or_else(|| anyhow!("gateway service metadata not found under {}", state_root.display()))
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
        let _ = Command::new("schtasks").args(["/Run", "/TN", task_name.as_str()]).status();
    }
    Ok((wrapper_path.clone(), wrapper_path, "schtasks".to_owned()))
}

#[cfg(windows)]
fn build_windows_task_install_error(
    operation: &str,
    task_name: &str,
    wrapper_path: &Path,
    output: &Output,
) -> anyhow::Error {
    let status =
        output.status.code().map(|value| value.to_string()).unwrap_or_else(|| "unknown".to_owned());
    let detail =
        summarize_command_output(output).unwrap_or_else(|| "no additional output".to_owned());
    anyhow!(
        "failed to {operation} Windows scheduled task {task_name} (wrapper: {}): schtasks exited with status {status}; {detail}. Use `palyra gateway run` for a foreground runtime, or remove the conflicting scheduled task / fix the current user-task permissions and retry `palyra gateway install --start`.",
        wrapper_path.display()
    )
}

fn summarize_command_output(output: &Output) -> Option<String> {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    match (stdout.is_empty(), stderr.is_empty()) {
        (false, false) => Some(format!("stdout: {stdout}; stderr: {stderr}")),
        (false, true) => Some(format!("stdout: {stdout}")),
        (true, false) => Some(format!("stderr: {stderr}")),
        (true, true) => None,
    }
}

#[cfg(windows)]
fn query_windows_task_status(metadata: &GatewayServiceMetadata) -> Result<GatewayServiceStatus> {
    let task_name = format!("\\{}", metadata.service_name);
    let output = Command::new("schtasks")
        .args(["/Query", "/TN", task_name.as_str(), "/V", "/FO", "LIST"])
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
            detail: Some(String::from_utf8_lossy(&output.stderr).trim().to_owned()),
        });
    }
    let text = String::from_utf8_lossy(&output.stdout);
    let running =
        text.contains("Status: Running") || text.contains("Scheduled Task State: Running");
    Ok(GatewayServiceStatus {
        installed: true,
        running,
        enabled: true,
        manager: metadata.manager.clone(),
        service_name: metadata.service_name.clone(),
        definition_path: Some(metadata.definition_path.clone()),
        stdout_log_path: Some(metadata.stdout_log_path.clone()),
        stderr_log_path: Some(metadata.stderr_log_path.clone()),
        detail: Some(
            text.lines()
                .find(|line| line.contains("Next Run Time"))
                .unwrap_or_default()
                .trim()
                .to_owned(),
        ),
    })
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

#[cfg(any(target_os = "macos", all(unix, not(target_os = "macos"))))]
fn home_dir() -> Result<PathBuf> {
    if cfg!(windows) {
        env::var_os("USERPROFILE")
            .map(PathBuf::from)
            .ok_or_else(|| anyhow!("USERPROFILE is not set"))
    } else {
        env::var_os("HOME").map(PathBuf::from).ok_or_else(|| anyhow!("HOME is not set"))
    }
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
    use super::{
        default_service_name, load_service_metadata, query_gateway_service_status,
        service_metadata_path, summarize_command_output, GatewayServiceMetadata,
        SERVICE_METADATA_SCHEMA_VERSION,
    };
    use std::fs;
    use tempfile::tempdir;

    #[cfg(windows)]
    use std::os::windows::process::ExitStatusExt;
    #[cfg(windows)]
    use std::process::Output;

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
            stdout:
                b"ERROR: Syst\xc3\xa9m nem\xc5\xaf\xc5\xbee nal\xc3\xa9zt uveden\xc3\xbd soubor."
                    .to_vec(),
            stderr: b"ERROR: P\xc5\x99\xc3\xadstup byl odep\xc5\x99en.".to_vec(),
        };

        let summary =
            summarize_command_output(&output).expect("summary should include command output");
        assert!(summary.contains("stdout: ERROR:"));
        assert!(summary.contains("stderr: ERROR:"));
    }
}
