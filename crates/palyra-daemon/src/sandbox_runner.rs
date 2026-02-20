#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use serde::Deserialize;
#[cfg(unix)]
use serde_json::json;

const MAX_COMMAND_LENGTH: usize = 256;
const MAX_ARGS_COUNT: usize = 128;
const MAX_ARG_LENGTH: usize = 4_096;
const CAPTURE_POLL_INTERVAL_MS: u64 = 5;
const CAPTURE_CHUNK_BYTES: usize = 4 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxProcessRunnerPolicy {
    pub enabled: bool,
    pub workspace_root: PathBuf,
    pub allowed_executables: Vec<String>,
    pub allowed_egress_hosts: Vec<String>,
    pub allowed_dns_suffixes: Vec<String>,
    pub cpu_time_limit_ms: u64,
    pub memory_limit_bytes: u64,
    pub max_output_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxProcessRunSuccess {
    pub output_json: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxProcessRunError {
    pub kind: SandboxProcessRunErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxProcessRunErrorKind {
    Disabled,
    #[cfg(not(unix))]
    UnsupportedPlatform,
    InvalidInput,
    WorkspaceScopeDenied,
    EgressDenied,
    QuotaExceeded,
    TimedOut,
    SpawnFailed,
    RuntimeFailure,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessRunnerInput {
    command: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    requested_egress_hosts: Vec<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug)]
struct ProcessExecutionCapture {
    exit_status: ExitStatus,
    stdout: StreamCapture,
    stderr: StreamCapture,
    timed_out: bool,
    quota_exceeded: bool,
    duration_ms: u64,
}

#[derive(Debug)]
struct StreamCapture {
    bytes: Vec<u8>,
    truncated: bool,
    read_error: Option<String>,
}

pub fn run_constrained_process(
    policy: &SandboxProcessRunnerPolicy,
    input_json: &[u8],
    execution_timeout: Duration,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    if !policy.enabled {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::Disabled,
            message: "sandbox process runner is disabled by runtime policy".to_owned(),
        });
    }

    #[cfg(not(unix))]
    {
        let _ = (policy, input_json, execution_timeout);
        Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::UnsupportedPlatform,
            message: "sandbox process runner requires unix resource controls for CPU/memory quotas"
                .to_owned(),
        })
    }

    #[cfg(unix)]
    {
        let input = parse_process_runner_input(input_json)?;
        validate_input_shape(&input)?;
        validate_allowed_executable(policy, input.command.as_str())?;

        let workspace_root = canonical_workspace_root(policy.workspace_root.as_path())?;
        let working_directory =
            resolve_working_directory(workspace_root.as_path(), input.cwd.as_deref())?;
        validate_argument_workspace_scope(
            workspace_root.as_path(),
            working_directory.as_path(),
            input.args.as_slice(),
        )?;

        let requested_hosts = collect_requested_egress_hosts(&input)?;
        validate_egress_hosts(policy, requested_hosts.as_slice())?;

        let per_call_timeout = input
            .timeout_ms
            .map(Duration::from_millis)
            .unwrap_or(execution_timeout)
            .min(execution_timeout);

        let capture =
            execute_process(policy, &input, working_directory.as_path(), per_call_timeout)?;
        if capture.timed_out {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::TimedOut,
                message: format!(
                    "sandbox process timed out after {}ms and was terminated",
                    per_call_timeout.as_millis()
                ),
            });
        }
        if capture.quota_exceeded {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::QuotaExceeded,
                message: format!(
                    "sandbox process exceeded output quota (max_output_bytes={}) and was terminated",
                    policy.max_output_bytes
                ),
            });
        }
        if let Some(error) = capture.stdout.read_error.as_ref() {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!("sandbox process stdout read failed: {error}"),
            });
        }
        if let Some(error) = capture.stderr.read_error.as_ref() {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!("sandbox process stderr read failed: {error}"),
            });
        }
        if !capture.exit_status.success() {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "sandbox process exited unsuccessfully (code={}) stderr={}",
                    capture.exit_status.code().unwrap_or(-1),
                    String::from_utf8_lossy(&capture.stderr.bytes)
                ),
            });
        }

        let stdout = String::from_utf8_lossy(&capture.stdout.bytes).to_string();
        let stderr = String::from_utf8_lossy(&capture.stderr.bytes).to_string();
        let output_json = serde_json::to_vec(&json!({
            "exit_code": capture.exit_status.code().unwrap_or(0),
            "stdout": stdout,
            "stderr": stderr,
            "stdout_truncated": capture.stdout.truncated,
            "stderr_truncated": capture.stderr.truncated,
            "duration_ms": capture.duration_ms,
        }))
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("failed to serialize sandbox process output JSON: {error}"),
        })?;
        Ok(SandboxProcessRunSuccess { output_json })
    }
}

fn parse_process_runner_input(
    input_json: &[u8],
) -> Result<ProcessRunnerInput, SandboxProcessRunError> {
    serde_json::from_slice::<ProcessRunnerInput>(input_json).map_err(|error| {
        SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.run input must be valid JSON object: {error}"),
        }
    })
}

fn validate_input_shape(input: &ProcessRunnerInput) -> Result<(), SandboxProcessRunError> {
    if input.command.trim().is_empty() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.run requires non-empty field 'command'".to_owned(),
        });
    }
    if input.command.len() > MAX_COMMAND_LENGTH {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.run command exceeds {MAX_COMMAND_LENGTH} characters"),
        });
    }
    if input.args.len() > MAX_ARGS_COUNT {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.run supports at most {MAX_ARGS_COUNT} args"),
        });
    }
    if input.args.iter().any(|arg| arg.len() > MAX_ARG_LENGTH) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.run arg exceeds {MAX_ARG_LENGTH} characters"),
        });
    }
    if let Some(timeout_ms) = input.timeout_ms {
        if timeout_ms == 0 {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: "palyra.process.run timeout_ms must be greater than 0".to_owned(),
            });
        }
    }
    Ok(())
}

fn validate_allowed_executable(
    policy: &SandboxProcessRunnerPolicy,
    command: &str,
) -> Result<(), SandboxProcessRunError> {
    let normalized = command.trim().to_ascii_lowercase();
    if normalized.contains('/') || normalized.contains('\\') {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message:
                "sandbox denied: command must be a bare executable name without path separators"
                    .to_owned(),
        });
    }
    if !policy
        .allowed_executables
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(normalized.as_str()))
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: executable '{command}' is not allowlisted for process runner"
            ),
        });
    }
    Ok(())
}

fn canonical_workspace_root(root: &Path) -> Result<PathBuf, SandboxProcessRunError> {
    let canonical = fs::canonicalize(root).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "sandbox denied: workspace_root '{}' is invalid: {error}",
            root.to_string_lossy()
        ),
    })?;
    if !canonical.is_dir() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: workspace_root '{}' is not a directory",
                canonical.to_string_lossy()
            ),
        });
    }
    Ok(canonical)
}

fn resolve_working_directory(
    workspace_root: &Path,
    cwd: Option<&str>,
) -> Result<PathBuf, SandboxProcessRunError> {
    let cwd_value = cwd.unwrap_or(".");
    let resolved = resolve_scoped_path(workspace_root, workspace_root, cwd_value, true)?;
    if !resolved.is_dir() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: cwd '{}' is not a directory within workspace scope",
                cwd_value
            ),
        });
    }
    Ok(resolved)
}

fn validate_argument_workspace_scope(
    workspace_root: &Path,
    cwd: &Path,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    for arg in args {
        if let Some(file_url_path) = parse_file_url_path(arg.as_str())? {
            let _ = resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)?;
            continue;
        }
        if let Some(value) = option_assignment_value(arg.as_str()) {
            let value = value.trim();
            if let Some(file_url_path) = parse_file_url_path(value)? {
                let _ = resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)?;
                continue;
            }
            if !argument_requires_path_validation(value) {
                continue;
            }
            let _ = resolve_scoped_path(workspace_root, cwd, value, false)?;
            continue;
        }
        if !argument_requires_path_validation(arg.as_str()) {
            continue;
        }
        let _ = resolve_scoped_path(workspace_root, cwd, arg.as_str(), false)?;
    }
    Ok(())
}

fn option_assignment_value(arg: &str) -> Option<&str> {
    let trimmed = arg.trim();
    if !trimmed.starts_with('-') {
        return None;
    }
    let (_, value) = trimmed.split_once('=')?;
    Some(value)
}

fn argument_requires_path_validation(arg: &str) -> bool {
    let trimmed = arg.trim();
    if trimmed.is_empty() || trimmed.starts_with('-') {
        return false;
    }
    match reqwest::Url::parse(trimmed) {
        Ok(url) => url.scheme().eq_ignore_ascii_case("file"),
        Err(_) => true,
    }
}

fn parse_file_url_path(arg: &str) -> Result<Option<String>, SandboxProcessRunError> {
    let trimmed = arg.trim();
    let url = match reqwest::Url::parse(trimmed) {
        Ok(url) => url,
        Err(_) => return Ok(None),
    };
    if !url.scheme().eq_ignore_ascii_case("file") {
        return Ok(None);
    }
    let file_path = url.to_file_path().map_err(|_| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!("sandbox denied: invalid file URL '{trimmed}'"),
    })?;
    Ok(Some(file_path.to_string_lossy().to_string()))
}

fn resolve_scoped_path(
    workspace_root: &Path,
    base: &Path,
    raw: &str,
    must_exist: bool,
) -> Result<PathBuf, SandboxProcessRunError> {
    if raw.contains('\0') {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "sandbox denied: path contains embedded NUL byte".to_owned(),
        });
    }
    let candidate = if Path::new(raw).is_absolute() { PathBuf::from(raw) } else { base.join(raw) };

    if candidate.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!("sandbox denied: path traversal is blocked for '{raw}'"),
        });
    }

    let inspected = if candidate.exists() {
        fs::canonicalize(&candidate).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!("sandbox denied: path '{}' is invalid: {error}", candidate.display()),
        })?
    } else if must_exist {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: required path '{}' does not exist",
                candidate.display()
            ),
        });
    } else {
        let ancestor = nearest_existing_ancestor(&candidate)?;
        fs::canonicalize(&ancestor).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: could not resolve parent path '{}' safely: {error}",
                ancestor.display()
            ),
        })?
    };

    if !inspected.starts_with(workspace_root) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: path '{}' escapes workspace scope '{}'",
                raw,
                workspace_root.display()
            ),
        });
    }

    if candidate.exists() {
        Ok(inspected)
    } else {
        Ok(candidate)
    }
}

fn nearest_existing_ancestor(path: &Path) -> Result<PathBuf, SandboxProcessRunError> {
    let mut current = Some(path.to_path_buf());
    while let Some(candidate) = current {
        if candidate.exists() {
            return Ok(candidate);
        }
        current = candidate.parent().map(Path::to_path_buf);
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "sandbox denied: could not resolve any existing parent for '{}'",
            path.display()
        ),
    })
}

fn collect_requested_egress_hosts(
    input: &ProcessRunnerInput,
) -> Result<Vec<String>, SandboxProcessRunError> {
    let mut hosts = Vec::new();
    for requested in &input.requested_egress_hosts {
        push_normalized_host(&mut hosts, requested)?;
    }
    for (index, arg) in input.args.iter().enumerate() {
        collect_hosts_from_token(&mut hosts, arg, false)?;
        if let Some((key, value)) = arg.split_once('=') {
            collect_hosts_from_token(&mut hosts, value, is_host_hint_key(key))?;
            continue;
        }
        if is_host_hint_key(arg.as_str()) {
            if let Some(next_value) = input.args.get(index + 1) {
                collect_hosts_from_token(&mut hosts, next_value, true)?;
            }
        }
    }
    Ok(hosts)
}

fn collect_hosts_from_token(
    hosts: &mut Vec<String>,
    raw: &str,
    host_context: bool,
) -> Result<(), SandboxProcessRunError> {
    let token = raw.trim().trim_matches(['"', '\'']);
    if token.is_empty() {
        return Ok(());
    }
    if let Ok(url) = reqwest::Url::parse(token) {
        if let Some(host) = url.host_str() {
            push_normalized_host(hosts, host)?;
            return Ok(());
        }
    }
    if let Some(host) = maybe_extract_bare_host(token, host_context) {
        push_normalized_host(hosts, host)?;
    }
    Ok(())
}

fn maybe_extract_bare_host(token: &str, host_context: bool) -> Option<&str> {
    let sanitized = token.trim_end_matches([')', ',', ';']);
    if sanitized.is_empty()
        || sanitized.starts_with('-')
        || sanitized.contains(char::is_whitespace)
        || sanitized.contains('/')
        || sanitized.contains('\\')
        || sanitized.contains('=')
    {
        return None;
    }

    if host_context && looks_like_domain_or_ipv4(sanitized) {
        return Some(sanitized);
    }

    let (host, port) = split_host_and_port(sanitized)?;
    if !port.chars().all(|ch| ch.is_ascii_digit()) || !looks_like_domain_or_ipv4(host) {
        return None;
    }
    Some(host)
}

fn split_host_and_port(token: &str) -> Option<(&str, &str)> {
    let (host, port) = token.rsplit_once(':')?;
    if host.is_empty() || port.is_empty() || host.contains(':') {
        return None;
    }
    Some((host, port))
}

fn looks_like_domain_or_ipv4(raw: &str) -> bool {
    let candidate = raw.trim_matches(['[', ']']).trim_end_matches('.').to_ascii_lowercase();

    if candidate.eq("localhost") || candidate.parse::<std::net::Ipv4Addr>().is_ok() {
        return true;
    }
    if !candidate.contains('.')
        || !candidate
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '-'))
        || candidate.starts_with('.')
        || candidate.ends_with('.')
        || candidate.starts_with('-')
        || candidate.ends_with('-')
        || candidate.contains("..")
    {
        return false;
    }
    candidate
        .rsplit('.')
        .next()
        .map(|suffix| suffix.len() >= 2 && suffix.chars().all(|ch| ch.is_ascii_alphabetic()))
        .unwrap_or(false)
}

fn is_host_hint_key(raw: &str) -> bool {
    let normalized = raw.trim().trim_start_matches('-').to_ascii_lowercase();
    normalized.split(|ch: char| !ch.is_ascii_alphanumeric()).any(|segment| {
        matches!(
            segment,
            "host"
                | "hostname"
                | "server"
                | "endpoint"
                | "url"
                | "uri"
                | "domain"
                | "proxy"
                | "address"
                | "addr"
        )
    })
}

fn push_normalized_host(hosts: &mut Vec<String>, raw: &str) -> Result<(), SandboxProcessRunError> {
    let normalized = normalize_host(raw)?;
    if !hosts.iter().any(|candidate| candidate == &normalized) {
        hosts.push(normalized);
    }
    Ok(())
}

fn normalize_host(raw: &str) -> Result<String, SandboxProcessRunError> {
    let normalized = raw.trim().trim_end_matches('.').to_ascii_lowercase();
    if normalized.is_empty()
        || !normalized
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '-'))
        || normalized.starts_with('.')
        || normalized.ends_with('.')
        || normalized.starts_with('-')
        || normalized.ends_with('-')
        || normalized.contains("..")
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.run contains invalid egress host '{raw}'"),
        });
    }
    Ok(normalized)
}

fn validate_egress_hosts(
    policy: &SandboxProcessRunnerPolicy,
    hosts: &[String],
) -> Result<(), SandboxProcessRunError> {
    for host in hosts {
        if is_host_allowlisted(policy, host.as_str()) {
            continue;
        }
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::EgressDenied,
            message: format!("sandbox denied: egress host '{host}' is not allowlisted"),
        });
    }
    Ok(())
}

fn is_host_allowlisted(policy: &SandboxProcessRunnerPolicy, host: &str) -> bool {
    if policy.allowed_egress_hosts.iter().any(|allowed| allowed.eq_ignore_ascii_case(host)) {
        return true;
    }
    policy.allowed_dns_suffixes.iter().any(|suffix| {
        let suffix = suffix.trim().to_ascii_lowercase();
        if suffix.is_empty() {
            return false;
        }
        let bare_suffix = suffix.trim_start_matches('.');
        let dotted_suffix = format!(".{bare_suffix}");
        host.eq_ignore_ascii_case(bare_suffix) || host.ends_with(dotted_suffix.as_str())
    })
}

fn execute_process(
    policy: &SandboxProcessRunnerPolicy,
    input: &ProcessRunnerInput,
    cwd: &Path,
    timeout: Duration,
) -> Result<ProcessExecutionCapture, SandboxProcessRunError> {
    let mut command = Command::new(input.command.as_str());
    command
        .args(input.args.as_slice())
        .current_dir(cwd)
        .env_clear()
        .env("PATH", sandbox_process_path())
        .env("LANG", "C")
        .env("LC_ALL", "C")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    attach_resource_limits_unix(&mut command, policy);

    let mut child = command.spawn().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: format!("sandbox process spawn failed for command '{}': {error}", input.command),
    })?;

    capture_child_output(&mut child, timeout, policy.max_output_bytes as usize)
}

fn sandbox_process_path() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "/usr/bin:/bin:/usr/sbin:/sbin"
    }
    #[cfg(not(target_os = "macos"))]
    {
        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
    }
}

#[cfg(unix)]
fn attach_resource_limits_unix(command: &mut Command, policy: &SandboxProcessRunnerPolicy) {
    use std::os::unix::process::CommandExt;

    let cpu_time_limit_ms = policy.cpu_time_limit_ms;
    let memory_limit_bytes = policy.memory_limit_bytes;
    unsafe {
        command.pre_exec(move || {
            set_cpu_rlimit(cpu_time_limit_ms)?;
            set_memory_rlimit(memory_limit_bytes)?;
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn attach_resource_limits_unix(_command: &mut Command, _policy: &SandboxProcessRunnerPolicy) {}

#[cfg(unix)]
fn set_rlimit(resource: libc::c_int, limit: libc::rlim_t) -> std::io::Result<()> {
    let rlimit = libc::rlimit { rlim_cur: limit, rlim_max: limit };
    let result = unsafe { libc::setrlimit(resource as _, &rlimit) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
fn set_cpu_rlimit(cpu_time_limit_ms: u64) -> std::io::Result<()> {
    map_macos_einval_to_ok(set_rlimit(
        libc::RLIMIT_CPU as libc::c_int,
        cpu_ms_to_rlimit_seconds(cpu_time_limit_ms),
    ))
}

#[cfg(unix)]
fn set_memory_rlimit(memory_limit_bytes: u64) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        return map_macos_einval_to_ok(set_rlimit(
            libc::RLIMIT_DATA as libc::c_int,
            memory_limit_bytes as libc::rlim_t,
        ));
    }
    #[cfg(not(target_os = "macos"))]
    {
        set_rlimit(libc::RLIMIT_AS as libc::c_int, memory_limit_bytes as libc::rlim_t)
    }
}

#[cfg(unix)]
fn map_macos_einval_to_ok(result: std::io::Result<()>) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        match result {
            Err(error) if error.raw_os_error() == Some(libc::EINVAL) => Ok(()),
            other => other,
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        result
    }
}

#[cfg(unix)]
fn cpu_ms_to_rlimit_seconds(cpu_time_limit_ms: u64) -> libc::rlim_t {
    cpu_time_limit_ms.max(1).div_ceil(1_000) as libc::rlim_t
}

fn capture_child_output(
    child: &mut Child,
    timeout: Duration,
    max_output_bytes: usize,
) -> Result<ProcessExecutionCapture, SandboxProcessRunError> {
    let stdout = child.stdout.take().ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: "sandbox process stdout pipe is unavailable".to_owned(),
    })?;
    let stderr = child.stderr.take().ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: "sandbox process stderr pipe is unavailable".to_owned(),
    })?;

    let quota_triggered = Arc::new(AtomicBool::new(false));
    let remaining_budget = Arc::new(AtomicUsize::new(max_output_bytes));
    let stdout_reader =
        spawn_capture_reader(stdout, Arc::clone(&remaining_budget), Arc::clone(&quota_triggered));
    let stderr_reader =
        spawn_capture_reader(stderr, Arc::clone(&remaining_budget), Arc::clone(&quota_triggered));

    let started_at = Instant::now();
    let mut timed_out = false;
    let mut quota_exceeded = false;
    let exit_status = loop {
        if quota_triggered.load(Ordering::Relaxed) {
            quota_exceeded = true;
            let _ = child.kill();
        }
        if started_at.elapsed() > timeout {
            timed_out = true;
            let _ = child.kill();
        }

        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => thread::sleep(Duration::from_millis(CAPTURE_POLL_INTERVAL_MS)),
            Err(error) => {
                return Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!("sandbox process wait failed: {error}"),
                });
            }
        }
    };

    let stdout = stdout_reader.join().map_err(|_| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: "sandbox stdout reader thread panicked".to_owned(),
    })?;
    let stderr = stderr_reader.join().map_err(|_| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: "sandbox stderr reader thread panicked".to_owned(),
    })?;
    quota_exceeded = quota_exceeded
        || quota_triggered.load(Ordering::Relaxed)
        || stdout.truncated
        || stderr.truncated;

    Ok(ProcessExecutionCapture {
        exit_status,
        stdout,
        stderr,
        timed_out,
        quota_exceeded,
        duration_ms: started_at.elapsed().as_millis() as u64,
    })
}

fn spawn_capture_reader<R>(
    mut reader: R,
    remaining_budget: Arc<AtomicUsize>,
    quota_triggered: Arc<AtomicBool>,
) -> thread::JoinHandle<StreamCapture>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0_u8; CAPTURE_CHUNK_BYTES];
        let mut bytes = Vec::new();
        let mut truncated = false;
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read_count) => {
                    let granted = reserve_output_budget(remaining_budget.as_ref(), read_count);
                    if granted > 0 {
                        bytes.extend_from_slice(&buffer[..granted]);
                    }
                    if granted < read_count {
                        truncated = true;
                        quota_triggered.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                Err(error) => {
                    return StreamCapture { bytes, truncated, read_error: Some(error.to_string()) };
                }
            }
        }
        StreamCapture { bytes, truncated, read_error: None }
    })
}

fn reserve_output_budget(remaining_budget: &AtomicUsize, requested_bytes: usize) -> usize {
    let mut available = remaining_budget.load(Ordering::Relaxed);
    loop {
        if available == 0 {
            return 0;
        }
        let granted = requested_bytes.min(available);
        match remaining_budget.compare_exchange_weak(
            available,
            available - granted,
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(_) => return granted,
            Err(updated) => available = updated,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        process::Command,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use super::{
        canonical_workspace_root, collect_requested_egress_hosts, is_host_allowlisted,
        run_constrained_process, validate_argument_workspace_scope, ProcessRunnerInput,
        SandboxProcessRunErrorKind, SandboxProcessRunnerPolicy,
    };

    fn sandbox_policy_with_allowed_executables(
        workspace_root: PathBuf,
        allowed_executables: Vec<String>,
    ) -> SandboxProcessRunnerPolicy {
        SandboxProcessRunnerPolicy {
            enabled: true,
            workspace_root,
            allowed_executables,
            allowed_egress_hosts: vec!["allowed.example".to_owned()],
            allowed_dns_suffixes: vec![".corp.local".to_owned()],
            cpu_time_limit_ms: 2_000,
            memory_limit_bytes: 128 * 1024 * 1024,
            max_output_bytes: 64 * 1024,
        }
    }

    fn sandbox_policy(workspace_root: PathBuf) -> SandboxProcessRunnerPolicy {
        sandbox_policy_with_allowed_executables(workspace_root, vec!["uname".to_owned()])
    }

    fn unique_temp_dir(suffix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after UNIX epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("palyra-sandbox-runner-{suffix}-{nanos}-{}", std::process::id()))
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_rejects_path_traversal_arguments() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let input = br#"{"command":"uname","args":["../outside.txt"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("path traversal must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("path traversal"));
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_rejects_non_allowlisted_egress_host() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let input = br#"{"command":"uname","args":["--version","https://blocked.example/path"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("blocked host must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(error.message.contains("blocked.example"));
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_rejects_non_allowlisted_egress_host_from_host_hint() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let input = br#"{"command":"uname","args":["--host=blocked.example"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("host hint should be validated against egress allowlists");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(error.message.contains("blocked.example"));
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_rejects_non_allowlisted_executable() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let input = br#"{"command":"cargo","args":["--version"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("non-allowlisted executable must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("not allowlisted"));
    }

    #[test]
    #[cfg(not(unix))]
    fn run_constrained_process_fails_closed_on_non_unix() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let input = br#"{"command":"uname","args":["--version"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("non-unix sandbox runner must fail closed");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::UnsupportedPlatform);
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_executes_allowlisted_command() {
        if Command::new("uname").output().is_err() {
            return;
        }
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let input = br#"{"command":"uname","args":[]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(3_000))
            .expect("allowlisted command should execute");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        let stdout = output
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .expect("stdout should be present in process output");
        assert!(!stdout.trim().is_empty(), "stdout should include uname output");
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_rejects_symlink_escape() {
        use std::os::unix::fs::symlink;

        let workspace = unique_temp_dir("workspace");
        let outside = unique_temp_dir("outside");
        fs::create_dir_all(&workspace).expect("workspace directory should be created");
        fs::create_dir_all(&outside).expect("outside directory should be created");

        let symlink_path = workspace.join("escape-link");
        symlink(&outside, &symlink_path).expect("symlink should be created");

        let policy = sandbox_policy(workspace.clone());
        let input =
            format!("{{\"command\":\"uname\",\"args\":[\"{}\"]}}", symlink_path.to_string_lossy());
        let error =
            run_constrained_process(&policy, input.as_bytes(), Duration::from_millis(1_000))
                .expect_err("symlink escape must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_file(&symlink_path);
        let _ = fs::remove_dir_all(&workspace);
        let _ = fs::remove_dir_all(&outside);
    }

    #[test]
    #[cfg(unix)]
    fn validate_argument_workspace_scope_rejects_file_url_outside_workspace() {
        let workspace = unique_temp_dir("workspace-file-url-outside");
        let outside = unique_temp_dir("outside-file-url-outside");
        fs::create_dir_all(&workspace).expect("workspace directory should be created");
        fs::create_dir_all(&outside).expect("outside directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");

        let outside_file = outside.join("secret.txt");
        fs::write(&outside_file, b"secret").expect("outside file should be created");
        let args = vec![format!("file://{}", outside_file.to_string_lossy())];

        let error = validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            &args,
        )
        .expect_err("file URLs outside workspace must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_file(&outside_file);
        let _ = fs::remove_dir_all(&workspace);
        let _ = fs::remove_dir_all(&outside);
    }

    #[test]
    #[cfg(unix)]
    fn validate_argument_workspace_scope_allows_file_url_inside_workspace() {
        let workspace = unique_temp_dir("workspace-file-url-inside");
        fs::create_dir_all(&workspace).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");

        let inside_file = workspace.join("inside.txt");
        fs::write(&inside_file, b"ok").expect("inside file should be created");
        let args = vec![format!("file://{}", inside_file.to_string_lossy())];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            &args,
        )
        .expect("file URLs inside workspace should be allowed");

        let _ = fs::remove_file(&inside_file);
        let _ = fs::remove_dir_all(&workspace);
    }

    #[test]
    #[cfg(unix)]
    fn validate_argument_workspace_scope_rejects_absolute_path_in_flag_assignment() {
        let workspace = unique_temp_dir("workspace-flag-assignment-absolute");
        fs::create_dir_all(&workspace).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["--config=/etc/passwd".to_owned()];

        let error = validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            &args,
        )
        .expect_err("absolute path in flag assignment must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_dir_all(&workspace);
    }

    #[test]
    #[cfg(unix)]
    fn validate_argument_workspace_scope_allows_workspace_relative_flag_assignment() {
        let workspace = unique_temp_dir("workspace-flag-assignment-relative");
        let inside_dir = workspace.join("inside");
        fs::create_dir_all(&inside_dir).expect("workspace subdirectory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["--config=inside/config.toml".to_owned()];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            &args,
        )
        .expect("workspace-relative path in flag assignment should be allowed");

        let _ = fs::remove_dir_all(&workspace);
    }

    #[test]
    #[cfg(unix)]
    fn is_host_allowlisted_enforces_dns_suffix_label_boundary() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.allowed_egress_hosts.clear();
        policy.allowed_dns_suffixes = vec!["corp.local".to_owned()];

        assert!(
            is_host_allowlisted(&policy, "api.corp.local"),
            "subdomain with label boundary should be allowlisted"
        );
        assert!(
            !is_host_allowlisted(&policy, "evilcorp.local"),
            "superdomain without label boundary must be denied"
        );
        assert!(
            is_host_allowlisted(&policy, "corp.local"),
            "exact suffix host should remain allowlisted"
        );
    }

    #[test]
    #[cfg(unix)]
    fn is_host_allowlisted_accepts_leading_dot_suffix_configuration() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.allowed_egress_hosts.clear();
        policy.allowed_dns_suffixes = vec![".corp.local".to_owned()];

        assert!(
            is_host_allowlisted(&policy, "api.corp.local"),
            "leading-dot suffix must allow matching subdomains"
        );
    }

    #[test]
    #[cfg(unix)]
    fn collect_requested_egress_hosts_extracts_hosts_from_host_hints() {
        let input = ProcessRunnerInput {
            command: "uname".to_owned(),
            args: vec![
                "--host=blocked.example".to_owned(),
                "--endpoint".to_owned(),
                "allowed.example:443".to_owned(),
                "README.md".to_owned(),
            ],
            cwd: None,
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
        };

        let hosts = collect_requested_egress_hosts(&input)
            .expect("host hint parsing should succeed for valid host values");
        assert!(hosts.iter().any(|host| host == "blocked.example"));
        assert!(hosts.iter().any(|host| host == "allowed.example"));
        assert!(
            !hosts.iter().any(|host| host == "readme.md"),
            "file-like args should not be treated as host candidates by default"
        );
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_sanitizes_child_environment() {
        if Command::new("env").output().is_err() {
            return;
        }

        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy_with_allowed_executables(workspace, vec!["env".to_owned()]);
        let input = br#"{"command":"env","args":[]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(3_000))
            .expect("allowlisted env command should execute");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        let stdout = output
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .expect("stdout should be present in process output");

        for line in stdout.lines().filter(|line| !line.trim().is_empty()) {
            let key = line.split_once('=').map(|(key, _)| key).unwrap_or(line);
            assert!(
                matches!(key, "PATH" | "LANG" | "LC_ALL"),
                "unexpected environment variable leaked into sandbox process: {line}"
            );
        }
        assert!(stdout.contains("PATH="), "sandbox process should retain deterministic PATH");
        assert!(stdout.contains("LANG=C"), "sandbox process should set LANG=C");
        assert!(stdout.contains("LC_ALL=C"), "sandbox process should set LC_ALL=C");
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_enforces_combined_output_quota() {
        if Command::new("sh").arg("-c").arg("true").output().is_err() {
            return;
        }

        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy_with_allowed_executables(workspace, vec!["sh".to_owned()]);
        policy.max_output_bytes = 16;
        let input =
            br#"{"command":"sh","args":["-c","printf '1234567890'; printf 'abcdefghij' 1>&2"]}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("combined stdout+stderr output should hit global quota");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::QuotaExceeded);
    }
}
