#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

use std::{
    fs,
    io::{self, Read},
    path::{Component, Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(windows)]
use std::{
    collections::HashMap,
    os::windows::{io::AsRawHandle, process::CommandExt},
    sync::OnceLock,
};

#[cfg(windows)]
use windows_sys::Win32::{
    Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE},
    System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
        JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
        TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    },
};

use palyra_common::{
    process_runner_input::{parse_process_runner_tool_input, ProcessRunnerToolInput},
    redaction::{redact_auth_error, redact_url_segments_in_text, REDACTED},
};
use palyra_safety::{redact_text_for_export, SafetyContentKind, SafetySourceKind, TrustLabel};
use palyra_sandbox::{
    build_tier_c_command_plan, current_backend_capabilities, current_backend_executor,
    current_backend_kind, TierCBackendError, TierCCommandRequest, TierCPolicy,
};
use serde_json::{json, Value};

const MAX_COMMAND_LENGTH: usize = 256;
const MAX_ARGS_COUNT: usize = 128;
const MAX_ARG_LENGTH: usize = 4_096;
const MAX_ENV_COUNT: usize = 32;
const MAX_ENV_KEY_LENGTH: usize = 128;
const MAX_ENV_VALUE_LENGTH: usize = 4_096;
const BUILTIN_LIST_MAX_ENTRIES: usize = 512;
const BUILTIN_READ_FILE_MAX_BYTES: usize = 64 * 1024;
const CAPTURE_POLL_INTERVAL_MS: u64 = 5;
const CAPTURE_CHUNK_BYTES: usize = 4 * 1024;
const PROCESS_FAILURE_OUTPUT_PREVIEW_BYTES: usize = 4 * 1024;
const BACKGROUND_STARTUP_CHECK_MS: u64 = 250;
#[cfg(windows)]
const BACKGROUND_STARTUP_OUTPUT_DRAIN_MS: u64 = 4_000;
#[cfg(not(windows))]
const BACKGROUND_STARTUP_OUTPUT_DRAIN_MS: u64 = 1_000;
const BACKGROUND_POST_OUTPUT_EXIT_CHECK_MS: u64 = 250;
const BACKGROUND_METADATA_RETURN_RESERVE_MS: u64 = 100;
const BACKGROUND_MONITOR_POLL_MS: u64 = 50;
const BACKGROUND_TERMINATION_WAIT_MS: u64 = 1_000;
const DEFAULT_FOREGROUND_PROCESS_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_BACKGROUND_PROCESS_LIFETIME_MS: u64 = 10 * 60_000;
const MAX_BACKGROUND_PROCESS_LIFETIME_MS: u64 = 30 * 60_000;
const PALYRA_CLI_PROFILE_ENV: &str = "PALYRA_CLI_PROFILE";
const PALYRA_CLI_PROFILES_PATH_ENV: &str = "PALYRA_CLI_PROFILES_PATH";
const PALYRA_STATE_ROOT_ENV: &str = "PALYRA_STATE_ROOT";
const CLI_PROFILES_RELATIVE_PATH: &str = "cli/profiles.toml";
const DESKTOP_CONTROL_CENTER_STATE_DIR: &str = "desktop-control-center";
const DESKTOP_RUNTIME_STATE_DIR: &str = "runtime";
const WORKSPACE_PYTHON_USER_BASE_RELATIVE_PATH: &[&str] = &[".palyra", "python-userbase"];
const WORKSPACE_PIP_CACHE_RELATIVE_PATH: &[&str] = &[".palyra", "pip-cache"];
const SENSITIVE_URL_PATH_MARKERS: &[&str] =
    &["token", "secret", "key", "password", "credential", "session"];
#[cfg(windows)]
const WINDOWS_DEFAULT_PATH_EXTENSIONS: &[&str] = &[".com", ".exe", ".bat", ".cmd"];
const INTERPRETER_EXECUTABLE_DENYLIST: &[&str] = &[
    "bash",
    "sh",
    "zsh",
    "fish",
    "powershell",
    "pwsh",
    "cmd",
    "python",
    "python3",
    "node",
    "ruby",
    "perl",
    "deno",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressEnforcementMode {
    None,
    Preflight,
    Strict,
}

impl EgressEnforcementMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Preflight => "preflight",
            Self::Strict => "strict",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxProcessRunnerTier {
    B,
    C,
}

impl SandboxProcessRunnerTier {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::B => "b",
            Self::C => "c",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxProcessRunnerPolicy {
    pub enabled: bool,
    pub tier: SandboxProcessRunnerTier,
    pub workspace_root: PathBuf,
    pub allowed_executables: Vec<String>,
    pub allow_interpreters: bool,
    pub egress_enforcement_mode: EgressEnforcementMode,
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
    #[cfg_attr(all(unix, not(target_os = "macos")), allow(dead_code))]
    UnsupportedPlatform,
    InvalidInput,
    WorkspaceScopeDenied,
    EgressDenied,
    QuotaExceeded,
    TimedOut,
    SpawnFailed,
    RuntimeFailure,
}

#[must_use]
pub fn process_runner_executor_name(policy: &SandboxProcessRunnerPolicy) -> String {
    if process_runner_allows_host_access(policy) {
        return "host_process".to_owned();
    }
    if matches!(policy.tier, SandboxProcessRunnerTier::C) {
        current_backend_executor().to_owned()
    } else {
        "sandbox_tier_b".to_owned()
    }
}

#[must_use]
pub fn process_runner_allows_host_access(policy: &SandboxProcessRunnerPolicy) -> bool {
    matches!(policy.tier, SandboxProcessRunnerTier::B)
        && matches!(policy.egress_enforcement_mode, EgressEnforcementMode::None)
        && policy.allowed_executables.iter().any(|allowed| allowed.trim() == "*")
}

type ProcessRunnerInput = ProcessRunnerToolInput;

#[derive(Debug)]
struct ProcessExecutionCapture {
    exit_status: ExitStatus,
    stdout: StreamCapture,
    stderr: StreamCapture,
    timed_out: bool,
    quota_exceeded: bool,
    duration_ms: u64,
}

#[derive(Debug, Clone)]
struct StreamCapture {
    bytes: Vec<u8>,
    truncated: bool,
    read_error: Option<String>,
}

#[derive(Debug)]
struct BackgroundOutputMonitor {
    stdout: Arc<Mutex<StreamCapture>>,
    stderr: Arc<Mutex<StreamCapture>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BackgroundProcessRuntimeStatus {
    direct_pid_alive: bool,
    process_tree_alive: bool,
    tracked_process_count: Option<u32>,
}

impl BackgroundProcessRuntimeStatus {
    fn alive(self) -> bool {
        self.process_tree_alive || self.direct_pid_alive
    }
}

#[cfg(windows)]
#[derive(Debug)]
struct WindowsBackgroundJob {
    handle: HANDLE,
    terminated: AtomicBool,
}

#[cfg(windows)]
unsafe impl Send for WindowsBackgroundJob {}

#[cfg(windows)]
unsafe impl Sync for WindowsBackgroundJob {}

#[cfg(windows)]
impl WindowsBackgroundJob {
    fn terminate(&self) -> io::Result<()> {
        if self.terminated.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        // SAFETY: `handle` is a valid job handle owned by this wrapper until Drop closes it.
        let terminated = unsafe { TerminateJobObject(self.handle, 1) };
        if terminated == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn active_process_count(&self) -> io::Result<u32> {
        let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: `accounting` is a valid writable buffer for the requested information class and
        // `handle` is owned by this wrapper until Drop.
        let queried = unsafe {
            QueryInformationJobObject(
                self.handle,
                JobObjectBasicAccountingInformation,
                (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                std::mem::size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                std::ptr::null_mut(),
            )
        };
        if queried == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(accounting.ActiveProcesses)
    }
}

#[cfg(windows)]
impl Drop for WindowsBackgroundJob {
    fn drop(&mut self) {
        if windows_handle_is_valid(self.handle) {
            // SAFETY: `handle` is owned by this wrapper and is closed exactly once in Drop.
            let _ = unsafe { CloseHandle(self.handle) };
        }
    }
}

#[cfg(windows)]
static WINDOWS_BACKGROUND_JOBS: OnceLock<Mutex<HashMap<u32, Arc<WindowsBackgroundJob>>>> =
    OnceLock::new();

impl BackgroundOutputMonitor {
    fn snapshot(&self) -> (StreamCapture, StreamCapture) {
        let stdout =
            self.stdout.lock().map(|capture| capture.clone()).unwrap_or_else(|_| StreamCapture {
                bytes: Vec::new(),
                truncated: false,
                read_error: Some("background stdout capture lock poisoned".to_owned()),
            });
        let stderr =
            self.stderr.lock().map(|capture| capture.clone()).unwrap_or_else(|_| StreamCapture {
                bytes: Vec::new(),
                truncated: false,
                read_error: Some("background stderr capture lock poisoned".to_owned()),
            });
        (stdout, stderr)
    }

    fn snapshot_after_startup_drain(&self, max_wait: Duration) -> (StreamCapture, StreamCapture) {
        let started_at = Instant::now();
        let mut snapshot = self.snapshot();
        loop {
            if background_snapshot_has_output_or_error(&snapshot)
                || started_at.elapsed() >= max_wait
            {
                return snapshot;
            }
            thread::sleep(Duration::from_millis(CAPTURE_POLL_INTERVAL_MS));
            snapshot = self.snapshot();
        }
    }
}

fn background_snapshot_has_output_or_error(snapshot: &(StreamCapture, StreamCapture)) -> bool {
    let (stdout, stderr) = snapshot;
    !stdout.bytes.is_empty()
        || !stderr.bytes.is_empty()
        || stdout.truncated
        || stderr.truncated
        || stdout.read_error.is_some()
        || stderr.read_error.is_some()
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

    let input = parse_process_runner_input(input_json)?;
    validate_input_shape(&input)?;
    validate_allowed_executable(policy, input.command.as_str())?;
    validate_no_embedded_command_line_arg(&input)?;
    validate_cmd_invocation_shape(input.command.as_str(), input.args.as_slice())?;
    validate_process_termination_scope(input.command.as_str(), input.args.as_slice())?;

    let host_access = process_runner_allows_host_access(policy);
    let workspace_root = canonical_workspace_root(policy.workspace_root.as_path())?;
    let working_directory = if host_access {
        resolve_host_working_directory(workspace_root.as_path(), input.cwd.as_deref())?
    } else {
        resolve_working_directory(workspace_root.as_path(), input.cwd.as_deref())?
    };
    if host_access {
        validate_host_interpreter_argument_guardrails(
            workspace_root.as_path(),
            working_directory.as_path(),
            input.command.as_str(),
            input.args.as_slice(),
        )?;
        validate_host_argument_scope(
            workspace_root.as_path(),
            working_directory.as_path(),
            input.command.as_str(),
            input.args.as_slice(),
        )?;
    } else {
        validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            working_directory.as_path(),
            input.command.as_str(),
            input.args.as_slice(),
        )?;
        validate_argument_workspace_scope(
            workspace_root.as_path(),
            working_directory.as_path(),
            input.command.as_str(),
            input.args.as_slice(),
        )?;
    }
    let requested_hosts = if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::None) {
        Vec::new()
    } else {
        collect_requested_egress_hosts(&input)?
    };
    if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::Strict) {
        validate_tier_c_strict_offline_egress_requests(policy, requested_hosts.as_slice())?;
    }
    if !matches!(policy.egress_enforcement_mode, EgressEnforcementMode::None) {
        validate_egress_hosts(policy, requested_hosts.as_slice())?;
    }
    if let Some(result) = execute_builtin_process_command(
        policy,
        &input,
        workspace_root.as_path(),
        working_directory.as_path(),
    )? {
        return Ok(result);
    }
    if !host_access {
        validate_platform_resource_quota_support(policy)?;
    }
    if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::Strict) {
        validate_runtime_egress_enforcement(policy)?;
    }

    if input.background {
        let per_call_timeout = background_process_lifetime(input.timeout_ms, execution_timeout);
        let max_background_lifetime = background_process_lifetime_limit(execution_timeout);
        return spawn_background_process(
            policy,
            &input,
            workspace_root.as_path(),
            working_directory.as_path(),
            per_call_timeout,
            max_background_lifetime,
        );
    }

    let per_call_timeout = foreground_process_timeout(input.timeout_ms, execution_timeout);

    let capture = execute_process(
        policy,
        &input,
        workspace_root.as_path(),
        working_directory.as_path(),
        per_call_timeout,
    )?;
    if capture.timed_out {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::TimedOut,
            message: format!(
                "sandbox process timed out after {}ms and was terminated; for dev servers or intentional long-running services, rerun with background=true and an explicit timeout_ms lifetime, then poll or stop the returned process handle. Do not use background=true to verify tests or builds; rerun those foreground with a longer timeout after fixing the hang.",
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
            message: process_failure_message(
                capture.exit_status.code().unwrap_or(-1),
                &capture.stdout,
                &capture.stderr,
            ),
        });
    }

    let RedactedProcessOutputText { text: stdout, redacted: stdout_redacted } =
        redacted_process_output(capture.stdout.bytes.as_slice());
    let RedactedProcessOutputText { text: stderr, redacted: stderr_redacted } =
        redacted_process_output(capture.stderr.bytes.as_slice());
    let output_json = serde_json::to_vec(&json!({
        "exit_code": capture.exit_status.code().unwrap_or(0),
        "stdout": stdout,
        "stderr": stderr,
        "stdout_truncated": capture.stdout.truncated,
        "stderr_truncated": capture.stderr.truncated,
        "stdout_redacted": stdout_redacted,
        "stderr_redacted": stderr_redacted,
        "duration_ms": capture.duration_ms,
        "tier": policy.tier.as_str(),
        "sandbox_backend": if matches!(policy.tier, SandboxProcessRunnerTier::C) {
            current_backend_kind().as_str()
        } else {
            "tier_b_in_process"
        },
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox process output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

fn process_failure_message(
    exit_code: i32,
    stdout: &StreamCapture,
    stderr: &StreamCapture,
) -> String {
    let stdout_preview = redacted_process_output_preview(stdout.bytes.as_slice())
        .map(|preview| format!(", stdout_preview={preview:?}"))
        .unwrap_or_default();
    let stderr_preview = redacted_process_output_preview(stderr.bytes.as_slice())
        .map(|preview| format!(", stderr_preview={preview:?}"))
        .unwrap_or_default();
    format!(
        "sandbox process exited unsuccessfully (code={exit_code}, stdout_bytes={}, stdout_truncated={}, stderr_bytes={}, stderr_truncated={}{}{})",
        stdout.bytes.len(),
        stdout.truncated,
        stderr.bytes.len(),
        stderr.truncated,
        stdout_preview,
        stderr_preview,
    )
}

fn redacted_process_output_preview(output: &[u8]) -> Option<String> {
    if output.is_empty() {
        return None;
    }
    let take_len = output.len().min(PROCESS_FAILURE_OUTPUT_PREVIEW_BYTES);
    let preview = String::from_utf8_lossy(&output[..take_len]);
    let normalized = preview
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character })
        .collect::<String>();
    let collapsed = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return None;
    }
    let redacted_urls = redact_url_segments_in_text(collapsed.as_str());
    let redacted = redact_auth_error(redacted_urls.as_str());
    let redacted = redact_sensitive_url_path_segments_in_text(redacted.as_str());
    if redacted.trim().is_empty() {
        None
    } else {
        Some(redacted)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedactedProcessOutputText {
    text: String,
    redacted: bool,
}

fn redacted_process_output(output: &[u8]) -> RedactedProcessOutputText {
    let text = String::from_utf8_lossy(output).to_string();
    redacted_process_output_text(text.as_str())
}

fn redacted_process_output_text(value: &str) -> RedactedProcessOutputText {
    let redacted_urls = redact_url_segments_in_text(value);
    let redacted_auth = redact_auth_error(redacted_urls.as_str());
    let redacted_paths = redact_sensitive_url_path_segments_in_text(redacted_auth.as_str());
    let export_redaction = redact_text_for_export(
        redacted_paths.as_str(),
        SafetySourceKind::ToolOutput,
        SafetyContentKind::PlainText,
        TrustLabel::TrustedLocal,
    );
    let redacted_text =
        restore_process_output_trailing_line_endings(value, export_redaction.redacted_text);
    let redacted = redacted_urls != value
        || redacted_auth != redacted_urls
        || redacted_paths != redacted_auth
        || redacted_text != value;

    RedactedProcessOutputText { text: redacted_text, redacted }
}

fn restore_process_output_trailing_line_endings(original: &str, redacted: String) -> String {
    let original_base_len = original.trim_end_matches(['\r', '\n']).len();
    if original_base_len == original.len() {
        return redacted;
    }

    let expected_suffix = &original[original_base_len..];
    let redacted_base_len = redacted.trim_end_matches(['\r', '\n']).len();
    if &redacted[redacted_base_len..] == expected_suffix {
        return redacted;
    }

    let mut restored = redacted[..redacted_base_len].to_owned();
    restored.push_str(expected_suffix);
    restored
}

fn redact_sensitive_url_path_segments_in_text(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut token = String::new();

    for character in value.chars() {
        if character.is_whitespace() {
            output.push_str(redact_sensitive_url_path_token(token.as_str()).as_str());
            token.clear();
            output.push(character);
            continue;
        }
        token.push(character);
    }

    output.push_str(redact_sensitive_url_path_token(token.as_str()).as_str());
    output
}

fn redact_sensitive_url_path_token(token: &str) -> String {
    if !token.contains("://") {
        return token.to_owned();
    }
    let mut output = token.to_owned();
    for marker in SENSITIVE_URL_PATH_MARKERS {
        let pattern = format!("/{marker}/");
        let mut search_start = 0;
        loop {
            let normalized = output[search_start..].to_ascii_lowercase();
            let Some(relative_pos) = normalized.find(pattern.as_str()) else {
                break;
            };
            let secret_start = search_start + relative_pos + pattern.len();
            let secret_end = output[secret_start..]
                .char_indices()
                .find_map(|(offset, character)| {
                    matches!(character, '/' | '?' | '#' | '&').then_some(secret_start + offset)
                })
                .unwrap_or(output.len());
            if secret_end > secret_start {
                output.replace_range(secret_start..secret_end, REDACTED);
                search_start = secret_start + REDACTED.len();
            } else {
                search_start = secret_start;
            }
            if search_start >= output.len() {
                break;
            }
        }
    }
    output
}

fn execute_builtin_process_command(
    policy: &SandboxProcessRunnerPolicy,
    input: &ProcessRunnerInput,
    workspace_root: &Path,
    cwd: &Path,
) -> Result<Option<SandboxProcessRunSuccess>, SandboxProcessRunError> {
    let command = input.command.trim();
    match command.to_ascii_lowercase().as_str() {
        "palyra.process.stop" | "palyra-process-stop" => {
            return Ok(Some(builtin_stop_process_success(command, input.args.as_slice())?));
        }
        "palyra.process.status" | "palyra-process-status" => {
            return Ok(Some(builtin_process_status_success(command, input.args.as_slice())?));
        }
        _ => {}
    }

    let stdout = match command.to_ascii_lowercase().as_str() {
        "pwd" => {
            if !input.args.is_empty() {
                return Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::InvalidInput,
                    message: "palyra.process.run builtin 'pwd' does not accept args".to_owned(),
                });
            }
            format!("{}\n", cwd.to_string_lossy())
        }
        "echo" => format!("{}\n", input.args.join(" ")),
        "ls" | "dir" => {
            builtin_list_directory_stdout(command, input.args.as_slice(), workspace_root, cwd)?
        }
        "cat" | "type" => builtin_read_files_stdout(
            command,
            input.args.as_slice(),
            workspace_root,
            cwd,
            policy.max_output_bytes,
        )?,
        "mkdir" => builtin_make_directory_stdout(
            process_runner_allows_host_access(policy),
            command,
            input.args.as_slice(),
            workspace_root,
            cwd,
        )?,
        _ => return Ok(None),
    };
    let RedactedProcessOutputText { text: stdout, redacted: stdout_redacted } =
        redacted_process_output_text(stdout.as_str());
    let output_json = serde_json::to_vec(&json!({
        "exit_code": 0,
        "stdout": stdout,
        "stderr": "",
        "stdout_truncated": false,
        "stderr_truncated": false,
        "stdout_redacted": stdout_redacted,
        "stderr_redacted": false,
        "duration_ms": 0,
        "tier": policy.tier.as_str(),
        "sandbox_backend": "builtin_portable",
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox builtin process output JSON: {error}"),
    })?;
    Ok(Some(SandboxProcessRunSuccess { output_json }))
}

fn builtin_stop_process_success(
    command: &str,
    args: &[String],
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let pid = parse_builtin_pid_arg(command, args)?;
    let before_status =
        background_process_runtime_status(pid).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run builtin '{command}' failed to inspect pid {pid}: {error}"
            ),
        })?;
    let was_running = before_status.alive();
    let mut stop_error = None;
    if was_running {
        if let Err(error) = terminate_background_process_tree(pid) {
            stop_error = Some(error.to_string());
        }
    }
    let stopped = !was_running
        || wait_for_process_not_alive(pid, Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS));
    let after_status = background_process_runtime_status(pid).ok();
    let alive = !stopped && after_status.map(BackgroundProcessRuntimeStatus::alive).unwrap_or(true);
    if let Some(error) = stop_error.as_ref().filter(|_| !stopped) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run builtin '{command}' failed to stop pid {pid}: {error}"
            ),
        });
    }
    let output_json = serde_json::to_vec(&json!({
        "exit_code": 0,
        "stdout": format!("pid={pid} stopped={stopped} was_running={was_running}\n"),
        "stderr": "",
        "stdout_truncated": false,
        "stderr_truncated": false,
        "stdout_redacted": false,
        "stderr_redacted": false,
        "duration_ms": 0,
        "pid": pid,
        "was_running": was_running,
        "stopped": stopped,
        "alive": alive,
        "direct_pid_alive_before_stop": before_status.direct_pid_alive,
        "process_tree_alive_before_stop": before_status.process_tree_alive,
        "tracked_process_count_before_stop": before_status.tracked_process_count,
        "direct_pid_alive": after_status.map(|status| status.direct_pid_alive),
        "process_tree_alive": after_status.map(|status| status.process_tree_alive),
        "tracked_process_count": after_status.and_then(|status| status.tracked_process_count),
        "tier": "builtin",
        "sandbox_backend": "builtin_portable",
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox process stop output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

fn builtin_process_status_success(
    command: &str,
    args: &[String],
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let pid = parse_builtin_pid_arg(command, args)?;
    let status =
        background_process_runtime_status(pid).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run builtin '{command}' failed to inspect pid {pid}: {error}"
            ),
        })?;
    let alive = status.alive();
    let output_json = serde_json::to_vec(&json!({
        "exit_code": 0,
        "stdout": format!(
            "pid={pid} alive={alive} direct_pid_alive={} process_tree_alive={}\n",
            status.direct_pid_alive,
            status.process_tree_alive
        ),
        "stderr": "",
        "stdout_truncated": false,
        "stderr_truncated": false,
        "stdout_redacted": false,
        "stderr_redacted": false,
        "duration_ms": 0,
        "pid": pid,
        "alive": alive,
        "direct_pid_alive": status.direct_pid_alive,
        "process_tree_alive": status.process_tree_alive,
        "tracked_process_count": status.tracked_process_count,
        "tier": "builtin",
        "sandbox_backend": "builtin_portable",
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox process status output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

pub(crate) fn stop_background_process_by_pid(
    pid: u32,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let args = [pid.to_string()];
    builtin_stop_process_success("palyra.process.stop", &args)
}

pub(crate) fn background_process_status_by_pid(
    pid: u32,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let args = [pid.to_string()];
    builtin_process_status_success("palyra.process.status", &args)
}

pub(crate) fn background_process_is_alive(pid: u32) -> io::Result<bool> {
    background_process_runtime_status(pid).map(BackgroundProcessRuntimeStatus::alive)
}

fn background_process_runtime_status(pid: u32) -> io::Result<BackgroundProcessRuntimeStatus> {
    let direct_pid_alive = process_id_is_alive(pid)?;
    let (process_tree_alive, tracked_process_count) =
        background_process_tree_status(pid, direct_pid_alive)?;
    Ok(BackgroundProcessRuntimeStatus {
        direct_pid_alive,
        process_tree_alive,
        tracked_process_count,
    })
}

#[cfg(windows)]
fn background_process_tree_status(
    pid: u32,
    direct_pid_alive: bool,
) -> io::Result<(bool, Option<u32>)> {
    match windows_background_job_active_process_count(pid) {
        Some(Ok(active_count)) => Ok((active_count > 0, Some(active_count))),
        Some(Err(error)) if !direct_pid_alive => Err(error),
        Some(Err(_)) | None => Ok((direct_pid_alive, None)),
    }
}

#[cfg(not(windows))]
fn background_process_tree_status(
    _pid: u32,
    direct_pid_alive: bool,
) -> io::Result<(bool, Option<u32>)> {
    Ok((direct_pid_alive, None))
}

fn wait_for_process_not_alive(pid: u32, max_wait: Duration) -> bool {
    let started_at = Instant::now();
    loop {
        match background_process_runtime_status(pid) {
            Ok(status) if !status.alive() => return true,
            Ok(_) => {}
            Err(_) => return false,
        }
        if started_at.elapsed() >= max_wait {
            return false;
        }
        thread::sleep(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS));
    }
}

fn parse_builtin_pid_arg(command: &str, args: &[String]) -> Result<u32, SandboxProcessRunError> {
    let pid_arg = match args {
        [pid] => pid.as_str(),
        [flag, pid] if matches!(flag.trim().to_ascii_lowercase().as_str(), "--pid" | "/pid") => {
            pid.as_str()
        }
        _ => {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.run builtin '{command}' requires a single numeric pid argument"
                ),
            });
        }
    };
    let pid = pid_arg.trim().parse::<u32>().map_err(|_| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: format!(
            "palyra.process.run builtin '{command}' requires a valid positive pid, got {pid_arg:?}"
        ),
    })?;
    if pid == 0 {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.run builtin '{command}' requires a valid positive pid, got 0"
            ),
        });
    }
    Ok(pid)
}

fn builtin_read_files_stdout(
    command: &str,
    args: &[String],
    workspace_root: &Path,
    cwd: &Path,
    max_output_bytes: u64,
) -> Result<String, SandboxProcessRunError> {
    let mut paths = Vec::new();
    let mut end_of_options = false;
    for arg in args {
        let trimmed = arg.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !end_of_options && trimmed == "--" {
            end_of_options = true;
            continue;
        }
        if !end_of_options && trimmed.starts_with('-') {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.run builtin '{command}' does not support flag '{trimmed}'"
                ),
            });
        }
        paths.push(trimmed);
    }

    if paths.is_empty() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.run builtin '{command}' requires a file path"),
        });
    }

    let max_bytes =
        usize::try_from(max_output_bytes).unwrap_or(usize::MAX).min(BUILTIN_READ_FILE_MAX_BYTES);
    let max_bytes = max_bytes.max(1);
    let mut output = Vec::new();
    let mut truncated = false;
    for path in paths {
        if output.len() >= max_bytes {
            truncated = true;
            break;
        }
        let file_path = resolve_scoped_path(workspace_root, cwd, path, true)?;
        if !file_path.is_file() {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.run builtin '{command}' target '{}' is not a file",
                    file_path.display()
                ),
            });
        }
        let remaining = max_bytes.saturating_sub(output.len());
        let mut file =
            fs::File::open(file_path.as_path()).map_err(|error| SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "palyra.process.run builtin '{command}' failed to open '{}': {error}",
                    file_path.display()
                ),
            })?;
        let mut chunk = Vec::new();
        file.by_ref().take((remaining + 1) as u64).read_to_end(&mut chunk).map_err(|error| {
            SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "palyra.process.run builtin '{command}' failed to read '{}': {error}",
                    file_path.display()
                ),
            }
        })?;
        if chunk.len() > remaining {
            output.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        output.extend_from_slice(chunk.as_slice());
    }

    let mut stdout = String::from_utf8_lossy(output.as_slice()).to_string();
    if truncated {
        stdout.push_str(&format!("\n... truncated after {max_bytes} bytes\n"));
    }
    Ok(stdout)
}

fn builtin_list_directory_stdout(
    command: &str,
    args: &[String],
    workspace_root: &Path,
    cwd: &Path,
) -> Result<String, SandboxProcessRunError> {
    let target = resolve_builtin_list_directory_target(command, args, workspace_root, cwd)?;
    let mut names = Vec::new();
    let mut truncated = false;
    let entries = fs::read_dir(target.as_path()).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "palyra.process.run builtin '{command}' failed to read directory '{}': {error}",
            target.display()
        ),
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run builtin '{command}' failed to read directory entry in '{}': {error}",
                target.display()
            ),
        })?;
        let mut name = entry.file_name().to_string_lossy().to_string();
        if entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
            name.push('/');
        }
        names.push(name);
        if names.len() >= BUILTIN_LIST_MAX_ENTRIES {
            truncated = true;
            break;
        }
    }
    names.sort_by_key(|name| name.to_ascii_lowercase());
    if truncated {
        names.push(format!("... truncated after {BUILTIN_LIST_MAX_ENTRIES} entries"));
    }
    if names.is_empty() {
        return Ok(String::new());
    }
    Ok(format!("{}\n", names.join("\n")))
}

fn resolve_builtin_list_directory_target(
    command: &str,
    args: &[String],
    workspace_root: &Path,
    cwd: &Path,
) -> Result<PathBuf, SandboxProcessRunError> {
    let mut target = None;
    for arg in args {
        let trimmed = arg.trim();
        if trimmed.is_empty() || is_builtin_list_flag(trimmed) {
            continue;
        }
        if target.replace(trimmed).is_some() {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.run builtin '{command}' supports at most one directory argument"
                ),
            });
        }
    }

    let raw = target.unwrap_or(".");
    let canonical = resolve_scoped_path(workspace_root, cwd, raw, true)?;
    if !canonical.is_dir() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.run builtin '{command}' target '{}' is not a directory",
                canonical.display()
            ),
        });
    }
    Ok(canonical)
}

fn is_builtin_list_flag(arg: &str) -> bool {
    matches!(
        arg.to_ascii_lowercase().as_str(),
        "-a" | "-l" | "-la" | "-al" | "--all" | "--long" | "/a" | "/b" | "/w"
    )
}

fn builtin_make_directory_stdout(
    host_access: bool,
    command: &str,
    args: &[String],
    workspace_root: &Path,
    cwd: &Path,
) -> Result<String, SandboxProcessRunError> {
    let mut parents = false;
    let mut directories = Vec::new();
    for arg in args {
        let trimmed = arg.trim();
        if trimmed.is_empty() {
            continue;
        }
        match trimmed {
            "-p" | "--parents" => {
                parents = true;
            }
            value if value.starts_with('-') => {
                return Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::InvalidInput,
                    message: format!(
                        "palyra.process.run builtin '{command}' unsupported flag '{value}'"
                    ),
                });
            }
            value => directories.push(value),
        }
    }

    if directories.is_empty() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.run builtin '{command}' requires a directory path"),
        });
    }

    let mut created = Vec::new();
    for directory in directories {
        let target = if host_access {
            resolve_host_mutation_path(workspace_root, cwd, directory)?
        } else {
            resolve_scoped_path(workspace_root, cwd, directory, false)?
        };
        if parents {
            fs::create_dir_all(target.as_path()).map_err(|error| SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "palyra.process.run builtin '{command}' failed to create directory '{}': {error}",
                    target.display()
                ),
            })?;
        } else {
            fs::create_dir(target.as_path()).map_err(|error| SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "palyra.process.run builtin '{command}' failed to create directory '{}': {error}",
                    target.display()
                ),
            })?;
        }
        created.push(target.to_string_lossy().to_string());
    }

    Ok(format!("{}\n", created.join("\n")))
}

fn resolve_host_mutation_path(
    workspace_root: &Path,
    cwd: &Path,
    raw: &str,
) -> Result<PathBuf, SandboxProcessRunError> {
    if raw.contains('\0') {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner denied path with embedded NUL byte".to_owned(),
        });
    }
    resolve_scoped_path(workspace_root, cwd, raw, false)
}

fn parse_process_runner_input(
    input_json: &[u8],
) -> Result<ProcessRunnerInput, SandboxProcessRunError> {
    parse_process_runner_tool_input(input_json).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: format!("palyra.process.run input must be valid JSON object: {error}"),
    })
}

fn validate_input_shape(input: &ProcessRunnerInput) -> Result<(), SandboxProcessRunError> {
    if input.command.trim().is_empty() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.run requires non-empty field 'command'".to_owned(),
        });
    }
    if input.command.chars().any(char::is_whitespace) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: process_runner_command_with_args_message(input.command.as_str()),
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
    validate_process_env_overrides(&input.env)?;
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

fn validate_process_env_overrides(
    env: &std::collections::BTreeMap<String, String>,
) -> Result<(), SandboxProcessRunError> {
    if env.len() > MAX_ENV_COUNT {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.run env supports at most {MAX_ENV_COUNT} entries"),
        });
    }

    for (key, value) in env {
        validate_process_env_key(key)?;
        if value.len() > MAX_ENV_VALUE_LENGTH {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.run env value for key '{}' exceeds {MAX_ENV_VALUE_LENGTH} characters",
                    redact_env_key_for_error(key)
                ),
            });
        }
        if value.contains('\0') {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.run env value for key '{}' contains a NUL byte",
                    redact_env_key_for_error(key)
                ),
            });
        }
    }

    Ok(())
}

fn validate_process_env_key(key: &str) -> Result<(), SandboxProcessRunError> {
    let trimmed = key.trim();
    if trimmed.is_empty()
        || trimmed.len() > MAX_ENV_KEY_LENGTH
        || trimmed != key
        || !valid_process_env_key_shape(trimmed)
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.run env keys must match [A-Za-z_][A-Za-z0-9_]*".to_owned(),
        });
    }

    if process_env_key_is_reserved(trimmed) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.run env key '{}' is reserved by the runtime; pass fixture values with a task-specific key such as PALYRA_E2E_HOME instead of overriding sandbox path, loader, or Palyra config variables",
                redact_env_key_for_error(trimmed)
            ),
        });
    }

    Ok(())
}

fn valid_process_env_key_shape(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn process_env_key_is_reserved(key: &str) -> bool {
    matches!(
        key.to_ascii_uppercase().as_str(),
        "PATH"
            | "PATHEXT"
            | "LD_PRELOAD"
            | "LD_LIBRARY_PATH"
            | "DYLD_INSERT_LIBRARIES"
            | "DYLD_LIBRARY_PATH"
            | "PALYRA_CONFIG"
            | "PALYRA_STATE_ROOT"
            | "PALYRA_HOME"
            | "PALYRA_CLI_PROFILE"
            | "PALYRA_CLI_PROFILES_PATH"
            | "PALYRA_VAULT_DIR"
    )
}

fn redact_env_key_for_error(key: &str) -> String {
    key.chars().take(MAX_ENV_KEY_LENGTH).collect()
}

fn process_runner_command_with_args_message(command: &str) -> String {
    let mut tokens = command.split_whitespace();
    let Some(executable) = tokens.next() else {
        return "palyra.process.run command must be a bare executable name; put arguments in args"
            .to_owned();
    };
    let args = tokens.map(|arg| format!("{arg:?}")).collect::<Vec<_>>().join(", ");
    if args.is_empty() {
        format!(
            "palyra.process.run command must be a bare executable name without whitespace; got {command:?}"
        )
    } else {
        format!(
            "palyra.process.run command must be a bare executable name without arguments; got {command:?}. Use command={executable:?} and args=[{args}]"
        )
    }
}

fn validate_no_embedded_command_line_arg(
    input: &ProcessRunnerInput,
) -> Result<(), SandboxProcessRunError> {
    if input.args.len() != 1 {
        return Ok(());
    }
    let arg = input.args[0].trim_start();
    let Some((first_token, _)) = arg.split_once(char::is_whitespace) else {
        return Ok(());
    };
    if !process_executable_tokens_match(input.command.as_str(), first_token) {
        return Ok(());
    }

    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: format!(
            "palyra.process.run args must be an array of executable arguments, not a single command-line string; got args=[{arg:?}]. Use command={:?} and split each argument into its own args entry, for example args=[\"-e\", \"console.log('ok')\"] for node eval.",
            input.command
        ),
    })
}

fn process_executable_tokens_match(command: &str, candidate: &str) -> bool {
    let command = normalize_process_executable_token(command);
    let candidate = normalize_process_executable_token(candidate);
    !command.is_empty() && command == candidate
}

fn normalize_process_executable_token(value: &str) -> String {
    let trimmed = value.trim().trim_matches('"').trim_matches('\'');
    let file_name = trimmed
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(trimmed)
        .trim()
        .trim_matches('"')
        .trim_matches('\'');
    file_name
        .strip_suffix(".exe")
        .or_else(|| file_name.strip_suffix(".cmd"))
        .or_else(|| file_name.strip_suffix(".bat"))
        .unwrap_or(file_name)
        .to_ascii_lowercase()
}

fn validate_cmd_invocation_shape(
    command: &str,
    _args: &[String],
) -> Result<(), SandboxProcessRunError> {
    if normalized_process_command_name(command) != "cmd" {
        return Ok(());
    }

    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: "palyra.process.run does not accept explicit command='cmd'; call the target executable directly with split args. On Windows, .cmd and .bat shims resolved from the workspace cwd or PATH are wrapped safely by the process runner.".to_owned(),
    })
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
        && !policy.allowed_executables.iter().any(|allowed| allowed.trim() == "*")
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: executable '{command}' is not allowlisted for process runner"
            ),
        });
    }
    if is_interpreter_executable(normalized.as_str()) && !policy.allow_interpreters {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: interpreter executable '{command}' requires explicit process runner allow_interpreters=true"
            ),
        });
    }
    Ok(())
}

fn validate_process_termination_scope(
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    let command = normalized_process_command_name(command);
    if matches!(command.as_str(), "pkill" | "killall") {
        return Err(broad_process_kill_error());
    }
    if command == "taskkill" && args_contain_switch(args, "/im") {
        return Err(broad_process_kill_error());
    }
    if command == "stop-process" && args_contain_switch(args, "-name") {
        return Err(broad_process_kill_error());
    }
    if matches!(command.as_str(), "powershell" | "pwsh") {
        let joined = args.join(" ").to_ascii_lowercase();
        let invokes_stop_process = joined.contains("stop-process");
        let invokes_name_based_taskkill = joined.contains("taskkill") && joined.contains("/im");
        let invokes_name_based_stop_process =
            invokes_stop_process && (joined.contains("-name") || joined.contains("get-process"));
        if invokes_name_based_taskkill || invokes_name_based_stop_process {
            return Err(broad_process_kill_error());
        }
    }
    Ok(())
}

fn normalized_process_command_name(command: &str) -> String {
    let trimmed = command.trim().to_ascii_lowercase();
    trimmed
        .strip_suffix(".exe")
        .or_else(|| trimmed.strip_suffix(".cmd"))
        .or_else(|| trimmed.strip_suffix(".bat"))
        .unwrap_or(trimmed.as_str())
        .to_owned()
}

fn args_contain_switch(args: &[String], switch: &str) -> bool {
    args.iter().any(|arg| arg.trim().eq_ignore_ascii_case(switch))
}

fn broad_process_kill_error() -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: "palyra.process.run denied broad process-name termination; stop only a PID returned by this run, a known background process id, or a workspace-scoped service port"
            .to_owned(),
    }
}

fn validate_interpreter_argument_guardrails(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    if !is_interpreter_executable(command.trim()) {
        return Ok(());
    }

    if args.iter().any(|arg| is_blocked_eval_flag(arg.as_str())) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: interpreter command '{}' cannot use shell-eval flags (-c/--command)",
                command
            ),
        });
    }

    for (index, argument) in args.iter().enumerate() {
        if argument_is_non_path_option_assignment(argument.as_str())
            || index
                .checked_sub(1)
                .and_then(|previous| args.get(previous))
                .is_some_and(|previous| option_consumes_non_path_value(previous.as_str()))
        {
            continue;
        }
        if !contains_embedded_absolute_path(argument.as_str()) {
            continue;
        }
        if interpreter_absolute_path_argument_stays_in_workspace(
            workspace_root,
            cwd,
            argument.as_str(),
        )? {
            continue;
        }
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: interpreter argument contains absolute path-like substring: '{argument}'"
            ),
        });
    }

    Ok(())
}

fn validate_host_interpreter_argument_guardrails(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    if !is_interpreter_executable(command.trim()) {
        return Ok(());
    }

    if args.iter().any(|arg| is_blocked_eval_flag(arg.as_str())) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: interpreter command '{}' cannot use shell-eval flags (-c/--command)",
                command
            ),
        });
    }

    for (index, argument) in args.iter().enumerate() {
        if argument_is_non_path_option_assignment(argument.as_str())
            || index
                .checked_sub(1)
                .and_then(|previous| args.get(previous))
                .is_some_and(|previous| option_consumes_non_path_value(previous.as_str()))
        {
            continue;
        }
        if !contains_embedded_absolute_path(argument.as_str()) {
            continue;
        }
        if interpreter_absolute_path_argument_stays_in_host_scope(
            workspace_root,
            cwd,
            argument.as_str(),
        )? {
            continue;
        }
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: host interpreter argument contains absolute path outside approved host roots: '{argument}'"
            ),
        });
    }

    Ok(())
}

fn interpreter_absolute_path_argument_stays_in_workspace(
    workspace_root: &Path,
    cwd: &Path,
    argument: &str,
) -> Result<bool, SandboxProcessRunError> {
    let trimmed = argument.trim();
    if trimmed.is_empty() {
        return Ok(false);
    }

    if let Some(file_url_path) = parse_file_url_path(trimmed)? {
        return Ok(resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false).is_ok());
    }

    if let Some(value) = option_assignment_value(trimmed) {
        return interpreter_absolute_path_argument_stays_in_workspace(workspace_root, cwd, value);
    }

    if let Some(value) = option_compact_value(trimmed) {
        return interpreter_absolute_path_argument_stays_in_workspace(workspace_root, cwd, value);
    }

    if let Some(stays_in_workspace) =
        interpreter_path_list_argument_stays_in_workspace(workspace_root, cwd, trimmed)?
    {
        return Ok(stays_in_workspace);
    }

    if !token_looks_like_absolute_path(trimmed) {
        return Ok(false);
    }

    Ok(resolve_scoped_path(workspace_root, cwd, trimmed, false).is_ok())
}

fn interpreter_absolute_path_argument_stays_in_host_scope(
    workspace_root: &Path,
    cwd: &Path,
    argument: &str,
) -> Result<bool, SandboxProcessRunError> {
    let trimmed = argument.trim();
    if trimmed.is_empty() {
        return Ok(false);
    }

    if let Some(file_url_path) = parse_file_url_path(trimmed)? {
        return Ok(
            resolve_host_access_path(workspace_root, cwd, file_url_path.as_str(), false).is_ok()
        );
    }

    if let Some(value) = option_assignment_value(trimmed) {
        return interpreter_absolute_path_argument_stays_in_host_scope(workspace_root, cwd, value);
    }

    if let Some(value) = option_compact_value(trimmed) {
        return interpreter_absolute_path_argument_stays_in_host_scope(workspace_root, cwd, value);
    }

    if let Some(stays_in_host_scope) =
        interpreter_path_list_argument_stays_in_host_scope(workspace_root, cwd, trimmed)?
    {
        return Ok(stays_in_host_scope);
    }

    if !token_looks_like_absolute_path(trimmed) {
        return Ok(false);
    }

    Ok(resolve_host_access_path(workspace_root, cwd, trimmed, false).is_ok())
}

fn interpreter_path_list_argument_stays_in_workspace(
    workspace_root: &Path,
    cwd: &Path,
    argument: &str,
) -> Result<Option<bool>, SandboxProcessRunError> {
    if argument.contains("://") {
        return Ok(None);
    }
    let components = interpreter_path_list_components(argument);
    if components.len() < 2 {
        return Ok(None);
    }

    let mut saw_absolute_path = false;
    for component in components {
        if !token_looks_like_absolute_path(component) {
            continue;
        }
        saw_absolute_path = true;
        let resolved = if let Some(file_url_path) = parse_file_url_path(component)? {
            resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)
        } else {
            resolve_scoped_path(workspace_root, cwd, component, false)
        };
        if resolved.is_err() {
            return Ok(Some(false));
        }
    }

    Ok(saw_absolute_path.then_some(true))
}

fn interpreter_path_list_argument_stays_in_host_scope(
    workspace_root: &Path,
    cwd: &Path,
    argument: &str,
) -> Result<Option<bool>, SandboxProcessRunError> {
    if argument.contains("://") {
        return Ok(None);
    }
    let components = interpreter_path_list_components(argument);
    if components.len() < 2 {
        return Ok(None);
    }

    let mut saw_absolute_path = false;
    for component in components {
        if !token_looks_like_absolute_path(component) {
            continue;
        }
        saw_absolute_path = true;
        let resolved = if let Some(file_url_path) = parse_file_url_path(component)? {
            resolve_host_access_path(workspace_root, cwd, file_url_path.as_str(), false)
        } else {
            resolve_host_access_path(workspace_root, cwd, component, false)
        };
        if resolved.is_err() {
            return Ok(Some(false));
        }
    }

    Ok(saw_absolute_path.then_some(true))
}

fn interpreter_path_list_components(raw: &str) -> Vec<&str> {
    let separator = if cfg!(windows) { ';' } else { ':' };
    if !raw.contains(separator) {
        return Vec::new();
    }
    raw.split(separator).map(str::trim).filter(|component| !component.is_empty()).collect()
}

fn is_interpreter_executable(command: &str) -> bool {
    let normalized = command.trim().to_ascii_lowercase();
    INTERPRETER_EXECUTABLE_DENYLIST.contains(&normalized.as_str())
}

fn is_blocked_eval_flag(arg: &str) -> bool {
    let normalized = arg.trim().to_ascii_lowercase();
    matches!(normalized.as_str(), "-c" | "/c" | "--command" | "-command" | "--eval")
}

fn contains_embedded_absolute_path(raw: &str) -> bool {
    raw.split(|ch: char| {
        ch.is_whitespace()
            || matches!(ch, '"' | '\'' | '`' | ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}')
    })
    .any(token_or_path_list_contains_absolute_path)
}

fn token_or_path_list_contains_absolute_path(raw: &str) -> bool {
    let token = raw.trim();
    if token_looks_like_absolute_path(token) {
        return true;
    }
    if token.contains("://") {
        return false;
    }
    interpreter_path_list_components(token).into_iter().any(token_looks_like_absolute_path)
}

fn token_looks_like_absolute_path(raw: &str) -> bool {
    let token = raw.trim();
    if token.is_empty() {
        return false;
    }

    if token.starts_with("file://") || token.starts_with('/') {
        return true;
    }
    if token.starts_with('\\') {
        return !token_is_escaped_string_fragment(token);
    }

    let bytes = token.as_bytes();
    bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && matches!(bytes[2], b'\\' | b'/')
}

fn token_is_escaped_string_fragment(token: &str) -> bool {
    let rest = token.trim_start_matches('\\');
    if rest.len() == token.len() {
        return false;
    }
    matches!(rest, "n" | "r" | "t" | "0" | "\"" | "'" | "`")
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

fn resolve_host_working_directory(
    workspace_root: &Path,
    cwd: Option<&str>,
) -> Result<PathBuf, SandboxProcessRunError> {
    let cwd_value = cwd.unwrap_or(".");
    if cwd_value.contains('\0') {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner denied cwd with embedded NUL byte".to_owned(),
        });
    }
    let resolved = resolve_host_access_path(workspace_root, workspace_root, cwd_value, true)?;
    if !resolved.is_dir() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!("host process runner cwd '{}' is not a directory", cwd_value),
        });
    }
    Ok(resolved)
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
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    let _ = rewrite_arguments_to_scoped_paths(workspace_root, cwd, command, args)?;
    Ok(())
}

fn validate_host_argument_scope(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    let mut index = 0usize;
    while index < args.len() {
        let arg = &args[index];
        if argument_is_non_path_option_assignment(arg.as_str()) {
            index = index.saturating_add(1);
            continue;
        }
        if command_option_consumes_non_path_value(command, arg.as_str()) {
            index = index.saturating_add(2);
            continue;
        }
        if is_windows_command_switch(command, arg.as_str())
            || command_positional_arg_is_non_path_value(command, arg.as_str())
        {
            index = index.saturating_add(1);
            continue;
        }
        validate_host_argument_path_scope(workspace_root, cwd, command, arg.as_str())?;
        index = index.saturating_add(1);
    }
    Ok(())
}

fn validate_host_argument_path_scope(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    arg: &str,
) -> Result<(), SandboxProcessRunError> {
    if let Some(file_url_path) = parse_file_url_path(arg)? {
        let _ = resolve_host_access_path(workspace_root, cwd, file_url_path.as_str(), false)?;
        return Ok(());
    }
    if let Some(value) = option_assignment_value(arg) {
        return validate_host_argument_path_scope(workspace_root, cwd, command, value.trim());
    }
    if let Some(value) = option_compact_value(arg) {
        return validate_host_argument_path_scope(workspace_root, cwd, command, value);
    }
    if !argument_requires_path_validation(arg) {
        return Ok(());
    }
    let _ = resolve_host_access_path(workspace_root, cwd, arg, false).map_err(|error| {
        SandboxProcessRunError {
            kind: error.kind,
            message: format!(
                "sandbox denied: host process argument for command '{command}' is outside approved host roots: {arg}; {}",
                error.message
            ),
        }
    })?;
    Ok(())
}

fn rewrite_arguments_to_scoped_paths(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
) -> Result<Vec<String>, SandboxProcessRunError> {
    let mut rewritten = Vec::with_capacity(args.len());
    let mut index = 0usize;
    while index < args.len() {
        let arg = &args[index];
        if argument_is_non_path_option_assignment(arg.as_str()) {
            rewritten.push(arg.clone());
            index = index.saturating_add(1);
            continue;
        }
        if command_option_consumes_non_path_value(command, arg.as_str()) {
            rewritten.push(arg.clone());
            if let Some(value) = args.get(index.saturating_add(1)) {
                rewritten.push(value.clone());
            }
            index = index.saturating_add(2);
            continue;
        }
        if is_windows_command_switch(command, arg.as_str()) {
            rewritten.push(arg.clone());
            index = index.saturating_add(1);
            continue;
        }
        if command_positional_arg_is_non_path_value(command, arg.as_str()) {
            rewritten.push(arg.clone());
            index = index.saturating_add(1);
            continue;
        }
        if let Some(file_url_path) = parse_file_url_path(arg.as_str())? {
            let scoped = resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)?;
            rewritten.push(scoped_file_url_argument(scoped.as_path())?);
            index = index.saturating_add(1);
            continue;
        }
        if let Some(value) = option_assignment_value(arg.as_str()) {
            let value = value.trim();
            if let Some(file_url_path) = parse_file_url_path(value)? {
                let scoped =
                    resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)?;
                let scoped = scoped_file_url_argument(scoped.as_path())?;
                rewritten.push(replace_option_assignment_value(arg.as_str(), scoped.as_str()));
                index = index.saturating_add(1);
                continue;
            }
            if !argument_requires_path_validation(value) {
                rewritten.push(arg.clone());
                index = index.saturating_add(1);
                continue;
            }
            let scoped = resolve_scoped_path(workspace_root, cwd, value, false)?;
            rewritten.push(replace_option_assignment_value(
                arg.as_str(),
                scoped.to_string_lossy().as_ref(),
            ));
            index = index.saturating_add(1);
            continue;
        }
        if let Some(value) = option_compact_value(arg.as_str()) {
            if let Some(file_url_path) = parse_file_url_path(value)? {
                let scoped =
                    resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)?;
                let scoped = scoped_file_url_argument(scoped.as_path())?;
                rewritten.push(replace_option_compact_value(arg.as_str(), scoped.as_str()));
                index = index.saturating_add(1);
                continue;
            }
            if !argument_requires_path_validation(value) {
                rewritten.push(arg.clone());
                index = index.saturating_add(1);
                continue;
            }
            let scoped = resolve_scoped_path(workspace_root, cwd, value, false)?;
            rewritten.push(replace_option_compact_value(
                arg.as_str(),
                scoped.to_string_lossy().as_ref(),
            ));
            index = index.saturating_add(1);
            continue;
        }
        if !argument_requires_path_validation(arg.as_str()) {
            rewritten.push(arg.clone());
            index = index.saturating_add(1);
            continue;
        }
        let scoped = resolve_scoped_path(workspace_root, cwd, arg.as_str(), false)?;
        rewritten.push(scoped.to_string_lossy().to_string());
        index = index.saturating_add(1);
    }
    Ok(rewritten)
}

fn scoped_file_url_argument(path: &Path) -> Result<String, SandboxProcessRunError> {
    reqwest::Url::from_file_path(path).map(|url| url.to_string()).map_err(|_| {
        SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: scoped file URL path '{}' cannot be represented safely",
                path.display()
            ),
        }
    })
}

fn replace_option_assignment_value(arg: &str, replacement: &str) -> String {
    match arg.trim().split_once('=') {
        Some((name, _)) => format!("{name}={replacement}"),
        None => arg.to_owned(),
    }
}

fn replace_option_compact_value(arg: &str, replacement: &str) -> String {
    let trimmed = arg.trim();
    if !trimmed.starts_with('-') || trimmed.starts_with("--") {
        return arg.to_owned();
    }

    let mut char_indices = trimmed.char_indices();
    let Some((_, _)) = char_indices.next() else {
        return arg.to_owned();
    };
    let Some((_, second)) = char_indices.next() else {
        return arg.to_owned();
    };
    if !second.is_ascii_alphabetic() {
        return arg.to_owned();
    }

    let Some((value_index, value_char)) = char_indices.next() else {
        return arg.to_owned();
    };
    if value_char == '=' || value_char.is_whitespace() {
        return arg.to_owned();
    }

    format!("{}{}", &trimmed[..value_index], replacement)
}

fn argument_is_non_path_option_assignment(arg: &str) -> bool {
    option_assignment_value(arg)
        .and_then(|_| arg.trim().split_once('=').map(|(name, _)| name))
        .is_some_and(option_consumes_non_path_value)
}

fn option_consumes_non_path_value(arg: &str) -> bool {
    matches!(
        arg.trim().to_ascii_lowercase().as_str(),
        "--test-name-pattern" | "--testnamepattern" | "--grep" | "--grep-invert"
    )
}

fn command_option_consumes_non_path_value(command: &str, arg: &str) -> bool {
    option_consumes_non_path_value(arg)
        || node_eval_option_consumes_non_path_value(command, arg)
        || windows_acl_option_consumes_non_path_value(command, arg)
}

fn node_eval_option_consumes_non_path_value(command: &str, arg: &str) -> bool {
    let command = normalize_process_executable_token(command);
    matches!(command.as_str(), "node" | "nodejs")
        && matches!(arg.trim().to_ascii_lowercase().as_str(), "-e" | "-p")
}

fn windows_acl_option_consumes_non_path_value(command: &str, arg: &str) -> bool {
    if !cfg!(windows) {
        return false;
    }
    let command = normalized_process_command_name(command);
    command == "icacls"
        && matches!(
            arg.trim().to_ascii_lowercase().as_str(),
            "/grant" | "/grant:r" | "/deny" | "/remove" | "/remove:g" | "/remove:d" | "/setowner"
        )
}

fn command_positional_arg_is_non_path_value(command: &str, arg: &str) -> bool {
    let command = normalize_process_executable_token(command);
    matches!(command.as_str(), "sleep") && is_sleep_duration_literal(arg)
}

fn is_sleep_duration_literal(arg: &str) -> bool {
    let trimmed = arg.trim();
    let numeric = trimmed
        .strip_suffix('s')
        .or_else(|| trimmed.strip_suffix('m'))
        .or_else(|| trimmed.strip_suffix('h'))
        .or_else(|| trimmed.strip_suffix('d'))
        .unwrap_or(trimmed);
    let mut saw_digit = false;
    let mut saw_dot = false;
    for ch in numeric.chars() {
        if ch.is_ascii_digit() {
            saw_digit = true;
        } else if ch == '.' && !saw_dot {
            saw_dot = true;
        } else {
            return false;
        }
    }
    saw_digit
}

fn is_windows_command_switch(command: &str, arg: &str) -> bool {
    if !cfg!(windows) {
        return false;
    }
    let command = normalized_process_command_name(command);
    let arg = arg.trim().to_ascii_uppercase();
    match command.as_str() {
        "taskkill" => matches!(arg.as_str(), "/PID" | "/T" | "/F"),
        "tasklist" => {
            matches!(arg.as_str(), "/FI" | "/FO" | "/NH" | "/V" | "/SVC" | "/M" | "/APPS")
        }
        "findstr" => is_findstr_windows_switch(arg.as_str()),
        "icacls" => matches!(
            arg.as_str(),
            "/C" | "/L" | "/Q" | "/T" | "/INHERITANCE:E" | "/INHERITANCE:D" | "/INHERITANCE:R"
        ),
        "whoami" => matches!(arg.as_str(), "/ALL"),
        _ => false,
    }
}

fn is_findstr_windows_switch(arg: &str) -> bool {
    matches!(
        arg,
        "/B" | "/E"
            | "/L"
            | "/R"
            | "/S"
            | "/I"
            | "/X"
            | "/V"
            | "/N"
            | "/M"
            | "/P"
            | "/OFFLINE"
            | "/?"
    ) || arg.starts_with("/C:")
}

fn option_assignment_value(arg: &str) -> Option<&str> {
    let trimmed = arg.trim();
    if !trimmed.starts_with('-') {
        return None;
    }
    let (_, value) = trimmed.split_once('=')?;
    Some(value)
}

fn option_compact_value(arg: &str) -> Option<&str> {
    let trimmed = arg.trim();
    if !trimmed.starts_with('-') || trimmed.starts_with("--") {
        return None;
    }

    let mut char_indices = trimmed.char_indices();
    let (_, first) = char_indices.next()?;
    debug_assert_eq!(first, '-');
    let (_, second) = char_indices.next()?;
    if !second.is_ascii_alphabetic() {
        return None;
    }

    let (value_index, value_char) = char_indices.next()?;
    if value_char == '=' || value_char.is_whitespace() {
        return None;
    }

    Some(&trimmed[value_index..])
}

fn argument_requires_path_validation(arg: &str) -> bool {
    let trimmed = arg.trim();
    if trimmed.is_empty() || trimmed.starts_with('-') || is_builtin_list_flag(trimmed) {
        return false;
    }
    if token_looks_like_absolute_path(trimmed) {
        return true;
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
    let candidate = if let Some(suffix) = virtual_workspace_path_suffix(raw) {
        workspace_root.join(suffix)
    } else if Path::new(raw).is_absolute() {
        PathBuf::from(raw)
    } else {
        base.join(raw)
    };

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

fn resolve_host_access_path(
    workspace_root: &Path,
    base: &Path,
    raw: &str,
    must_exist: bool,
) -> Result<PathBuf, SandboxProcessRunError> {
    if let Some(suffix) = virtual_workspace_path_suffix(raw) {
        return resolve_scoped_path(
            workspace_root,
            workspace_root,
            suffix.to_string_lossy().as_ref(),
            must_exist,
        );
    }
    if raw.contains('\0') {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner denied path with embedded NUL byte".to_owned(),
        });
    }
    let raw_path = Path::new(raw);
    if raw_path.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!("host process runner denied path traversal for '{raw}'"),
        });
    }
    let candidate = if raw_path.is_absolute() { PathBuf::from(raw) } else { base.join(raw) };
    if candidate.components().any(|component| matches!(component, Component::ParentDir)) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!("host process runner denied path traversal for '{raw}'"),
        });
    }

    let inspected = if candidate.exists() {
        fs::canonicalize(&candidate).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "host process runner denied invalid path '{}': {error}",
                candidate.display()
            ),
        })?
    } else if must_exist {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "host process runner required path '{}' does not exist",
                candidate.display()
            ),
        });
    } else {
        let ancestor = nearest_existing_ancestor(&candidate)?;
        fs::canonicalize(&ancestor).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "host process runner could not resolve parent path '{}' safely: {error}",
                ancestor.display()
            ),
        })?
    };

    ensure_host_access_path_allowed(workspace_root, inspected.as_path())?;
    if candidate.exists() {
        Ok(inspected)
    } else {
        Ok(candidate)
    }
}

fn ensure_host_access_path_allowed(
    workspace_root: &Path,
    inspected: &Path,
) -> Result<(), SandboxProcessRunError> {
    if protected_host_path(inspected) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "host process runner denied protected OS path '{}'",
                inspected.display()
            ),
        });
    }
    if path_starts_with_case_aware(inspected, workspace_root)
        || user_owned_host_roots()
            .iter()
            .any(|root| path_starts_with_case_aware(inspected, root.as_path()))
    {
        return Ok(());
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "host process runner path '{}' is outside workspace and approved user-owned OS roots",
            inspected.display()
        ),
    })
}

fn user_owned_host_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for key in ["USERPROFILE", "HOME"] {
        if let Some(value) = std::env::var_os(key) {
            push_canonical_host_root(&mut roots, PathBuf::from(value));
        }
    }
    push_canonical_host_root(&mut roots, std::env::temp_dir());
    #[cfg(unix)]
    {
        push_canonical_host_root(&mut roots, PathBuf::from("/var/tmp"));
    }
    roots
}

fn push_canonical_host_root(roots: &mut Vec<PathBuf>, root: PathBuf) {
    let Ok(canonical) = fs::canonicalize(root.as_path()) else {
        return;
    };
    if !canonical.is_dir() {
        return;
    }
    if !roots.iter().any(|existing| same_path_case_aware(existing.as_path(), canonical.as_path())) {
        roots.push(canonical);
    }
}

fn protected_host_path(path: &Path) -> bool {
    #[cfg(windows)]
    {
        let normalized = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        normalized.ends_with(":/")
            || normalized.contains(":/windows")
            || normalized.contains(":/program files")
            || normalized.contains(":/program files (x86)")
            || normalized.contains(":/system volume information")
    }
    #[cfg(not(windows))]
    {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized == "/" {
            return true;
        }
        for prefix in ["/etc", "/bin", "/sbin", "/usr", "/lib", "/lib64", "/System", "/Library"] {
            if normalized == prefix || normalized.starts_with(format!("{prefix}/").as_str()) {
                return true;
            }
        }
        false
    }
}

fn path_starts_with_case_aware(path: &Path, root: &Path) -> bool {
    if path.starts_with(root) {
        return true;
    }
    #[cfg(windows)]
    {
        let path = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        let root = root.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        path == root || path.starts_with(format!("{root}/").as_str())
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn same_path_case_aware(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy()
            .replace('\\', "/")
            .eq_ignore_ascii_case(&right.to_string_lossy().replace('\\', "/"))
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn virtual_workspace_path_suffix(raw: &str) -> Option<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let normalized = trimmed.replace('\\', "/");
    if normalized == "/" {
        return Some(PathBuf::new());
    }

    if normalized == "workspace" || normalized == "/workspace" {
        return Some(PathBuf::new());
    }

    normalized
        .strip_prefix("/workspace/")
        .or_else(|| normalized.strip_prefix("workspace/"))
        .map(PathBuf::from)
}

fn named_virtual_workspace_path_suffix(raw: &str) -> Option<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let normalized = trimmed.replace('\\', "/");
    if normalized == "workspace" || normalized == "/workspace" {
        return Some(PathBuf::new());
    }

    normalized
        .strip_prefix("/workspace/")
        .or_else(|| normalized.strip_prefix("workspace/"))
        .map(PathBuf::from)
}

fn rewrite_host_virtual_workspace_args(
    args: &[String],
    workspace_root: &Path,
) -> Result<Vec<String>, SandboxProcessRunError> {
    args.iter()
        .map(|arg| rewrite_host_virtual_workspace_arg(arg.as_str(), workspace_root))
        .collect()
}

fn rewrite_host_virtual_workspace_arg(
    arg: &str,
    workspace_root: &Path,
) -> Result<String, SandboxProcessRunError> {
    if let Some(path) = resolve_host_virtual_workspace_arg_path(arg, workspace_root)? {
        return Ok(path);
    }

    if let Some((name, value)) = arg.trim().split_once('=') {
        if name.starts_with('-') {
            if let Some(path) = resolve_host_virtual_workspace_arg_path(value, workspace_root)? {
                return Ok(format!("{name}={path}"));
            }
        }
    }

    Ok(arg.to_owned())
}

fn resolve_host_virtual_workspace_arg_path(
    raw: &str,
    workspace_root: &Path,
) -> Result<Option<String>, SandboxProcessRunError> {
    if named_virtual_workspace_path_suffix(raw).is_none() {
        return Ok(None);
    }

    let resolved = resolve_scoped_path(workspace_root, workspace_root, raw, false)?;
    Ok(Some(resolved.to_string_lossy().to_string()))
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

fn validate_tier_c_strict_offline_egress_requests(
    policy: &SandboxProcessRunnerPolicy,
    requested_hosts: &[String],
) -> Result<(), SandboxProcessRunError> {
    if !matches!(policy.tier, SandboxProcessRunnerTier::C) || requested_hosts.is_empty() {
        return Ok(());
    }

    let sample_hosts = requested_hosts.iter().take(3).cloned().collect::<Vec<_>>().join(", ");
    let overflow_suffix = if requested_hosts.len() > 3 {
        format!(" (+{} more)", requested_hosts.len() - 3)
    } else {
        String::new()
    };
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::EgressDenied,
        message: format!(
            "sandbox denied: tier-c strict mode is offline-only; requested outbound host(s) [{sample_hosts}]{overflow_suffix} are blocked. Route network access through dedicated browser/http tools"
        ),
    })
}

fn validate_runtime_egress_enforcement(
    policy: &SandboxProcessRunnerPolicy,
) -> Result<(), SandboxProcessRunError> {
    if matches!(policy.tier, SandboxProcessRunnerTier::B) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::EgressDenied,
            message: "sandbox denied: runtime egress enforcement is unavailable in tier-b strict mode; use preflight/none or opt into tier-c backend".to_owned(),
        });
    }

    let capabilities = current_backend_capabilities();
    if !capabilities.runtime_network_isolation {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::EgressDenied,
            message: format!(
                "sandbox denied: tier-c backend '{}' cannot enforce runtime network isolation",
                current_backend_kind().as_str()
            ),
        });
    }
    if (!policy.allowed_egress_hosts.is_empty() || !policy.allowed_dns_suffixes.is_empty())
        && !capabilities.host_allowlists
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::EgressDenied,
            message: format!(
                "sandbox denied: tier-c strict mode is offline-only; backend '{}' cannot enforce host-level egress allowlists. Clear allowlists or route network through dedicated browser/http tools",
                current_backend_kind().as_str()
            ),
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
    workspace_root: &Path,
    cwd: &Path,
    timeout: Duration,
) -> Result<ProcessExecutionCapture, SandboxProcessRunError> {
    let mut command = build_process_command(policy, input, workspace_root, cwd)?;
    configure_child_process_group(&mut command);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    if !process_runner_allows_host_access(policy) {
        attach_resource_limits_unix(&mut command, policy);
    }

    let mut child = command.spawn().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: format!("sandbox process spawn failed for command '{}': {error}", input.command),
    })?;

    capture_child_output(&mut child, timeout, policy.max_output_bytes as usize)
}

#[cfg(unix)]
fn configure_child_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_child_process_group(_command: &mut Command) {}

fn spawn_background_process(
    policy: &SandboxProcessRunnerPolicy,
    input: &ProcessRunnerInput,
    workspace_root: &Path,
    cwd: &Path,
    lifetime: Duration,
    max_lifetime: Duration,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let mut command = build_process_command(policy, input, workspace_root, cwd)?;
    configure_child_process_group(&mut command);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    if !process_runner_allows_host_access(policy) {
        attach_resource_limits_unix(&mut command, policy);
    }

    let lifetime_ms = lifetime.as_millis() as u64;
    let startup_budget = background_process_startup_metadata_budget(lifetime)
        .ok_or_else(|| background_process_startup_budget_expired_error(input, lifetime_ms))?;
    let mut child = command.spawn().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: format!("sandbox process spawn failed for command '{}': {error}", input.command),
    })?;
    let pid = child.id();
    #[cfg(windows)]
    let windows_job_bound = bind_child_to_windows_background_job(&child, pid).is_ok();
    #[cfg(not(windows))]
    let windows_job_bound = false;
    let started_at = Instant::now();
    let output_monitor =
        match start_background_output_monitor(&mut child, policy.max_output_bytes as usize) {
            Ok(output_monitor) => output_monitor,
            Err(error) => {
                terminate_background_child(child);
                return Err(error);
            }
        };
    let Some(startup_check_wait) = bounded_background_process_wait(
        startup_budget,
        started_at.elapsed(),
        Duration::from_millis(BACKGROUND_STARTUP_CHECK_MS),
    ) else {
        terminate_background_child(child);
        return Err(background_process_startup_budget_expired_error(input, lifetime_ms));
    };
    thread::sleep(startup_check_wait);
    if remaining_background_process_lifetime(startup_budget, started_at.elapsed()).is_none() {
        terminate_background_child(child);
        return Err(background_process_startup_budget_expired_error(input, lifetime_ms));
    }
    if let Some(status) = child.try_wait().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "sandbox background process startup check failed for command '{}': {error}",
            input.command
        ),
    })? {
        let startup_output_drain = bounded_background_process_wait(
            startup_budget,
            started_at.elapsed(),
            Duration::from_millis(BACKGROUND_STARTUP_OUTPUT_DRAIN_MS),
        )
        .unwrap_or(Duration::ZERO);
        let (stdout, stderr) = output_monitor.snapshot_after_startup_drain(startup_output_drain);
        terminate_background_child(child);
        return Err(background_process_startup_failure(input, status, &stdout, &stderr));
    }
    let cleanup = background_cleanup_metadata(pid, lifetime_ms, windows_job_bound);
    let startup_output_drain = bounded_background_process_wait(
        startup_budget,
        started_at.elapsed(),
        Duration::from_millis(BACKGROUND_STARTUP_OUTPUT_DRAIN_MS),
    )
    .unwrap_or(Duration::ZERO);
    let (stdout, stderr) = output_monitor.snapshot_after_startup_drain(startup_output_drain);
    let post_output_exit_check = bounded_background_process_wait(
        startup_budget,
        started_at.elapsed(),
        Duration::from_millis(BACKGROUND_POST_OUTPUT_EXIT_CHECK_MS),
    )
    .unwrap_or(Duration::ZERO);
    if let Some(status) = wait_for_background_process_exit(&mut child, post_output_exit_check)? {
        terminate_background_child(child);
        return Err(background_process_startup_failure(input, status, &stdout, &stderr));
    }
    let Some(remaining_lifetime) =
        remaining_background_process_lifetime(lifetime, started_at.elapsed())
    else {
        terminate_background_child(child);
        return Err(background_process_lifetime_expired_error(input, lifetime_ms));
    };
    thread::spawn(move || monitor_background_child_until_lifetime(child, remaining_lifetime));

    let RedactedProcessOutputText { text: stdout_text, redacted: stdout_redacted } =
        redacted_process_output(stdout.bytes.as_slice());
    let RedactedProcessOutputText { text: stderr_text, redacted: stderr_redacted } =
        redacted_process_output(stderr.bytes.as_slice());
    let max_lifetime_ms = max_lifetime.as_millis() as u64;
    let output_json = serde_json::to_vec(&json!({
        "exit_code": Value::Null,
        "stdout": stdout_text,
        "stderr": stderr_text,
        "stdout_truncated": stdout.truncated,
        "stderr_truncated": stderr.truncated,
        "stdout_redacted": stdout_redacted,
        "stderr_redacted": stderr_redacted,
        "background_output_note": "stdout/stderr are bounded startup snapshots captured during the startup check, not command completion output; use an explicit fixed port if a dynamic port is not printed here",
        "duration_ms": 0,
        "background": true,
        "started": true,
        "completed": false,
        "startup_success": true,
        "process_state": "running",
        "pid": pid,
        "lifetime_ms": lifetime_ms,
        "max_lifetime_ms": max_lifetime_ms,
        "background_lifetime_note": format!(
            "Palyra will auto-terminate this background process after {lifetime_ms}ms; set timeout_ms up to {max_lifetime_ms}ms within the operator-configured tool execution timeout for long browser verification loops, and use cleanup.portable_stop_command when finished."
        ),
        "process_handle": {
            "kind": "pid",
            "direct_process_pid": pid,
            "process_tree": cfg!(windows),
            "windows_job_object": windows_job_bound,
            "identity_note": "pid is the direct process spawned by palyra.process.run; a descendant process may own listening sockets"
        },
        "cleanup": cleanup,
        "tier": policy.tier.as_str(),
        "sandbox_backend": process_runner_executor_name(policy),
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox background process output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

fn wait_for_background_process_exit(
    child: &mut Child,
    max_wait: Duration,
) -> Result<Option<ExitStatus>, SandboxProcessRunError> {
    let started_at = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(Some(status)),
            Ok(None) if started_at.elapsed() < max_wait => {
                thread::sleep(Duration::from_millis(CAPTURE_POLL_INTERVAL_MS));
            }
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!("sandbox background process exit check failed: {error}"),
                });
            }
        }
    }
}

fn background_process_startup_failure(
    input: &ProcessRunnerInput,
    status: ExitStatus,
    stdout: &StreamCapture,
    stderr: &StreamCapture,
) -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "sandbox background process exited before startup check (code={}) for command '{}'{}{}; use the cwd field instead of command-line cwd flags, verify the server command, and probe the expected port before browser navigation",
            status.code().unwrap_or(-1),
            input.command,
            redacted_process_output_preview(stdout.bytes.as_slice())
                .map(|preview| format!(", stdout_preview={preview:?}"))
                .unwrap_or_default(),
            redacted_process_output_preview(stderr.bytes.as_slice())
                .map(|preview| format!(", stderr_preview={preview:?}"))
                .unwrap_or_default(),
        ),
    }
}

fn start_background_output_monitor(
    child: &mut Child,
    max_output_bytes: usize,
) -> Result<BackgroundOutputMonitor, SandboxProcessRunError> {
    let stdout = child.stdout.take().ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: "sandbox background process stdout pipe is unavailable".to_owned(),
    })?;
    let stderr = child.stderr.take().ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: "sandbox background process stderr pipe is unavailable".to_owned(),
    })?;
    let remaining_budget = Arc::new(AtomicUsize::new(max_output_bytes));
    let quota_triggered = Arc::new(AtomicBool::new(false));
    let stdout_capture = Arc::new(Mutex::new(StreamCapture {
        bytes: Vec::new(),
        truncated: false,
        read_error: None,
    }));
    let stderr_capture = Arc::new(Mutex::new(StreamCapture {
        bytes: Vec::new(),
        truncated: false,
        read_error: None,
    }));
    spawn_background_capture_reader(
        stdout,
        Arc::clone(&remaining_budget),
        Arc::clone(&quota_triggered),
        Arc::clone(&stdout_capture),
    );
    spawn_background_capture_reader(
        stderr,
        remaining_budget,
        quota_triggered,
        Arc::clone(&stderr_capture),
    );
    Ok(BackgroundOutputMonitor { stdout: stdout_capture, stderr: stderr_capture })
}

fn spawn_background_capture_reader<R>(
    mut reader: R,
    remaining_budget: Arc<AtomicUsize>,
    quota_triggered: Arc<AtomicBool>,
    capture: Arc<Mutex<StreamCapture>>,
) where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0_u8; CAPTURE_CHUNK_BYTES];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(read_count) => {
                    let granted = reserve_output_budget(remaining_budget.as_ref(), read_count);
                    if let Ok(mut capture) = capture.lock() {
                        if granted > 0 {
                            capture.bytes.extend_from_slice(&buffer[..granted]);
                        }
                        if granted < read_count {
                            capture.truncated = true;
                        }
                    }
                    if granted < read_count {
                        quota_triggered.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                Err(error) => {
                    if let Ok(mut capture) = capture.lock() {
                        capture.read_error = Some(error.to_string());
                    }
                    break;
                }
            }
        }
    });
}

fn terminate_background_child(mut child: Child) {
    terminate_child_process_tree(&mut child);
    let _ = child.wait();
}

fn monitor_background_child_until_lifetime(mut child: Child, lifetime: Duration) {
    let started_at = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(_) => return,
        }

        let elapsed = started_at.elapsed();
        if elapsed >= lifetime {
            terminate_child_process_tree(&mut child);
            let _ = wait_for_background_process_exit(
                &mut child,
                Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS),
            );
            return;
        }

        let remaining = lifetime.saturating_sub(elapsed);
        thread::sleep(remaining.min(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS)));
    }
}

#[cfg(windows)]
fn bind_child_to_windows_background_job(child: &Child, pid: u32) -> io::Result<()> {
    let job = create_windows_background_job()?;
    let child_handle = child.as_raw_handle() as HANDLE;
    if !windows_handle_is_valid(child_handle) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("child process handle for pid {pid} is invalid"),
        ));
    }

    // SAFETY: `job.handle` is a valid job handle and `child_handle` is the live child process
    // handle exposed by `std::process::Child`.
    let assigned = unsafe { AssignProcessToJobObject(job.handle, child_handle) };
    if assigned == 0 {
        return Err(io::Error::last_os_error());
    }

    register_windows_background_job(pid, Arc::new(job))
}

#[cfg(windows)]
fn create_windows_background_job() -> io::Result<WindowsBackgroundJob> {
    // SAFETY: null security attributes and an unnamed job object are valid inputs.
    let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    if !windows_handle_is_valid(handle) {
        return Err(io::Error::last_os_error());
    }

    let job = WindowsBackgroundJob { handle, terminated: AtomicBool::new(false) };
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
    // SAFETY: `limits` points to a properly initialized JOBOBJECT_EXTENDED_LIMIT_INFORMATION
    // value for the requested JobObjectExtendedLimitInformation class.
    let configured = unsafe {
        SetInformationJobObject(
            job.handle,
            JobObjectExtendedLimitInformation,
            (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        )
    };
    if configured == 0 {
        let error = io::Error::last_os_error();
        drop(job);
        return Err(error);
    }

    Ok(job)
}

#[cfg(windows)]
fn windows_handle_is_valid(handle: HANDLE) -> bool {
    !handle.is_null() && handle != INVALID_HANDLE_VALUE
}

#[cfg(windows)]
fn windows_background_jobs() -> &'static Mutex<HashMap<u32, Arc<WindowsBackgroundJob>>> {
    WINDOWS_BACKGROUND_JOBS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(windows)]
fn register_windows_background_job(pid: u32, job: Arc<WindowsBackgroundJob>) -> io::Result<()> {
    match windows_background_jobs().lock() {
        Ok(mut jobs) => {
            jobs.insert(pid, job);
            Ok(())
        }
        Err(error) => Err(io::Error::other(format!(
            "windows background job registry lock poisoned for pid {pid}: {error}"
        ))),
    }
}

#[cfg(windows)]
fn take_windows_background_job(pid: u32) -> Option<Arc<WindowsBackgroundJob>> {
    match windows_background_jobs().lock() {
        Ok(mut jobs) => jobs.remove(&pid),
        Err(_) => None,
    }
}

#[cfg(windows)]
fn windows_background_job_active_process_count(pid: u32) -> Option<io::Result<u32>> {
    let job = match windows_background_jobs().lock() {
        Ok(jobs) => jobs.get(&pid).cloned(),
        Err(error) => {
            return Some(Err(io::Error::other(format!(
                "windows background job registry lock poisoned for pid {pid}: {error}"
            ))));
        }
    }?;
    Some(job.active_process_count())
}

#[cfg(windows)]
pub(crate) fn terminate_background_process_tree(pid: u32) -> io::Result<()> {
    let mut succeeded = false;
    let mut errors = Vec::new();

    if let Some(job) = take_windows_background_job(pid) {
        match job.terminate() {
            Ok(()) => succeeded = true,
            Err(error) => errors.push(format!("job object termination failed: {error}")),
        }
    }

    match terminate_windows_process_tree(pid) {
        Ok(()) => succeeded = true,
        Err(error) => errors.push(format!("taskkill fallback failed: {error}")),
    }

    if succeeded {
        return Ok(());
    }

    Err(io::Error::other(format!(
        "failed to terminate background process tree rooted at pid {pid}: {}",
        errors.join("; ")
    )))
}

#[cfg(windows)]
fn terminate_windows_process_tree(pid: u32) -> io::Result<()> {
    let pid_arg = pid.to_string();
    let system32_dir = trusted_windows_system32_dir()?;
    let taskkill_path = system32_dir.join("taskkill.exe");
    let mut command = Command::new(taskkill_path);
    command
        .args(["/PID", pid_arg.as_str(), "/T", "/F"])
        .env_clear()
        .current_dir(system32_dir.as_path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(windows_dir) = system32_dir.parent() {
        command.env("SystemRoot", windows_dir).env("WINDIR", windows_dir);
    }
    let status = command.status()?;
    if status.success() {
        return Ok(());
    }
    Err(io::Error::other(format!("taskkill failed for process tree rooted at pid {pid}: {status}")))
}

#[cfg(windows)]
fn trusted_windows_system32_dir() -> io::Result<PathBuf> {
    use std::{ffi::OsString, os::windows::ffi::OsStringExt};
    use windows_sys::Win32::System::SystemInformation::GetSystemDirectoryW;

    const MAX_SYSTEM_DIRECTORY_CHARS: usize = 32_768;

    let mut buffer = vec![0_u16; 260];
    loop {
        // SAFETY: `buffer` is a valid writable UTF-16 buffer for the provided length. The Win32
        // call writes at most that many code units and returns either the copied length or the
        // required length when the buffer is too small.
        let written = unsafe { GetSystemDirectoryW(buffer.as_mut_ptr(), buffer.len() as u32) };
        if written == 0 {
            return Err(io::Error::last_os_error());
        }

        let required = written as usize;
        if required < buffer.len() {
            let system32_dir = PathBuf::from(OsString::from_wide(&buffer[..required]));
            if system32_dir.is_absolute() {
                return Ok(system32_dir);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Windows system directory is not absolute: {}", system32_dir.display()),
            ));
        }

        if required >= MAX_SYSTEM_DIRECTORY_CHARS {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Windows system directory path exceeds supported length",
            ));
        }
        buffer.resize(required + 1, 0);
    }
}

#[cfg(unix)]
pub(crate) fn terminate_background_process_tree(pid: u32) -> io::Result<()> {
    match terminate_unix_process_group(pid) {
        Ok(()) => Ok(()),
        Err(group_error) => {
            let process_id = pid as libc::pid_t;
            let result = unsafe { libc::kill(process_id, libc::SIGKILL) };
            if result == 0 {
                return Ok(());
            }
            let process_error = io::Error::last_os_error();
            Err(io::Error::other(format!(
                "failed to terminate process group or pid {pid}: group kill failed: {group_error}; pid kill failed: {process_error}"
            )))
        }
    }
}

#[cfg(not(any(unix, windows)))]
pub(crate) fn terminate_background_process_tree(pid: u32) -> io::Result<()> {
    let _ = pid;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "portable background process termination is unsupported on this platform",
    ))
}

#[cfg(unix)]
fn terminate_unix_process_group(pid: u32) -> io::Result<()> {
    let process_group_id = pid as libc::pid_t;
    let result = unsafe { libc::kill(-process_group_id, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    Err(io::Error::last_os_error())
}

#[cfg(windows)]
fn process_id_is_alive(pid: u32) -> io::Result<bool> {
    palyra_common::windows_security::process_is_alive(pid)
}

#[cfg(unix)]
fn process_id_is_alive(pid: u32) -> io::Result<bool> {
    let process_id = pid as libc::pid_t;
    let result = unsafe { libc::kill(process_id, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(false);
    }
    Err(error)
}

#[cfg(not(any(unix, windows)))]
fn process_id_is_alive(pid: u32) -> io::Result<bool> {
    let _ = pid;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "portable process status is unsupported on this platform",
    ))
}

fn terminate_child_process_tree(child: &mut Child) {
    #[cfg(windows)]
    {
        let pid = child.id();
        if terminate_background_process_tree(pid).is_ok() {
            return;
        }
    }
    #[cfg(unix)]
    {
        let pid = child.id();
        if terminate_background_process_tree(pid).is_ok() {
            return;
        }
    }
    let _ = child.kill();
}

fn background_cleanup_metadata(
    pid: u32,
    lifetime_ms: u64,
    windows_job_bound: bool,
) -> serde_json::Value {
    json!({
        "auto_kill_after_ms": lifetime_ms,
        "process_tree": cfg!(any(unix, windows)),
        "windows_job_object": windows_job_bound,
        "portable_stop_command": {
            "command": "palyra.process.stop",
            "args": [pid.to_string()],
        },
        "portable_status_command": {
            "command": "palyra.process.status",
            "args": [pid.to_string()],
        },
        "manual_command": background_cleanup_command(pid),
        "note": background_cleanup_note(),
    })
}

fn background_cleanup_command(pid: u32) -> serde_json::Value {
    #[cfg(windows)]
    {
        json!({
            "command": "taskkill",
            "args": ["/PID", pid.to_string(), "/T", "/F"],
        })
    }
    #[cfg(not(windows))]
    {
        json!({
            "command": "kill",
            "args": ["-TERM", format!("-{pid}")],
        })
    }
}

fn background_cleanup_note() -> &'static str {
    #[cfg(windows)]
    {
        "Use cleanup.portable_stop_command to terminate the direct process and its descendants; manual_command is a platform fallback if the run fails before automatic lifetime cleanup runs."
    }
    #[cfg(not(windows))]
    {
        "Use cleanup.portable_stop_command to terminate the direct process group; manual_command is a platform fallback if the run fails before automatic lifetime cleanup runs."
    }
}

fn background_process_lifetime(timeout_ms: Option<u64>, execution_timeout: Duration) -> Duration {
    let lifetime_limit = background_process_lifetime_limit(execution_timeout);
    let default_lifetime = Duration::from_millis(DEFAULT_BACKGROUND_PROCESS_LIFETIME_MS);
    timeout_ms.map(Duration::from_millis).unwrap_or(default_lifetime).min(lifetime_limit)
}

fn foreground_process_timeout(timeout_ms: Option<u64>, execution_timeout: Duration) -> Duration {
    let default_timeout = Duration::from_millis(DEFAULT_FOREGROUND_PROCESS_TIMEOUT_MS);
    timeout_ms.map(Duration::from_millis).unwrap_or(default_timeout).min(execution_timeout)
}

fn background_process_lifetime_limit(execution_timeout: Duration) -> Duration {
    execution_timeout.min(Duration::from_millis(MAX_BACKGROUND_PROCESS_LIFETIME_MS))
}

fn background_process_startup_metadata_budget(lifetime: Duration) -> Option<Duration> {
    lifetime
        .checked_sub(Duration::from_millis(BACKGROUND_METADATA_RETURN_RESERVE_MS))
        .filter(|budget| !budget.is_zero())
}

fn remaining_background_process_lifetime(
    lifetime: Duration,
    elapsed: Duration,
) -> Option<Duration> {
    let remaining = lifetime.saturating_sub(elapsed);
    if remaining.is_zero() {
        None
    } else {
        Some(remaining)
    }
}

fn bounded_background_process_wait(
    lifetime: Duration,
    elapsed: Duration,
    max_wait: Duration,
) -> Option<Duration> {
    remaining_background_process_lifetime(lifetime, elapsed)
        .map(|remaining| remaining.min(max_wait))
        .filter(|wait| !wait.is_zero())
}

fn background_process_lifetime_expired_error(
    input: &ProcessRunnerInput,
    lifetime_ms: u64,
) -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::TimedOut,
        message: format!(
            "sandbox background process exceeded its {lifetime_ms}ms lifetime during startup checks and was terminated; increase the operator-configured tool execution timeout before requesting a longer background process lifetime for command '{}'",
            input.command
        ),
    }
}

fn background_process_startup_budget_expired_error(
    input: &ProcessRunnerInput,
    lifetime_ms: u64,
) -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::TimedOut,
        message: format!(
            "sandbox background process startup checks could not complete within the {lifetime_ms}ms lifetime while preserving time to return process metadata before the tool timeout; increase the operator-configured tool execution timeout before requesting a longer background process lifetime for command '{}'",
            input.command
        ),
    }
}

fn validate_platform_resource_quota_support(
    _policy: &SandboxProcessRunnerPolicy,
) -> Result<(), SandboxProcessRunError> {
    #[cfg(target_os = "macos")]
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::UnsupportedPlatform,
            message: "sandbox process runner is unavailable on macOS until reliable fail-closed CPU/memory quota enforcement is implemented"
                .to_owned(),
        });
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(())
    }
}

fn build_process_command(
    policy: &SandboxProcessRunnerPolicy,
    input: &ProcessRunnerInput,
    workspace_root: &Path,
    cwd: &Path,
) -> Result<Command, SandboxProcessRunError> {
    if process_runner_allows_host_access(policy) {
        let program = resolve_tier_b_process_program(input.command.as_str(), cwd);
        let args = rewrite_host_virtual_workspace_args(input.args.as_slice(), workspace_root)?;
        let mut command = build_tier_b_process_command(program.as_path(), args.as_slice(), cwd)?;
        configure_host_access_process_environment(
            &mut command,
            input.command.as_str(),
            program.as_path(),
            workspace_root,
        );
        apply_process_env_overrides(&mut command, input);
        return Ok(command);
    }

    let scoped_args = rewrite_arguments_to_scoped_paths(
        workspace_root,
        cwd,
        input.command.as_str(),
        &input.args,
    )?;

    if matches!(policy.tier, SandboxProcessRunnerTier::C) {
        let tier_c_policy = TierCPolicy {
            workspace_root: workspace_root.to_path_buf(),
            cwd: cwd.to_path_buf(),
            enforce_network_isolation: matches!(
                policy.egress_enforcement_mode,
                EgressEnforcementMode::Strict
            ),
            allowed_egress_hosts: policy.allowed_egress_hosts.clone(),
            allowed_dns_suffixes: policy.allowed_dns_suffixes.clone(),
        };
        let tier_c_request =
            TierCCommandRequest { command: input.command.clone(), args: scoped_args.clone() };
        let plan = build_tier_c_command_plan(&tier_c_policy, &tier_c_request)
            .map_err(map_tier_c_backend_error)?;
        let mut command = Command::new(plan.program);
        command
            .args(plan.args)
            .current_dir(cwd)
            .env_clear()
            .env("PATH", sandbox_process_path())
            .env("LANG", "C")
            .env("LC_ALL", "C");
        apply_process_env_overrides(&mut command, input);
        return Ok(command);
    }

    let program = resolve_tier_b_process_program(input.command.as_str(), cwd);
    let mut command = build_tier_b_process_command(program.as_path(), scoped_args.as_slice(), cwd)?;
    configure_tier_b_process_environment(
        &mut command,
        input.command.as_str(),
        program.as_path(),
        policy,
    );
    apply_process_env_overrides(&mut command, input);
    Ok(command)
}

fn apply_process_env_overrides(command: &mut Command, input: &ProcessRunnerInput) {
    for (key, value) in &input.env {
        command.env(key, value);
    }
}

fn build_tier_b_process_command(
    program: &Path,
    args: &[String],
    cwd: &Path,
) -> Result<Command, SandboxProcessRunError> {
    #[cfg(windows)]
    {
        build_windows_tier_b_process_command(program, args, cwd)
    }
    #[cfg(not(windows))]
    {
        let mut command = Command::new(program);
        command.args(args).current_dir(cwd);
        Ok(command)
    }
}

fn configure_tier_b_process_environment(
    command: &mut Command,
    process_command: &str,
    program: &Path,
    policy: &SandboxProcessRunnerPolicy,
) {
    command.env_clear();
    #[cfg(windows)]
    {
        configure_windows_tier_b_process_environment(command, program, policy);
    }
    #[cfg(not(windows))]
    {
        let _ = program;
        let _ = policy;
        command.env("PATH", sandbox_process_path()).env("LANG", "C").env("LC_ALL", "C");
    }
    configure_workspace_python_environment(
        command,
        process_command,
        policy.workspace_root.as_path(),
    );
}

fn configure_host_access_process_environment(
    command: &mut Command,
    process_command: &str,
    program: &Path,
    workspace_root: &Path,
) {
    configure_workspace_python_environment(command, process_command, workspace_root);
    if !is_palyra_cli_program(program) {
        return;
    }
    if !should_repair_palyra_cli_profile_env(
        std::env::var_os(PALYRA_CLI_PROFILE_ENV).as_deref(),
        std::env::var_os(PALYRA_CLI_PROFILES_PATH_ENV).as_deref(),
    ) {
        return;
    }
    if let Some(profiles_path) = std::env::var_os(PALYRA_STATE_ROOT_ENV)
        .map(PathBuf::from)
        .and_then(|state_root| infer_desktop_cli_profiles_path(state_root.as_path()))
        .filter(|profiles_path| profiles_path.is_file())
    {
        command.env(PALYRA_CLI_PROFILES_PATH_ENV, profiles_path);
    } else {
        command.env_remove(PALYRA_CLI_PROFILE_ENV);
    }
}

fn configure_workspace_python_environment(
    command: &mut Command,
    process_command: &str,
    workspace_root: &Path,
) {
    let Some(environment) = workspace_python_environment(process_command, workspace_root) else {
        return;
    };
    command
        .env("PYTHONUSERBASE", environment.user_base)
        .env("PIP_CACHE_DIR", environment.pip_cache)
        .env("PIP_DISABLE_PIP_VERSION_CHECK", "1");
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspacePythonEnvironment {
    user_base: PathBuf,
    pip_cache: PathBuf,
}

fn workspace_python_environment(
    process_command: &str,
    workspace_root: &Path,
) -> Option<WorkspacePythonEnvironment> {
    if !is_python_runtime_command(process_command) {
        return None;
    }

    Some(WorkspacePythonEnvironment {
        user_base: join_relative_components(
            workspace_root,
            WORKSPACE_PYTHON_USER_BASE_RELATIVE_PATH,
        ),
        pip_cache: join_relative_components(workspace_root, WORKSPACE_PIP_CACHE_RELATIVE_PATH),
    })
}

fn join_relative_components(root: &Path, components: &[&str]) -> PathBuf {
    components.iter().fold(root.to_path_buf(), |path, component| path.join(component))
}

fn is_python_runtime_command(process_command: &str) -> bool {
    let command = normalized_process_command_name(process_command);
    matches!(command.as_str(), "py" | "pip" | "pip3")
        || command == "python"
        || command == "python3"
        || command.starts_with("python3.")
}

fn is_palyra_cli_program(program: &Path) -> bool {
    program
        .file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|stem| stem.eq_ignore_ascii_case("palyra"))
}

fn should_repair_palyra_cli_profile_env(
    profile: Option<&std::ffi::OsStr>,
    profiles_path: Option<&std::ffi::OsStr>,
) -> bool {
    profile.is_some_and(|value| !value.is_empty())
        && profiles_path.is_none_or(|value| value.is_empty())
}

fn infer_desktop_cli_profiles_path(state_root: &Path) -> Option<PathBuf> {
    let runtime_dir = state_root.file_name()?.to_str()?;
    let desktop_dir = state_root.parent()?.file_name()?.to_str()?;
    if !runtime_dir.eq_ignore_ascii_case(DESKTOP_RUNTIME_STATE_DIR)
        || !desktop_dir.eq_ignore_ascii_case(DESKTOP_CONTROL_CENTER_STATE_DIR)
    {
        return None;
    }
    state_root.parent()?.parent().map(|root| root.join(CLI_PROFILES_RELATIVE_PATH))
}

fn resolve_tier_b_process_program(command: &str, cwd: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        resolve_windows_process_program(command, cwd).unwrap_or_else(|| PathBuf::from(command))
    }
    #[cfg(not(windows))]
    {
        let _ = cwd;
        PathBuf::from(command)
    }
}

#[cfg(windows)]
fn resolve_windows_process_program(command: &str, cwd: &Path) -> Option<PathBuf> {
    let command_path = Path::new(command);
    if command_path.components().count() != 1 {
        return None;
    }

    windows_command_candidates(command)
        .into_iter()
        .map(|candidate| cwd.join(candidate))
        .find(|candidate| candidate.is_file())
        .or_else(|| windows_path_program_candidates(command).into_iter().next())
}

#[cfg(windows)]
fn windows_path_program_candidates(command: &str) -> Vec<PathBuf> {
    let command_path = Path::new(command);
    if command_path.components().count() != 1 {
        return Vec::new();
    }

    let candidates = windows_command_candidates(command);
    let Some(path) = std::env::var_os("PATH") else {
        return Vec::new();
    };

    std::env::split_paths(&path)
        .flat_map(|directory| candidates.iter().map(move |candidate| directory.join(candidate)))
        .filter(|candidate| candidate.is_file())
        .collect()
}

#[cfg(windows)]
fn windows_command_candidates(command: &str) -> Vec<String> {
    let has_extension = Path::new(command).extension().is_some();
    if has_extension {
        return vec![command.to_owned()];
    }

    let raw_pathext = std::env::var("PATHEXT").unwrap_or_default();
    windows_command_candidates_from_pathext(command, raw_pathext.as_str())
}

#[cfg(windows)]
fn windows_command_candidates_from_pathext(command: &str, raw_pathext: &str) -> Vec<String> {
    let extensions = raw_pathext
        .split(';')
        .map(str::trim)
        .filter(|extension| !extension.is_empty())
        .collect::<Vec<_>>();
    let extensions =
        if extensions.is_empty() { WINDOWS_DEFAULT_PATH_EXTENSIONS.to_vec() } else { extensions };
    let mut candidates = Vec::with_capacity(extensions.len().saturating_add(1));
    candidates.extend(extensions.into_iter().map(|extension| {
        if extension.starts_with('.') {
            format!("{command}{extension}")
        } else {
            format!("{command}.{extension}")
        }
    }));
    candidates.push(command.to_owned());
    candidates
}

#[cfg(windows)]
fn build_windows_tier_b_process_command(
    program: &Path,
    args: &[String],
    cwd: &Path,
) -> Result<Command, SandboxProcessRunError> {
    if windows_program_requires_cmd_wrapper(program) {
        let mut command = Command::new(windows_command_processor());
        command.raw_arg(format!("/D /S /C {}", windows_cmd_wrapper_command_line(program, args)?));
        command.current_dir(cwd);
        return Ok(command);
    }

    let mut command = Command::new(program);
    command.args(args).current_dir(cwd);
    Ok(command)
}

#[cfg(windows)]
fn windows_program_requires_cmd_wrapper(program: &Path) -> bool {
    program.extension().and_then(|extension| extension.to_str()).is_some_and(|extension| {
        extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat")
    })
}

#[cfg(windows)]
fn windows_command_processor() -> PathBuf {
    std::env::var_os("COMSPEC")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(r"C:\Windows\System32\cmd.exe"))
}

#[cfg(windows)]
fn windows_cmd_wrapper_command_line(
    program: &Path,
    args: &[String],
) -> Result<String, SandboxProcessRunError> {
    let program = windows_cmd_compatible_path_string(program);
    let mut command_line = String::from("\"");
    command_line.push_str(windows_cmd_wrapper_quote_arg(program.as_str())?.as_str());
    for arg in args {
        command_line.push(' ');
        command_line.push_str(windows_cmd_wrapper_quote_arg(arg.as_str())?.as_str());
    }
    command_line.push('"');
    Ok(command_line)
}

#[cfg(windows)]
fn windows_cmd_compatible_path_string(path: &Path) -> String {
    windows_deverbatim_path_string(path).unwrap_or_else(|| path.to_string_lossy().into_owned())
}

#[cfg(windows)]
fn windows_deverbatim_path_string(path: &Path) -> Option<String> {
    let normalized = path.to_string_lossy().replace('\\', "/");
    let lower = normalized.to_ascii_lowercase();
    let deverbatim = if lower.starts_with("//?/unc/") {
        format!("//{}", &normalized[8..])
    } else if lower.starts_with("//?/") || lower.starts_with("//./") {
        normalized[4..].to_owned()
    } else {
        return None;
    };
    Some(deverbatim.replace('/', "\\"))
}

#[cfg(windows)]
fn windows_cmd_wrapper_quote_arg(raw: &str) -> Result<String, SandboxProcessRunError> {
    if raw.chars().any(|ch| matches!(ch, '\0' | '\r' | '\n' | '"' | '%' | '!')) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "sandbox denied: Windows .cmd/.bat wrapper arguments cannot contain quotes, environment expansion markers, or newlines; pass safe split args or write a workspace script file".to_owned(),
        });
    }

    let mut quoted = String::with_capacity(raw.len().saturating_add(2));
    quoted.push('"');
    for ch in raw.chars() {
        if ch == '^' {
            quoted.push_str("^^");
        } else {
            quoted.push(ch);
        }
    }
    quoted.push('"');
    Ok(quoted)
}

#[cfg(windows)]
fn configure_windows_tier_b_process_environment(
    command: &mut Command,
    program: &Path,
    policy: &SandboxProcessRunnerPolicy,
) {
    for key in WINDOWS_TIER_B_SAFE_ENV_KEYS {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    command
        .env("PATH", windows_tier_b_process_path(program, policy))
        .env("TEMP", policy.workspace_root.as_path())
        .env("TMP", policy.workspace_root.as_path())
        .env("LANG", "C")
        .env("LC_ALL", "C");
}

#[cfg(windows)]
const WINDOWS_TIER_B_SAFE_ENV_KEYS: &[&str] = &[
    "COMSPEC",
    "PATHEXT",
    "PROGRAMDATA",
    "ProgramFiles",
    "ProgramFiles(x86)",
    "ProgramW6432",
    "SystemDrive",
    "SystemRoot",
    "WINDIR",
];

#[cfg(windows)]
fn windows_tier_b_process_path(program: &Path, policy: &SandboxProcessRunnerPolicy) -> String {
    let mut directories = std::env::split_paths(sandbox_process_path()).collect::<Vec<_>>();
    if let Some(parent) = program.parent() {
        push_unique_windows_path(&mut directories, parent.to_path_buf());
    }
    for allowed in &policy.allowed_executables {
        for candidate in windows_path_program_candidates(allowed) {
            if let Some(parent) = candidate.parent() {
                push_unique_windows_path(&mut directories, parent.to_path_buf());
            }
        }
    }
    std::env::join_paths(directories)
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| sandbox_process_path().to_owned())
}

#[cfg(windows)]
fn push_unique_windows_path(directories: &mut Vec<PathBuf>, candidate: PathBuf) {
    let candidate_key = candidate.to_string_lossy().to_ascii_lowercase();
    if directories
        .iter()
        .any(|existing| existing.to_string_lossy().to_ascii_lowercase() == candidate_key)
    {
        return;
    }
    directories.push(candidate);
}

fn map_tier_c_backend_error(error: TierCBackendError) -> SandboxProcessRunError {
    match error {
        TierCBackendError::HostAllowlistUnsupported { .. }
        | TierCBackendError::NetworkIsolationUnsupported { .. } => SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::EgressDenied,
            message: format!("sandbox denied: {error}"),
        },
        TierCBackendError::BackendBinaryMissing { .. }
        | TierCBackendError::BackendUnavailable { .. } => SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::SpawnFailed,
            message: format!("sandbox denied: {error}"),
        },
    }
}

fn sandbox_process_path() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "/usr/bin:/bin:/usr/sbin:/sbin"
    }
    #[cfg(windows)]
    {
        r"C:\Windows\System32;C:\Windows;C:\Windows\System32\WindowsPowerShell\v1.0"
    }
    #[cfg(all(not(target_os = "macos"), not(windows)))]
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
            set_cpu_rlimit(cpu_time_limit_ms).map_err(|error| {
                std::io::Error::new(error.kind(), format!("failed to set CPU rlimit: {error}"))
            })?;
            set_memory_rlimit(memory_limit_bytes).map_err(|error| {
                std::io::Error::new(error.kind(), format!("failed to set memory rlimit: {error}"))
            })?;
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
    let cpu_limit_seconds = current_process_cpu_rlimit_seconds(cpu_time_limit_ms)?;
    set_rlimit(libc::RLIMIT_CPU as libc::c_int, cpu_limit_seconds as libc::rlim_t)
}

#[cfg(unix)]
fn set_memory_rlimit(memory_limit_bytes: u64) -> std::io::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let _ = memory_limit_bytes;
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "macOS does not expose a reliable total-memory rlimit for fail-closed sandbox execution",
        ));
    }
    #[cfg(not(target_os = "macos"))]
    {
        set_rlimit(libc::RLIMIT_AS as libc::c_int, memory_limit_bytes as libc::rlim_t)
    }
}

#[cfg(unix)]
fn current_process_cpu_rlimit_seconds(cpu_time_limit_ms: u64) -> std::io::Result<u64> {
    let used_micros = current_process_cpu_time_micros()?;
    Ok(cpu_rlimit_seconds_from_usage_micros(cpu_time_limit_ms, used_micros))
}

#[cfg(unix)]
fn current_process_cpu_time_micros() -> std::io::Result<u128> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let usage = unsafe { usage.assume_init() };
    Ok(timeval_micros(usage.ru_utime).saturating_add(timeval_micros(usage.ru_stime)))
}

fn cpu_rlimit_seconds_from_usage_micros(cpu_time_limit_ms: u64, cpu_time_used_micros: u128) -> u64 {
    let requested_seconds = cpu_ms_to_rlimit_seconds(cpu_time_limit_ms) as u128;
    let used_seconds = cpu_time_used_micros.div_ceil(1_000_000);
    requested_seconds.saturating_add(used_seconds).min(u64::MAX as u128) as u64
}

#[cfg(unix)]
fn timeval_micros(value: libc::timeval) -> u128 {
    let seconds = value.tv_sec.max(0) as u128;
    let micros = value.tv_usec.max(0) as u128;
    seconds.saturating_mul(1_000_000).saturating_add(micros)
}

fn cpu_ms_to_rlimit_seconds(cpu_time_limit_ms: u64) -> u64 {
    cpu_time_limit_ms.max(1).div_ceil(1_000)
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
    let mut termination_requested = false;
    let exit_status = loop {
        if quota_triggered.load(Ordering::Relaxed) {
            quota_exceeded = true;
            if !termination_requested {
                terminate_child_process_tree(child);
                termination_requested = true;
            }
        }
        if started_at.elapsed() > timeout {
            timed_out = true;
            if !termination_requested {
                terminate_child_process_tree(child);
                termination_requested = true;
            }
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
        collections::BTreeMap,
        fs, io,
        path::{Path, PathBuf},
        process::Command,
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use super::{
        build_process_command, builtin_list_directory_stdout, canonical_workspace_root,
        collect_requested_egress_hosts, cpu_rlimit_seconds_from_usage_micros, is_host_allowlisted,
        process_failure_message, redacted_process_output_preview, redacted_process_output_text,
        resolve_host_working_directory, resolve_working_directory,
        rewrite_arguments_to_scoped_paths, rewrite_host_virtual_workspace_args,
        run_constrained_process, validate_argument_workspace_scope, validate_cmd_invocation_shape,
        validate_host_argument_scope, validate_host_interpreter_argument_guardrails,
        validate_interpreter_argument_guardrails, validate_no_embedded_command_line_arg,
        validate_process_env_overrides, validate_process_termination_scope,
        validate_runtime_egress_enforcement, EgressEnforcementMode, ProcessRunnerInput,
        SandboxProcessRunErrorKind, SandboxProcessRunnerPolicy, SandboxProcessRunnerTier,
        StreamCapture, BACKGROUND_MONITOR_POLL_MS, BACKGROUND_TERMINATION_WAIT_MS,
    };

    const BACKGROUND_TEST_EXECUTION_TIMEOUT_MS: u64 = 10_000;
    const BACKGROUND_TEST_SCRIPT_SLEEP_SECS: u64 = 8;

    fn background_test_execution_timeout() -> Duration {
        Duration::from_millis(BACKGROUND_TEST_EXECUTION_TIMEOUT_MS)
    }

    fn portable_test_memory_limit_bytes() -> u64 {
        #[cfg(target_os = "macos")]
        {
            // The child inherits the test binary's resident footprint before pre_exec applies
            // RLIMIT_AS, so tiny test-only quotas can fail before the behavior under test runs.
            return 512 * 1024 * 1024;
        }
        #[cfg(not(target_os = "macos"))]
        {
            128 * 1024 * 1024
        }
    }

    fn sandbox_policy_with_allowed_executables(
        workspace_root: PathBuf,
        allowed_executables: Vec<String>,
    ) -> SandboxProcessRunnerPolicy {
        SandboxProcessRunnerPolicy {
            enabled: true,
            tier: SandboxProcessRunnerTier::B,
            workspace_root,
            allowed_executables,
            allow_interpreters: false,
            egress_enforcement_mode: EgressEnforcementMode::Strict,
            allowed_egress_hosts: Vec::new(),
            allowed_dns_suffixes: Vec::new(),
            cpu_time_limit_ms: 2_000,
            memory_limit_bytes: portable_test_memory_limit_bytes(),
            max_output_bytes: 64 * 1024,
        }
    }

    fn sandbox_policy(workspace_root: PathBuf) -> SandboxProcessRunnerPolicy {
        sandbox_policy_with_allowed_executables(workspace_root, vec!["uname".to_owned()])
    }

    fn host_access_policy(workspace_root: PathBuf) -> SandboxProcessRunnerPolicy {
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace_root, vec!["*".to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        policy
    }

    fn unique_temp_dir(suffix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after UNIX epoch")
            .as_nanos();
        std::env::temp_dir()
            .join(format!("palyra-sandbox-runner-{suffix}-{nanos}-{}", std::process::id()))
    }

    #[cfg(unix)]
    fn create_directory_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::unix::fs::symlink(target, link)
    }

    #[cfg(windows)]
    fn create_directory_symlink(target: &Path, link: &Path) -> io::Result<()> {
        std::os::windows::fs::symlink_dir(target, link)
    }

    #[test]
    fn resolve_working_directory_treats_virtual_roots_as_workspace_root() {
        let workspace = unique_temp_dir("workspace-virtual-root");
        let nested = workspace.join("e2e-file-workflow");
        fs::create_dir_all(nested.as_path()).expect("workspace subdirectory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let canonical_nested =
            fs::canonicalize(nested.as_path()).expect("nested workspace path should canonicalize");

        for alias in ["/", "\\", "workspace", "/workspace", "\\workspace"] {
            let resolved = resolve_working_directory(canonical_workspace.as_path(), Some(alias))
                .expect("virtual workspace root aliases should resolve");
            assert_eq!(resolved, canonical_workspace, "alias {alias}");
        }

        for alias in [
            "workspace/e2e-file-workflow",
            "/workspace/e2e-file-workflow",
            "\\workspace\\e2e-file-workflow",
        ] {
            let resolved = resolve_working_directory(canonical_workspace.as_path(), Some(alias))
                .expect("virtual workspace child alias should resolve");
            assert_eq!(resolved, canonical_nested, "alias {alias}");
        }

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn resolve_host_working_directory_treats_named_virtual_roots_as_workspace_root() {
        let workspace = unique_temp_dir("workspace-host-virtual-root");
        let nested = workspace.join("fixtures").join("todo-app");
        fs::create_dir_all(nested.as_path()).expect("workspace subdirectory should be created");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let canonical_nested =
            fs::canonicalize(nested.as_path()).expect("nested workspace path should canonicalize");

        for alias in ["workspace", "/workspace", "\\workspace"] {
            let resolved =
                resolve_host_working_directory(canonical_workspace.as_path(), Some(alias))
                    .expect("host access should resolve named virtual workspace root aliases");
            assert_eq!(resolved, canonical_workspace, "alias {alias}");
        }

        for alias in [
            "workspace/fixtures/todo-app",
            "/workspace/fixtures/todo-app",
            "\\workspace\\fixtures\\todo-app",
        ] {
            let resolved =
                resolve_host_working_directory(canonical_workspace.as_path(), Some(alias))
                    .expect("host access should resolve named virtual workspace child aliases");
            assert_eq!(resolved, canonical_nested, "alias {alias}");
        }

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn resolve_host_working_directory_allows_user_owned_os_roots() {
        let workspace = unique_temp_dir("workspace-host-cwd");
        let outside = unique_temp_dir("outside-host-cwd");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let canonical_outside =
            fs::canonicalize(outside.as_path()).expect("outside cwd should canonicalize");

        let resolved = resolve_host_working_directory(
            canonical_workspace.as_path(),
            Some(canonical_outside.to_string_lossy().as_ref()),
        )
        .expect("host access should allow user-owned OS cwd");

        assert_eq!(resolved, canonical_outside);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_access_allows_user_owned_script_argument() {
        let workspace = unique_temp_dir("workspace-host-script-arg");
        let outside = unique_temp_dir("outside-host-script-arg");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let script = outside.join("slow-preview.cjs");
        fs::write(script.as_path(), b"console.log('ok');\n").expect("helper should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let canonical_outside =
            fs::canonicalize(outside.as_path()).expect("outside cwd should canonicalize");
        let canonical_script =
            fs::canonicalize(script.as_path()).expect("outside script should canonicalize");
        let args = vec![canonical_script.to_string_lossy().to_string()];

        validate_host_interpreter_argument_guardrails(
            canonical_workspace.as_path(),
            canonical_outside.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("host access should allow interpreter scripts under user-owned OS roots");
        validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_outside.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("host access should allow absolute script args under user-owned OS roots");

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_access_rejects_relative_traversal_script_argument() {
        let workspace = unique_temp_dir("workspace-host-script-traversal");
        let outside = unique_temp_dir("outside-host-script-traversal");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let canonical_outside =
            fs::canonicalize(outside.as_path()).expect("outside cwd should canonicalize");
        let args = vec!["../slow-preview.cjs".to_owned()];

        let error = validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_outside.as_path(),
            "node",
            args.as_slice(),
        )
        .expect_err("host access should reject parent-directory traversal in script args");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("path traversal"));

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn validate_argument_workspace_scope_allows_virtual_workspace_paths() {
        let workspace = unique_temp_dir("workspace-virtual-arg");
        let nested = workspace.join("e2e-file-workflow");
        fs::create_dir_all(nested.as_path()).expect("workspace subdirectory should be created");
        fs::write(nested.join("test.js"), b"console.log('ok');\n")
            .expect("workspace fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![
            "/workspace/e2e-file-workflow/test.js".to_owned(),
            "--config=\\workspace\\e2e-file-workflow\\test.js".to_owned(),
        ];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("virtual workspace path aliases should stay within scope");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn builtin_list_directory_resolves_virtual_root_alias_to_workspace() {
        let workspace = unique_temp_dir("workspace-builtin-list-virtual-root");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("WORKSPACE_SENTINEL_ONLY"), b"ok")
            .expect("workspace sentinel should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["/".to_owned()];

        let output = builtin_list_directory_stdout(
            "ls",
            args.as_slice(),
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
        )
        .expect("virtual root alias should list the workspace root");

        assert!(output.contains("WORKSPACE_SENTINEL_ONLY"), "{output}");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn builtin_list_directory_rejects_dash_prefixed_symlink_escape() {
        let workspace = unique_temp_dir("workspace-builtin-list-dash-symlink");
        let outside = unique_temp_dir("outside-builtin-list-dash-symlink");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        fs::write(outside.join("OUTSIDE_LIST_MARKER"), b"outside")
            .expect("outside marker should be written");
        let link_path = workspace.join("-x");
        if let Err(error) = create_directory_symlink(outside.as_path(), link_path.as_path()) {
            eprintln!(
                "skipping dash-prefixed symlink escape regression because symlink creation failed: {error}"
            );
            let _ = fs::remove_dir_all(workspace.as_path());
            let _ = fs::remove_dir_all(outside.as_path());
            return;
        }
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-x".to_owned()];

        let error = builtin_list_directory_stdout(
            "ls",
            args.as_slice(),
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
        )
        .expect_err("dash-prefixed symlink target must not list outside workspace");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("escapes workspace scope"), "unexpected error: {error:?}");

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn rewrite_arguments_to_scoped_paths_replaces_virtual_workspace_aliases() {
        let workspace = unique_temp_dir("workspace-virtual-arg-rewrite");
        let nested = workspace.join("e2e-file-workflow");
        fs::create_dir_all(nested.as_path()).expect("workspace subdirectory should be created");
        fs::write(nested.join("test.js"), b"console.log('ok');\n")
            .expect("workspace fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let expected_script = canonical_workspace
            .join("e2e-file-workflow")
            .join("test.js")
            .to_string_lossy()
            .to_string();
        let expected_directory =
            canonical_workspace.join("e2e-file-workflow").to_string_lossy().to_string();
        let args = vec![
            "/workspace/e2e-file-workflow/test.js".to_owned(),
            "--config=\\workspace\\e2e-file-workflow\\test.js".to_owned(),
            "-C/workspace/e2e-file-workflow".to_owned(),
            "--grep".to_owned(),
            "/not/a/path/pattern".to_owned(),
        ];

        let rewritten = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("virtual workspace path aliases should rewrite to scoped paths");

        assert_eq!(rewritten[0], expected_script);
        assert_eq!(rewritten[1], format!("--config={expected_script}"));
        assert_eq!(rewritten[2], format!("-C{expected_directory}"));
        assert_eq!(rewritten[3], "--grep");
        assert_eq!(rewritten[4], "/not/a/path/pattern");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn rewrite_arguments_to_scoped_paths_preserves_node_eval_code() {
        let workspace = unique_temp_dir("workspace-node-eval-arg-rewrite");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-e".to_owned(), "console.log('PALYRA_PROCESS_OK')".to_owned()];

        let rewritten = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("node eval code should remain a literal argument");

        assert_eq!(rewritten, args);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn rewrite_arguments_to_scoped_paths_preserves_sleep_durations() {
        let workspace = unique_temp_dir("workspace-sleep-arg-rewrite");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["2".to_owned(), "0.5s".to_owned()];

        let rewritten = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "sleep",
            args.as_slice(),
        )
        .expect("sleep durations should remain literal arguments");

        assert_eq!(rewritten, args);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn build_process_command_uses_rewritten_sandbox_args() {
        let workspace = unique_temp_dir("workspace-build-command-virtual-arg");
        let nested = workspace.join("e2e-file-workflow");
        fs::create_dir_all(nested.as_path()).expect("workspace subdirectory should be created");
        fs::write(nested.join("test.js"), b"console.log('ok');\n")
            .expect("workspace fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let expected_script = canonical_workspace
            .join("e2e-file-workflow")
            .join("test.js")
            .to_string_lossy()
            .to_string();
        let policy = sandbox_policy_with_allowed_executables(
            canonical_workspace.clone(),
            vec!["node".into()],
        );
        let input = ProcessRunnerInput {
            command: "node".to_owned(),
            args: vec!["/".to_owned(), "--config=/workspace/e2e-file-workflow/test.js".to_owned()],
            cwd: None,
            env: Default::default(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
        };

        let command = build_process_command(
            &policy,
            &input,
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
        )
        .expect("sandboxed process command should be built");
        let args =
            command.get_args().map(|arg| arg.to_string_lossy().to_string()).collect::<Vec<_>>();

        assert_eq!(args[0], canonical_workspace.to_string_lossy());
        assert_eq!(args[1], format!("--config={expected_script}"));

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_access_rewrites_named_virtual_workspace_args() {
        let workspace = unique_temp_dir("workspace-host-virtual-arg");
        let nested = workspace.join("fixtures").join("todo-app");
        fs::create_dir_all(nested.as_path()).expect("workspace subdirectory should be created");
        fs::write(nested.join("package.json"), b"{\"scripts\":{}}\n")
            .expect("workspace fixture should be written");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let expected_manifest = canonical_workspace
            .join("fixtures")
            .join("todo-app")
            .join("package.json")
            .to_string_lossy()
            .to_string();
        #[cfg(windows)]
        let host_absolute = "C:\\Windows\\System32\\drivers\\etc\\hosts";
        #[cfg(not(windows))]
        let host_absolute = "/etc/hosts";
        let args = vec![
            "/workspace/fixtures/todo-app/package.json".to_owned(),
            "--config=\\workspace\\fixtures\\todo-app\\package.json".to_owned(),
            host_absolute.to_owned(),
        ];

        let rewritten =
            rewrite_host_virtual_workspace_args(args.as_slice(), canonical_workspace.as_path())
                .expect("host access should rewrite named virtual workspace path aliases");

        assert_eq!(rewritten[0], expected_manifest);
        assert_eq!(rewritten[1], format!("--config={expected_manifest}"));
        assert_eq!(rewritten[2], host_absolute);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_access_rejects_traversal_in_named_virtual_workspace_args() {
        let workspace = unique_temp_dir("workspace-host-virtual-arg-traversal");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let args = vec!["/workspace/../outside".to_owned()];

        let error =
            rewrite_host_virtual_workspace_args(args.as_slice(), canonical_workspace.as_path())
                .expect_err("host virtual workspace aliases must not escape workspace scope");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(
            error.message.contains("path traversal") || error.message.contains("escapes workspace"),
            "{}",
            error.message
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn validate_argument_workspace_scope_rejects_non_alias_host_absolute_paths() {
        let workspace = unique_temp_dir("workspace-host-absolute-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        #[cfg(windows)]
        let host_path = "C:\\Windows\\System32\\drivers\\etc\\hosts";
        #[cfg(not(windows))]
        let host_path = "/etc/passwd";
        let args = vec![host_path.to_owned()];

        let error = validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect_err("host absolute path should remain denied");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("escapes workspace"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn validate_argument_workspace_scope_allows_taskkill_cleanup_switches() {
        let workspace = unique_temp_dir("workspace-taskkill-switches");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["/PID".to_owned(), "41176".to_owned(), "/T".to_owned(), "/F".to_owned()];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "taskkill",
            args.as_slice(),
        )
        .expect("taskkill cleanup switches should not be treated as absolute paths");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn validate_argument_workspace_scope_allows_tasklist_filter_switches() {
        let workspace = unique_temp_dir("workspace-tasklist-switches");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![
            "/FI".to_owned(),
            "IMAGENAME eq node.exe".to_owned(),
            "/FO".to_owned(),
            "LIST".to_owned(),
        ];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "tasklist",
            args.as_slice(),
        )
        .expect("tasklist filter switches should not be treated as absolute paths");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn validate_argument_workspace_scope_allows_findstr_pattern_switches() {
        let workspace = unique_temp_dir("workspace-findstr-switches");
        fs::create_dir_all(workspace.join("docs")).expect("workspace directory should be created");
        fs::write(workspace.join("docs").join("index.md"), "trailing space \n")
            .expect("fixture file should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args =
            vec!["/R".to_owned(), "/N".to_owned(), "/C: $".to_owned(), "docs\\index.md".to_owned()];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "findstr",
            args.as_slice(),
        )
        .expect("findstr pattern switches should not be treated as absolute paths");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn validate_host_argument_scope_allows_icacls_grant_principal() {
        let workspace = unique_temp_dir("workspace-icacls-grant");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let target_root = unique_temp_dir("outside-icacls-grant");
        let target = target_root.join("palyra-e2e-helper.exe");
        fs::create_dir_all(target_root.as_path()).expect("target root should be created");
        fs::write(target.as_path(), "test helper").expect("target file should be written");
        let args =
            vec![target.display().to_string(), "/grant".to_owned(), "%USERNAME%:RX".to_owned()];

        validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "icacls",
            args.as_slice(),
        )
        .expect("icacls ACL switches and principals should not be treated as host paths");

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(target_root.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn validate_argument_scopes_allow_whoami_all_switch() {
        let workspace = unique_temp_dir("workspace-whoami-switch");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["/all".to_owned()];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "whoami",
            args.as_slice(),
        )
        .expect("whoami /all should be treated as a Windows switch, not a filesystem path");
        validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "whoami",
            args.as_slice(),
        )
        .expect("host access should also treat whoami /all as a Windows switch");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn validate_argument_workspace_scope_does_not_skip_path_after_short_t_flag() {
        let workspace = unique_temp_dir("workspace-short-t-flag-path");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let outside = unique_temp_dir("outside-short-t-flag-path");
        let args = vec!["-t".to_owned(), outside.display().to_string(), "inside.txt".to_owned()];

        let error = validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "cp",
            args.as_slice(),
        )
        .expect_err("paths following generic -t flag must still be workspace-scoped");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn validate_argument_workspace_scope_allows_test_name_pattern_route_text() {
        let workspace = unique_temp_dir("workspace-test-name-pattern");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["--test-name-pattern".to_owned(), "^GET /notes returns empty".to_owned()];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("test-name route patterns are matcher text, not filesystem paths");
        validate_interpreter_argument_guardrails(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("interpreter guardrails should not treat route matcher text as host paths");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn validate_process_runner_rejects_embedded_command_line_arg() {
        let input = ProcessRunnerInput {
            command: "node".to_owned(),
            args: vec!["node -e \"(() => console.log('ok'))()\"".to_owned()],
            cwd: None,
            env: Default::default(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
        };

        let error = validate_no_embedded_command_line_arg(&input)
            .expect_err("single command-line args should be rejected before execution");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("split each argument"), "{}", error.message);
    }

    #[test]
    fn process_runner_env_overrides_accept_fixture_keys_and_reject_reserved_runtime_keys() {
        let mut env = BTreeMap::new();
        env.insert("PALYRA_E2E_HOME".to_owned(), "/tmp/palyra-e2e-home".to_owned());
        validate_process_env_overrides(&env).expect("fixture env keys should be accepted");

        env.insert("PATH".to_owned(), "/tmp/bin".to_owned());
        let error =
            validate_process_env_overrides(&env).expect_err("PATH overrides must be reserved");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("reserved by the runtime"), "{}", error.message);
    }

    #[test]
    fn validate_cmd_invocation_rejects_explicit_shell_dispatch() {
        let args = vec!["/c".to_owned(), "echo".to_owned(), "hello".to_owned()];
        let error = validate_cmd_invocation_shape("cmd", args.as_slice())
            .expect_err("explicit cmd shell dispatch should not look successful");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(
            error.message.contains("does not accept explicit command='cmd'"),
            "{}",
            error.message
        );
        assert!(error.message.contains(".cmd and .bat shims"), "{}", error.message);
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
        let mut policy = sandbox_policy(workspace);
        policy.allowed_egress_hosts = vec!["allowed.example".to_owned()];
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
        let mut policy = sandbox_policy(workspace);
        policy.allowed_egress_hosts = vec!["allowed.example".to_owned()];
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
    fn run_constrained_process_rejects_command_field_with_embedded_args() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = host_access_policy(workspace);
        let input = br#"{"command":"npm test","args":[]}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("command strings with embedded args must be rejected before spawn");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("bare executable name"), "{}", error.message);
        assert!(error.message.contains("command=\"npm\""), "{}", error.message);
        assert!(error.message.contains("args=[\"test\"]"), "{}", error.message);
    }

    #[test]
    fn run_constrained_process_rejects_name_based_taskkill() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy_with_allowed_executables(workspace, vec!["*".to_owned()]);
        policy.allow_interpreters = true;
        let input = br#"{"command":"taskkill","args":["/IM","node.exe","/F"]}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("name-based process termination should be denied before spawn");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("broad process-name termination"));
    }

    #[test]
    fn run_constrained_process_rejects_powershell_name_based_stop_process() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy_with_allowed_executables(workspace, vec!["*".to_owned()]);
        policy.allow_interpreters = true;
        let input = br#"{"command":"pwsh","args":["Get-Process","-Name","node","|","Stop-Process","-Force"]}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("pipeline-based process-name termination should be denied before spawn");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("known background process id"));
    }

    #[test]
    fn process_termination_scope_allows_pid_based_cleanup() {
        validate_process_termination_scope(
            "taskkill",
            &["/PID".to_owned(), "12345".to_owned(), "/T".to_owned(), "/F".to_owned()],
        )
        .expect("pid-based taskkill cleanup should stay available");
        validate_process_termination_scope(
            "pwsh",
            &["Stop-Process".to_owned(), "-Id".to_owned(), "12345".to_owned()],
        )
        .expect("PowerShell Stop-Process by id should stay available");
    }

    #[test]
    #[cfg(target_os = "macos")]
    fn run_constrained_process_fails_closed_on_macos() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let input = br#"{"command":"uname","args":["--version"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("macos sandbox runner must fail closed");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::UnsupportedPlatform);
    }

    #[test]
    #[cfg(windows)]
    fn tier_b_resource_quota_check_allows_explicit_windows_local_processes() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);

        super::validate_platform_resource_quota_support(&policy)
            .expect("windows tier-b explicit local commands rely on timeout and output guards");
    }

    #[test]
    #[cfg(windows)]
    fn windows_tier_b_safe_env_keys_exclude_host_profile_locations() {
        for key in ["APPDATA", "LOCALAPPDATA", "TEMP", "TMP", "USERPROFILE", "VOLTA_HOME"] {
            assert!(
                !super::WINDOWS_TIER_B_SAFE_ENV_KEYS
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(key)),
                "{key} must not be copied from the daemon host environment"
            );
        }
        for key in ["COMSPEC", "PATHEXT", "SystemRoot", "WINDIR"] {
            assert!(
                super::WINDOWS_TIER_B_SAFE_ENV_KEYS
                    .iter()
                    .any(|candidate| candidate.eq_ignore_ascii_case(key)),
                "{key} should remain available for Windows process startup"
            );
        }
    }

    #[test]
    #[cfg(windows)]
    fn windows_command_candidates_prefer_pathext_before_extensionless_shims() {
        let candidates =
            super::windows_command_candidates_from_pathext("npm", ".COM;.EXE;.BAT;.CMD");

        assert_eq!(
            candidates,
            vec![
                "npm.COM".to_owned(),
                "npm.EXE".to_owned(),
                "npm.BAT".to_owned(),
                "npm.CMD".to_owned(),
                "npm".to_owned(),
            ]
        );
    }

    #[test]
    #[cfg(windows)]
    fn windows_cmd_wrapper_command_line_quotes_script_and_split_args() {
        let command_line = super::windows_cmd_wrapper_command_line(
            Path::new(r"C:\Tools\nodejs\npm.cmd"),
            &["run".to_owned(), "test suite".to_owned()],
        )
        .expect("ordinary package-manager args should be representable for cmd wrapper");

        assert_eq!(command_line, r#"""C:\Tools\nodejs\npm.cmd" "run" "test suite"""#);
    }

    #[test]
    #[cfg(windows)]
    fn windows_cmd_wrapper_deverbatims_canonical_script_paths() {
        let command_line = super::windows_cmd_wrapper_command_line(
            Path::new(r"\\?\C:\Tools\nodejs\npm.cmd"),
            &["test".to_owned()],
        )
        .expect("canonical Windows paths should be representable for cmd wrapper");

        assert_eq!(command_line, r#"""C:\Tools\nodejs\npm.cmd" "test"""#);
    }

    #[test]
    #[cfg(windows)]
    fn windows_cmd_wrapper_rejects_env_expansion_arguments() {
        let error = super::windows_cmd_wrapper_command_line(
            Path::new(r"C:\Tools\nodejs\npm.cmd"),
            &["%USERPROFILE%".to_owned()],
        )
        .expect_err("cmd wrapper args should not permit environment expansion");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("environment expansion markers"), "{}", error.message);
    }

    #[test]
    #[cfg(windows)]
    fn host_access_process_runner_executes_workspace_cmd_script_through_wrapper() {
        let workspace = unique_temp_dir("workspace-cmd-wrapper");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("hello.cmd"), b"@echo off\r\necho batch-wrapper:%~1\r\n")
            .expect("workspace batch script should be written");
        let policy = host_access_policy(workspace.clone());
        let input = br#"{"command":"hello.cmd","args":["world"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(2_000))
            .expect("workspace .cmd scripts should run through the safe cmd wrapper");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        let stdout = output
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .expect("stdout should be present in process output");

        assert!(stdout.contains("batch-wrapper:world"), "{stdout:?}");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn workspace_python_environment_scopes_userbase_and_cache_to_workspace() {
        let workspace = PathBuf::from("workspace-root");
        let environment = super::workspace_python_environment("python", workspace.as_path())
            .expect("python commands should receive workspace-local Python environment");

        assert_eq!(environment.user_base, workspace.join(".palyra").join("python-userbase"));
        assert_eq!(environment.pip_cache, workspace.join(".palyra").join("pip-cache"));
    }

    #[test]
    fn workspace_python_environment_covers_pip_and_versioned_python_commands() {
        for command in ["python", "python3", "python3.14", "py", "pip", "pip3"] {
            assert!(
                super::workspace_python_environment(command, Path::new("workspace-root")).is_some(),
                "{command} should be treated as a Python runtime command"
            );
        }
        assert!(
            super::workspace_python_environment("npm", Path::new("workspace-root")).is_none(),
            "non-Python commands should not receive Python-specific environment"
        );
    }

    #[test]
    fn palyra_cli_profile_repair_targets_only_dangling_profile_env() {
        assert!(super::should_repair_palyra_cli_profile_env(
            Some(std::ffi::OsStr::new("desktop-local")),
            None,
        ));
        assert!(!super::should_repair_palyra_cli_profile_env(
            Some(std::ffi::OsStr::new("desktop-local")),
            Some(std::ffi::OsStr::new("C:/state/cli/profiles.toml")),
        ));
        assert!(!super::should_repair_palyra_cli_profile_env(
            None,
            Some(std::ffi::OsStr::new("C:/state/cli/profiles.toml")),
        ));
    }

    #[test]
    fn nested_desktop_runtime_profiles_path_infers_parent_cli_registry() {
        let state_root = Path::new("C:/Palyra/state/desktop-control-center/runtime");

        let inferred = super::infer_desktop_cli_profiles_path(state_root)
            .expect("desktop runtime state root should infer profiles path");

        assert_eq!(inferred, PathBuf::from("C:/Palyra/state/cli/profiles.toml"));
        assert!(super::infer_desktop_cli_profiles_path(Path::new("C:/Palyra/state")).is_none());
    }

    #[test]
    fn run_constrained_process_executes_portable_pwd_builtin() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["pwd".to_owned()]);
        let input = br#"{"command":"pwd","args":[]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("portable pwd builtin should execute without spawning a platform process");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(
            output.get("sandbox_backend").and_then(serde_json::Value::as_str),
            Some("builtin_portable")
        );
        let stdout = output
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .expect("stdout should be present in process output");
        let expected_workspace = fs::canonicalize(workspace.as_path()).unwrap_or(workspace);
        assert_eq!(stdout.trim(), expected_workspace.to_string_lossy());
    }

    #[test]
    fn run_constrained_process_executes_portable_echo_builtin() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy_with_allowed_executables(workspace, vec!["echo".to_owned()]);
        let input = br#"{"command":"echo","args":["PALYRA_TERMINAL_OK"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("portable echo builtin should execute split command args");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(
            output.get("stdout").and_then(serde_json::Value::as_str),
            Some("PALYRA_TERMINAL_OK\n")
        );
        assert_eq!(
            output.get("sandbox_backend").and_then(serde_json::Value::as_str),
            Some("builtin_portable")
        );
    }

    #[test]
    fn run_constrained_process_executes_portable_directory_listing_builtin() {
        let workspace = unique_temp_dir("workspace-list-builtin");
        let nested = workspace.join("src");
        fs::create_dir_all(nested.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("Cargo.toml"), b"[package]\n")
            .expect("workspace file should be created");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["ls".to_owned()]);
        let input = br#"{"command":"ls","args":["-la"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("portable ls builtin should execute without spawning a platform process");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        let stdout = output
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .expect("stdout should be present in process output");

        assert!(stdout.contains("Cargo.toml"), "{stdout}");
        assert!(stdout.contains("src/"), "{stdout}");
        assert_eq!(
            output.get("sandbox_backend").and_then(serde_json::Value::as_str),
            Some("builtin_portable")
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn run_constrained_process_executes_portable_cat_builtin() {
        let workspace = unique_temp_dir("workspace-cat-builtin");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("README.md"), b"hello from workspace\n")
            .expect("workspace file should be created");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["cat".to_owned()]);
        let input = br#"{"command":"cat","args":["README.md"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("portable cat builtin should execute without spawning a platform process");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(
            output.get("stdout").and_then(serde_json::Value::as_str),
            Some("hello from workspace\n")
        );
        assert_eq!(
            output.get("sandbox_backend").and_then(serde_json::Value::as_str),
            Some("builtin_portable")
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn process_output_text_redacts_secret_like_values() {
        let redacted = redacted_process_output_text(
            "MINIMAX_API_KEY=sk-test-secret-value\npublic_setting=true\n",
        );

        assert!(redacted.redacted, "{redacted:?}");
        assert!(redacted.text.contains("public_setting=true"), "{}", redacted.text);
        assert!(!redacted.text.contains("sk-test-secret-value"), "{}", redacted.text);
        assert!(redacted.text.contains("REDACTED"), "{}", redacted.text);
    }

    #[test]
    fn process_output_text_preserves_benign_token_fixture_and_password_selector() {
        let output = "fixture token=a%3Db%3Dc selector=#password\n";
        let redacted = redacted_process_output_text(output);

        assert!(!redacted.redacted, "{redacted:?}");
        assert_eq!(redacted.text, output);
    }

    #[test]
    fn run_constrained_process_redacts_secret_like_builtin_stdout() {
        let workspace = unique_temp_dir("workspace-cat-builtin-secret");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join(".env"),
            b"MINIMAX_API_KEY=sk-test-secret-value\npublic_setting=true\n",
        )
        .expect("workspace env file should be created");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["type".to_owned()]);
        let input = br#"{"command":"type","args":[".env"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("portable type builtin should redact secret-like stdout");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        let stdout = output
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .expect("stdout should be present in process output");

        assert!(stdout.contains("public_setting=true"), "{stdout}");
        assert!(!stdout.contains("sk-test-secret-value"), "{stdout}");
        assert!(stdout.contains("REDACTED"), "{stdout}");
        assert_eq!(output.get("stdout_redacted").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(output.get("stderr_redacted").and_then(serde_json::Value::as_bool), Some(false));

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn run_constrained_process_rejects_directory_listing_outside_workspace() {
        let workspace = unique_temp_dir("workspace-list-deny");
        let outside = unique_temp_dir("outside-list-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["dir".to_owned()]);
        let input = format!(
            r#"{{"command":"dir","args":["{}"]}}"#,
            outside.to_string_lossy().replace('\\', "\\\\")
        );

        let error =
            run_constrained_process(&policy, input.as_bytes(), Duration::from_millis(1_000))
                .expect_err("portable dir builtin should reject paths outside workspace");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("escapes workspace"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn run_constrained_process_executes_portable_mkdir_builtin() {
        let workspace = unique_temp_dir("workspace-mkdir-builtin");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["mkdir".to_owned()]);
        let input = br#"{"command":"mkdir","args":["-p","reports/e2e"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("portable mkdir builtin should create scoped directories");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert!(workspace.join("reports").join("e2e").is_dir());
        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(
            output.get("sandbox_backend").and_then(serde_json::Value::as_str),
            Some("builtin_portable")
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn run_constrained_process_rejects_mkdir_outside_workspace() {
        let workspace = unique_temp_dir("workspace-mkdir-deny");
        let outside = unique_temp_dir("outside-mkdir-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["mkdir".to_owned()]);
        let input = format!(
            r#"{{"command":"mkdir","args":["{}"]}}"#,
            outside.join("child").to_string_lossy().replace('\\', "\\\\")
        );

        let error =
            run_constrained_process(&policy, input.as_bytes(), Duration::from_millis(1_000))
                .expect_err("portable mkdir builtin should reject paths outside workspace");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("escapes workspace"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_access_process_runner_rejects_builtin_mkdir_outside_workspace() {
        let workspace = unique_temp_dir("workspace-host-mkdir");
        let outside = unique_temp_dir("outside-host-mkdir");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let policy = host_access_policy(workspace.clone());
        let target = outside.join("child");
        let input = format!(
            r#"{{"command":"mkdir","args":["{}"]}}"#,
            target.to_string_lossy().replace('\\', "\\\\")
        );

        let error =
            run_constrained_process(&policy, input.as_bytes(), Duration::from_millis(1_000))
                .expect_err(
                    "host-access profile must still reject absolute paths outside workspace",
                );

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("escapes workspace"), "{}", error.message);
        assert!(!target.exists(), "outside-workspace mkdir target must not be created");

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn run_constrained_process_starts_allowlisted_python_background_when_available() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-python-background");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join("background_ready.py"),
            format!(
                "import time\nprint('ready', flush=True)\ntime.sleep({BACKGROUND_TEST_SCRIPT_SLEEP_SECS})\n"
            ),
        )
        .expect("background script should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["background_ready.py"],
            "background": true,
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS
        }))
        .expect("input should serialize");

        let result =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect("allowlisted python should start as a bounded background process");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(output.get("background").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(output.get("exit_code"), Some(&serde_json::Value::Null));
        assert_eq!(output.get("started").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(output.get("completed").and_then(serde_json::Value::as_bool), Some(false));
        assert_eq!(output.get("startup_success").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(
            output.get("process_state").and_then(serde_json::Value::as_str),
            Some("running")
        );
        assert_eq!(
            output.get("lifetime_ms").and_then(serde_json::Value::as_u64),
            Some(BACKGROUND_TEST_EXECUTION_TIMEOUT_MS)
        );
        assert_eq!(
            output.get("max_lifetime_ms").and_then(serde_json::Value::as_u64),
            Some(BACKGROUND_TEST_EXECUTION_TIMEOUT_MS)
        );
        assert!(output
            .get("background_lifetime_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("auto-terminate"));
        assert!(output
            .get("background_lifetime_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("operator-configured tool execution timeout"));
        assert!(output
            .get("background_lifetime_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("cleanup.portable_stop_command"));
        assert!(output.get("pid").and_then(serde_json::Value::as_u64).is_some());
        assert_eq!(
            output
                .pointer("/process_handle/direct_process_pid")
                .and_then(serde_json::Value::as_u64),
            output.get("pid").and_then(serde_json::Value::as_u64)
        );
        assert_eq!(
            output
                .pointer("/process_handle/windows_job_object")
                .and_then(serde_json::Value::as_bool),
            Some(cfg!(windows))
        );
        assert_eq!(
            output.pointer("/cleanup/windows_job_object").and_then(serde_json::Value::as_bool),
            Some(cfg!(windows))
        );
        assert_eq!(
            output
                .pointer("/cleanup/portable_stop_command/command")
                .and_then(serde_json::Value::as_str),
            Some("palyra.process.stop")
        );
        assert_eq!(
            output
                .pointer("/cleanup/portable_status_command/command")
                .and_then(serde_json::Value::as_str),
            Some("palyra.process.status")
        );
        assert!(output.pointer("/cleanup/manual_command/command").is_some());

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn background_process_returns_startup_output_snapshot() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-background-startup-output");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let script = format!(
            "import time\nprint('PORT=54321', flush=True)\ntime.sleep({BACKGROUND_TEST_SCRIPT_SLEEP_SECS})\n"
        );
        fs::write(workspace.join("print_port.py"), script)
            .expect("startup output script should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["print_port.py"],
            "background": true,
            "timeout_ms": 60_000
        }))
        .expect("input should serialize");

        let result =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect("background process should start");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(output.get("background").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(output.get("exit_code"), Some(&serde_json::Value::Null));
        assert_eq!(output.get("completed").and_then(serde_json::Value::as_bool), Some(false));
        assert_eq!(
            output.get("process_state").and_then(serde_json::Value::as_str),
            Some("running")
        );
        let stdout = output
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .replace("\r\n", "\n");
        assert_eq!(stdout, "PORT=54321\n");
        assert_eq!(
            output.get("stdout_truncated").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert!(output
            .get("background_output_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("startup snapshots"));
        assert!(output
            .get("background_output_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("not command completion output"));

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn background_process_can_be_stopped_with_portable_builtin() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-background-portable-stop");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join("sleep.py"),
            "import time\nprint('ready', flush=True)\ntime.sleep(30)\n",
        )
        .expect("background script should be written");
        let mut policy = sandbox_policy_with_allowed_executables(
            workspace.clone(),
            vec![
                python.to_owned(),
                "palyra.process.stop".to_owned(),
                "palyra.process.status".to_owned(),
            ],
        );
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;

        let start_input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["sleep.py"],
            "background": true,
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS
        }))
        .expect("input should serialize");
        let started = run_constrained_process(
            &policy,
            start_input.as_slice(),
            background_test_execution_timeout(),
        )
        .expect("background process should start");
        let started_output: serde_json::Value =
            serde_json::from_slice(&started.output_json).expect("output should parse");
        let pid = started_output
            .get("pid")
            .and_then(serde_json::Value::as_u64)
            .expect("background process should return pid");

        let status_input = serde_json::to_vec(&serde_json::json!({
            "command": "palyra.process.status",
            "args": [pid.to_string()],
        }))
        .expect("status input should serialize");
        let status = run_constrained_process(
            &policy,
            status_input.as_slice(),
            background_test_execution_timeout(),
        )
        .expect("portable status should run");
        let status_output: serde_json::Value =
            serde_json::from_slice(&status.output_json).expect("status output should parse");
        assert_eq!(status_output.get("alive").and_then(serde_json::Value::as_bool), Some(true));

        let stop_input = serde_json::to_vec(&serde_json::json!({
            "command": "palyra.process.stop",
            "args": [pid.to_string()],
        }))
        .expect("stop input should serialize");
        let stopped = run_constrained_process(
            &policy,
            stop_input.as_slice(),
            background_test_execution_timeout(),
        )
        .expect("portable stop should run");
        let stopped_output: serde_json::Value =
            serde_json::from_slice(&stopped.output_json).expect("stop output should parse");
        assert_eq!(stopped_output.get("stopped").and_then(serde_json::Value::as_bool), Some(true));

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let status = run_constrained_process(
                &policy,
                status_input.as_slice(),
                background_test_execution_timeout(),
            )
            .expect("portable status should run after stop");
            let status_output: serde_json::Value =
                serde_json::from_slice(&status.output_json).expect("status output should parse");
            if status_output.get("alive").and_then(serde_json::Value::as_bool) == Some(false) {
                break;
            }
            assert!(Instant::now() < deadline, "background pid {pid} should stop promptly");
            thread::sleep(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS));
        }

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn background_status_tracks_windows_job_after_launcher_exits() {
        let Some(python) = ["python", "py", "python3"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-background-windows-job-child");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("child.py"), "import time\ntime.sleep(30)\n")
            .expect("child script should be written");
        fs::write(
            workspace.join("launcher.py"),
            "import os, subprocess, sys, time\nsubprocess.Popen([sys.executable, 'child.py'], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)\nprint('ready', flush=True)\ntime.sleep(6)\nos._exit(0)\n",
        )
        .expect("launcher script should be written");
        let mut policy = sandbox_policy_with_allowed_executables(
            workspace.clone(),
            vec![
                python.to_owned(),
                "palyra.process.stop".to_owned(),
                "palyra.process.status".to_owned(),
            ],
        );
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;

        let start_input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["launcher.py"],
            "background": true,
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS
        }))
        .expect("input should serialize");
        let started = run_constrained_process(
            &policy,
            start_input.as_slice(),
            background_test_execution_timeout(),
        )
        .expect("launcher should start as a bounded background process");
        let started_output: serde_json::Value =
            serde_json::from_slice(&started.output_json).expect("output should parse");
        let pid = started_output
            .get("pid")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .expect("background process should return pid");
        assert_eq!(
            started_output
                .pointer("/process_handle/windows_job_object")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "windows background process should be bound to a cleanup job: {started_output}"
        );

        let deadline = Instant::now() + Duration::from_secs(9);
        loop {
            if super::process_id_is_alive(pid).map(|alive| !alive).unwrap_or(false) {
                break;
            }
            if Instant::now() >= deadline {
                let _ = super::stop_background_process_by_pid(pid);
                let _ = fs::remove_dir_all(workspace.as_path());
                panic!("launcher pid {pid} should exit while child remains in the job");
            }
            thread::sleep(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS));
        }

        let status = super::background_process_status_by_pid(pid);
        let stopped = super::stop_background_process_by_pid(pid);
        let _ = fs::remove_dir_all(workspace.as_path());

        let status = status.expect("status should inspect the Windows job after direct pid exits");
        let status_output: serde_json::Value =
            serde_json::from_slice(&status.output_json).expect("status output should parse");
        assert_eq!(
            status_output.get("direct_pid_alive").and_then(serde_json::Value::as_bool),
            Some(false),
            "direct launcher should have exited: {status_output}"
        );
        assert_eq!(
            status_output.get("process_tree_alive").and_then(serde_json::Value::as_bool),
            Some(true),
            "job should still report the child process as alive: {status_output}"
        );
        assert_eq!(status_output.get("alive").and_then(serde_json::Value::as_bool), Some(true));

        let stopped = stopped.expect("stop should terminate the Windows job");
        let stopped_output: serde_json::Value =
            serde_json::from_slice(&stopped.output_json).expect("stop output should parse");
        assert_eq!(
            stopped_output.get("stopped").and_then(serde_json::Value::as_bool),
            Some(true),
            "stop should terminate the child process tree: {stopped_output}"
        );
        assert_eq!(
            stopped_output
                .get("process_tree_alive_before_stop")
                .and_then(serde_json::Value::as_bool),
            Some(true),
            "stop should recognize the tree as running before termination: {stopped_output}"
        );
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn run_constrained_process_rejects_background_process_that_exits_immediately() {
        #[cfg(windows)]
        let command = "where.exe";
        #[cfg(not(windows))]
        let command = "true";

        if Command::new(command).output().is_err() {
            return;
        }
        let workspace = unique_temp_dir("workspace-background-immediate-exit");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![command.to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = format!(r#"{{"command":"{command}","args":[],"background":true}}"#);

        let error =
            run_constrained_process(&policy, input.as_bytes(), Duration::from_millis(1_000))
                .expect_err("background=true should reject commands that exit before startup");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::RuntimeFailure);
        assert!(error.message.contains("exited before startup check"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn run_constrained_process_rejects_background_process_that_exits_after_startup_output() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-background-delayed-failure");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join("delayed_fail.py"),
            "import sys\nprint('Unknown command: \"node\"', flush=True)\nsys.exit(1)\n",
        )
        .expect("delayed failure script should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["delayed_fail.py"],
            "background": true,
            "timeout_ms": 60_000
        }))
        .expect("input should serialize");

        let error =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect_err("background startup should reject delayed immediate failures");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::RuntimeFailure);
        assert!(error.message.contains("exited before startup check"), "{}", error.message);
        assert!(error.message.contains("Unknown command"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn background_startup_failure_terminates_spawned_child_process_tree() {
        let Some(python) = ["python3", "python"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-background-escaped-child");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let child_script = workspace.join("child.py");
        let launcher_script = workspace.join("launcher.py");
        let child_pid_path = workspace.join("child.pid");
        fs::write(
            child_script.as_path(),
            format!(
                "import os, pathlib, sys, time\npathlib.Path(sys.argv[1]).write_text(str(os.getpid()), encoding='utf-8')\ntime.sleep({BACKGROUND_TEST_SCRIPT_SLEEP_SECS})\n"
            ),
        )
        .expect("child script should be written");
        fs::write(
            launcher_script.as_path(),
            "import pathlib, subprocess, sys, time\nroot = pathlib.Path(__file__).resolve().parent\npid_path = root / 'child.pid'\nchild = root / 'child.py'\nsubprocess.Popen([sys.executable, str(child), str(pid_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\ndeadline = time.time() + 5\nwhile not pid_path.exists() and time.time() < deadline:\n    time.sleep(0.01)\nsys.exit(0)\n",
        )
        .expect("launcher script should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": [launcher_script.to_string_lossy()],
            "background": true,
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS
        }))
        .expect("input should serialize");

        let error =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect_err("launcher should exit before the background startup check");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::RuntimeFailure);
        assert!(error.message.contains("exited before startup check"), "{}", error.message);
        let child_pid = fs::read_to_string(child_pid_path.as_path())
            .expect("launcher should wait for child pid file")
            .trim()
            .parse::<u32>()
            .expect("child pid should be numeric");
        assert!(
            super::wait_for_process_not_alive(
                child_pid,
                Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS)
            ),
            "child pid {child_pid} should be terminated after startup failure"
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn background_cleanup_metadata_exposes_platform_cleanup_command() {
        let metadata = super::background_cleanup_metadata(1234, 60_000, cfg!(windows));

        assert_eq!(
            metadata.get("auto_kill_after_ms").and_then(serde_json::Value::as_u64),
            Some(60_000)
        );
        #[cfg(windows)]
        {
            assert_eq!(
                metadata.pointer("/manual_command/args/1").and_then(serde_json::Value::as_str),
                Some("1234")
            );
            assert_eq!(
                metadata.pointer("/manual_command/command").and_then(serde_json::Value::as_str),
                Some("taskkill")
            );
            assert_eq!(
                metadata.pointer("/manual_command/args/2").and_then(serde_json::Value::as_str),
                Some("/T")
            );
            assert_eq!(
                metadata.get("process_tree").and_then(serde_json::Value::as_bool),
                Some(true)
            );
            assert_eq!(
                metadata.get("windows_job_object").and_then(serde_json::Value::as_bool),
                Some(true)
            );
        }
        #[cfg(not(windows))]
        {
            assert_eq!(
                metadata.pointer("/manual_command/command").and_then(serde_json::Value::as_str),
                Some("kill")
            );
            assert_eq!(
                metadata.pointer("/manual_command/args/1").and_then(serde_json::Value::as_str),
                Some("-1234")
            );
            assert_eq!(
                metadata.get("process_tree").and_then(serde_json::Value::as_bool),
                Some(true)
            );
            assert_eq!(
                metadata.get("windows_job_object").and_then(serde_json::Value::as_bool),
                Some(false)
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn trusted_windows_system32_dir_resolves_taskkill_without_search_path() {
        let system32_dir = super::trusted_windows_system32_dir()
            .expect("Windows system directory should resolve through Win32 API");

        assert!(system32_dir.is_absolute(), "{}", system32_dir.display());
        let taskkill_path = system32_dir.join("taskkill.exe");
        assert_eq!(
            taskkill_path
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_ascii_lowercase)
                .as_deref(),
            Some("taskkill.exe")
        );
        assert!(taskkill_path.is_file(), "{}", taskkill_path.display());
    }

    #[test]
    fn foreground_process_timeout_caps_implicit_timeout() {
        let timeout = super::foreground_process_timeout(None, Duration::from_millis(120_000));

        assert_eq!(timeout, Duration::from_millis(super::DEFAULT_FOREGROUND_PROCESS_TIMEOUT_MS));
    }

    #[test]
    fn foreground_process_timeout_honors_explicit_timeout_and_execution_cap() {
        let short = super::foreground_process_timeout(Some(100), Duration::from_millis(750));
        let execution_capped =
            super::foreground_process_timeout(Some(60_000), Duration::from_millis(750));
        let implicit_execution_capped =
            super::foreground_process_timeout(None, Duration::from_millis(750));

        assert_eq!(short, Duration::from_millis(100));
        assert_eq!(execution_capped, Duration::from_millis(750));
        assert_eq!(implicit_execution_capped, Duration::from_millis(750));
    }

    #[test]
    fn background_process_lifetime_is_bounded_by_execution_timeout() {
        let lifetime = super::background_process_lifetime(None, Duration::from_millis(750));

        assert_eq!(lifetime, Duration::from_millis(750));
    }

    #[test]
    fn background_process_startup_budget_reserves_metadata_return_time() {
        let lifetime = super::background_process_lifetime(None, Duration::from_millis(750));
        let startup_budget = super::background_process_startup_metadata_budget(lifetime)
            .expect("default background lifetime should leave a metadata return budget");

        assert_eq!(startup_budget, Duration::from_millis(650));
        assert_eq!(
            super::bounded_background_process_wait(
                startup_budget,
                Duration::from_millis(super::BACKGROUND_STARTUP_CHECK_MS),
                Duration::from_millis(super::BACKGROUND_STARTUP_OUTPUT_DRAIN_MS)
            ),
            Some(Duration::from_millis(400))
        );
        assert_eq!(
            super::background_process_startup_metadata_budget(Duration::from_millis(
                super::BACKGROUND_METADATA_RETURN_RESERVE_MS
            )),
            None
        );
    }

    #[test]
    fn background_process_lifetime_preserves_default_inside_execution_timeout() {
        let lifetime = super::background_process_lifetime(None, Duration::from_millis(20 * 60_000));

        assert_eq!(lifetime, Duration::from_millis(10 * 60_000));
    }

    #[test]
    fn background_process_lifetime_honors_short_explicit_timeout_and_execution_cap() {
        let short = super::background_process_lifetime(Some(100), Duration::from_millis(750));
        let execution_capped =
            super::background_process_lifetime(Some(60_000), Duration::from_millis(750));
        let capped =
            super::background_process_lifetime(Some(60 * 60_000), Duration::from_millis(750));

        assert_eq!(short, Duration::from_millis(100));
        assert_eq!(execution_capped, Duration::from_millis(750));
        assert_eq!(capped, Duration::from_millis(750));
    }

    #[test]
    fn background_process_lifetime_caps_large_execution_timeout_at_runtime_max() {
        let capped =
            super::background_process_lifetime(Some(60 * 60_000), Duration::from_millis(u64::MAX));

        assert_eq!(capped, Duration::from_millis(super::MAX_BACKGROUND_PROCESS_LIFETIME_MS));
    }

    #[test]
    fn remaining_background_process_lifetime_uses_elapsed_startup_time() {
        assert_eq!(
            super::remaining_background_process_lifetime(
                Duration::from_millis(1_000),
                Duration::from_millis(250)
            ),
            Some(Duration::from_millis(750))
        );
        assert_eq!(
            super::remaining_background_process_lifetime(
                Duration::from_millis(1_000),
                Duration::from_millis(1_000)
            ),
            None
        );
        assert_eq!(
            super::bounded_background_process_wait(
                Duration::from_millis(1_000),
                Duration::from_millis(250),
                Duration::from_millis(500)
            ),
            Some(Duration::from_millis(500))
        );
        assert_eq!(
            super::bounded_background_process_wait(
                Duration::from_millis(1_000),
                Duration::from_millis(900),
                Duration::from_millis(500)
            ),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            super::bounded_background_process_wait(
                Duration::from_millis(1_000),
                Duration::from_millis(1_000),
                Duration::from_millis(500)
            ),
            None
        );
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_executes_allowlisted_node_eval_when_available() {
        if Command::new("node").arg("--version").output().is_err() {
            return;
        }
        let workspace = unique_temp_dir("workspace-node-eval");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["node".to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        let input = br#"{"command":"node","args":["-e","console.log('PALYRA_PROCESS_OK')"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(20_000))
            .expect("allowlisted node eval should run with the sanitized Windows environment");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(
            output.get("stdout").and_then(serde_json::Value::as_str),
            Some("PALYRA_PROCESS_OK\n")
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_applies_explicit_env_without_shell_wrapper() {
        if Command::new("node").arg("--version").output().is_err() {
            return;
        }
        let workspace = unique_temp_dir("workspace-node-env");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["node".to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        let input = br#"{"command":"node","args":["-e","console.log(process.env.PALYRA_E2E_HOME || 'missing')"],"env":{"PALYRA_E2E_HOME":"C:\\Users\\Palo\\AppData\\Local\\Palyra-TestHarness\\home\\S100"}}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(20_000))
            .expect("explicit process env should be applied without shell syntax");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(
            output.get("stdout").and_then(serde_json::Value::as_str),
            Some("C:\\Users\\Palo\\AppData\\Local\\Palyra-TestHarness\\home\\S100\n")
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_timeout_terminates_node_process_tree_when_available() {
        if Command::new("node").arg("--version").output().is_err() {
            return;
        }
        let workspace = unique_temp_dir("workspace-node-timeout-tree");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["node".to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        let script = "const { spawn } = require('child_process'); \
            const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: ['ignore', 'inherit', 'inherit'] }); \
            child.unref(); \
            setInterval(() => {}, 1000);";
        let input = serde_json::to_vec(&serde_json::json!({
            "command": "node",
            "args": ["-e", script],
            "timeout_ms": 200
        }))
        .expect("input should serialize");

        let started_at = std::time::Instant::now();
        let error =
            run_constrained_process(&policy, input.as_slice(), Duration::from_millis(2_000))
                .expect_err("foreground process tree should be terminated at timeout");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::TimedOut);
        assert!(error.message.contains("background=true"), "{}", error.message);
        assert!(
            started_at.elapsed() < Duration::from_secs(5),
            "process tree cleanup should not hang on inherited stdout/stderr handles"
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_executes_allowlisted_command() {
        if Command::new("uname").output().is_err() {
            return;
        }
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
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
    #[cfg(target_os = "macos")]
    fn run_constrained_process_fails_closed_on_macos_without_reliable_resource_quotas() {
        if Command::new("uname").output().is_err() {
            return;
        }
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = br#"{"command":"uname","args":[]}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(3_000))
            .expect_err("macOS process runner must fail closed without reliable resource quotas");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::UnsupportedPlatform);
        assert!(
            error.message.contains("unavailable on macOS"),
            "macOS denial should explain missing fail-closed quota support: {}",
            error.message
        );
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
            "uname",
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
            "uname",
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
            "uname",
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
            "uname",
            &args,
        )
        .expect("workspace-relative path in flag assignment should be allowed");

        let _ = fs::remove_dir_all(&workspace);
    }

    #[test]
    #[cfg(unix)]
    fn validate_argument_workspace_scope_rejects_compact_short_option_absolute_path() {
        let workspace = unique_temp_dir("workspace-compact-short-option-absolute");
        fs::create_dir_all(&workspace).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-C/etc".to_owned()];

        let error = validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "tar",
            &args,
        )
        .expect_err("compact short option with absolute path must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_dir_all(&workspace);
    }

    #[test]
    fn validate_argument_workspace_scope_rejects_compact_short_option_path_traversal() {
        let workspace = unique_temp_dir("workspace-compact-short-option-traversal");
        fs::create_dir_all(&workspace).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-C../..".to_owned()];

        let error = validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "tar",
            &args,
        )
        .expect_err("compact short option with path traversal must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_dir_all(&workspace);
    }

    #[test]
    fn validate_argument_workspace_scope_allows_compact_short_option_workspace_relative_path() {
        let workspace = unique_temp_dir("workspace-compact-short-option-relative");
        let inside_dir = workspace.join("inside");
        fs::create_dir_all(&inside_dir).expect("workspace subdirectory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-Cinside".to_owned()];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "tar",
            &args,
        )
        .expect("compact short option with workspace-relative path should be allowed");

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
            env: Default::default(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
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
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_sanitizes_child_environment() {
        if Command::new("env").output().is_err() {
            return;
        }

        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy_with_allowed_executables(workspace, vec!["env".to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
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
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_enforces_combined_output_quota() {
        if Command::new("yes").arg("--version").output().is_err() {
            return;
        }

        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy_with_allowed_executables(workspace, vec!["yes".to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        policy.max_output_bytes = 16;
        let input = br#"{"command":"yes","args":["0123456789abcdef"]}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("combined stdout+stderr output should hit global quota");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::QuotaExceeded);
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_runtime_failure_redacts_child_stderr_payload() {
        if Command::new("wc").arg("--version").output().is_err() {
            return;
        }

        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy_with_allowed_executables(workspace, vec!["wc".to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let secret_marker = "token=abc123";
        let input = serde_json::to_vec(&serde_json::json!({
            "command": "wc",
            "args": [secret_marker],
        }))
        .expect("input should serialize");

        let error =
            run_constrained_process(&policy, input.as_slice(), Duration::from_millis(1_000))
                .expect_err("missing file should report runtime failure");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::RuntimeFailure);
        assert!(
            error.message.contains("code=")
                && error.message.contains("stderr_bytes=")
                && error.message.contains("stderr_truncated="),
            "runtime failure should remain diagnosable without stderr content: {}",
            error.message
        );
        assert!(
            !error.message.contains(secret_marker),
            "runtime failure message must not leak raw stderr payload"
        );
        assert!(
            error.message.contains("stderr_preview="),
            "runtime failure should include a redacted stderr preview: {}",
            error.message
        );
    }

    #[test]
    fn process_stderr_preview_redacts_secret_like_values() {
        let preview = redacted_process_output_preview(
            b"wc: token=abc123: No such file or directory\nnode failed for https://example.com/token/abc123?api_key=qwerty\nnext",
        )
        .expect("preview should be present");

        assert!(preview.contains("<redacted>"), "{preview}");
        assert!(!preview.contains("abc123"), "{preview}");
        assert!(!preview.contains("qwerty"), "{preview}");
        assert!(preview.contains("node failed"), "{preview}");
    }

    #[test]
    fn process_failure_message_includes_redacted_stdout_and_stderr_previews() {
        let stdout = StreamCapture {
            bytes: b"AssertionError: expected 180 but got 190\naccess_token=stdout-secret".to_vec(),
            truncated: false,
            read_error: None,
        };
        let stderr = StreamCapture {
            bytes: b"stderr token=stderr-secret\n".to_vec(),
            truncated: false,
            read_error: None,
        };

        let message = process_failure_message(1, &stdout, &stderr);

        assert!(message.contains("stdout_bytes="), "{message}");
        assert!(message.contains("stderr_bytes="), "{message}");
        assert!(message.contains("stdout_preview="), "{message}");
        assert!(message.contains("stderr_preview="), "{message}");
        assert!(message.contains("AssertionError"), "{message}");
        assert!(!message.contains("stdout-secret"), "{message}");
        assert!(!message.contains("stderr-secret"), "{message}");
        assert!(message.contains("<redacted>"), "{message}");
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_denies_interpreters_without_explicit_opt_in() {
        if Command::new("bash").arg("--version").output().is_err() {
            return;
        }
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy_with_allowed_executables(workspace, vec!["bash".to_owned()]);
        let input = br#"{"command":"bash","args":["--version"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("interpreter execution must require explicit allow_interpreters opt-in");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("allow_interpreters=true"));
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_denies_interpreter_shell_eval_flags() {
        if Command::new("bash").arg("--version").output().is_err() {
            return;
        }
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace, vec!["bash".to_owned()]);
        policy.allow_interpreters = true;
        let input = br#"{"command":"bash","args":["-c","echo safe"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("interpreter shell-eval flags must be rejected");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"));
    }

    #[test]
    fn interpreter_guardrails_allow_absolute_workspace_script_argument() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let script = fs::canonicalize(workspace_root.join("Cargo.toml"))
            .expect("workspace fixture file should canonicalize");
        let args = vec![script.to_string_lossy().to_string()];

        validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("absolute script path inside workspace should be allowed");
    }

    #[test]
    fn interpreter_guardrails_reject_embedded_absolute_path_substrings() {
        #[cfg(windows)]
        let outside_path = r"C:\Windows\win.ini";
        #[cfg(not(windows))]
        let outside_path = "/etc/passwd";
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![format!("print(open('{outside_path}').read())")];

        let error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "python3",
            args.as_slice(),
        )
        .expect_err("embedded absolute host paths should stay blocked");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("absolute path-like substring"));
    }

    #[test]
    fn interpreter_guardrails_allow_inline_node_code_with_relative_paths_and_newline_escape() {
        let workspace = unique_temp_dir("workspace-node-inline-relative-paths");
        fs::create_dir_all(workspace.join("src").as_path())
            .expect("workspace src directory should be created");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![
            "-e".to_owned(),
            "const fs = require('fs'); const lines = fs.readFileSync('src/reporting.ts', 'utf8').split('\\n'); console.log('reporting.ts line count:', lines.length);".to_owned(),
        ];

        validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("inline node code with relative paths should not be treated as host-absolute");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn interpreter_guardrails_reject_path_list_with_outside_absolute_component() {
        let workspace = unique_temp_dir("workspace-interpreter-path-list-deny");
        let outside = unique_temp_dir("outside-interpreter-path-list-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let separator = if cfg!(windows) { ';' } else { ':' };
        let inside_component = workspace_root.join("missing");
        let args = vec![format!("{}{separator}{}", inside_component.display(), outside.display())];

        let error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "ruby",
            args.as_slice(),
        )
        .expect_err("interpreter path-list args must validate every absolute component");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("absolute path-like substring"));

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn interpreter_guardrails_allow_path_list_inside_workspace() {
        let workspace = unique_temp_dir("workspace-interpreter-path-list-allow");
        let inside_one = workspace.join("lib");
        let inside_two = workspace.join("vendor");
        fs::create_dir_all(inside_one.as_path()).expect("first workspace directory should exist");
        fs::create_dir_all(inside_two.as_path()).expect("second workspace directory should exist");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let separator = if cfg!(windows) { ';' } else { ':' };
        let args = vec![format!("{}{separator}{}", inside_one.display(), inside_two.display())];

        validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "ruby",
            args.as_slice(),
        )
        .expect("path-list args whose absolute components stay inside workspace should be allowed");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn interpreter_guardrails_do_not_treat_non_file_urls_as_path_lists() {
        let workspace = unique_temp_dir("workspace-interpreter-url-arg");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["https://example.test/callback".to_owned()];

        validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "python3",
            args.as_slice(),
        )
        .expect("non-file URL arguments should not be interpreted as filesystem path lists");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_denies_interpreter_embedded_absolute_paths() {
        if Command::new("python3").arg("--version").output().is_err() {
            return;
        }
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace, vec!["python3".to_owned()]);
        policy.allow_interpreters = true;
        let input = br#"{"command":"python3","args":["print(open('/etc/passwd').read())"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("interpreter absolute path substring must be rejected");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("absolute path-like substring"));
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_skips_egress_validation_in_none_mode() {
        if Command::new("echo").arg("ok").output().is_err() {
            return;
        }

        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace, vec!["echo".to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        policy.allowed_egress_hosts = vec!["allowed.example".to_owned()];
        let input = br#"{"command":"echo","args":["https://blocked.example/path"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("none mode should skip argument-level egress validation");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_allows_preflight_mode_without_runtime_network_backend() {
        if Command::new("echo").arg("ok").output().is_err() {
            return;
        }

        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace, vec!["echo".to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        policy.allowed_egress_hosts = vec!["allowed.example".to_owned()];
        let input = br#"{"command":"echo","args":["https://allowed.example/path"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("preflight mode should not require unavailable strict runtime enforcement");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
    }

    #[test]
    #[cfg(unix)]
    fn run_constrained_process_rejects_args_over_count_limit_deterministically() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let args = vec!["a"; 129];
        let input = serde_json::to_vec(&serde_json::json!({ "command": "uname", "args": args }))
            .expect("input JSON should serialize");
        let error =
            run_constrained_process(&policy, input.as_slice(), Duration::from_millis(1_000))
                .expect_err("argv count over limit must be denied deterministically");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("supports at most"));
    }

    #[test]
    fn validate_runtime_egress_enforcement_rejects_tier_b_strict_mode_without_allowlists() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let error = validate_runtime_egress_enforcement(&policy)
            .expect_err("tier-b strict mode must fail closed even when allowlists are empty");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(
            error.message.contains("tier-b strict mode"),
            "error should explain strict tier-b runtime egress enforcement requirement"
        );
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_fails_closed_for_tier_b_strict_mode_even_without_allowlists() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy(workspace);
        let input = br#"{"command":"uname","args":[]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("tier-b strict mode must fail closed even when allowlists are empty");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(
            error.message.contains("tier-b strict mode"),
            "error should explain strict tier-b runtime egress enforcement requirement"
        );
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_fails_closed_without_runtime_egress_enforcement() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.allowed_egress_hosts = vec!["allowed.example".to_owned()];
        let input = br#"{"command":"uname","args":[]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("runner must fail closed when runtime egress enforcement is unavailable");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(
            error.message.contains("runtime egress enforcement is unavailable"),
            "error should explain fail-closed runtime egress requirement"
        );
    }

    #[test]
    #[cfg(unix)]
    fn tier_c_strict_mode_rejects_requested_egress_hosts_as_offline_only() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.tier = SandboxProcessRunnerTier::C;
        policy.allowed_egress_hosts = vec!["allowed.example".to_owned()];
        let input = br#"{"command":"uname","args":["https://allowed.example/path"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("tier-c strict mode must reject outbound requests as offline-only");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(
            error.message.contains("offline-only") && error.message.contains("browser/http tools"),
            "strict tier-c denial should explain offline-only posture and dedicated network tools"
        );
    }

    #[test]
    #[cfg(unix)]
    fn tier_c_strict_mode_rejects_requested_egress_hosts_field_as_offline_only() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.tier = SandboxProcessRunnerTier::C;
        let input =
            br#"{"command":"uname","args":[],"requested_egress_hosts":["api.example.com"]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("tier-c strict mode must reject requested_egress_hosts in offline mode");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(error.message.contains("offline-only"));
    }

    #[test]
    fn process_runner_executor_name_tracks_selected_tier() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        assert_eq!(
            super::process_runner_executor_name(&policy),
            "sandbox_tier_b",
            "tier-b executions should use stable tier-b executor label"
        );
        policy.tier = SandboxProcessRunnerTier::C;
        assert!(
            super::process_runner_executor_name(&policy).starts_with("sandbox_tier_c_"),
            "tier-c executions should expose backend-specific tier-c executor label"
        );
        let mut policy = host_access_policy(std::env::current_dir().expect("cwd should resolve"));
        assert_eq!(
            super::process_runner_executor_name(&policy),
            "host_process",
            "host access executions should expose unsandboxed host executor label"
        );
        policy.tier = SandboxProcessRunnerTier::C;
        assert!(
            super::process_runner_executor_name(&policy).starts_with("sandbox_tier_c_"),
            "tier-c should not be downgraded to host access by wildcard allowlists"
        );
    }

    #[test]
    #[cfg(unix)]
    fn cpu_rlimit_seconds_accounts_for_consumed_process_time() {
        let limit = cpu_rlimit_seconds_from_usage_micros(2_000, 3_250_000);
        assert_eq!(limit, 6);
    }

    #[test]
    #[cfg(all(unix, not(target_os = "macos")))]
    fn run_constrained_process_tier_c_executes_when_backend_binary_is_available() {
        if Command::new("uname").output().is_err() {
            return;
        }
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.tier = SandboxProcessRunnerTier::C;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = br#"{"command":"uname","args":[]}"#;
        let outcome = run_constrained_process(&policy, input, Duration::from_millis(2_000));
        let result = match outcome {
            Ok(result) => result,
            Err(error) => {
                if matches!(error.kind, SandboxProcessRunErrorKind::SpawnFailed)
                    && error.message.contains("requires binary")
                {
                    return;
                }
                panic!("unexpected tier-c execution failure: {}", error.message);
            }
        };
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("tier-c output should parse");
        assert_eq!(output.get("tier").and_then(serde_json::Value::as_str), Some("c"));
        assert!(
            output
                .get("sandbox_backend")
                .and_then(serde_json::Value::as_str)
                .map(|value| !value.is_empty())
                .unwrap_or(false),
            "tier-c output should include backend metadata"
        );
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn tier_c_strict_mode_rejects_host_allowlists_when_backend_cannot_enforce_them() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.tier = SandboxProcessRunnerTier::C;
        policy.allowed_egress_hosts = vec!["allowed.example".to_owned()];
        let input = br#"{"command":"uname","args":[]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("tier-c strict mode must fail closed for unsupported host allowlists");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(
            error.message.contains("offline-only")
                && error.message.contains("cannot enforce host-level egress allowlists"),
            "tier-c strict denial should explain offline-only host-level enforcement limits"
        );
    }

    #[test]
    #[cfg(windows)]
    fn tier_c_strict_mode_fails_closed_without_runtime_network_isolation_backend() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.tier = SandboxProcessRunnerTier::C;
        let input = br#"{"command":"uname","args":[]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("tier-c strict mode must fail closed when backend lacks network isolation");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(
            error.message.contains("cannot enforce runtime network isolation"),
            "denial should explain tier-c runtime network enforcement requirement"
        );
    }

    #[test]
    #[cfg(windows)]
    fn tier_c_preflight_mode_fails_closed_when_backend_is_unavailable() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.tier = SandboxProcessRunnerTier::C;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = br#"{"command":"uname","args":[]}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("tier-c preflight mode must fail closed when backend is unavailable");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::SpawnFailed);
        assert!(
            error.message.contains("is unavailable"),
            "denial should explain that the tier-c backend is unavailable"
        );
    }
}
