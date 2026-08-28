//! Sandboxed execution of `palyra.process.run` tool calls: input validation, tier selection,
//! process spawn, bounded output capture, and kill/timeout handling for foreground and
//! background processes.
//!
//! Three execution modes share one pipeline:
//! - Tier B spawns the child directly with a scrubbed environment, workspace-scoped path
//!   arguments, and Unix rlimit quotas.
//! - Tier C delegates isolation planning to the `palyra-sandbox` backend planners
//!   ([`build_tier_c_command_plan`]) and fails closed when the compiled backend cannot enforce
//!   the requested network isolation.
//! - Host path access is explicit and limited to the workspace plus configured OS roots.
//!
//! Every validation failure here is a deny-by-default security decision. Error message strings
//! are pinned by tests and critical attack-scenario fixtures; do not reword them casually.

#![cfg_attr(not(unix), allow(dead_code, unused_imports))]

use std::{
    collections::{hash_map::DefaultHasher, BTreeMap, BTreeSet, HashMap},
    ffi::{OsStr, OsString},
    fs,
    hash::{Hash, Hasher},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, OnceLock,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[cfg(test)]
use std::collections::HashSet;

#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

#[cfg(windows)]
use std::os::windows::{ffi::OsStringExt, io::AsRawHandle, process::CommandExt};

#[cfg(windows)]
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, GetLastError, ERROR_ACCESS_DENIED, ERROR_INVALID_PARAMETER,
        ERROR_NO_MORE_FILES, FILETIME, HANDLE, INVALID_HANDLE_VALUE,
    },
    System::{
        Diagnostics::ToolHelp::{
            CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
        },
        JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
            JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
            TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        },
        Threading::{
            GetProcessTimes, OpenProcess, OpenThread, QueryFullProcessImageNameW, ResumeThread,
            CREATE_SUSPENDED, PROCESS_QUERY_LIMITED_INFORMATION, THREAD_SUSPEND_RESUME,
        },
    },
};

use palyra_common::{
    default_state_root,
    process_risk::{
        classify_process_run, ProcessRiskClass, ProcessRiskContext, ProcessRiskReport,
        TARGET_HOST_WINDOWS_VS_DOCKER_POSIX_PERMISSIONS,
    },
    process_runner_input::{
        interpreter_args_contain_blocked_eval_flag, parse_process_runner_tool_input,
        process_executable_is_interpreter, BackgroundLifetimeMode, ProcessRunnerToolInput,
        ProcessWatchStream,
    },
    qa_fault_injection::{QaFaultAction, QaFaultDirective, QaFaultRecoveryClass},
    redaction::{redact_auth_error, redact_url_segments_in_text, REDACTED},
    runtime_contracts::{
        CleanupOutcome, CleanupReportV1, CleanupStepDisposition, CleanupStepKind,
        CleanupStepRecord, ProcessLeaseV1, ProcessOwnershipKind, ProcessProvenance,
        ProcessProvenanceDisposition, RuntimeGeneration, RuntimeInstanceId, RuntimeLeaseId,
        RUNTIME_HANDLE_SCHEMA_VERSION,
    },
};
use palyra_safety::{
    redact_text_for_export, SafetyContentKind, SafetyFindingCategory, SafetySourceKind, TrustLabel,
};
use palyra_sandbox::{
    build_tier_c_command_plan, current_backend_capabilities, current_backend_executor,
    current_backend_kind, TierCBackendError, TierCCommandPlan, TierCCommandRequest, TierCPolicy,
};
use palyra_vault::ensure_owner_only_dir;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::warn;

#[cfg(unix)]
use crate::unix_process_supervisor::{
    UnixProcessSupervisorControl, UnixSupervisorLaunchSpec, UnixSupervisorLimits,
};

// Input-shape caps applied before any spawn. They bound attacker-controlled argv/env size and
// the allocations derived from it; raising any of them is a security-review change.
const MAX_COMMAND_LENGTH: usize = 256;
const MAX_ARGS_COUNT: usize = 128;
const MAX_ARG_LENGTH: usize = 4_096;
const MAX_ENV_COUNT: usize = 32;
const MAX_ENV_KEY_LENGTH: usize = 128;
const MAX_ENV_VALUE_LENGTH: usize = 4_096;
const MAX_PREPEND_PATH_COUNT: usize = 16;
const MAX_PREPEND_PATH_LENGTH: usize = 1_024;
const MAX_ENV_PROFILE_ID_LENGTH: usize = 128;
const MAX_WATCH_PATTERNS: usize = 8;
const MAX_WATCH_PATTERN_NAME_LENGTH: usize = 64;
const MAX_WATCH_PATTERN_LENGTH: usize = 256;
const BUILTIN_LIST_MAX_ENTRIES: usize = 512;
const BUILTIN_READ_FILE_MAX_BYTES: usize = 64 * 1024;
const CAPTURE_POLL_INTERVAL_MS: u64 = 5;
const CAPTURE_CHUNK_BYTES: usize = 4 * 1024;
const PROCESS_OUTPUT_PREVIEW_BYTES: usize = 4 * 1024;
const PROCESS_STREAM_INLINE_TEXT_BYTES: usize = 8 * 1024;
const PROCESS_STREAM_HEAD_BYTES: usize = 4 * 1024;
const PROCESS_STREAM_TAIL_BYTES: usize = 4 * 1024;
const PROCESS_PROGRESS_MIN_ELAPSED_MS: u64 = 5_000;
const PROCESS_PROGRESS_INTERVAL_MS: u64 = 2_000;
const PROCESS_PROGRESS_TAIL_BYTES: usize = 1024;
const PROCESS_PROGRESS_NO_OUTPUT_MS: u64 = u64::MAX;
const BACKGROUND_STARTUP_CHECK_MS: u64 = 250;
// Windows process startup and pipe readiness are noticeably slower, so the window for draining
// startup output (port announcements etc.) before returning metadata is longer there.
#[cfg(windows)]
const BACKGROUND_STARTUP_OUTPUT_DRAIN_MS: u64 = 4_000;
#[cfg(not(windows))]
const BACKGROUND_STARTUP_OUTPUT_DRAIN_MS: u64 = 1_000;
const BACKGROUND_POST_OUTPUT_EXIT_CHECK_MS: u64 = 250;
const BACKGROUND_METADATA_RETURN_RESERVE_MS: u64 = 100;
const BACKGROUND_MONITOR_POLL_MS: u64 = 50;
const BACKGROUND_TERMINATION_WAIT_MS: u64 = 1_000;
// A retained Windows Job Object can acknowledge termination before a heavily loaded host updates
// both the job membership and direct-process views. Recovery owns the exact capability, so it can
// wait longer without falling back to a reusable PID or weakening the ordinary cleanup bound.
#[cfg(windows)]
const RETAINED_BACKGROUND_TERMINATION_WAIT_MS: u64 = 5_000;
#[cfg(not(windows))]
const RETAINED_BACKGROUND_TERMINATION_WAIT_MS: u64 = BACKGROUND_TERMINATION_WAIT_MS;
#[cfg(target_os = "macos")]
const MAX_MACOS_BACKGROUND_PROCESS_GROUP_MEMBERS: usize = 4_096;
#[cfg(target_os = "macos")]
const MAX_MACOS_PROCESS_GROUP_SNAPSHOT_ATTEMPTS: usize = 3;
#[cfg(target_os = "macos")]
const MAX_MACOS_ZOMBIE_ANCHOR_SNAPSHOT_ATTEMPTS: usize = 64;
#[cfg(target_os = "macos")]
const MACOS_PROC_PIDINFO_INCLUDE_ZOMBIES: u64 = 1;
const PROCESS_STDIN_INPUT_MAX_BYTES: usize = 8 * 1024;
const PROCESS_STDIN_TOTAL_MAX_BYTES: usize = 64 * 1024;
const PROCESS_STDIN_MAX_EVENTS: usize = 64;
const PROCESS_SEND_KEYS_MAX_ACTIONS: usize = 32;
const PROCESS_SEND_KEYS_MAX_REPEAT: u8 = 16;
const PROCESS_SEND_KEYS_TEXT_MAX_BYTES: usize = 1024;
const PROCESS_TERMINAL_FRAME_TEXT_BYTES: usize = 1024;
const MAX_PROCESS_PORT_HINTS: usize = 16;
const MAX_PROCESS_EXECUTABLE_HASH_BYTES: u64 = 512 * 1024 * 1024;
#[cfg(any(target_os = "linux", target_os = "android"))]
const MAX_LINUX_PROC_STAT_BYTES: usize = 4 * 1024;
#[cfg(target_os = "linux")]
const MAX_LINUX_PROCESS_COUNT: usize = 65_536;
#[cfg(target_os = "linux")]
const MAX_LINUX_PROCESS_GROUP_SNAPSHOT_ATTEMPTS: usize = 3;
#[cfg(windows)]
const MAX_WINDOWS_PROCESS_IMAGE_CHARS: usize = 32_768;
const DEFAULT_FOREGROUND_PROCESS_TIMEOUT_MS: u64 = 30_000;
// Background lifetimes have a floor (short timeouts would kill dev servers mid-verification)
// and a hard ceiling (no background process may outlive operator expectations unsupervised).
const MIN_BACKGROUND_PROCESS_LIFETIME_MS: u64 = 120_000;
const DEFAULT_BACKGROUND_PROCESS_LIFETIME_MS: u64 = 10 * 60_000;
const MAX_BACKGROUND_PROCESS_LIFETIME_MS: u64 = 30 * 60_000;
const PALYRA_CLI_PROFILE_ENV: &str = "PALYRA_CLI_PROFILE";
const PALYRA_CLI_PROFILES_PATH_ENV: &str = "PALYRA_CLI_PROFILES_PATH";
const PALYRA_STATE_ROOT_ENV: &str = "PALYRA_STATE_ROOT";
const PALYRA_OS_FILE_ROOTS_ENV: &str = "PALYRA_OS_FILE_ROOTS";
const NODE_DISABLE_COMPILE_CACHE_ENV: &str = "NODE_DISABLE_COMPILE_CACHE";
// Child environments are rebuilt deny-by-default from these allowlists so daemon secrets
// (admin tokens, provider keys, vault paths) can never leak into spawned processes.
const HOST_ACCESS_SAFE_ENV_KEYS: &[&str] = &["HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TERM"];
const HOST_ACCESS_SAFE_PALYRA_ENV_KEYS: &[&str] = &["PALYRA_E2E_HOME", "PALYRA_E2E_OS_ROOT"];
const CLI_PROFILES_RELATIVE_PATH: &str = "cli/profiles.toml";
const DESKTOP_CONTROL_CENTER_STATE_DIR: &str = "desktop-control-center";
const DESKTOP_RUNTIME_STATE_DIR: &str = "runtime";
const PROCESS_RUNNER_TEMP_RELATIVE_PATH: &[&str] = &["process-runner", "tmp"];
const PROCESS_RUNNER_PYTHON_ENV_RELATIVE_PATH: &[&str] = &["process-runner", "python-env"];
const PROCESS_RUNNER_PYTHON_ENV_CREATE_ATTEMPTS: usize = 4;
const PYTHON_USER_BASE_DIR: &str = "python-userbase";
const PIP_CACHE_DIR: &str = "pip-cache";
// URL path segments following one of these markers (e.g. a path like .../<marker>/<value>) are
// treated as secret material and replaced before any output leaves the runner.
const SENSITIVE_URL_PATH_MARKERS: &[&str] =
    &["token", "secret", "key", "password", "credential", "session"];
#[cfg(windows)]
const WINDOWS_DEFAULT_PATH_EXTENSIONS: &[&str] = &[".com", ".exe", ".bat", ".cmd"];
/// How outbound network access requested by a process run is policed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressEnforcementMode {
    /// No egress validation; also a precondition for host-access mode.
    None,
    /// Hosts referenced by the input are checked against allowlists before spawn, but the
    /// running child is not network-isolated.
    Preflight,
    /// Runtime network isolation must be enforced by a tier-C backend; tier B fails closed.
    Strict,
}

impl EgressEnforcementMode {
    /// Returns the stable lowercase label used in telemetry and tool output.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Preflight => "preflight",
            Self::Strict => "strict",
        }
    }
}

/// Isolation tier for process execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxProcessRunnerTier {
    /// In-process spawn with scrubbed environment, path scoping, and Unix rlimits.
    B,
    /// Spawn through a platform sandbox backend planned by the `palyra-sandbox` crate.
    C,
}

impl SandboxProcessRunnerTier {
    /// Returns the stable lowercase label used in tool output JSON (`"tier"` field).
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::B => "b",
            Self::C => "c",
        }
    }
}

/// Path validation posture for process-runner host fields and runtime arguments.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathAccessMode {
    /// Allow only the workspace and explicitly configured OS roots.
    ApprovedRoots,
    /// Confine process paths to the configured workspace root.
    WorkspaceOnly,
}

impl PathAccessMode {
    /// Returns the stable snake_case config and diagnostics label.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ApprovedRoots => "approved_roots",
            Self::WorkspaceOnly => "workspace_only",
        }
    }
}

/// Operator-configured policy governing what a process run may execute and consume.
///
/// All checks in this module evaluate against one immutable policy snapshot per run; the policy
/// is never derived from tool input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxProcessRunnerPolicy {
    /// Master switch; when false every run fails with [`SandboxProcessRunErrorKind::Disabled`].
    pub enabled: bool,
    /// Requested isolation tier (see [`SandboxProcessRunnerTier`]).
    pub tier: SandboxProcessRunnerTier,
    /// Root directory that scopes working directories and path-like arguments.
    pub workspace_root: PathBuf,
    /// Filesystem path posture for `cwd`, command paths, and process arguments.
    pub path_access_mode: PathAccessMode,
    /// Case-insensitive executable allowlist; a `*` entry allows any executable.
    pub allowed_executables: Vec<String>,
    /// Explicit opt-in required before any denylisted interpreter may run.
    pub allow_interpreters: bool,
    /// Outbound network posture (see [`EgressEnforcementMode`]).
    pub egress_enforcement_mode: EgressEnforcementMode,
    /// Exact hostnames permitted for egress in preflight/strict modes.
    pub allowed_egress_hosts: Vec<String>,
    /// DNS suffixes permitted for egress; matched on label boundaries only.
    pub allowed_dns_suffixes: Vec<String>,
    /// Child CPU-time quota, enforced via `RLIMIT_CPU` on Unix.
    pub cpu_time_limit_ms: u64,
    /// Child address-space quota, enforced via `RLIMIT_AS` on Unix (unsupported on macOS).
    pub memory_limit_bytes: u64,
    /// Combined stdout+stderr capture budget; exceeding it terminates the process.
    pub max_output_bytes: u64,
}

/// Successful process-run result carrying the serialized tool output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxProcessRunSuccess {
    /// JSON object with exit code, redacted stdout/stderr, truncation/redaction flags, and
    /// tier/backend metadata; background runs add process-handle and cleanup metadata.
    pub output_json: Vec<u8>,
}

/// Failed process-run result; the message is already redacted and safe to surface to callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SandboxProcessRunError {
    /// Failure category callers can match on.
    pub kind: SandboxProcessRunErrorKind,
    /// Human-readable detail; string content is pinned by tests and security fixtures.
    pub message: String,
}

/// Failure categories for a process run, ordered roughly by pipeline stage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SandboxProcessRunErrorKind {
    /// The runner is switched off by policy.
    Disabled,
    /// The platform cannot provide fail-closed quota enforcement (currently macOS).
    #[cfg_attr(all(unix, not(target_os = "macos")), allow(dead_code))]
    UnsupportedPlatform,
    /// The run was cancelled by request and the process tree was terminated.
    Cancelled,
    /// The tool input failed shape or content validation.
    InvalidInput,
    /// A path, executable, or interpreter check denied the run (deny-by-default scope rules).
    WorkspaceScopeDenied,
    /// An egress host check or missing runtime network enforcement denied the run.
    EgressDenied,
    /// The process exceeded its output budget and was terminated.
    QuotaExceeded,
    /// The process exceeded its timeout or background lifetime and was terminated.
    TimedOut,
    /// The OS-level spawn failed or the tier-C backend was unavailable.
    SpawnFailed,
    /// The process ran but exited unsuccessfully, or capture/serialization failed.
    RuntimeFailure,
}

/// Stable model-visible failure classes for `palyra.process.run`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProcessFailureClass {
    Disabled,
    InvalidInput,
    CommandNotFound,
    PermissionDenied,
    TimedOut,
    Killed,
    NonzeroExit,
    OutputLimit,
    SandboxDenied,
    EgressDenied,
    UnsupportedPlatform,
    RuntimeFailure,
}

impl ProcessFailureClass {
    #[must_use]
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::InvalidInput => "invalid_input",
            Self::CommandNotFound => "command_not_found",
            Self::PermissionDenied => "permission_denied",
            Self::TimedOut => "timed_out",
            Self::Killed => "killed",
            Self::NonzeroExit => "nonzero_exit",
            Self::OutputLimit => "output_limit",
            Self::SandboxDenied => "sandbox_denied",
            Self::EgressDenied => "egress_denied",
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::RuntimeFailure => "runtime_failure",
        }
    }

    const fn reason_code(self) -> &'static str {
        match self {
            Self::Disabled => "process.failure.disabled",
            Self::InvalidInput => "process.failure.invalid_input",
            Self::CommandNotFound => "process.failure.command_not_found",
            Self::PermissionDenied => "process.failure.permission_denied",
            Self::TimedOut => "process.failure.timed_out",
            Self::Killed => "process.failure.killed",
            Self::NonzeroExit => "process.failure.nonzero_exit",
            Self::OutputLimit => "process.failure.output_limit",
            Self::SandboxDenied => "process.failure.sandbox_denied",
            Self::EgressDenied => "process.failure.egress_denied",
            Self::UnsupportedPlatform => "process.failure.unsupported_platform",
            Self::RuntimeFailure => "process.failure.runtime_failure",
        }
    }

    const fn summary(self) -> &'static str {
        match self {
            Self::Disabled => "process runner is disabled by runtime policy",
            Self::InvalidInput => "process request failed schema or safety validation",
            Self::CommandNotFound => "the executable was not found on the daemon process path",
            Self::PermissionDenied => {
                "the operating system denied permission to start the executable"
            }
            Self::TimedOut => "the process exceeded its configured timeout and was terminated",
            Self::Killed => {
                "the process was terminated by cancellation or signal before successful exit"
            }
            Self::NonzeroExit => "the process ran and returned a non-zero exit code",
            Self::OutputLimit => "the process exceeded its bounded stdout/stderr capture budget",
            Self::SandboxDenied => "sandbox policy denied the process request before execution",
            Self::EgressDenied => "egress policy denied the process request before execution",
            Self::UnsupportedPlatform => {
                "the selected platform cannot provide the required enforcement"
            }
            Self::RuntimeFailure => "the process runner failed outside a more specific class",
        }
    }

    const fn repair_hint(self) -> &'static str {
        match self {
            Self::Disabled => {
                "enable tool_call.process_runner and allowlist palyra.process.run before retrying"
            }
            Self::InvalidInput => {
                "fix the process.run JSON shape, path, lifetime, or interaction flags before retrying"
            }
            Self::CommandNotFound => {
                "install the executable on the daemon PATH, use an allowed exact path, or provide a trusted prepend_path"
            }
            Self::PermissionDenied => {
                "fix file permissions or choose an executable the daemon user can run"
            }
            Self::TimedOut => {
                "for tests/builds, fix the hang or increase timeout_ms; for servers, use background=true and stop the returned handle"
            }
            Self::Killed => {
                "inspect cancellation/signal cause and rerun only if the process tree can complete within the run lifecycle"
            }
            Self::NonzeroExit => {
                "inspect the bounded redacted stdout_preview and stderr_preview, fix the command or inputs, then rerun"
            }
            Self::OutputLimit => {
                "rerun with narrower output, redirect verbose logs to a file, or raise max_output_bytes by policy"
            }
            Self::SandboxDenied => {
                "keep paths and executables inside the configured workspace/policy or adjust the policy explicitly"
            }
            Self::EgressDenied => {
                "declare allowed requested_egress_hosts or use an offline command"
            }
            Self::UnsupportedPlatform => {
                "select a supported runner/backend or change policy to a mode the platform can enforce"
            }
            Self::RuntimeFailure => {
                "inspect the error and retry only after the underlying runtime issue is fixed"
            }
        }
    }

    const fn cleanup_status(self) -> &'static str {
        match self {
            Self::TimedOut | Self::Killed | Self::OutputLimit => "process_tree_terminated",
            Self::NonzeroExit | Self::RuntimeFailure => "process_reaped_or_not_started",
            _ => "process_not_started",
        }
    }
}

impl SandboxProcessRunErrorKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::UnsupportedPlatform => "unsupported_platform",
            Self::Cancelled => "cancelled",
            Self::InvalidInput => "invalid_input",
            Self::WorkspaceScopeDenied => "workspace_scope_denied",
            Self::EgressDenied => "egress_denied",
            Self::QuotaExceeded => "quota_exceeded",
            Self::TimedOut => "timed_out",
            Self::SpawnFailed => "spawn_failed",
            Self::RuntimeFailure => "runtime_failure",
        }
    }
}

#[must_use]
pub(crate) fn process_failure_class(error: &SandboxProcessRunError) -> ProcessFailureClass {
    if let Some(class) = process_failure_class_from_message(error.message.as_str()) {
        return class;
    }
    match error.kind {
        SandboxProcessRunErrorKind::Disabled => ProcessFailureClass::Disabled,
        SandboxProcessRunErrorKind::UnsupportedPlatform => ProcessFailureClass::UnsupportedPlatform,
        SandboxProcessRunErrorKind::Cancelled => ProcessFailureClass::Killed,
        SandboxProcessRunErrorKind::InvalidInput => ProcessFailureClass::InvalidInput,
        SandboxProcessRunErrorKind::WorkspaceScopeDenied => ProcessFailureClass::SandboxDenied,
        SandboxProcessRunErrorKind::EgressDenied => ProcessFailureClass::EgressDenied,
        SandboxProcessRunErrorKind::QuotaExceeded => ProcessFailureClass::OutputLimit,
        SandboxProcessRunErrorKind::TimedOut => ProcessFailureClass::TimedOut,
        SandboxProcessRunErrorKind::SpawnFailed => classify_spawn_failure_message(&error.message),
        SandboxProcessRunErrorKind::RuntimeFailure => ProcessFailureClass::RuntimeFailure,
    }
}

#[must_use]
pub(crate) fn process_failure_output_json(
    error: &SandboxProcessRunError,
    executor: &str,
    sandbox_enforcement: &str,
) -> Vec<u8> {
    let failure_class = process_failure_class(error);
    let payload = json!({
        "success": false,
        "schema_version": 2,
        "tool": "palyra.process.run",
        "failure_class": failure_class.as_str(),
        "failure_reason_code": failure_class.reason_code(),
        "error": error.message.as_str(),
        "recovery_hint": failure_class.repair_hint(),
        "timed_out": matches!(failure_class, ProcessFailureClass::TimedOut),
        "executor": executor,
        "sandbox_enforcement": sandbox_enforcement,
        "model_summary": {
            "failure_class": failure_class.as_str(),
            "summary": failure_class.summary(),
            "repair_hint": failure_class.repair_hint(),
        },
        "audit_detail": {
            "error_kind": error.kind.as_str(),
            "message": error.message.as_str(),
        },
        "cleanup_policy": {
            "strategy": "local_sandbox_process_lifecycle",
            "status": failure_class.cleanup_status(),
        },
    });
    serde_json::to_vec(&payload)
        .unwrap_or_else(|_| br#"{"success":false,"failure_class":"runtime_failure"}"#.to_vec())
}

fn process_failure_class_from_message(message: &str) -> Option<ProcessFailureClass> {
    let class = message
        .split(|character: char| character.is_whitespace() || character == ',' || character == ')')
        .find_map(|token| {
            token
                .trim_matches(|character| matches!(character, '(' | ':' | ';'))
                .strip_prefix("failure_class=")
        })?;
    process_failure_class_from_str(class)
}

fn process_failure_class_from_str(value: &str) -> Option<ProcessFailureClass> {
    match value {
        "disabled" => Some(ProcessFailureClass::Disabled),
        "invalid_input" => Some(ProcessFailureClass::InvalidInput),
        "command_not_found" => Some(ProcessFailureClass::CommandNotFound),
        "permission_denied" => Some(ProcessFailureClass::PermissionDenied),
        "timed_out" => Some(ProcessFailureClass::TimedOut),
        "killed" => Some(ProcessFailureClass::Killed),
        "nonzero_exit" => Some(ProcessFailureClass::NonzeroExit),
        "output_limit" => Some(ProcessFailureClass::OutputLimit),
        "sandbox_denied" => Some(ProcessFailureClass::SandboxDenied),
        "egress_denied" => Some(ProcessFailureClass::EgressDenied),
        "unsupported_platform" => Some(ProcessFailureClass::UnsupportedPlatform),
        "runtime_failure" => Some(ProcessFailureClass::RuntimeFailure),
        _ => None,
    }
}

fn process_spawn_failure_class(error: &io::Error) -> ProcessFailureClass {
    match error.kind() {
        io::ErrorKind::NotFound => ProcessFailureClass::CommandNotFound,
        io::ErrorKind::PermissionDenied => ProcessFailureClass::PermissionDenied,
        _ => ProcessFailureClass::RuntimeFailure,
    }
}

fn classify_spawn_failure_message(message: &str) -> ProcessFailureClass {
    let lower = message.to_ascii_lowercase();
    if lower.contains("not found") || lower.contains("no such file") {
        ProcessFailureClass::CommandNotFound
    } else if lower.contains("permission denied") || lower.contains("access is denied") {
        ProcessFailureClass::PermissionDenied
    } else {
        ProcessFailureClass::RuntimeFailure
    }
}

fn process_exit_failure_class(status: ExitStatus) -> ProcessFailureClass {
    if status.code().is_none() {
        return ProcessFailureClass::Killed;
    }
    ProcessFailureClass::NonzeroExit
}

/// Returns the stable executor label for telemetry: `host_process` for host path access, the
/// backend-specific `sandbox_tier_c_*` name for tier C, and `sandbox_tier_b` otherwise.
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

/// Reports whether the policy selects host path access instead of workspace-only scoping.
#[must_use]
pub fn process_runner_allows_host_access(policy: &SandboxProcessRunnerPolicy) -> bool {
    matches!(policy.tier, SandboxProcessRunnerTier::B)
        && matches!(
            process_runner_effective_path_access_mode(policy),
            PathAccessMode::ApprovedRoots
        )
}

/// Returns whether an exact host-owned managed command may reuse the current
/// process-tool policy without weakening its sandbox posture.
///
/// The managed coding supervisor executes directly on the host, so this
/// adapter is available only when the existing process policy already grants
/// host access and independently approves the requested executable (including
/// the interpreter opt-in).
#[must_use]
pub(crate) fn process_runner_permits_managed_command(
    policy: &SandboxProcessRunnerPolicy,
    command: &str,
) -> bool {
    process_runner_allows_host_access(policy)
        && validate_allowed_executable(policy, command).is_ok()
}

fn validate_supported_target_runtime(
    process_risk: &ProcessRiskReport,
) -> Result<(), SandboxProcessRunError> {
    let Some(finding) = process_risk.findings.iter().find(|finding| {
        finding.risk_class == ProcessRiskClass::TargetRuntimeMismatch
            && finding.target.as_deref() == Some(TARGET_HOST_WINDOWS_VS_DOCKER_POSIX_PERMISSIONS)
    }) else {
        return Ok(());
    };

    if !cfg!(windows) {
        return Ok(());
    }

    let target_runtime = process_risk
        .target_runtime
        .as_ref()
        .map(|runtime| runtime.kind.as_str())
        .unwrap_or("unknown");
    let safer_default = finding
        .safer_default
        .as_deref()
        .unwrap_or("run the command inside the target runtime or stop before mutating host files");
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "target runtime unsupported: error_code=target_runtime_unsupported \
target_runtime={target_runtime} host_runtime=windows target={} message=\"{}\" \
safer_default=\"{}\"",
            TARGET_HOST_WINDOWS_VS_DOCKER_POSIX_PERMISSIONS, finding.message, safer_default
        ),
    })
}

#[must_use]
pub fn process_runner_sandbox_enforcement_label(policy: &SandboxProcessRunnerPolicy) -> String {
    if process_runner_allows_host_access(policy) {
        return process_runner_effective_path_access_mode(policy).as_str().to_owned();
    }
    policy.egress_enforcement_mode.as_str().to_owned()
}

pub(crate) fn process_runner_effective_path_access_mode(
    policy: &SandboxProcessRunnerPolicy,
) -> PathAccessMode {
    if matches!(policy.tier, SandboxProcessRunnerTier::C) {
        return PathAccessMode::WorkspaceOnly;
    }
    policy.path_access_mode
}

fn process_runner_accepts_host_path_fields(policy: &SandboxProcessRunnerPolicy) -> bool {
    !matches!(process_runner_effective_path_access_mode(policy), PathAccessMode::WorkspaceOnly)
}

/// Returns whether a valid process proposal describes a destructive filesystem operation.
///
/// Classification uses the same workspace and cwd resolution as execution. Invalid proposals
/// return `false` because the process runner will reject them before spawning.
pub(crate) fn process_runner_input_requires_user_approval(
    policy: &SandboxProcessRunnerPolicy,
    input_json: &[u8],
) -> bool {
    let Ok(input) = parse_process_runner_input(input_json) else {
        return false;
    };
    let Ok(workspace_root) = canonical_workspace_root(policy.workspace_root.as_path()) else {
        return false;
    };
    let path_access_mode = process_runner_effective_path_access_mode(policy);
    let host_access_roots = process_runner_accepts_host_path_fields(policy).then(host_access_roots);
    let host_access_path_env = process_runner_accepts_host_path_fields(policy)
        .then(|| host_access_path_env_for_input(&input));
    let working_directory = match path_access_mode {
        PathAccessMode::ApprovedRoots => resolve_host_working_directory_with_roots(
            workspace_root.as_path(),
            input.cwd.as_deref(),
            host_access_roots.as_ref().expect("host roots should be initialized").as_slice(),
            host_access_path_env.as_ref().expect("host path env should be initialized"),
        ),
        PathAccessMode::WorkspaceOnly => {
            resolve_working_directory(workspace_root.as_path(), input.cwd.as_deref())
        }
    };
    let Ok(working_directory) = working_directory else {
        return false;
    };
    classify_process_run(
        &input,
        ProcessRiskContext {
            workspace_root: Some(workspace_root.as_path()),
            resolved_cwd: Some(working_directory.as_path()),
        },
    )
    .requires_user_approval
}

type ProcessRunnerInput = ProcessRunnerToolInput;

/// Callback used by run-stream execution to publish foreground process progress.
pub type ProcessProgressSink = Arc<dyn Fn(ProcessProgressEvent) + Send + Sync + 'static>;

/// Exact durable-registration input captured before a local background process is acknowledged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BackgroundProcessRegistrationRequest {
    pub(crate) pid: u32,
    pub(crate) provenance: ProcessProvenance,
    pub(crate) lifetime_ms: u64,
    pub(crate) lifetime_mode: BackgroundLifetimeMode,
}

/// Local-only callback that must commit process ownership before launch is acknowledged.
pub(crate) type BackgroundProcessRegistrationFence = Arc<
    dyn Fn(BackgroundProcessRegistrationRequest) -> Result<(), SandboxProcessRunError>
        + Send
        + Sync
        + 'static,
>;

/// Bounded foreground-process progress snapshot emitted before the process exits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessProgressEvent {
    pub pid: u32,
    pub elapsed_ms: u64,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
    pub stdout_tail: String,
    pub stderr_tail: String,
    pub last_output_at_ms: Option<u64>,
}

#[derive(Debug)]
struct ProcessExecutionCapture {
    exit_status: ExitStatus,
    stdout: StreamCapture,
    stderr: StreamCapture,
    cancelled: bool,
    timed_out: bool,
    quota_exceeded: bool,
    duration_ms: u64,
}

struct ManagedChildGuard {
    child: Option<Child>,
    exit_status: Option<ExitStatus>,
    termination_requested: bool,
    termination_strategy: ManagedChildTerminationStrategy,
    #[cfg(test)]
    termination_probe: Option<Arc<AtomicUsize>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedChildTerminationStrategy {
    OwnedProcessTree,
    ReapOnly,
}

impl ManagedChildGuard {
    fn new(child: Child) -> Self {
        Self::with_termination_strategy(child, ManagedChildTerminationStrategy::OwnedProcessTree)
    }

    #[cfg(unix)]
    fn new_reap_only(child: Child) -> Self {
        Self::with_termination_strategy(child, ManagedChildTerminationStrategy::ReapOnly)
    }

    fn with_termination_strategy(
        child: Child,
        termination_strategy: ManagedChildTerminationStrategy,
    ) -> Self {
        Self {
            child: Some(child),
            exit_status: None,
            termination_requested: false,
            termination_strategy,
            #[cfg(test)]
            termination_probe: None,
        }
    }

    #[cfg(test)]
    fn with_termination_probe(child: Child, termination_probe: Arc<AtomicUsize>) -> Self {
        Self {
            child: Some(child),
            exit_status: None,
            termination_requested: false,
            termination_strategy: ManagedChildTerminationStrategy::OwnedProcessTree,
            termination_probe: Some(termination_probe),
        }
    }

    fn id(&self) -> u32 {
        self.child.as_ref().expect("managed child guard must own a child").id()
    }

    #[cfg(windows)]
    fn child(&self) -> &Child {
        self.child.as_ref().expect("managed child guard must own a child")
    }

    fn child_mut(&mut self) -> &mut Child {
        self.child.as_mut().expect("managed child guard must own a child")
    }

    fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        if self.exit_status.is_some() {
            return Ok(self.exit_status);
        }
        let status = self.child_mut().try_wait()?;
        if let Some(status) = status {
            self.exit_status = Some(status);
        }
        Ok(status)
    }

    fn request_termination(&mut self) -> io::Result<()> {
        if self.try_wait()?.is_some() || self.termination_requested {
            return Ok(());
        }
        self.force_termination()
    }

    fn force_termination(&mut self) -> io::Result<()> {
        if self.termination_requested {
            return Ok(());
        }
        if self.termination_strategy == ManagedChildTerminationStrategy::ReapOnly {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "reap-only child requires its exact external cleanup authority",
            ));
        }
        #[cfg(test)]
        if let Some(probe) = self.termination_probe.as_ref() {
            probe.fetch_add(1, Ordering::SeqCst);
        }
        terminate_child_process_tree(self.child_mut())?;
        self.termination_requested = true;
        Ok(())
    }

    fn note_owned_tree_termination_requested(&mut self) {
        self.termination_requested = true;
    }

    fn wait_for_exit(&mut self, max_wait: Duration) -> io::Result<Option<ExitStatus>> {
        let started_at = Instant::now();
        loop {
            if let Some(status) = self.try_wait()? {
                return Ok(Some(status));
            }
            if started_at.elapsed() >= max_wait {
                return Ok(None);
            }
            thread::sleep(Duration::from_millis(CAPTURE_POLL_INTERVAL_MS));
        }
    }

    fn terminate_and_reap(&mut self, max_wait: Duration) -> io::Result<Option<ExitStatus>> {
        if let Some(status) = self.try_wait()? {
            return Ok(Some(status));
        }
        self.request_termination()?;
        self.wait_for_exit(max_wait)
    }
}

impl Drop for ManagedChildGuard {
    fn drop(&mut self) {
        if self.child.is_none() || self.exit_status.is_some() {
            return;
        }
        if self.termination_strategy == ManagedChildTerminationStrategy::ReapOnly {
            match self.wait_for_exit(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS)) {
                Ok(Some(_)) => {}
                Ok(None) => warn!(
                    pid = self.id(),
                    "reap-only managed child remained live after exact authority was released"
                ),
                Err(error) => warn!(
                    error = ?error,
                    pid = self.id(),
                    "reap-only managed child reap verification failed"
                ),
            }
            return;
        }
        match self.terminate_and_reap(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS)) {
            Ok(Some(_)) => {}
            Ok(None) => warn!(
                pid = self.id(),
                "managed child did not exit within the bounded drop cleanup window"
            ),
            Err(error) => {
                warn!(
                    error = ?error,
                    pid = self.id(),
                    "managed child drop cleanup wait failed; forcing one bounded best-effort termination"
                );
                if let Err(termination_error) = self.force_termination() {
                    warn!(
                        error = ?termination_error,
                        pid = self.id(),
                        "managed child drop termination failed"
                    );
                } else {
                    match self.wait_for_exit(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS))
                    {
                        Ok(Some(_)) => {}
                        Ok(None) => warn!(
                            pid = self.id(),
                            "managed child remained live after drop termination"
                        ),
                        Err(wait_error) => warn!(
                            error = ?wait_error,
                            pid = self.id(),
                            "managed child drop reap verification failed"
                        ),
                    }
                }
            }
        }
    }
}

/// Exact launch plan for a long-lived stdio runtime owned by the daemon.
#[derive(Debug, Clone)]
pub(crate) struct ManagedStdioProcessConfig {
    pub(crate) executable: PathBuf,
    pub(crate) args: Vec<String>,
    pub(crate) cwd: PathBuf,
    pub(crate) env: BTreeMap<String, String>,
    pub(crate) generation: u64,
    pub(crate) lease_duration: Duration,
}

/// Process-backed stdio runtime retaining the platform ownership anchor.
pub(crate) struct ManagedStdioProcess {
    child: ManagedChildGuard,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
    lease: ProcessLeaseV1,
}

impl ManagedStdioProcess {
    /// Returns the exact process lease captured before the runtime is acknowledged.
    #[must_use]
    pub(crate) const fn lease(&self) -> &ProcessLeaseV1 {
        &self.lease
    }

    /// Transfers the child's stdin to the runtime transport actor.
    pub(crate) fn take_stdin(&mut self) -> Result<ChildStdin, SandboxProcessRunError> {
        self.stdin.take().ok_or_else(|| managed_stdio_error("stdin was already transferred"))
    }

    /// Transfers the child's stdout to the bounded frame reader.
    pub(crate) fn take_stdout(&mut self) -> Result<ChildStdout, SandboxProcessRunError> {
        self.stdout.take().ok_or_else(|| managed_stdio_error("stdout was already transferred"))
    }

    /// Transfers the child's stderr to the bounded diagnostic reader.
    pub(crate) fn take_stderr(&mut self) -> Result<ChildStderr, SandboxProcessRunError> {
        self.stderr.take().ok_or_else(|| managed_stdio_error("stderr was already transferred"))
    }

    /// Returns a terminal status when the direct process has exited.
    pub(crate) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    /// Closes I/O, terminates the owned process tree, and returns structured evidence.
    pub(crate) fn cleanup(mut self, graceful_stop_attempted: bool) -> CleanupReportV1 {
        let completed_at_unix_ms = unix_time_ms();
        self.stdin.take();
        self.stdout.take();
        self.stderr.take();
        let already_exited = self.child.try_wait().ok().flatten().is_some();
        let cleanup_result = if already_exited {
            Ok(Some(self.child.exit_status.expect("observed exit status must be retained")))
        } else {
            self.child.terminate_and_reap(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS))
        };
        #[cfg(windows)]
        remove_windows_background_job(self.lease.pid);
        let verified_absent = self.child.try_wait().ok().flatten().is_some()
            || cleanup_result.as_ref().is_ok_and(Option::is_some);
        let cleanup_failed = cleanup_result.is_err() || !verified_absent;
        let mut steps = vec![
            CleanupStepRecord {
                ordinal: 0,
                step: CleanupStepKind::GracefulStop,
                disposition: if graceful_stop_attempted {
                    CleanupStepDisposition::Completed
                } else {
                    CleanupStepDisposition::SkippedNotRequired
                },
                reason_code: if graceful_stop_attempted {
                    "runtime.cleanup.graceful_stop_requested"
                } else {
                    "runtime.cleanup.graceful_stop_not_required"
                }
                .to_owned(),
                evidence_sha256: None,
                completed_at_unix_ms,
            },
            CleanupStepRecord {
                ordinal: 1,
                step: CleanupStepKind::CloseIo,
                disposition: CleanupStepDisposition::Completed,
                reason_code: "runtime.cleanup.io_closed".to_owned(),
                evidence_sha256: None,
                completed_at_unix_ms,
            },
            CleanupStepRecord {
                ordinal: 2,
                step: CleanupStepKind::KillTree,
                disposition: if already_exited {
                    CleanupStepDisposition::SkippedNotRequired
                } else if cleanup_result.is_ok() {
                    CleanupStepDisposition::Completed
                } else {
                    CleanupStepDisposition::Failed
                },
                reason_code: if already_exited {
                    "runtime.cleanup.process_already_exited"
                } else if cleanup_result.is_ok() {
                    "runtime.cleanup.process_tree_terminated"
                } else {
                    "runtime.cleanup.process_tree_termination_failed"
                }
                .to_owned(),
                evidence_sha256: None,
                completed_at_unix_ms,
            },
            CleanupStepRecord {
                ordinal: 3,
                step: CleanupStepKind::ReleaseLease,
                disposition: CleanupStepDisposition::Completed,
                reason_code: "runtime.cleanup.lease_released".to_owned(),
                evidence_sha256: None,
                completed_at_unix_ms,
            },
            CleanupStepRecord {
                ordinal: 4,
                step: CleanupStepKind::VerifyAbsence,
                disposition: if verified_absent {
                    CleanupStepDisposition::Completed
                } else {
                    CleanupStepDisposition::Failed
                },
                reason_code: if verified_absent {
                    "runtime.cleanup.absence_verified"
                } else {
                    "runtime.cleanup.absence_unverified"
                }
                .to_owned(),
                evidence_sha256: None,
                completed_at_unix_ms,
            },
        ];
        for (ordinal, step) in steps.iter_mut().enumerate() {
            step.ordinal = u32::try_from(ordinal).unwrap_or(u32::MAX);
        }
        CleanupReportV1 {
            schema_version: RUNTIME_HANDLE_SCHEMA_VERSION,
            report_id: format!("cleanup_{}", ulid::Ulid::generate()),
            instance_id: self.lease.instance_id.clone(),
            lease_id: Some(self.lease.lease_id.clone()),
            outcome: if cleanup_failed {
                CleanupOutcome::Partial
            } else {
                CleanupOutcome::Completed
            },
            steps,
            reason_code: if cleanup_failed {
                "runtime.cleanup.partial"
            } else {
                "runtime.cleanup.completed"
            }
            .to_owned(),
            completed_at_unix_ms,
        }
    }
}

/// Spawns a stdio runtime under the same process-tree ownership primitive as sandbox children.
///
/// # Errors
/// Returns [`SandboxProcessRunError`] when the launch plan, process ownership,
/// provenance capture, or stdio setup cannot be established before acknowledgement.
pub(crate) fn spawn_managed_stdio_process(
    config: &ManagedStdioProcessConfig,
) -> Result<ManagedStdioProcess, SandboxProcessRunError> {
    validate_managed_stdio_process_config(config)?;
    let mut command = Command::new(config.executable.as_path());
    command
        .args(config.args.iter())
        .current_dir(config.cwd.as_path())
        .env_clear()
        .envs(config.env.iter());
    spawn_prepared_managed_stdio_process(config, command, config.executable.as_path())
}

/// Validates and launches a persistent stdio service through the same sandbox
/// policy, path, interpreter, egress, and quota gates as `palyra.process.run`.
///
/// Durable runtime-handle registration remains the caller's responsibility and
/// must complete before the returned process is acknowledged to an actor.
///
/// # Errors
/// Returns [`SandboxProcessRunError`] before spawn when any configured sandbox
/// invariant cannot be enforced, with no direct-host fallback.
pub(crate) fn spawn_sandboxed_managed_stdio_process(
    policy: &SandboxProcessRunnerPolicy,
    configured_command: &str,
    config: &ManagedStdioProcessConfig,
) -> Result<ManagedStdioProcess, SandboxProcessRunError> {
    validate_managed_stdio_process_config(config)?;
    if !policy.enabled {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::Disabled,
            message: "sandbox process runner is disabled by runtime policy".to_owned(),
        });
    }
    if normalize_process_executable_token(configured_command)
        != normalize_process_executable_token(config.executable.to_string_lossy().as_ref())
    {
        return Err(managed_stdio_error(
            "configured command does not match its trusted executable",
        ));
    }
    let mut input = ProcessRunnerToolInput {
        command: configured_command.to_owned(),
        args: config.args.clone(),
        cwd: Some(config.cwd.to_string_lossy().into_owned()),
        env: config.env.clone(),
        prepend_path: Vec::new(),
        requested_egress_hosts: Vec::new(),
        timeout_ms: None,
        background: true,
        notify_on_complete: false,
        watch_patterns: Vec::new(),
        interactive: false,
        stdin: true,
        pty: false,
        port_hints: Vec::new(),
        lifetime_mode: BackgroundLifetimeMode::RunOwned,
        keep_running_after_run: false,
        env_profile_id: None,
        elevated_intent: false,
        facade_mapping: None,
    };
    validate_input_shape(&input)?;
    validate_background_lifetime_mode(&input)?;
    validate_allowed_executable(policy, input.command.as_str())?;
    validate_no_embedded_command_line_arg(&input)?;
    validate_cmd_invocation_shape(input.command.as_str(), input.args.as_slice())?;
    validate_process_termination_scope(input.command.as_str(), input.args.as_slice())?;

    let path_access_mode = process_runner_effective_path_access_mode(policy);
    let workspace_root = canonical_workspace_root(policy.workspace_root.as_path())?;
    let host_access_roots = process_runner_accepts_host_path_fields(policy).then(host_access_roots);
    let host_access_path_env = process_runner_accepts_host_path_fields(policy)
        .then(|| host_access_path_env_for_input(&input));
    let working_directory = match path_access_mode {
        PathAccessMode::ApprovedRoots => resolve_host_working_directory_with_roots(
            workspace_root.as_path(),
            input.cwd.as_deref(),
            host_access_roots.as_ref().expect("host roots should be initialized").as_slice(),
            host_access_path_env.as_ref().expect("host path env should be initialized"),
        )?,
        PathAccessMode::WorkspaceOnly => {
            resolve_working_directory(workspace_root.as_path(), input.cwd.as_deref())?
        }
    };
    let process_risk = classify_process_run(
        &input,
        ProcessRiskContext {
            workspace_root: Some(workspace_root.as_path()),
            resolved_cwd: Some(working_directory.as_path()),
        },
    );
    validate_supported_target_runtime(&process_risk)?;
    match path_access_mode {
        PathAccessMode::ApprovedRoots => {
            let roots = host_access_roots.as_ref().expect("host roots should be initialized");
            let path_env =
                host_access_path_env.as_ref().expect("host path env should be initialized");
            input.args = rewrite_host_access_process_args(
                input.args.as_slice(),
                workspace_root.as_path(),
                path_env,
            )?;
            validate_host_command_path_scope_with_roots(
                workspace_root.as_path(),
                working_directory.as_path(),
                input.command.as_str(),
                roots.as_slice(),
            )?;
            validate_host_interpreter_argument_guardrails_with_roots(
                workspace_root.as_path(),
                working_directory.as_path(),
                input.command.as_str(),
                input.args.as_slice(),
                roots.as_slice(),
            )?;
            validate_host_argument_scope_with_roots(
                workspace_root.as_path(),
                working_directory.as_path(),
                input.command.as_str(),
                input.args.as_slice(),
                roots.as_slice(),
            )?;
        }
        PathAccessMode::WorkspaceOnly => {
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
    }
    let requested_hosts = if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::None) {
        validate_requested_egress_hosts_require_enforcement(&input)?;
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
    if matches!(path_access_mode, PathAccessMode::WorkspaceOnly) {
        validate_platform_resource_quota_support(policy)?;
    }
    if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::Strict) {
        validate_runtime_egress_enforcement(policy)?;
    }

    let command = build_process_command(
        policy,
        &input,
        workspace_root.as_path(),
        working_directory.as_path(),
    )?;
    let provenance_executable =
        resolve_prepared_managed_stdio_program(&command, working_directory.as_path())?;
    spawn_prepared_managed_stdio_process(config, command, provenance_executable.as_path())
}

fn resolve_prepared_managed_stdio_program(
    command: &Command,
    cwd: &Path,
) -> Result<PathBuf, SandboxProcessRunError> {
    let configured = PathBuf::from(command.get_program());
    if configured.is_absolute() {
        return configured.canonicalize().map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::SpawnFailed,
            message: format!("failed to resolve sandbox runtime executable: {error}"),
        });
    }
    let raw = configured
        .to_str()
        .ok_or_else(|| managed_stdio_error("sandbox runtime executable is not valid UTF-8"))?;
    resolve_tier_b_process_program(raw, cwd, OsStr::new(sandbox_process_path()), true, false)
}

fn validate_managed_stdio_process_config(
    config: &ManagedStdioProcessConfig,
) -> Result<(), SandboxProcessRunError> {
    if !config.executable.is_absolute()
        || !config.executable.is_file()
        || !config.cwd.is_absolute()
        || !config.cwd.is_dir()
        || config.args.len() > MAX_ARGS_COUNT
        || config.args.iter().any(|arg| arg.len() > MAX_ARG_LENGTH)
        || config.env.len() > MAX_ENV_COUNT
        || config.generation == 0
        || config.lease_duration.is_zero()
    {
        return Err(managed_stdio_error("launch plan violates bounded process policy"));
    }
    Ok(())
}

fn spawn_prepared_managed_stdio_process(
    config: &ManagedStdioProcessConfig,
    mut command: Command,
    provenance_executable: &Path,
) -> Result<ManagedStdioProcess, SandboxProcessRunError> {
    validate_managed_stdio_process_config(config)?;
    #[cfg(windows)]
    let _ = provenance_executable;
    #[cfg(unix)]
    let executable_sha256 =
        sha256_file_bounded(provenance_executable).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::SpawnFailed,
            message: format!("failed to hash managed stdio runtime executable: {error}"),
        })?;
    command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());
    configure_managed_stdio_process_ownership(&mut command);
    configure_background_child_suspended(&mut command);
    let child = command.spawn().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: format!("managed stdio runtime spawn failed: {error}"),
    })?;
    let child = ManagedChildGuard::new(child);
    #[cfg(windows)]
    let mut child = bind_windows_background_child(child)?;
    #[cfg(not(windows))]
    let mut child = child;
    let pid = child.id();
    // A short-lived Unix command can exit after its start token is read but before its image
    // metadata is resolved, so use the validated launch image hashed before spawning. Windows
    // keeps the child suspended below and verifies the actual loaded image instead.
    #[cfg(unix)]
    let trusted_executable_sha256 = Some(executable_sha256.as_str());
    #[cfg(not(unix))]
    let trusted_executable_sha256 = None;
    let provenance = match capture_background_process_provenance_with_executable_sha256(
        pid,
        trusted_executable_sha256,
    ) {
        Ok(provenance) => provenance,
        Err(error) => {
            let _ = child.terminate_and_reap(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS));
            #[cfg(windows)]
            remove_windows_background_job(pid);
            return Err(error);
        }
    };
    // Windows launches the child suspended and binds the kill-on-close job first. Keeping the
    // initial thread suspended through provenance capture prevents short-lived commands from
    // exiting before their exact ownership identity is durably available.
    #[cfg(windows)]
    if let Err(resume_error) = resume_windows_background_child(&child) {
        let cleanup = terminate_background_child(child);
        remove_windows_background_job(pid);
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "managed stdio runtime {pid} could not resume after provenance capture: {resume_error}; bounded owned-tree cleanup verification: {cleanup:?}"
            ),
        });
    }
    let stdin = child
        .child_mut()
        .stdin
        .take()
        .ok_or_else(|| managed_stdio_error("managed stdio runtime stdin is unavailable"))?;
    let stdout = child
        .child_mut()
        .stdout
        .take()
        .ok_or_else(|| managed_stdio_error("managed stdio runtime stdout is unavailable"))?;
    let stderr = child
        .child_mut()
        .stderr
        .take()
        .ok_or_else(|| managed_stdio_error("managed stdio runtime stderr is unavailable"))?;
    let issued_at_unix_ms = unix_time_ms();
    let duration_ms = i64::try_from(config.lease_duration.as_millis()).unwrap_or(i64::MAX);
    let lease = ProcessLeaseV1 {
        schema_version: RUNTIME_HANDLE_SCHEMA_VERSION,
        lease_id: RuntimeLeaseId::parse(&format!("lease_{}", ulid::Ulid::generate()))
            .map_err(|_| managed_stdio_error("failed to issue process lease identity"))?,
        instance_id: RuntimeInstanceId::parse(&format!("instance_{}", ulid::Ulid::generate()))
            .map_err(|_| managed_stdio_error("failed to issue runtime instance identity"))?,
        generation: RuntimeGeneration::new(config.generation)
            .map_err(|_| managed_stdio_error("failed to issue runtime generation"))?,
        pid,
        provenance,
        issued_at_unix_ms,
        expires_at_unix_ms: issued_at_unix_ms.saturating_add(duration_ms),
        verified_at_unix_ms: issued_at_unix_ms,
    };
    lease.validate().map_err(|error| {
        managed_stdio_error(format!("managed stdio runtime lease is invalid: {error}").as_str())
    })?;
    Ok(ManagedStdioProcess {
        child,
        stdin: Some(stdin),
        stdout: Some(stdout),
        stderr: Some(stderr),
        lease,
    })
}

fn managed_stdio_error(message: &str) -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: message.to_owned(),
    }
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

struct ForegroundProcessExecutionRequest<'a> {
    policy: &'a SandboxProcessRunnerPolicy,
    input: &'a ProcessRunnerInput,
    workspace_root: &'a Path,
    cwd: &'a Path,
    timeout: Duration,
    cancellation_requested: Option<Arc<AtomicBool>>,
    progress_sink: Option<ProcessProgressSink>,
    fault_injection: &'a crate::qa_fault_injection::QaFaultRuntime,
}

#[derive(Debug, Clone, Default)]
struct StreamCapture {
    bytes: Vec<u8>,
    truncated: bool,
    read_error: Option<String>,
}

impl StreamCapture {
    fn from_text(text: String) -> Self {
        Self { bytes: text.into_bytes(), truncated: false, read_error: None }
    }
}

#[derive(Debug, Clone)]
struct BackgroundOutputMonitor {
    stdout: Arc<Mutex<StreamCapture>>,
    stderr: Arc<Mutex<StreamCapture>>,
    quota_triggered: Arc<AtomicBool>,
}

#[derive(Debug)]
struct ProcessProgressMonitor {
    stdout: Arc<Mutex<StreamCapture>>,
    stderr: Arc<Mutex<StreamCapture>>,
    last_output_elapsed_ms: Arc<AtomicU64>,
    started_at: Instant,
}

#[derive(Debug, Clone)]
struct ProcessProgressStreamCapture {
    capture: Arc<Mutex<StreamCapture>>,
    last_output_elapsed_ms: Arc<AtomicU64>,
    started_at: Instant,
}

struct BackgroundProcessSpawnRequest<'a> {
    policy: &'a SandboxProcessRunnerPolicy,
    input: &'a ProcessRunnerInput,
    workspace_root: &'a Path,
    cwd: &'a Path,
    process_risk: &'a ProcessRiskReport,
    lifetime: Duration,
    max_lifetime: Duration,
    auto_background_reason: Option<&'static str>,
    lifetime_mode: BackgroundLifetimeMode,
    registration_fence: Option<BackgroundProcessRegistrationFence>,
    fault_injection: &'a crate::qa_fault_injection::QaFaultRuntime,
}

struct BackgroundLauncherCompletedContext<'a> {
    policy: &'a SandboxProcessRunnerPolicy,
    status: ExitStatus,
    stdout: &'a StreamCapture,
    stderr: &'a StreamCapture,
    duration: Duration,
    auto_background_reason: Option<&'static str>,
    lifetime_mode: BackgroundLifetimeMode,
    process_risk: &'a ProcessRiskReport,
}

#[cfg(unix)]
struct PreparedUnixSupervisedBackgroundChild {
    child: ManagedChildGuard,
    control: Arc<UnixProcessSupervisorControl>,
    supervisor_executable_sha256: String,
}

#[cfg(unix)]
struct UnixSupervisedBackgroundChild {
    child: ManagedChildGuard,
    control: Arc<UnixProcessSupervisorControl>,
    target_pid: u32,
}

impl ProcessProgressMonitor {
    fn new(started_at: Instant) -> Self {
        Self {
            stdout: Arc::new(Mutex::new(StreamCapture::default())),
            stderr: Arc::new(Mutex::new(StreamCapture::default())),
            last_output_elapsed_ms: Arc::new(AtomicU64::new(PROCESS_PROGRESS_NO_OUTPUT_MS)),
            started_at,
        }
    }

    fn stdout_capture(&self) -> ProcessProgressStreamCapture {
        ProcessProgressStreamCapture {
            capture: Arc::clone(&self.stdout),
            last_output_elapsed_ms: Arc::clone(&self.last_output_elapsed_ms),
            started_at: self.started_at,
        }
    }

    fn stderr_capture(&self) -> ProcessProgressStreamCapture {
        ProcessProgressStreamCapture {
            capture: Arc::clone(&self.stderr),
            last_output_elapsed_ms: Arc::clone(&self.last_output_elapsed_ms),
            started_at: self.started_at,
        }
    }

    fn snapshot(&self, pid: u32, elapsed_ms: u64) -> ProcessProgressEvent {
        let stdout = stream_capture_snapshot(&self.stdout, "stdout");
        let stderr = stream_capture_snapshot(&self.stderr, "stderr");
        let last_output_at_ms = match self.last_output_elapsed_ms.load(Ordering::Relaxed) {
            PROCESS_PROGRESS_NO_OUTPUT_MS => None,
            value => Some(value),
        };
        ProcessProgressEvent {
            pid,
            elapsed_ms,
            stdout_bytes: stdout.bytes.len() as u64,
            stderr_bytes: stderr.bytes.len() as u64,
            stdout_tail: process_progress_tail("stdout", stdout.bytes.as_slice()),
            stderr_tail: process_progress_tail("stderr", stderr.bytes.as_slice()),
            last_output_at_ms,
        }
    }
}

impl ProcessProgressStreamCapture {
    fn record_bytes(&self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        if let Ok(mut capture) = self.capture.lock() {
            capture.bytes.extend_from_slice(chunk);
        }
        self.record_output_timestamp();
    }

    fn mark_truncated(&self) {
        if let Ok(mut capture) = self.capture.lock() {
            capture.truncated = true;
        }
        self.record_output_timestamp();
    }

    fn record_read_error(&self, error: String) {
        if let Ok(mut capture) = self.capture.lock() {
            capture.read_error = Some(error);
        }
        self.record_output_timestamp();
    }

    fn record_output_timestamp(&self) {
        self.last_output_elapsed_ms.store(elapsed_millis_u64(self.started_at), Ordering::Relaxed);
    }
}

/// Liveness snapshot of a background process and its tracked descendants.
///
/// The tree view comes from the exact platform ownership domain: a Job Object on Windows or the
/// anchored process group on Unix. Unsupported platforms mirror direct-pid liveness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackgroundProcessRuntimeStatus {
    /// Whether the registered ownership-root pid is still alive.
    pub(crate) direct_pid_alive: bool,
    /// Whether any process in the tracked ownership domain is still alive.
    pub(crate) process_tree_alive: bool,
    /// Number of live processes in the tracked tree, when the platform can count them.
    pub(crate) tracked_process_count: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackgroundProcessHandleCapabilities {
    pub(crate) stdin: bool,
    pub(crate) pty_requested: bool,
    pub(crate) pty: bool,
    pub(crate) signals: bool,
    pub(crate) background: bool,
}

impl BackgroundProcessHandleCapabilities {
    const fn from_input(input: &ProcessRunnerInput) -> Self {
        Self {
            stdin: input.background && (input.stdin_requested() || input.pty),
            pty_requested: input.pty,
            pty: false,
            signals: true,
            background: input.background,
        }
    }
}

#[derive(Debug)]
struct RegisteredBackgroundProcess {
    active: bool,
    unix_cleanup_acknowledged: bool,
    cleanup_authority_retained: bool,
    capabilities: BackgroundProcessHandleCapabilities,
    lifetime_mode: BackgroundLifetimeMode,
    provenance: ProcessProvenance,
    target_pid: Option<u32>,
    stdin: Option<Arc<Mutex<BackgroundStdinState>>>,
    output_monitor: Option<BackgroundOutputMonitor>,
    terminal: Option<BackgroundProcessTerminalState>,
    #[cfg(unix)]
    supervisor_control: Option<Arc<UnixProcessSupervisorControl>>,
    #[cfg(windows)]
    windows_job: Option<Arc<WindowsBackgroundJob>>,
}

#[derive(Debug, Clone)]
struct BackgroundProcessTerminalState {
    process_state: &'static str,
    completion_reason: &'static str,
    exit_code: Option<i32>,
    completed_at_unix_ms: i64,
}

#[derive(Debug)]
struct BackgroundStdinState {
    stdin: ChildStdin,
    bytes_written: usize,
    events_written: usize,
}

#[derive(Debug, Clone)]
struct BackgroundProcessIdentity {
    pid: u32,
    provenance: ProcessProvenance,
    #[cfg(unix)]
    supervisor_control: Option<Arc<UnixProcessSupervisorControl>>,
    #[cfg(windows)]
    windows_job: Option<Arc<WindowsBackgroundJob>>,
}

#[derive(Debug, Clone)]
struct RegisteredBackgroundProcessSnapshot {
    active: bool,
    unix_cleanup_acknowledged: bool,
    capabilities: BackgroundProcessHandleCapabilities,
    lifetime_mode: BackgroundLifetimeMode,
    provenance: ProcessProvenance,
    identity: BackgroundProcessIdentity,
    output_monitor: Option<BackgroundOutputMonitor>,
    terminal: Option<BackgroundProcessTerminalState>,
}

/// Durable-safe ownership snapshot captured before a background process is acknowledged.
#[derive(Debug, Clone)]
pub(crate) struct BackgroundProcessProvenanceSnapshot {
    pub(crate) pid: u32,
    pub(crate) provenance: ProcessProvenance,
}

static REGISTERED_BACKGROUND_PROCESSES: OnceLock<Mutex<HashMap<u32, RegisteredBackgroundProcess>>> =
    OnceLock::new();
#[cfg(test)]
static FORCED_RETAINED_BACKGROUND_CLEANUP_FAILURES: OnceLock<Mutex<HashSet<u32>>> = OnceLock::new();

impl BackgroundProcessRuntimeStatus {
    /// Returns true while the tracked ownership domain has a live process.
    ///
    /// On Unix an unreaped root zombie still answers a direct PID probe, but it
    /// must not keep an otherwise empty process group logically active.
    pub(crate) fn alive(self) -> bool {
        #[cfg(unix)]
        {
            self.process_tree_alive
        }
        #[cfg(not(unix))]
        {
            self.process_tree_alive || self.direct_pid_alive
        }
    }

    /// Returns whether the registered ownership-root pid is still alive.
    pub(crate) fn direct_pid_alive(self) -> bool {
        self.direct_pid_alive
    }

    /// Returns whether any process in the tracked ownership domain is still alive.
    pub(crate) fn process_tree_alive(self) -> bool {
        self.process_tree_alive
    }

    /// Returns the live tracked-process count, when the platform can report one.
    pub(crate) fn tracked_process_count(self) -> Option<u32> {
        self.tracked_process_count
    }
}

fn registered_background_processes() -> &'static Mutex<HashMap<u32, RegisteredBackgroundProcess>> {
    REGISTERED_BACKGROUND_PROCESSES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn background_process_identity(
    pid: u32,
    process: &RegisteredBackgroundProcess,
) -> BackgroundProcessIdentity {
    BackgroundProcessIdentity {
        pid,
        provenance: process.provenance.clone(),
        #[cfg(unix)]
        supervisor_control: process.supervisor_control.clone(),
        #[cfg(windows)]
        windows_job: process.windows_job.clone(),
    }
}

fn registered_background_process_identity_matches(
    process: &RegisteredBackgroundProcess,
    expected: &BackgroundProcessIdentity,
) -> bool {
    if process.provenance != expected.provenance {
        return false;
    }
    #[cfg(unix)]
    {
        match (process.supervisor_control.as_ref(), expected.supervisor_control.as_ref()) {
            (Some(current), Some(expected)) => Arc::ptr_eq(current, expected),
            (None, None) => true,
            _ => false,
        }
    }
    #[cfg(windows)]
    {
        match (process.windows_job.as_ref(), expected.windows_job.as_ref()) {
            (Some(current), Some(expected)) => Arc::ptr_eq(current, expected),
            (None, None) => true,
            _ => false,
        }
    }
    #[cfg(not(any(unix, windows)))]
    {
        true
    }
}

fn compare_update_registered_background_process<R>(
    expected: &BackgroundProcessIdentity,
    update: impl FnOnce(&mut RegisteredBackgroundProcess) -> R,
) -> Result<Option<R>, SandboxProcessRunError> {
    let mut processes =
        registered_background_processes().lock().map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "background process registry lock poisoned for pid {}: {error}",
                expected.pid
            ),
        })?;
    let Some(process) = processes.get_mut(&expected.pid) else {
        return Ok(None);
    };
    if !registered_background_process_identity_matches(process, expected) {
        return Ok(None);
    }
    Ok(Some(update(process)))
}

#[cfg(test)]
fn forced_retained_background_cleanup_failures() -> &'static Mutex<HashSet<u32>> {
    FORCED_RETAINED_BACKGROUND_CLEANUP_FAILURES.get_or_init(|| Mutex::new(HashSet::new()))
}

fn register_background_process_pid(
    pid: u32,
    capabilities: BackgroundProcessHandleCapabilities,
    lifetime_mode: BackgroundLifetimeMode,
    provenance: ProcessProvenance,
    stdin: Option<ChildStdin>,
    #[cfg(unix)] supervisor_control: Option<Arc<UnixProcessSupervisorControl>>,
) -> Result<BackgroundProcessIdentity, SandboxProcessRunError> {
    provenance.validate().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("background process provenance is invalid for pid {pid}: {error}"),
    })?;
    #[cfg(all(unix, not(test)))]
    if supervisor_control.is_none()
        && provenance.ownership_kind == ProcessOwnershipKind::UnixProcessGroup
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "background process {pid} cannot register a Unix supervisor anchor without exact control authority"
            ),
        });
    }
    #[cfg(windows)]
    let windows_job = windows_background_job(pid);
    #[cfg(all(windows, not(test)))]
    if windows_job.is_none() && provenance.ownership_kind == ProcessOwnershipKind::WindowsJobObject
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "background process {pid} cannot register a Windows ownership anchor without its exact Job Object capability"
            ),
        });
    }
    match registered_background_processes().lock() {
        Ok(mut processes) => {
            if let Some(existing) = processes.get(&pid) {
                if existing.active || existing.cleanup_authority_retained {
                    let reason = if existing.active {
                        "an active owned entry"
                    } else {
                        "retained cleanup authority"
                    };
                    return Err(SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::RuntimeFailure,
                        message: format!(
                            "background process registry already contains {reason} for pid {pid}"
                        ),
                    });
                }
            }
            processes.remove(&pid);
            let process = RegisteredBackgroundProcess {
                active: true,
                unix_cleanup_acknowledged: false,
                cleanup_authority_retained: false,
                capabilities,
                lifetime_mode,
                provenance,
                target_pid: None,
                stdin: stdin.map(|stdin| {
                    Arc::new(Mutex::new(BackgroundStdinState {
                        stdin,
                        bytes_written: 0,
                        events_written: 0,
                    }))
                }),
                output_monitor: None,
                terminal: None,
                #[cfg(unix)]
                supervisor_control,
                #[cfg(windows)]
                windows_job,
            };
            let identity = background_process_identity(pid, &process);
            processes.insert(pid, process);
            Ok(identity)
        }
        Err(error) => Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("background process registry lock poisoned for pid {pid}: {error}"),
        }),
    }
}

fn registered_background_process(
    command: &str,
    pid: u32,
) -> Result<RegisteredBackgroundProcessSnapshot, SandboxProcessRunError> {
    match registered_background_processes().lock() {
        Ok(processes) => {
            processes
                .get(&pid)
                .map(|process| RegisteredBackgroundProcessSnapshot {
                    active: process.active,
                    unix_cleanup_acknowledged: process.unix_cleanup_acknowledged,
                    capabilities: process.capabilities,
                    lifetime_mode: process.lifetime_mode,
                    provenance: process.provenance.clone(),
                    identity: background_process_identity(pid, process),
                    output_monitor: process.output_monitor.clone(),
                    terminal: process.terminal.clone(),
                })
                .ok_or_else(|| SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::InvalidInput,
                    message: format!(
                        "palyra.process.run builtin '{command}' requires a pid returned by a live palyra.process.run background result; pid {pid} is not registered"
                    ),
                })
        }
        Err(error) => Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("background process registry lock poisoned for pid {pid}: {error}"),
        }),
    }
}

#[cfg(not(unix))]
fn capture_background_process_provenance(
    pid: u32,
) -> Result<ProcessProvenance, SandboxProcessRunError> {
    capture_background_process_provenance_with_executable_sha256(pid, None)
}

fn capture_background_process_provenance_with_executable_sha256(
    pid: u32,
    trusted_executable_sha256: Option<&str>,
) -> Result<ProcessProvenance, SandboxProcessRunError> {
    let start_token = current_process_start_token(pid)
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("failed to capture process start token for pid {pid}: {error}"),
        })?
        .ok_or_else(|| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("background process pid {pid} exited before provenance capture"),
        })?;
    let executable_sha256 = match trusted_executable_sha256 {
        Some(digest) => digest.to_owned(),
        None => {
            let executable_path =
                current_process_executable_path(pid).map_err(|error| SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!("failed to resolve executable image for pid {pid}: {error}"),
                })?;
            sha256_file_bounded(executable_path.as_path()).map_err(|error| {
                SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!("failed to hash executable image for pid {pid}: {error}"),
                }
            })?
        }
    };
    let owner_nonce = process_owner_nonce().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to issue process owner nonce for pid {pid}: {error}"),
    })?;
    let ownership_kind = current_process_ownership_kind();
    verify_live_ownership_anchor(pid).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to verify process ownership anchor for pid {pid}: {error}"),
    })?;
    let ownership_identity_sha256 = sha256_text(
        format!("{}:{pid}:{start_token}:{owner_nonce}", ownership_kind.as_str()).as_str(),
    );
    let provenance = ProcessProvenance {
        ownership_kind,
        start_token,
        executable_sha256,
        owner_nonce,
        ownership_identity_sha256,
    };
    provenance.validate().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("captured process provenance is invalid for pid {pid}: {error}"),
    })?;
    Ok(provenance)
}

/// Returns the provenance captured before a live background process was acknowledged.
pub(crate) fn background_process_provenance_snapshot(
    pid: u32,
) -> Option<BackgroundProcessProvenanceSnapshot> {
    let processes = registered_background_processes().lock().ok()?;
    let process = processes.get(&pid)?;
    Some(BackgroundProcessProvenanceSnapshot { pid, provenance: process.provenance.clone() })
}

#[cfg(test)]
pub(crate) fn registered_background_process_pids() -> Vec<u32> {
    registered_background_processes()
        .lock()
        .map(|processes| processes.keys().copied().collect())
        .unwrap_or_default()
}

#[cfg(test)]
pub(crate) fn background_process_cleanup_authority_is_retained(pid: u32) -> bool {
    background_process_cleanup_authority_retained(pid)
}

#[cfg(test)]
pub(crate) fn force_next_retained_background_cleanup_failure(pid: u32) {
    let _ = forced_retained_background_cleanup_failures().lock().map(|mut failures| {
        failures.insert(pid);
    });
}

#[cfg(all(test, windows))]
pub(crate) fn windows_background_job_process_count(pid: u32) -> Option<io::Result<u32>> {
    windows_background_job_active_process_count(pid)
}

/// Verifies the current OS process/tree against the exact registered provenance.
pub(crate) fn verify_background_process_provenance(
    pid: u32,
    expected: &ProcessProvenance,
) -> ProcessProvenanceDisposition {
    let Ok(registered) = registered_background_process("verify provenance", pid) else {
        return ProcessProvenanceDisposition::Missing;
    };
    if registered.provenance != *expected {
        return ProcessProvenanceDisposition::Mismatch;
    }
    #[cfg(windows)]
    {
        match background_process_runtime_status_for_identity(&registered.identity) {
            Ok(status) if !status.process_tree_alive => ProcessProvenanceDisposition::Missing,
            Ok(_) => ProcessProvenanceDisposition::Match,
            Err(_) => ProcessProvenanceDisposition::Unsupported,
        }
    }
    #[cfg(not(windows))]
    if let Some(disposition) = registered_process_liveness_disposition(
        expected.ownership_kind,
        owned_background_process_tree_is_alive(pid),
        process_id_is_alive(pid),
    ) {
        return disposition;
    }
    #[cfg(unix)]
    {
        verify_process_identity_with(pid, expected, true, current_process_start_token, |_| {
            Ok(expected.executable_sha256.clone())
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        verify_process_identity(pid, expected, true)
    }
}

fn registered_process_liveness_disposition(
    ownership_kind: ProcessOwnershipKind,
    process_tree_alive: io::Result<bool>,
    direct_pid_alive: io::Result<bool>,
) -> Option<ProcessProvenanceDisposition> {
    match process_tree_alive {
        Ok(false) => Some(ProcessProvenanceDisposition::Missing),
        Err(_) => Some(ProcessProvenanceDisposition::Unsupported),
        Ok(true) => match direct_pid_alive {
            Ok(true) => None,
            // A retained Job Object is a stable kernel capability even after its root exits. Unix
            // process-group IDs are numeric and reusable, so group liveness without the root's
            // stable identity can never authorize signalling.
            Ok(false)
                if cfg!(windows) && ownership_kind == ProcessOwnershipKind::WindowsJobObject =>
            {
                Some(ProcessProvenanceDisposition::Match)
            }
            Ok(false) | Err(_) => Some(ProcessProvenanceDisposition::Unsupported),
        },
    }
}

/// Verifies restart-visible process identity without adopting or signalling the process.
///
/// A matching PID/start token/executable proves the process instance, but restart loses the
/// host's retained process-group or Job Object capability. Live matches therefore remain
/// unsupported for ownership-sensitive actions. Missing direct identities remain unverifiable:
/// an absent numeric Unix process group cannot prove that an untrusted descendant did not escape,
/// while Windows Job Object and remote ownership also require their lost live control capability.
pub(crate) fn verify_persisted_process_provenance(
    pid: u32,
    expected: &ProcessProvenance,
) -> ProcessProvenanceDisposition {
    verify_process_identity_with(
        pid,
        expected,
        false,
        current_process_start_token,
        current_process_executable_sha256,
    )
}

#[cfg(not(any(unix, windows)))]
fn verify_process_identity(
    pid: u32,
    expected: &ProcessProvenance,
    has_retained_ownership_anchor: bool,
) -> ProcessProvenanceDisposition {
    verify_process_identity_with(
        pid,
        expected,
        has_retained_ownership_anchor,
        current_process_start_token,
        current_process_executable_sha256,
    )
}

fn verify_process_identity_with<StartToken, ExecutableDigest>(
    pid: u32,
    expected: &ProcessProvenance,
    has_retained_ownership_anchor: bool,
    current_start_token: StartToken,
    current_executable_sha256: ExecutableDigest,
) -> ProcessProvenanceDisposition
where
    StartToken: FnOnce(u32) -> io::Result<Option<String>>,
    ExecutableDigest: FnOnce(u32) -> io::Result<String>,
{
    match current_start_token(pid) {
        Ok(Some(start_token)) if start_token != expected.start_token => {
            ProcessProvenanceDisposition::Mismatch
        }
        Ok(Some(_)) => {
            match current_executable_sha256(pid) {
                Ok(digest) if digest != expected.executable_sha256 => {
                    return ProcessProvenanceDisposition::Mismatch;
                }
                Ok(_) => {}
                Err(_) => return ProcessProvenanceDisposition::Unsupported,
            }
            if !has_retained_ownership_anchor {
                return ProcessProvenanceDisposition::Unsupported;
            }
            match verify_live_ownership_anchor(pid) {
                Ok(()) => ProcessProvenanceDisposition::Match,
                Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                    ProcessProvenanceDisposition::Unsupported
                }
                Err(_) => ProcessProvenanceDisposition::Mismatch,
            }
        }
        // A numeric process-group ID can be reused after the direct launcher exits. Without the
        // launcher's stable identity, group liveness alone cannot authorize signalling descendants.
        Ok(None) if has_retained_ownership_anchor => ProcessProvenanceDisposition::Unsupported,
        Ok(None) => ProcessProvenanceDisposition::Unsupported,
        Err(_) => ProcessProvenanceDisposition::Unsupported,
    }
}

fn current_process_executable_sha256(pid: u32) -> io::Result<String> {
    let executable_path = current_process_executable_path(pid)?;
    sha256_file_bounded(executable_path.as_path())
}

fn require_background_process_provenance(
    pid: u32,
    expected: &ProcessProvenance,
    operation: &str,
) -> Result<(), SandboxProcessRunError> {
    let disposition = verify_background_process_provenance(pid, expected);
    if matches!(disposition, ProcessProvenanceDisposition::Match) {
        return Ok(());
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: format!(
            "{operation} refused pid {pid} because process provenance was {}",
            disposition.as_str()
        ),
    })
}

fn process_owner_nonce() -> io::Result<String> {
    let mut bytes = [0_u8; 16];
    getrandom::fill(&mut bytes).map_err(|error| io::Error::other(error.to_string()))?;
    Ok(hex::encode(bytes))
}

fn sha256_text(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    hex::encode(digest)
}

fn sha256_file_bounded(path: &Path) -> io::Result<String> {
    let metadata = fs::metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_PROCESS_EXECUTABLE_HASH_BYTES {
        return Err(io::Error::other("process executable exceeds bounded hash policy"));
    }
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total.saturating_add(u64::try_from(read).unwrap_or(u64::MAX));
        if total > MAX_PROCESS_EXECUTABLE_HASH_BYTES {
            return Err(io::Error::other("process executable exceeded bounded hash policy"));
        }
        digest.update(&buffer[..read]);
    }
    Ok(hex::encode(digest.finalize()))
}

#[cfg(unix)]
fn current_process_ownership_kind() -> ProcessOwnershipKind {
    ProcessOwnershipKind::UnixProcessGroup
}

#[cfg(windows)]
fn current_process_ownership_kind() -> ProcessOwnershipKind {
    ProcessOwnershipKind::WindowsJobObject
}

#[cfg(not(any(unix, windows)))]
fn current_process_ownership_kind() -> ProcessOwnershipKind {
    ProcessOwnershipKind::RemoteExecutionInstance
}

#[cfg(unix)]
fn verify_live_ownership_anchor(pid: u32) -> io::Result<()> {
    let process_id = unix_pid_from_u32(pid)?;
    // SAFETY: getpgid reads process metadata for a validated positive pid.
    let process_group_id = unsafe { libc::getpgid(process_id) };
    if process_group_id < 0 {
        let error = io::Error::last_os_error();
        #[cfg(target_os = "macos")]
        if matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::ENOENT)) {
            return verify_macos_zombie_ownership_anchor(pid);
        }
        return Err(error);
    }
    // SAFETY: getsid reads process metadata for a validated positive pid.
    let session_id = unsafe { libc::getsid(process_id) };
    if session_id < 0 {
        let error = io::Error::last_os_error();
        #[cfg(target_os = "macos")]
        if matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::ENOENT)) {
            return verify_macos_zombie_ownership_anchor(pid);
        }
        return Err(error);
    }
    if process_group_id == process_id && session_id == process_id {
        return Ok(());
    }
    Err(io::Error::other(format!(
        "pid {pid} has process group {process_group_id} and session {session_id}, expected both anchors to equal {pid}"
    )))
}

#[cfg(target_os = "macos")]
fn verify_macos_zombie_ownership_anchor(pid: u32) -> io::Result<()> {
    let process_id = unix_pid_from_u32(pid)?;
    let information_size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
        .map_err(|_| io::Error::other("macOS process information buffer is invalid"))?;
    // Darwin removes an unreaped child from getpgid/getsid visibility after exit, while libproc
    // retains its exact zombie record. Successful spawn already proves the pre-exec setsid call;
    // require that same reserved PID to remain its process-group leader before accepting it. The
    // live-to-zombie libproc projection can briefly expose an intermediate record, so retry only
    // complete mismatching snapshots and never relax the exact identity checks. Admission also
    // requires a zombie-only group because reaping the leader would release the PID before later
    // cleanup could safely signal any surviving descendants.
    for attempt in 0..MAX_MACOS_ZOMBIE_ANCHOR_SNAPSHOT_ATTEMPTS {
        let mut information = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
        // SAFETY: `information` is the fixed writable PROC_PIDTBSDINFO ABI buffer.
        let read = unsafe {
            macos_proc_pidinfo(
                process_id,
                libc::PROC_PIDTBSDINFO,
                MACOS_PROC_PIDINFO_INCLUDE_ZOMBIES,
                information.as_mut_ptr().cast(),
                information_size,
            )
        };
        if read <= 0 {
            return Err(io::Error::last_os_error());
        }
        if read != information_size {
            return Err(io::Error::other("macOS process information response was incomplete"));
        }
        // SAFETY: proc_pidinfo reported a complete fixed-size structure.
        let information = unsafe { information.assume_init() };
        if information.pbi_status == libc::SZOMB
            && information.pbi_pid == pid
            && information.pbi_pgid == pid
        {
            return verify_macos_zombie_group_is_quiescent(pid, unix_process_group_is_alive(pid)?);
        }
        if attempt + 1 == MAX_MACOS_ZOMBIE_ANCHOR_SNAPSHOT_ATTEMPTS {
            return Err(io::Error::other(format!(
                "macOS pid {pid} is not the exact exited ownership anchor: status={}, observed_pid={}, process_group_id={}",
                information.pbi_status, information.pbi_pid, information.pbi_pgid
            )));
        }
        thread::sleep(Duration::from_millis(1));
    }
    Err(io::Error::other("macOS zombie ownership snapshot attempts are disabled"))
}

#[cfg(any(target_os = "macos", test))]
fn verify_macos_zombie_group_is_quiescent(
    pid: u32,
    process_group_is_alive: bool,
) -> io::Result<()> {
    if process_group_is_alive {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!(
                "macOS pid {pid} retains live process-group members after its ownership anchor exited"
            ),
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn verify_live_ownership_anchor(pid: u32) -> io::Result<()> {
    if windows_background_job(pid).is_some() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("pid {pid} has no retained Windows Job Object"),
        ))
    }
}

#[cfg(not(any(unix, windows)))]
fn verify_live_ownership_anchor(_pid: u32) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "process ownership verification is unsupported"))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LinuxProcessStat {
    state: u8,
    process_group_id: Option<libc::pid_t>,
    start_token: u64,
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn read_linux_process_stat(pid: u32) -> io::Result<Option<LinuxProcessStat>> {
    let path = PathBuf::from(format!("/proc/{pid}/stat"));
    let mut stat = Vec::new();
    match fs::File::open(path) {
        Ok(file) => {
            if let Err(error) = file
                .take(u64::try_from(MAX_LINUX_PROC_STAT_BYTES).unwrap_or(u64::MAX) + 1)
                .read_to_end(&mut stat)
            {
                if linux_process_vanished(&error) {
                    return Ok(None);
                }
                return Err(error);
            }
        }
        Err(error) if linux_process_vanished(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    if stat.len() > MAX_LINUX_PROC_STAT_BYTES {
        return Err(io::Error::other("Linux process stat exceeded bounded capacity"));
    }
    parse_linux_process_stat(pid, stat.as_slice()).map(Some)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn parse_linux_process_stat(expected_pid: u32, stat: &[u8]) -> io::Result<LinuxProcessStat> {
    let command_start = stat
        .iter()
        .position(|byte| *byte == b'(')
        .ok_or_else(|| io::Error::other("Linux process stat command start missing"))?;
    let command_end = stat
        .windows(2)
        .rposition(|window| window == b") ")
        .ok_or_else(|| io::Error::other("Linux process stat command terminator missing"))?;
    if command_end <= command_start {
        return Err(io::Error::other("Linux process stat command bounds invalid"));
    }
    let reported_pid = std::str::from_utf8(&stat[..command_start])
        .map_err(|_| io::Error::other("Linux process stat pid invalid"))?
        .trim()
        .parse::<u32>()
        .map_err(|_| io::Error::other("Linux process stat pid invalid"))?;
    if reported_pid != expected_pid {
        return Err(io::Error::other("Linux process stat pid changed"));
    }
    let fields = std::str::from_utf8(&stat[command_end.saturating_add(2)..])
        .map_err(|_| io::Error::other("Linux process stat fields were not UTF-8"))?
        .split_whitespace()
        .collect::<Vec<_>>();
    if fields.len() <= 19 {
        return Err(io::Error::other("Linux process stat fields missing"));
    }
    let state = fields[0].as_bytes();
    if state.len() != 1 {
        return Err(io::Error::other("Linux process stat state invalid"));
    }
    let reported_process_group_id = fields[2]
        .parse::<libc::pid_t>()
        .map_err(|_| io::Error::other("Linux process stat process group invalid"))?;
    // The kernel leaves this field at -1 when an exiting task's sighand cannot be locked, and a
    // namespace-relative process-group id can be zero. Neither can match an owned positive PGID.
    let process_group_id = (reported_process_group_id > 0).then_some(reported_process_group_id);
    let start_token = fields[19]
        .parse::<u64>()
        .map_err(|_| io::Error::other("Linux process stat start token invalid"))?;
    Ok(LinuxProcessStat { state: state[0], process_group_id, start_token })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_process_vanished(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::NotFound
        || matches!(error.raw_os_error(), Some(libc::ENOENT) | Some(libc::ESRCH))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn current_process_start_token(pid: u32) -> io::Result<Option<String>> {
    Ok(read_linux_process_stat(pid)?.map(|stat| format!("linux:{}", stat.start_token)))
}

#[cfg(target_os = "macos")]
fn current_process_start_token(pid: u32) -> io::Result<Option<String>> {
    let process_id = i32::try_from(pid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "macOS pid exceeds i32"))?;
    let mut information = std::mem::MaybeUninit::<MacProcessUniqueInfo>::zeroed();
    let information_size = i32::try_from(std::mem::size_of::<MacProcessUniqueInfo>())
        .map_err(|_| io::Error::other("macOS process identity buffer is invalid"))?;
    // A managed child may finish before provenance capture while its retained `Child` still
    // reserves the exact PID. Darwin moves that unreaped process to `zombproc`; a nonzero
    // argument makes PROC_PIDUNIQIDENTIFIERINFO search both live and zombie records, preserving
    // the kernel-issued unique identity without synthesizing a reusable numeric-PID token.
    // SAFETY: the buffer exactly matches PROC_PIDUNIQIDENTIFIERINFO's fixed ABI.
    let read = unsafe {
        macos_proc_pidinfo(
            process_id,
            17,
            MACOS_PROC_PIDINFO_INCLUDE_ZOMBIES,
            information.as_mut_ptr().cast(),
            information_size,
        )
    };
    if read <= 0 {
        let error = io::Error::last_os_error();
        return if matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::ENOENT)) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    if read != information_size {
        return Err(io::Error::other("macOS process identity response was incomplete"));
    }
    // SAFETY: proc_pidinfo reported a complete fixed-size structure.
    let information = unsafe { information.assume_init() };
    Ok(Some(format!(
        "macos:{}:{}",
        information.unique_id,
        u32::from_ne_bytes(information.id_version.to_ne_bytes())
    )))
}

#[cfg(windows)]
fn current_process_start_token(pid: u32) -> io::Result<Option<String>> {
    let Some(process) = open_windows_process_for_identity(pid)? else {
        return Ok(None);
    };
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: all FILETIME pointers are valid writable outputs and the process
    // handle remains owned by `process` for the call.
    if unsafe { GetProcessTimes(process.get(), &mut creation, &mut exit, &mut kernel, &mut user) }
        == 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(Some(format!("windows:{:08x}{:08x}", creation.dwHighDateTime, creation.dwLowDateTime)))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android", target_os = "macos"))))]
fn current_process_start_token(_pid: u32) -> io::Result<Option<String>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable Unix process identity is unsupported on this platform",
    ))
}

#[cfg(not(any(unix, windows)))]
fn current_process_start_token(_pid: u32) -> io::Result<Option<String>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "stable process identity is unsupported on this platform",
    ))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn current_process_executable_path(pid: u32) -> io::Result<PathBuf> {
    fs::read_link(format!("/proc/{pid}/exe"))
}

#[cfg(target_os = "macos")]
fn current_process_executable_path(pid: u32) -> io::Result<PathBuf> {
    let process_id = i32::try_from(pid)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "macOS pid exceeds i32"))?;
    let buffer_size = usize::try_from(libc::PROC_PIDPATHINFO_MAXSIZE)
        .map_err(|_| io::Error::other("macOS process path buffer size is invalid"))?;
    let buffer_size_u32 = u32::try_from(buffer_size)
        .map_err(|_| io::Error::other("macOS process path buffer size exceeds u32"))?;
    let mut buffer = vec![0_u8; buffer_size];
    // SAFETY: buffer is writable for the provided length and process_id is positive.
    let written =
        unsafe { macos_proc_pidpath(process_id, buffer.as_mut_ptr().cast(), buffer_size_u32) };
    if written <= 0 {
        return Err(io::Error::last_os_error());
    }
    let written = usize::try_from(written)
        .map_err(|_| io::Error::other("macOS process path length is invalid"))?;
    buffer.truncate(written);
    Ok(PathBuf::from(OsString::from_vec(buffer)))
}

#[cfg(windows)]
fn current_process_executable_path(pid: u32) -> io::Result<PathBuf> {
    let process = open_windows_process_for_identity(pid)?.ok_or_else(|| {
        io::Error::new(io::ErrorKind::NotFound, format!("process {pid} no longer exists"))
    })?;
    let mut buffer = vec![0_u16; 260];
    loop {
        let mut length = u32::try_from(buffer.len()).unwrap_or(u32::MAX);
        // SAFETY: the process handle is valid and the UTF-16 buffer is writable
        // for the supplied character count.
        if unsafe { QueryFullProcessImageNameW(process.get(), 0, buffer.as_mut_ptr(), &mut length) }
            != 0
        {
            let length = usize::try_from(length)
                .map_err(|_| io::Error::other("Windows process image length is invalid"))?;
            return Ok(PathBuf::from(OsString::from_wide(&buffer[..length])));
        }
        let error = io::Error::last_os_error();
        if buffer.len() >= MAX_WINDOWS_PROCESS_IMAGE_CHARS {
            return Err(error);
        }
        buffer.resize((buffer.len() * 2).min(MAX_WINDOWS_PROCESS_IMAGE_CHARS), 0);
    }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android", target_os = "macos"))))]
fn current_process_executable_path(_pid: u32) -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process executable lookup is unsupported on this Unix platform",
    ))
}

#[cfg(not(any(unix, windows)))]
fn current_process_executable_path(_pid: u32) -> io::Result<PathBuf> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process executable lookup is unsupported on this platform",
    ))
}

#[cfg(windows)]
fn open_windows_process_for_identity(pid: u32) -> io::Result<Option<WindowsOwnedHandle>> {
    // SAFETY: OpenProcess has no pointer inputs; null is handled below.
    let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if handle.is_null() {
        // SAFETY: GetLastError reads thread-local Win32 state immediately after OpenProcess.
        return match unsafe { GetLastError() } {
            ERROR_INVALID_PARAMETER => Ok(None),
            ERROR_ACCESS_DENIED => Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("access denied opening process {pid} for identity verification"),
            )),
            _ => Err(io::Error::last_os_error()),
        };
    }
    WindowsOwnedHandle::new(handle, "process identity").map(Some)
}

fn attach_background_output_monitor(
    expected: &BackgroundProcessIdentity,
    output_monitor: BackgroundOutputMonitor,
) -> Result<(), SandboxProcessRunError> {
    compare_update_registered_background_process(expected, |process| {
        process.output_monitor = Some(output_monitor);
    })?
    .ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "palyra.process.run refused stale output monitor attachment for pid {}",
            expected.pid
        ),
    })
}

#[cfg(unix)]
fn attach_background_target_pid(
    expected: &BackgroundProcessIdentity,
    target_pid: u32,
) -> Result<(), SandboxProcessRunError> {
    compare_update_registered_background_process(expected, |process| match process.target_pid {
        Some(existing) if existing != target_pid => Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "background ownership root pid {} already tracks a different target process",
                expected.pid
            ),
        }),
        Some(_) => Ok(()),
        None => {
            process.target_pid = Some(target_pid);
            Ok(())
        }
    })?
    .ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "palyra.process.run refused stale target metadata attachment for ownership root pid {}",
            expected.pid
        ),
    })?
}

fn mark_background_process_stopped(expected: &BackgroundProcessIdentity) {
    set_background_process_stopped(expected, false);
}

fn mark_background_process_stopped_after_unix_cleanup(expected: &BackgroundProcessIdentity) {
    set_background_process_stopped(expected, true);
}

fn set_background_process_stopped(
    expected: &BackgroundProcessIdentity,
    unix_cleanup_acknowledged: bool,
) {
    let _ = compare_update_registered_background_process(expected, |process| {
        process.active = false;
        process.unix_cleanup_acknowledged |= unix_cleanup_acknowledged;
        process.stdin.take();
        process.terminal.get_or_insert_with(|| BackgroundProcessTerminalState {
            process_state: "exited",
            completion_reason: "process_tree_inactive",
            exit_code: None,
            completed_at_unix_ms: unix_time_ms(),
        });
    });
}

fn record_background_process_terminal_state(
    expected: &BackgroundProcessIdentity,
    process_state: &'static str,
    completion_reason: &'static str,
    exit_code: Option<i32>,
) {
    let _ = compare_update_registered_background_process(expected, |process| {
        process.active = false;
        process.stdin.take();
        process.terminal = Some(BackgroundProcessTerminalState {
            process_state,
            completion_reason,
            exit_code,
            completed_at_unix_ms: unix_time_ms(),
        });
    });
}

/// Releases bounded terminal diagnostics after the owning run has ended.
pub(crate) fn release_background_process_history(pid: u32, expected: &ProcessProvenance) {
    if let Ok(mut processes) = registered_background_processes().lock() {
        let removable = processes.get(&pid).is_some_and(|process| {
            !process.active
                && !process.cleanup_authority_retained
                && process.provenance == *expected
        });
        if removable {
            processes.remove(&pid);
        }
    }
}

#[cfg(test)]
fn mark_current_background_process_stopped(pid: u32) {
    if let Ok(snapshot) = registered_background_process("palyra.process.run", pid) {
        mark_background_process_stopped(&snapshot.identity);
    }
}

#[cfg(test)]
fn mark_current_background_process_stopped_after_unix_cleanup(pid: u32) {
    if let Ok(snapshot) = registered_background_process("palyra.process.run", pid) {
        mark_background_process_stopped_after_unix_cleanup(&snapshot.identity);
    }
}

#[cfg(test)]
pub(crate) fn mark_background_process_stopped_for_test(pid: u32) {
    mark_current_background_process_stopped(pid);
}

#[cfg(test)]
pub(crate) fn register_background_process_for_test(
    pid: u32,
    provenance: ProcessProvenance,
) -> Result<(), SandboxProcessRunError> {
    register_background_process_pid(
        pid,
        BackgroundProcessHandleCapabilities {
            stdin: false,
            pty_requested: false,
            pty: false,
            signals: true,
            background: true,
        },
        BackgroundLifetimeMode::RunOwned,
        provenance,
        None,
        #[cfg(unix)]
        None,
    )
    .map(|_| ())
}

/// Retains the exact platform ownership anchor until durable process cleanup is finalized.
///
/// # Errors
/// Returns an error when the process is missing, its provenance differs, or the registry lock is
/// poisoned.
pub(crate) fn retain_background_process_cleanup_authority(
    pid: u32,
    expected: &ProcessProvenance,
) -> io::Result<()> {
    let mut processes = registered_background_processes().lock().map_err(|error| {
        io::Error::other(format!(
            "background process registry lock poisoned for pid {pid}: {error}"
        ))
    })?;
    let process = processes.get_mut(&pid).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("background process {pid} has no registered cleanup authority"),
        )
    })?;
    if process.provenance != *expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("background process {pid} cleanup authority provenance does not match"),
        ));
    }
    process.cleanup_authority_retained = true;
    Ok(())
}

/// Releases a retained cleanup anchor after durable process finalization has committed.
pub(crate) fn release_background_process_cleanup_authority(pid: u32, expected: &ProcessProvenance) {
    let identity = registered_background_processes().lock().ok().and_then(|mut processes| {
        let process = processes.get_mut(&pid)?;
        if process.provenance != *expected {
            return None;
        }
        process.cleanup_authority_retained = false;
        Some(background_process_identity(pid, process))
    });
    if let Some(identity) = identity {
        release_background_process_tracking_if_stopped_exact(&identity);
    }
}

#[cfg(test)]
fn background_process_cleanup_authority_retained(pid: u32) -> bool {
    registered_background_processes()
        .lock()
        .ok()
        .and_then(|processes| processes.get(&pid).map(|process| process.cleanup_authority_retained))
        .unwrap_or(true)
}

#[cfg(not(unix))]
fn background_process_cleanup_authority_retained_exact(
    identity: &BackgroundProcessIdentity,
) -> bool {
    registered_background_processes()
        .lock()
        .ok()
        .and_then(|processes| {
            let process = processes.get(&identity.pid)?;
            registered_background_process_identity_matches(process, identity)
                .then_some(process.cleanup_authority_retained)
        })
        .unwrap_or(true)
}

/// Returns the exact registered process activity flag when provenance still matches.
///
/// `Some(false)` is emitted only after this runtime has independently verified ownership-domain
/// absence and marked the registration stopped. Missing or conflicting registry evidence returns
/// `None` so durable reconciliation keeps the lease fail-closed.
///
/// # Errors
/// Returns an error when the process registry lock is poisoned.
pub(crate) fn background_process_registration_is_active(
    pid: u32,
    expected: &ProcessProvenance,
) -> io::Result<Option<bool>> {
    let processes = registered_background_processes().lock().map_err(|error| {
        io::Error::other(format!(
            "background process registry lock poisoned for pid {pid}: {error}"
        ))
    })?;
    Ok(processes
        .get(&pid)
        .filter(|process| process.provenance == *expected)
        .map(|process| process.active))
}

fn require_background_process_cleanup_authority(
    pid: u32,
    expected: &ProcessProvenance,
) -> io::Result<()> {
    let processes = registered_background_processes().lock().map_err(|error| {
        io::Error::other(format!(
            "background process registry lock poisoned for pid {pid}: {error}"
        ))
    })?;
    let process = processes.get(&pid).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("background process {pid} has no retained cleanup authority"),
        )
    })?;
    if process.provenance != *expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("background process {pid} cleanup authority provenance does not match"),
        ));
    }
    if !process.cleanup_authority_retained {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("background process {pid} cleanup authority is not retained"),
        ));
    }
    Ok(())
}

/// RAII owner for temporary Win32 handles used while a background process is initialized.
#[cfg(windows)]
#[derive(Debug)]
struct WindowsOwnedHandle {
    handle: HANDLE,
    kind: &'static str,
}

#[cfg(windows)]
impl WindowsOwnedHandle {
    fn new(handle: HANDLE, kind: &'static str) -> io::Result<Self> {
        if !windows_handle_is_valid(handle) {
            return Err(io::Error::last_os_error());
        }
        Ok(Self { handle, kind })
    }

    fn get(&self) -> HANDLE {
        self.handle
    }

    fn close(mut self) -> io::Result<()> {
        let handle = std::mem::replace(&mut self.handle, std::ptr::null_mut());
        // SAFETY: ownership was transferred to this wrapper at construction and the null
        // replacement prevents Drop from closing the same handle twice.
        if unsafe { CloseHandle(handle) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

#[cfg(windows)]
impl Drop for WindowsOwnedHandle {
    fn drop(&mut self) {
        if !windows_handle_is_valid(self.handle) {
            return;
        }
        // SAFETY: the wrapper still owns this valid handle and this is its final close path.
        if unsafe { CloseHandle(self.handle) } == 0 {
            warn!(
                error = ?io::Error::last_os_error(),
                handle_kind = self.kind,
                "temporary Windows handle close failed"
            );
        }
    }
}

/// Owning wrapper around a Windows job object that tracks a background process tree.
///
/// The job is created with kill-on-close semantics, so dropping the last handle also tears the
/// tree down if explicit termination never ran.
#[cfg(windows)]
#[derive(Debug)]
struct WindowsBackgroundJob {
    handle: HANDLE,
    termination_succeeded: Mutex<bool>,
}

// SAFETY: `HANDLE` is a process-wide kernel handle, valid from any thread; this wrapper owns it
// exclusively until Drop and serializes its mutable termination state behind a mutex.
#[cfg(windows)]
unsafe impl Send for WindowsBackgroundJob {}

// SAFETY: see the Send rationale above; all &self methods are thread-safe Win32 calls guarded
// by immutable handle ownership or the termination mutex.
#[cfg(windows)]
unsafe impl Sync for WindowsBackgroundJob {}

#[cfg(windows)]
impl WindowsBackgroundJob {
    fn terminate(&self) -> io::Result<()> {
        self.terminate_with(|handle| {
            // SAFETY: `handle` is a valid job handle owned by this wrapper until Drop closes it.
            if unsafe { TerminateJobObject(handle, 1) } == 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        })
    }

    fn terminate_with(
        &self,
        terminate_operation: impl FnOnce(HANDLE) -> io::Result<()>,
    ) -> io::Result<()> {
        let mut termination_succeeded = match self.termination_succeeded.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                warn!("Windows background job termination lock was poisoned; retrying cleanup");
                poisoned.into_inner()
            }
        };
        if *termination_succeeded {
            return Ok(());
        }

        terminate_operation(self.handle)?;
        // A failed OS operation leaves this false, so the next caller retries instead of
        // reporting an idempotent false success while descendants may still be alive.
        *termination_succeeded = true;
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

    fn termination_was_requested_and_succeeded(&self) -> bool {
        self.termination_succeeded.lock().map(|state| *state).unwrap_or(false)
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

// Registry of live background jobs keyed by the direct child pid, consulted by the portable
// stop/status builtins so they can act on the whole tree instead of just the launcher pid.
// NOTE: keep the job registered through termination verification. If stop removes the
// handle before the post-stop status probe, a dead wrapper pid can make detached descendants look
// cleaned up even while the job still had process-tree evidence.
#[cfg(windows)]
static WINDOWS_BACKGROUND_JOBS: OnceLock<Mutex<HashMap<u32, Arc<WindowsBackgroundJob>>>> =
    OnceLock::new();

impl BackgroundOutputMonitor {
    fn quota_exceeded(&self) -> bool {
        self.quota_triggered.load(Ordering::Acquire)
    }

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

    // Polls until the child produced any output (or an error/truncation) or `max_wait` elapses,
    // so background metadata can include early port announcements without blocking the caller
    // for the full drain window when output arrives quickly.
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

fn stream_capture_snapshot(
    capture: &Arc<Mutex<StreamCapture>>,
    stream_name: &str,
) -> StreamCapture {
    capture.lock().map(|capture| capture.clone()).unwrap_or_else(|_| StreamCapture {
        bytes: Vec::new(),
        truncated: false,
        read_error: Some(format!("{stream_name} progress capture lock poisoned")),
    })
}

fn elapsed_millis_u64(started_at: Instant) -> u64 {
    let elapsed = started_at.elapsed().as_millis();
    elapsed.min(u128::from(u64::MAX)) as u64
}

fn process_progress_tail(stream_name: &str, output: &[u8]) -> String {
    if output.is_empty() {
        return String::new();
    }
    if process_output_looks_binary(output) {
        return format!(
            "<binary {stream_name} tail omitted: size_bytes={} sha256={}>",
            output.len(),
            sha256_hex(output)
        );
    }
    let decoded = decode_process_output_text(output);
    let RedactedProcessOutputText { text, .. } =
        redacted_process_output_text(decoded.text.as_str());
    process_text_suffix(text.as_str(), PROCESS_PROGRESS_TAIL_BYTES)
}

/// Test-only convenience wrapper over [`run_constrained_process_with_cancellation`] without a
/// cancellation flag.
#[cfg(test)]
pub(crate) fn run_constrained_process(
    policy: &SandboxProcessRunnerPolicy,
    input_json: &[u8],
    execution_timeout: Duration,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let registration_fence: BackgroundProcessRegistrationFence = Arc::new(|_| Ok(()));
    run_constrained_process_with_fault_injection(
        policy,
        input_json,
        execution_timeout,
        None,
        None,
        Some(registration_fence),
        crate::qa_fault_injection::QaFaultRuntime::default(),
    )
}

/// Validates and executes one `palyra.process.run` invocation under `policy`.
///
/// The pipeline is strictly ordered: input-shape validation, executable/interpreter allowlist
/// checks, working-directory and argument path scoping, egress preflight, portable builtins,
/// platform quota and runtime-egress fail-closed checks, then foreground or background spawn.
/// `execution_timeout` is the operator-level ceiling that caps both foreground timeouts and
/// background lifetimes; `cancellation_requested` is polled during foreground capture and
/// terminates the whole process tree when set.
///
/// Returns redacted, serialized tool output on success (see [`SandboxProcessRunSuccess`]).
///
/// # Errors
///
/// Returns a [`SandboxProcessRunError`] whose [`kind`](SandboxProcessRunError::kind) identifies
/// the failing stage: `Disabled`, `InvalidInput`, `WorkspaceScopeDenied`, `EgressDenied`,
/// `UnsupportedPlatform`, `SpawnFailed`, `Cancelled`, `TimedOut`, `QuotaExceeded`, or
/// `RuntimeFailure` (non-zero exit, capture failure, or serialization failure).
#[cfg(test)]
pub fn run_constrained_process_with_cancellation(
    policy: &SandboxProcessRunnerPolicy,
    input_json: &[u8],
    execution_timeout: Duration,
    cancellation_requested: Option<Arc<AtomicBool>>,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    run_constrained_process_with_fault_injection(
        policy,
        input_json,
        execution_timeout,
        cancellation_requested,
        None,
        None,
        crate::qa_fault_injection::QaFaultRuntime::default(),
    )
}

pub(crate) fn run_constrained_process_with_fault_injection(
    policy: &SandboxProcessRunnerPolicy,
    input_json: &[u8],
    execution_timeout: Duration,
    cancellation_requested: Option<Arc<AtomicBool>>,
    progress_sink: Option<ProcessProgressSink>,
    background_registration_fence: Option<BackgroundProcessRegistrationFence>,
    fault_injection: crate::qa_fault_injection::QaFaultRuntime,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    if !policy.enabled {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::Disabled,
            message: "sandbox process runner is disabled by runtime policy".to_owned(),
        });
    }

    let mut input = parse_process_runner_input(input_json)?;
    validate_input_shape(&input)?;
    validate_background_lifetime_mode(&input)?;
    validate_background_registration_fence(&input, background_registration_fence.as_ref())?;
    validate_allowed_executable(policy, input.command.as_str())?;
    validate_no_embedded_command_line_arg(&input)?;
    validate_cmd_invocation_shape(input.command.as_str(), input.args.as_slice())?;
    validate_process_termination_scope(input.command.as_str(), input.args.as_slice())?;

    let path_access_mode = process_runner_effective_path_access_mode(policy);
    let workspace_root = canonical_workspace_root(policy.workspace_root.as_path())?;
    let host_access_roots = process_runner_accepts_host_path_fields(policy).then(host_access_roots);
    let host_access_path_env = process_runner_accepts_host_path_fields(policy)
        .then(|| host_access_path_env_for_input(&input));
    let working_directory = match path_access_mode {
        PathAccessMode::ApprovedRoots => resolve_host_working_directory_with_roots(
            workspace_root.as_path(),
            input.cwd.as_deref(),
            host_access_roots.as_ref().expect("host roots should be initialized").as_slice(),
            host_access_path_env.as_ref().expect("host path env should be initialized"),
        )?,
        PathAccessMode::WorkspaceOnly => {
            resolve_working_directory(workspace_root.as_path(), input.cwd.as_deref())?
        }
    };
    let process_risk = classify_process_run(
        &input,
        ProcessRiskContext {
            workspace_root: Some(workspace_root.as_path()),
            resolved_cwd: Some(working_directory.as_path()),
        },
    );
    validate_supported_target_runtime(&process_risk)?;
    match path_access_mode {
        PathAccessMode::ApprovedRoots => {
            let host_access_roots =
                host_access_roots.as_ref().expect("host roots should be initialized");
            let path_env =
                host_access_path_env.as_ref().expect("host path env should be initialized");
            input.args = rewrite_host_access_process_args(
                input.args.as_slice(),
                workspace_root.as_path(),
                path_env,
            )?;
            validate_host_command_path_scope_with_roots(
                workspace_root.as_path(),
                working_directory.as_path(),
                input.command.as_str(),
                host_access_roots.as_slice(),
            )?;
            validate_host_interpreter_argument_guardrails_with_roots(
                workspace_root.as_path(),
                working_directory.as_path(),
                input.command.as_str(),
                input.args.as_slice(),
                host_access_roots.as_slice(),
            )?;
            validate_host_argument_scope_with_roots(
                workspace_root.as_path(),
                working_directory.as_path(),
                input.command.as_str(),
                input.args.as_slice(),
                host_access_roots.as_slice(),
            )?;
        }
        PathAccessMode::WorkspaceOnly => {
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
    }
    let requested_hosts = if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::None) {
        validate_requested_egress_hosts_require_enforcement(&input)?;
        Vec::new()
    } else {
        collect_requested_egress_hosts(&input)?
    };
    // Strict tier C is offline-only: requested hosts are denied before the allowlist check so
    // configured allowlists cannot re-open network access in that mode.
    if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::Strict) {
        validate_tier_c_strict_offline_egress_requests(policy, requested_hosts.as_slice())?;
    }
    if !matches!(policy.egress_enforcement_mode, EgressEnforcementMode::None) {
        validate_egress_hosts(policy, requested_hosts.as_slice())?;
    }
    // Portable builtins (pwd/echo/ls/cat/mkdir and process stop/status) run in-process after
    // the same scope validation as real spawns, so they behave identically on every platform.
    if let Some(result) = execute_builtin_process_command(
        policy,
        &input,
        workspace_root.as_path(),
        working_directory.as_path(),
        &process_risk,
    )? {
        return Ok(result);
    }
    // Fail closed before spawn when the platform cannot enforce CPU/memory quotas. Host-access
    // mode is exempt: it is explicitly unsandboxed and bounded by timeout and output caps only.
    if matches!(path_access_mode, PathAccessMode::WorkspaceOnly) {
        validate_platform_resource_quota_support(policy)?;
    }
    if matches!(policy.egress_enforcement_mode, EgressEnforcementMode::Strict) {
        validate_runtime_egress_enforcement(policy)?;
    }

    if input.background {
        let per_call_timeout = background_process_lifetime(input.timeout_ms, execution_timeout);
        let max_background_lifetime = background_process_lifetime_limit(execution_timeout);
        return spawn_background_process(BackgroundProcessSpawnRequest {
            policy,
            input: &input,
            workspace_root: workspace_root.as_path(),
            cwd: working_directory.as_path(),
            process_risk: &process_risk,
            lifetime: per_call_timeout,
            max_lifetime: max_background_lifetime,
            auto_background_reason: None,
            lifetime_mode: input.effective_lifetime_mode(),
            registration_fence: background_registration_fence,
            fault_injection: &fault_injection,
        });
    }

    let per_call_timeout = foreground_process_timeout(input.timeout_ms, execution_timeout);

    let capture = execute_process(ForegroundProcessExecutionRequest {
        policy,
        input: &input,
        workspace_root: workspace_root.as_path(),
        cwd: working_directory.as_path(),
        timeout: per_call_timeout,
        cancellation_requested,
        progress_sink,
        fault_injection: &fault_injection,
    })?;
    if capture.cancelled {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::Cancelled,
            message: "sandbox process cancelled by run cancellation request and process tree was terminated".to_owned(),
        });
    }
    if capture.timed_out {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::TimedOut,
            message: format!(
                "sandbox process timed out after {}ms and was terminated; process_output_summary={}; for dev servers or intentional long-running services, rerun with background=true and an explicit timeout_ms lifetime, then poll or stop the returned process handle. Do not use background=true to verify tests or builds; rerun those foreground with a longer timeout after fixing the hang.",
                per_call_timeout.as_millis(),
                process_output_diagnostic_summary(&capture.stdout, &capture.stderr)
            ),
        });
    }
    if capture.quota_exceeded {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::QuotaExceeded,
            message: format!(
                "sandbox process exceeded output quota (max_output_bytes={}) and was terminated; process_output_summary={}",
                policy.max_output_bytes,
                process_output_diagnostic_summary(&capture.stdout, &capture.stderr)
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
        let failure_class = process_exit_failure_class(capture.exit_status);
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: process_failure_message(
                failure_class,
                capture.exit_status.code().unwrap_or(-1),
                &capture.stdout,
                &capture.stderr,
            ),
        });
    }

    let output_json = process_success_output_json(ProcessSuccessOutputJsonInput {
        exit_code: capture.exit_status.code().unwrap_or(0),
        stdout: &capture.stdout,
        stderr: &capture.stderr,
        duration_ms: capture.duration_ms,
        tier: policy.tier.as_str(),
        sandbox_backend: if matches!(policy.tier, SandboxProcessRunnerTier::C) {
            current_backend_kind().as_str()
        } else {
            "tier_b_in_process"
        },
        process_risk: &process_risk,
        input: Some(&input),
    })
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox process output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

fn process_failure_message(
    failure_class: ProcessFailureClass,
    exit_code: i32,
    stdout: &StreamCapture,
    stderr: &StreamCapture,
) -> String {
    let diagnostic_hint = process_failure_diagnostic_hint(stdout, stderr)
        .map(|hint| format!(", hint={hint:?}"))
        .unwrap_or_default();
    let stdout_preview = redacted_process_failure_preview(stdout.bytes.as_slice());
    let stderr_preview = redacted_process_failure_preview(stderr.bytes.as_slice());
    format!(
        "sandbox process exited unsuccessfully (failure_class={}, code={exit_code}, stdout_bytes={}, stdout_truncated={}, stderr_bytes={}, stderr_truncated={}{}); stdout_preview={stdout_preview:?}; stderr_preview={stderr_preview:?}",
        failure_class.as_str(),
        stdout.bytes.len(),
        stdout.truncated,
        stderr.bytes.len(),
        stderr.truncated,
        diagnostic_hint,
    )
}

// Failure diagnostics scan the complete bounded capture before truncation. This
// prevents a credential marker near the head from exposing a value suffix near
// the tail while still giving the model an actionable single-line preview.
fn redacted_process_failure_preview(output: &[u8]) -> Option<String> {
    if output.is_empty() {
        return None;
    }
    let redacted = redact_labelled_process_failure_secrets(redacted_process_output(output).text);
    let normalized = redacted
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character })
        .collect::<String>();
    let collapsed = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        return None;
    }
    let mut boundary = collapsed.len().min(PROCESS_OUTPUT_PREVIEW_BYTES);
    while !collapsed.is_char_boundary(boundary) {
        boundary = boundary.saturating_sub(1);
    }
    Some(collapsed[..boundary].to_owned())
}

// Plain-text tools sometimes emit credential labels with spaces (for example
// `api key: value`), which the structured assignment scanner intentionally
// does not treat as source-code syntax. Failure previews add this conservative
// line-oriented layer before becoming model-visible.
fn redact_labelled_process_failure_secrets(value: String) -> String {
    let mut output = String::with_capacity(value.len());
    for line in value.split_inclusive('\n') {
        let (body, line_ending) = line
            .strip_suffix("\r\n")
            .map(|body| (body, "\r\n"))
            .or_else(|| line.strip_suffix('\n').map(|body| (body, "\n")))
            .unwrap_or((line, ""));
        let Some((separator_index, _)) =
            body.char_indices().find(|(_, character)| matches!(character, ':' | '='))
        else {
            output.push_str(line);
            continue;
        };
        let label = body[..separator_index]
            .trim()
            .chars()
            .filter(|character| !matches!(character, ' ' | '_' | '-'))
            .collect::<String>()
            .to_ascii_lowercase();
        let value_start = separator_index.saturating_add(1);
        let secret_value = body[value_start..].trim();
        if !matches!(
            label.as_str(),
            "apikey"
                | "accesstoken"
                | "authtoken"
                | "password"
                | "secret"
                | "clientsecret"
                | "credential"
        ) || secret_value.is_empty()
            || secret_value == "[REDACTED_SECRET]"
            || secret_value == "<redacted>"
        {
            output.push_str(line);
            continue;
        }
        output.push_str(&body[..value_start]);
        output.push_str(" [REDACTED_SECRET]");
        output.push_str(line_ending);
    }
    output
}

fn process_failure_diagnostic_hint(
    stdout: &StreamCapture,
    stderr: &StreamCapture,
) -> Option<&'static str> {
    let stdout = decode_process_output_text(stdout.bytes.as_slice());
    let stderr = decode_process_output_text(stderr.bytes.as_slice());
    let output = format!("{}\n{}", stdout.text, stderr.text,).to_ascii_lowercase();
    if (output.contains("windows subsystem for linux") || output.contains("wsl"))
        && output.contains("no installed")
        && output.contains("distribution")
    {
        return Some(
            "WSL reports no installed Linux distributions; use a workspace script-file invocation such as command='pwsh', args=['-NoProfile','-File','scripts/check.ps1'] on Windows, or install and configure WSL before running bash scripts",
        );
    }
    None
}

// Structured success output previews flatten control characters and collapse whitespace before
// redaction.
fn redacted_process_output_preview(output: &[u8]) -> Option<String> {
    if output.is_empty() {
        return None;
    }
    let take_len = output.len().min(PROCESS_OUTPUT_PREVIEW_BYTES);
    redacted_process_output_single_line(&output[..take_len])
}

fn redacted_process_output_single_line(output: &[u8]) -> Option<String> {
    let text = decode_process_output_text(output).text;
    let redacted = redacted_process_output_text(text.as_str()).text;
    let normalized = redacted
        .chars()
        .map(|character| if character.is_control() { ' ' } else { character })
        .collect::<String>();
    let collapsed = normalized.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.is_empty() {
        None
    } else {
        Some(collapsed)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RedactedProcessOutputText {
    text: String,
    redacted: bool,
    redaction_reasons: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedProcessOutputText {
    text: String,
    encoding: &'static str,
    decode_replacement_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessStreamOutputView {
    model_text: String,
    redacted: bool,
    metadata: Value,
}

struct ProcessSuccessOutputJsonInput<'a> {
    exit_code: i32,
    stdout: &'a StreamCapture,
    stderr: &'a StreamCapture,
    duration_ms: u64,
    tier: &'a str,
    sandbox_backend: &'a str,
    process_risk: &'a ProcessRiskReport,
    input: Option<&'a ProcessRunnerInput>,
}

fn process_success_output_json(
    request: ProcessSuccessOutputJsonInput<'_>,
) -> serde_json::Result<Vec<u8>> {
    let stdout_view = process_stream_output_view("stdout", request.stdout);
    let stderr_view = process_stream_output_view("stderr", request.stderr);
    serde_json::to_vec(&json!({
        "schema_version": 2,
        "exit_code": request.exit_code,
        "stdout": stdout_view.model_text,
        "stderr": stderr_view.model_text,
        "stdout_truncated": request.stdout.truncated,
        "stderr_truncated": request.stderr.truncated,
        "stdout_redacted": stdout_view.redacted,
        "stderr_redacted": stderr_view.redacted,
        "stdout_bytes": request.stdout.bytes.len(),
        "stderr_bytes": request.stderr.bytes.len(),
        "duration_ms": request.duration_ms,
        "tier": request.tier,
        "sandbox_backend": request.sandbox_backend,
        "process_risk": request.process_risk,
        "streams": {
            "stdout": stdout_view.metadata,
            "stderr": stderr_view.metadata,
        },
        "runtime_request": process_runtime_request_projection(request.input),
        "notification": process_completion_notification_projection(
            request.input,
            ProcessCompletionState::Delivered,
            None,
            stdout_view.model_text.as_str(),
            stderr_view.model_text.as_str(),
        ),
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessCompletionState {
    Disabled,
    Subscribed,
    Delivered,
}

impl ProcessCompletionState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Subscribed => "subscribed",
            Self::Delivered => "delivered",
        }
    }
}

fn process_runtime_request_projection(input: Option<&ProcessRunnerInput>) -> Value {
    let Some(input) = input else {
        return Value::Null;
    };
    json!({
        "schema_version": 1,
        "notify_on_complete": input.notify_on_complete,
        "watch_pattern_count": input.watch_patterns.len(),
        "env_profile": input.env_profile_id.as_ref().map(|profile_id| json!({
            "profile_id": profile_id,
            "profile_id_sha256": sha256_hex(profile_id.as_bytes()),
            "daemon_env_inherited": false,
        })),
        "provided_env_key_count": input.env.len(),
        "elevated_intent": input.elevated_intent,
        "elevated_intent_detected": process_input_has_elevated_intent(input),
        "facade_mapping": input.facade_mapping.as_ref().map(|mapping| json!({
            "original_tool_name": mapping.original_tool_name.as_str(),
            "canonical_tool_name": mapping.canonical_tool_name.as_str(),
        })),
        "reason_code": "process.runtime_request.accepted",
    })
}

fn process_completion_notification_projection(
    input: Option<&ProcessRunnerInput>,
    state: ProcessCompletionState,
    pid: Option<u32>,
    stdout: &str,
    stderr: &str,
) -> Value {
    let Some(input) = input else {
        return Value::Null;
    };
    let watch = process_watch_events_projection(input, stdout, stderr);
    let completion_requested = input.notify_on_complete || !input.watch_patterns.is_empty();
    let completion_state =
        if completion_requested { state } else { ProcessCompletionState::Disabled };
    json!({
        "schema_version": 1,
        "requested": completion_requested,
        "completion": {
            "state": completion_state.as_str(),
            "delivery": if completion_state == ProcessCompletionState::Delivered {
                "synthetic_system_input_ready"
            } else if completion_state == ProcessCompletionState::Subscribed {
                "background_subscription_pending"
            } else {
                "not_requested"
            },
            "exactly_once": completion_requested,
            "sequence": if completion_state == ProcessCompletionState::Delivered { Some(1_u8) } else { None },
            "subscription_key_sha256": completion_requested.then(|| {
                process_notification_subscription_key(input, pid)
            }),
            "reason_code": match completion_state {
                ProcessCompletionState::Disabled => "process.notification.not_requested",
                ProcessCompletionState::Subscribed => "process.notification.subscribed",
                ProcessCompletionState::Delivered => "process.notification.delivered",
            },
        },
        "watch": watch,
    })
}

fn process_notification_subscription_key(input: &ProcessRunnerInput, pid: Option<u32>) -> String {
    let payload = json!({
        "command": input.command,
        "args": input.args,
        "cwd": input.cwd,
        "pid": pid,
        "notify_on_complete": input.notify_on_complete,
        "watch_patterns": input.watch_patterns.iter().map(|pattern| {
            json!({
                "name": pattern.name,
                "pattern_sha256": sha256_hex(pattern.pattern.as_bytes()),
                "stream": pattern.stream.as_str(),
                "notify_once": pattern.notify_once,
            })
        }).collect::<Vec<_>>(),
    });
    sha256_hex(serde_json::to_vec(&payload).unwrap_or_default().as_slice())
}

fn process_watch_events_projection(
    input: &ProcessRunnerInput,
    stdout: &str,
    stderr: &str,
) -> Value {
    let mut events = Vec::new();
    let mut suppressed = 0_u64;
    let mut degraded = false;
    for pattern in &input.watch_patterns {
        let occurrence_count = process_watch_pattern_occurrences(
            pattern.stream,
            stdout,
            stderr,
            pattern.pattern.as_str(),
        );
        if occurrence_count > 16 {
            degraded = true;
            suppressed = suppressed.saturating_add(occurrence_count);
            continue;
        }
        if occurrence_count > 0 {
            events.push(json!({
                "name": pattern.name,
                "stream": pattern.stream.as_str(),
                "pattern_sha256": sha256_hex(pattern.pattern.as_bytes()),
                "occurrences": occurrence_count,
                "notify_once": pattern.notify_once,
                "delivery_state": "ready",
                "reason_code": "process.watch_pattern.matched",
            }));
        }
    }
    json!({
        "requested": !input.watch_patterns.is_empty(),
        "state": if degraded { "degraded_completion_only" } else { "ready" },
        "event_count": events.len(),
        "suppressed_occurrences": suppressed,
        "rate_limited": degraded,
        "reason_code": if degraded {
            "process.watch_pattern.spam_degraded"
        } else if events.is_empty() {
            "process.watch_pattern.no_match"
        } else {
            "process.watch_pattern.ready"
        },
        "events": if degraded { Vec::<Value>::new() } else { events },
    })
}

fn process_watch_pattern_occurrences(
    stream: ProcessWatchStream,
    stdout: &str,
    stderr: &str,
    pattern: &str,
) -> u64 {
    match stream {
        ProcessWatchStream::Both => count_non_overlapping_occurrences(stdout, pattern)
            .saturating_add(count_non_overlapping_occurrences(stderr, pattern)),
        ProcessWatchStream::Stdout => count_non_overlapping_occurrences(stdout, pattern),
        ProcessWatchStream::Stderr => count_non_overlapping_occurrences(stderr, pattern),
    }
}

fn count_non_overlapping_occurrences(haystack: &str, needle: &str) -> u64 {
    if needle.is_empty() {
        return 0;
    }
    haystack.matches(needle).count().try_into().unwrap_or(u64::MAX)
}

fn process_stream_output_view(
    stream_name: &str,
    stream: &StreamCapture,
) -> ProcessStreamOutputView {
    let size_bytes = stream.bytes.len();
    let sha256 = sha256_hex(stream.bytes.as_slice());
    if process_output_looks_binary(stream.bytes.as_slice()) {
        let model_text = if stream.bytes.is_empty() {
            String::new()
        } else {
            format!("<binary {stream_name} omitted: size_bytes={size_bytes} sha256={sha256}>")
        };
        return ProcessStreamOutputView {
            model_text,
            redacted: false,
            metadata: json!({
                "size_bytes": size_bytes,
                "captured_bytes": size_bytes,
                "truncated": stream.truncated,
                "binary": true,
                "binary_output_omitted": !stream.bytes.is_empty(),
                "encoding": null,
                "decode_replacement_count": 0,
                "sha256": sha256,
            }),
        };
    }

    let decoded = decode_process_output_text(stream.bytes.as_slice());
    let RedactedProcessOutputText { text, redacted, redaction_reasons } =
        redacted_process_output_text(decoded.text.as_str());
    let inline_truncated = text.len() > PROCESS_STREAM_INLINE_TEXT_BYTES;
    let model_text = if inline_truncated {
        format!(
            "<{stream_name} omitted: size_bytes={size_bytes} sha256={sha256}; see streams.{stream_name}.head and streams.{stream_name}.tail>"
        )
    } else {
        text.clone()
    };
    let decode_warning = (decoded.decode_replacement_count > 0).then(|| {
        format!(
            "process {stream_name} decoding used replacement characters; inspect raw bytes by sha256"
        )
    });

    ProcessStreamOutputView {
        model_text,
        redacted,
        metadata: json!({
            "size_bytes": size_bytes,
            "captured_bytes": size_bytes,
            "truncated": stream.truncated,
            "binary": false,
            "binary_output_omitted": false,
            "encoding": decoded.encoding,
            "decode_replacement_count": decoded.decode_replacement_count,
            "decode_warning": decode_warning,
            "sha256": sha256,
            "inline_truncated": inline_truncated,
            "head": process_text_prefix(text.as_str(), PROCESS_STREAM_HEAD_BYTES),
            "tail": process_text_suffix(text.as_str(), PROCESS_STREAM_TAIL_BYTES),
            "redacted": redacted,
            "redaction_reasons": redaction_reasons,
        }),
    }
}

fn process_output_diagnostic_summary(stdout: &StreamCapture, stderr: &StreamCapture) -> String {
    let summary = json!({
        "stdout": process_stream_diagnostic_summary("stdout", stdout),
        "stderr": process_stream_diagnostic_summary("stderr", stderr),
    });
    serde_json::to_string(&summary).unwrap_or_else(|_| "{}".to_owned())
}

fn process_stream_diagnostic_summary(stream_name: &str, stream: &StreamCapture) -> Value {
    let size_bytes = stream.bytes.len();
    if process_output_looks_binary(stream.bytes.as_slice()) {
        return json!({
            "stream": stream_name,
            "size_bytes": size_bytes,
            "truncated": stream.truncated,
            "binary": true,
            "binary_output_omitted": !stream.bytes.is_empty(),
        });
    }

    let decoded = decode_process_output_text(stream.bytes.as_slice());
    json!({
        "stream": stream_name,
        "size_bytes": size_bytes,
        "truncated": stream.truncated,
        "binary": false,
        "encoding": decoded.encoding,
        "decode_replacement_count": decoded.decode_replacement_count,
        "content_omitted": !stream.bytes.is_empty(),
    })
}

fn redacted_process_output(output: &[u8]) -> RedactedProcessOutputText {
    let decoded = decode_process_output_text(output);
    redacted_process_output_text(decoded.text.as_str())
}

fn redacted_process_output_text(value: &str) -> RedactedProcessOutputText {
    let normalized = strip_process_output_terminal_controls(value);
    let redacted_urls = redact_url_segments_in_text(normalized.as_str());
    let redacted_auth = redact_auth_error(redacted_urls.as_str());
    let redacted_paths = redact_sensitive_url_path_segments_in_text(redacted_auth.as_str());
    let export_redaction = redact_text_for_export(
        redacted_paths.as_str(),
        SafetySourceKind::ToolOutput,
        SafetyContentKind::PlainText,
        TrustLabel::TrustedLocal,
    );
    let redaction_reasons = process_redaction_reason_codes(
        normalized.as_str(),
        redacted_urls.as_str(),
        redacted_auth.as_str(),
        redacted_paths.as_str(),
        &export_redaction,
    );
    let mut redaction_reasons = redaction_reasons;
    if normalized != value {
        redaction_reasons.push("terminal_control_sequence".to_owned());
        redaction_reasons.sort();
        redaction_reasons.dedup();
    }
    let redacted_text = restore_process_output_trailing_line_endings(
        normalized.as_str(),
        export_redaction.redacted_text,
    );
    let redacted = normalized != value
        || redacted_urls != normalized
        || redacted_auth != redacted_urls
        || redacted_paths != redacted_auth
        || redacted_text != normalized;

    RedactedProcessOutputText { text: redacted_text, redacted, redaction_reasons }
}

fn strip_process_output_terminal_controls(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0usize;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte == 0x1b {
            index = index.saturating_add(ansi_escape_sequence_len(&bytes[index..]).unwrap_or(1));
            continue;
        }
        if matches!(byte, 0x01..=0x08 | 0x0b | 0x0c | 0x0e..=0x1f | 0x7f) {
            index = index.saturating_add(1);
            continue;
        }
        output.push(byte);
        index = index.saturating_add(1);
    }
    String::from_utf8_lossy(output.as_slice()).into_owned()
}

/// Applies the canonical process-output redaction boundary for another
/// host-owned runtime component.
///
/// The tuple keeps the private redaction implementation out of public
/// contracts while ensuring every model-visible process projection uses the
/// same URL, credential, secret, and export-safety policy.
pub(crate) fn redact_process_output_projection(value: &str) -> (String, bool, Vec<String>) {
    let redacted = redacted_process_output_text(value);
    (redacted.text, redacted.redacted, redacted.redaction_reasons)
}

fn process_redaction_reason_codes(
    original: &str,
    url_redacted: &str,
    auth_redacted: &str,
    path_redacted: &str,
    export_redaction: &palyra_safety::ExportRedactionOutcome,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if url_redacted != original {
        reasons.push("url_sensitive_segment".to_owned());
    }
    if auth_redacted != url_redacted {
        reasons.push("auth_or_assignment_secret".to_owned());
    }
    if path_redacted != auth_redacted {
        reasons.push("sensitive_url_path_segment".to_owned());
    }
    reasons.extend(
        export_redaction
            .scan
            .findings
            .iter()
            .filter(|finding| finding.category == SafetyFindingCategory::SecretLeak)
            .map(|finding| finding.code.clone()),
    );
    if reasons.is_empty() && export_redaction.redacted {
        reasons.push("safety_export_redaction".to_owned());
    }
    reasons.sort();
    reasons.dedup();
    reasons
}

// Export redaction may normalize away the trailing newline run; consumers assert on exact
// stdout shapes (e.g. a single trailing newline), so the original CR/LF suffix is restored.
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

fn decode_process_output_text(output: &[u8]) -> DecodedProcessOutputText {
    match std::str::from_utf8(output) {
        Ok(text) => DecodedProcessOutputText {
            text: text.to_owned(),
            encoding: "utf-8",
            decode_replacement_count: 0,
        },
        Err(_) => decode_windows_1252_process_output(output),
    }
}

fn decode_windows_1252_process_output(output: &[u8]) -> DecodedProcessOutputText {
    let mut text = String::with_capacity(output.len());
    let mut decode_replacement_count = 0;
    for byte in output {
        match windows_1252_char(*byte) {
            Some(character) => text.push(character),
            None => {
                text.push('\u{fffd}');
                decode_replacement_count += 1;
            }
        }
    }
    DecodedProcessOutputText { text, encoding: "windows-1252", decode_replacement_count }
}

fn windows_1252_char(byte: u8) -> Option<char> {
    match byte {
        0x80 => Some('\u{20ac}'),
        0x81 => None,
        0x82 => Some('\u{201a}'),
        0x83 => Some('\u{0192}'),
        0x84 => Some('\u{201e}'),
        0x85 => Some('\u{2026}'),
        0x86 => Some('\u{2020}'),
        0x87 => Some('\u{2021}'),
        0x88 => Some('\u{02c6}'),
        0x89 => Some('\u{2030}'),
        0x8a => Some('\u{0160}'),
        0x8b => Some('\u{2039}'),
        0x8c => Some('\u{0152}'),
        0x8d => None,
        0x8e => Some('\u{017d}'),
        0x8f => None,
        0x90 => None,
        0x91 => Some('\u{2018}'),
        0x92 => Some('\u{2019}'),
        0x93 => Some('\u{201c}'),
        0x94 => Some('\u{201d}'),
        0x95 => Some('\u{2022}'),
        0x96 => Some('\u{2013}'),
        0x97 => Some('\u{2014}'),
        0x98 => Some('\u{02dc}'),
        0x99 => Some('\u{2122}'),
        0x9a => Some('\u{0161}'),
        0x9b => Some('\u{203a}'),
        0x9c => Some('\u{0153}'),
        0x9d => None,
        0x9e => Some('\u{017e}'),
        0x9f => Some('\u{0178}'),
        _ => char::from_u32(u32::from(byte)),
    }
}

fn process_output_looks_binary(output: &[u8]) -> bool {
    if output.is_empty() {
        return false;
    }
    if output.contains(&0) {
        return true;
    }
    let control_bytes = non_terminal_control_byte_count(output);
    control_bytes > 8 && control_bytes.saturating_mul(100) > output.len()
}

fn non_terminal_control_byte_count(output: &[u8]) -> usize {
    let mut index = 0usize;
    let mut count = 0usize;
    while index < output.len() {
        if output[index] == 0x1b {
            if let Some(sequence_len) = ansi_escape_sequence_len(&output[index..]) {
                index = index.saturating_add(sequence_len);
                continue;
            }
        }
        if matches!(output[index], 0x01..=0x08 | 0x0b | 0x0c | 0x0e..=0x1f | 0x7f) {
            count = count.saturating_add(1);
        }
        index = index.saturating_add(1);
    }
    count
}

fn ansi_escape_sequence_len(bytes: &[u8]) -> Option<usize> {
    if bytes.len() < 2 || bytes[0] != 0x1b {
        return None;
    }
    match bytes[1] {
        b'[' => csi_escape_sequence_len(bytes),
        b']' => osc_escape_sequence_len(bytes),
        b'(' | b')' | b'*' | b'+' => (bytes.len() >= 3).then_some(3),
        b'7' | b'8' | b'c' | b'D' | b'E' | b'H' | b'M' => Some(2),
        _ => None,
    }
}

fn csi_escape_sequence_len(bytes: &[u8]) -> Option<usize> {
    let max_len = bytes.len().min(64);
    for (index, byte) in bytes.iter().copied().enumerate().take(max_len).skip(2) {
        if (0x40..=0x7e).contains(&byte) {
            return Some(index + 1);
        }
        if !(0x20..=0x3f).contains(&byte) {
            return None;
        }
    }
    None
}

fn osc_escape_sequence_len(bytes: &[u8]) -> Option<usize> {
    let max_len = bytes.len().min(1024);
    let mut index = 2usize;
    while index < max_len {
        if bytes[index] == 0x07 {
            return Some(index + 1);
        }
        if bytes[index] == 0x1b && bytes.get(index + 1) == Some(&b'\\') {
            return Some(index + 2);
        }
        index = index.saturating_add(1);
    }
    None
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn process_text_prefix(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    text.char_indices()
        .take_while(|(index, character)| index.saturating_add(character.len_utf8()) <= max_bytes)
        .map(|(_, character)| character)
        .collect()
}

fn process_text_suffix(text: &str, max_bytes: usize) -> String {
    if text.len() <= max_bytes {
        return text.to_owned();
    }
    let mut start = text.len().saturating_sub(max_bytes);
    while start < text.len() && !text.is_char_boundary(start) {
        start += 1;
    }
    text[start..].to_owned()
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
        // Case-insensitive scan: the remaining slice is lowercased per iteration while the
        // replacement edits `output` in original case; `search_start` always advances past the
        // replacement so overlapping marker hits cannot loop forever.
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

// Dispatches commands the runner implements in-process instead of spawning: process stop and
// status plus a portable subset of shell basics (pwd/echo/ls/dir/cat/type/mkdir). Returns
// Ok(None) for anything else so the caller falls through to a real spawn.
fn execute_builtin_process_command(
    policy: &SandboxProcessRunnerPolicy,
    input: &ProcessRunnerInput,
    workspace_root: &Path,
    cwd: &Path,
    process_risk: &ProcessRiskReport,
) -> Result<Option<SandboxProcessRunSuccess>, SandboxProcessRunError> {
    let command = input.command.trim();
    match command.to_ascii_lowercase().as_str() {
        "palyra.process.stop" | "palyra-process-stop" => {
            return Ok(Some(builtin_stop_process_success(command, input.args.as_slice())?));
        }
        "palyra.process.status" | "palyra-process-status" => {
            return Ok(Some(builtin_process_status_success(command, input.args.as_slice(), None)?));
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
            StreamCapture::from_text(format!("{}\n", cwd.to_string_lossy()))
        }
        "echo" => StreamCapture::from_text(format!("{}\n", input.args.join(" "))),
        "ls" | "dir" => StreamCapture::from_text(builtin_list_directory_stdout(
            command,
            input.args.as_slice(),
            workspace_root,
            cwd,
        )?),
        "cat" | "type" => builtin_read_files_stdout(
            command,
            input.args.as_slice(),
            workspace_root,
            cwd,
            policy.max_output_bytes,
        )?,
        "mkdir" => StreamCapture::from_text(builtin_make_directory_stdout(
            command,
            input.args.as_slice(),
            workspace_root,
            cwd,
        )?),
        _ => return Ok(None),
    };
    let stderr = StreamCapture::default();
    let output_json = process_success_output_json(ProcessSuccessOutputJsonInput {
        exit_code: 0,
        stdout: &stdout,
        stderr: &stderr,
        duration_ms: 0,
        tier: policy.tier.as_str(),
        sandbox_backend: "builtin_portable",
        process_risk,
        input: Some(input),
    })
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox builtin process output JSON: {error}"),
    })?;
    Ok(Some(SandboxProcessRunSuccess { output_json }))
}

const PROCESS_STOP_ACKNOWLEDGEMENT_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Serialize)]
#[serde(deny_unknown_fields)]
struct ProcessStopAcknowledgementV1<'a> {
    schema_version: u16,
    pid: u32,
    ownership_kind: &'a str,
    ownership_identity_sha256: &'a str,
    observed_at_unix_ms: i64,
    proof: ProcessStopProofV1,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
enum ProcessStopProofV1 {
    UnixSupervisorCleanupAcknowledged,
    WindowsJobObjectEmpty { active_process_count: u32 },
}

fn process_stop_acknowledgement(
    pid: u32,
    registration: &RegisteredBackgroundProcessSnapshot,
    status: BackgroundProcessRuntimeStatus,
) -> Option<ProcessStopAcknowledgementV1<'_>> {
    if status.alive() {
        return None;
    }
    let proof = match registration.provenance.ownership_kind {
        ProcessOwnershipKind::UnixProcessGroup
            if registration.unix_cleanup_acknowledged && !status.process_tree_alive =>
        {
            ProcessStopProofV1::UnixSupervisorCleanupAcknowledged
        }
        ProcessOwnershipKind::WindowsJobObject
            if !status.process_tree_alive && status.tracked_process_count == Some(0) =>
        {
            ProcessStopProofV1::WindowsJobObjectEmpty { active_process_count: 0 }
        }
        ProcessOwnershipKind::RemoteExecutionInstance => return None,
        _ => return None,
    };
    Some(ProcessStopAcknowledgementV1 {
        schema_version: PROCESS_STOP_ACKNOWLEDGEMENT_SCHEMA_VERSION,
        pid,
        ownership_kind: registration.provenance.ownership_kind.as_str(),
        ownership_identity_sha256: registration.provenance.ownership_identity_sha256.as_str(),
        observed_at_unix_ms: crate::gateway::current_unix_ms(),
        proof,
    })
}

fn builtin_stop_process_success(
    command: &str,
    args: &[String],
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let pid = parse_builtin_pid_arg(command, args)?;
    let registration = registered_background_process(command, pid)?;
    #[cfg(unix)]
    let mut registration = registration;
    // Unix termination is authorized by the registered exact supervisor capability. Requiring a
    // still-live root PID here would reject the queued acknowledgement after autonomous cleanup.
    #[cfg(not(unix))]
    if registration.active {
        require_background_process_provenance(
            pid,
            &registration.provenance,
            "palyra.process.stop",
        )?;
    }
    if !registration.active {
        #[cfg(unix)]
        let status = if registration.unix_cleanup_acknowledged {
            // The exact supervisor acknowledgement proves target-group absence. Its own process
            // can remain as an unreaped zombie until the monitor thread regains the child handle.
            Some(BackgroundProcessRuntimeStatus {
                direct_pid_alive: false,
                process_tree_alive: false,
                tracked_process_count: Some(0),
            })
        } else {
            background_process_runtime_status(pid).ok()
        };
        #[cfg(not(unix))]
        let status = background_process_runtime_status(pid).ok();
        let stop_acknowledgement = status
            .filter(|status| !status.alive())
            .and_then(|status| process_stop_acknowledgement(pid, &registration, status));
        let output_json = serde_json::to_vec(&json!({
            "exit_code": 0,
            "stdout": format!("pid={pid} stopped=true was_running=false\n"),
            "stderr": "",
            "stdout_truncated": false,
            "stderr_truncated": false,
            "stdout_redacted": false,
            "stderr_redacted": false,
            "duration_ms": 0,
            "pid": pid,
            "was_running": false,
            "stopped": true,
            "alive": false,
            "direct_pid_alive_before_stop": false,
            "process_tree_alive_before_stop": false,
            "tracked_process_count_before_stop": Option::<u32>::None,
            "direct_pid_alive": status.map(|status| status.direct_pid_alive),
            "process_tree_alive": status.map(|status| status.process_tree_alive),
            "tracked_process_count": status.and_then(|status| status.tracked_process_count),
            "stop_acknowledgement": stop_acknowledgement,
            "tier": "builtin",
            "sandbox_backend": "builtin_portable",
        }))
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("failed to serialize sandbox process stop output JSON: {error}"),
        })?;
        return Ok(SandboxProcessRunSuccess { output_json });
    }
    #[cfg(windows)]
    // The lifetime monitor may remove the registry entry as soon as termination is observed.
    // Retain the job handle so this stop call can still report its verified final process count.
    let retained_windows_job = registration.identity.windows_job.clone();
    #[cfg(unix)]
    let before_status =
        background_process_runtime_status(pid).unwrap_or(BackgroundProcessRuntimeStatus {
            direct_pid_alive: false,
            process_tree_alive: false,
            tracked_process_count: None,
        });
    #[cfg(not(unix))]
    let before_status = background_process_runtime_status_for_identity(&registration.identity)
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run builtin '{command}' failed to inspect pid {pid}: {error}"
            ),
        })?;
    let was_running = before_status.alive();
    let mut stop_error = None;
    #[cfg(unix)]
    if let Err(error) = terminate_background_process_tree_exact(pid, &registration.provenance) {
        stop_error = Some(error.to_string());
    }
    #[cfg(not(unix))]
    if was_running {
        if let Err(error) = terminate_background_process_tree_exact(pid, &registration.provenance) {
            stop_error = Some(error.to_string());
        }
    }
    #[cfg(unix)]
    let stopped = stop_error.is_none();
    #[cfg(windows)]
    let stopped = !was_running
        || retained_windows_job
            .as_deref()
            .and_then(|job| {
                wait_for_windows_background_process_inactive(
                    pid,
                    &registration.provenance,
                    job,
                    Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS),
                )
                .ok()
                .flatten()
            })
            .is_some();
    #[cfg(not(any(unix, windows)))]
    let stopped = !was_running
        || wait_for_process_not_alive(pid, Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS));
    #[cfg(windows)]
    let after_status = match retained_windows_job.as_deref() {
        Some(job) => {
            background_process_runtime_status_from_windows_job(pid, &registration.provenance, job)
        }
        None => background_process_runtime_status(pid),
    }
    .ok();
    #[cfg(unix)]
    let after_status = stopped.then_some(BackgroundProcessRuntimeStatus {
        direct_pid_alive: false,
        process_tree_alive: false,
        tracked_process_count: Some(0),
    });
    #[cfg(not(any(unix, windows)))]
    let after_status = background_process_runtime_status(pid).ok();
    // When the post-stop probe fails, report alive=true: claiming a process is gone without
    // evidence would let callers skip cleanup.
    let alive = !stopped && after_status.map(BackgroundProcessRuntimeStatus::alive).unwrap_or(true);
    // A termination error only fails the builtin if the process is in fact still running;
    // e.g. a race where the tree exited between the kill attempt and the liveness probe.
    if let Some(error) = stop_error.as_ref().filter(|_| !stopped) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run builtin '{command}' failed to stop pid {pid}: {error}"
            ),
        });
    }
    if !alive {
        #[cfg(unix)]
        if stopped && stop_error.is_none() {
            mark_background_process_stopped_after_unix_cleanup(&registration.identity);
            registration.unix_cleanup_acknowledged = true;
        } else {
            mark_background_process_stopped(&registration.identity);
        }
        #[cfg(not(unix))]
        mark_background_process_stopped(&registration.identity);
        record_background_process_terminal_state(
            &registration.identity,
            "stopped",
            "explicit_stop",
            None,
        );
    }
    let stop_acknowledgement = after_status
        .filter(|status| !status.alive())
        .and_then(|status| process_stop_acknowledgement(pid, &registration, status));
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
        "stop_acknowledgement": stop_acknowledgement,
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
    expected_provenance: Option<&ProcessProvenance>,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let pid = parse_builtin_pid_arg(command, args)?;
    let registration = registered_background_process(command, pid)?;
    if expected_provenance.is_some_and(|expected| registration.provenance != *expected) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.run builtin '{command}' rejected stale provenance for pid {pid}"
            ),
        });
    }
    if registration.active {
        require_background_process_provenance(
            pid,
            &registration.provenance,
            "palyra.process.status",
        )?;
    }
    if !registration.active {
        let terminal = registration.terminal.unwrap_or(BackgroundProcessTerminalState {
            process_state: "exited",
            completion_reason: "process_tree_inactive",
            exit_code: None,
            completed_at_unix_ms: unix_time_ms(),
        });
        let terminal_frame =
            background_terminal_frame_snapshot(registration.output_monitor.as_ref());
        let output_json = serde_json::to_vec(&json!({
            "exit_code": 0,
            "stdout": format!("pid={pid} alive=false direct_pid_alive=false process_tree_alive=false\n"),
            "stderr": "",
            "stdout_truncated": false,
            "stderr_truncated": false,
            "stdout_redacted": false,
            "stderr_redacted": false,
            "duration_ms": 0,
            "pid": pid,
            "completed": true,
            "process_state": terminal.process_state,
            "completion_reason": terminal.completion_reason,
            "process_exit_code": terminal.exit_code,
            "completed_at_unix_ms": terminal.completed_at_unix_ms,
            "terminal_frame": terminal_frame,
            "alive": false,
            "direct_pid_alive": false,
            "process_tree_alive": false,
            "tracked_process_count": Option::<u32>::None,
            "tier": "builtin",
            "sandbox_backend": "builtin_portable",
        }))
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("failed to serialize sandbox process status output JSON: {error}"),
        })?;
        return Ok(SandboxProcessRunSuccess { output_json });
    }
    let status = background_process_runtime_status_for_identity(&registration.identity).map_err(
        |error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run builtin '{command}' failed to inspect pid {pid}: {error}"
            ),
        },
    )?;
    let alive = status.alive();
    if !alive {
        mark_background_process_stopped(&registration.identity);
    }
    let terminal =
        (!alive).then(|| registered_background_process(command, pid).ok()?.terminal).flatten();
    let process_state = terminal
        .as_ref()
        .map_or(if alive { "running" } else { "exited" }, |state| state.process_state);
    let completion_reason = terminal.as_ref().map(|state| state.completion_reason);
    let process_exit_code = terminal.as_ref().and_then(|state| state.exit_code);
    let completed_at_unix_ms = terminal.as_ref().map(|state| state.completed_at_unix_ms);
    let terminal_frame = if alive {
        Value::Null
    } else {
        background_terminal_frame_snapshot(registration.output_monitor.as_ref())
    };
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
        "completed": !alive,
        "process_state": process_state,
        "completion_reason": completion_reason,
        "process_exit_code": process_exit_code,
        "completed_at_unix_ms": completed_at_unix_ms,
        "terminal_frame": terminal_frame,
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

/// Terminates the background process tree rooted at `pid` via the portable stop builtin and
/// returns its serialized stop report.
///
/// # Errors
///
/// Returns `RuntimeFailure` when the pid cannot be inspected or the tree is still alive after
/// the termination attempt.
pub(crate) fn stop_background_process_by_pid(
    pid: u32,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let args = [pid.to_string()];
    builtin_stop_process_success("palyra.process.stop", &args)
}

/// Reports liveness of the background process tree rooted at `pid` via the portable status
/// builtin.
///
/// # Errors
///
/// Returns `RuntimeFailure` when the pid cannot be inspected on this platform.
pub(crate) fn background_process_status_by_pid(
    pid: u32,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let args = [pid.to_string()];
    builtin_process_status_success("palyra.process.status", &args, None)
}

/// Reports a retained process status only when its full provenance still matches.
///
/// # Errors
/// Returns `InvalidInput` when the PID has been reused or no longer identifies the expected
/// process registration.
pub(crate) fn background_process_status_by_pid_exact(
    pid: u32,
    expected_provenance: &ProcessProvenance,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let args = [pid.to_string()];
    builtin_process_status_success("palyra.process.status", &args, Some(expected_provenance))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessInputToolInput {
    pid: u32,
    input: String,
    #[serde(default)]
    append_newline: bool,
}

/// Writes bounded stdin text into a live background process that was started
/// with stdin capability.
///
/// # Errors
/// Returns `InvalidInput` for malformed input, unregistered/non-stdin handles,
/// exhausted per-process write budgets, or inactive processes; returns
/// `RuntimeFailure` for registry lock or pipe-write failures.
pub(crate) fn write_background_process_stdin(
    input_json: &[u8],
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let input = parse_process_input_tool_input(input_json)?;
    let mut payload = input.input.into_bytes();
    if input.append_newline {
        payload.push(b'\n');
    }
    validate_process_stdin_payload(input.pid, payload.as_slice())?;
    let registered = registered_background_process("palyra.process.input", input.pid)?;
    if !registered.active {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.input pid {} is no longer active", input.pid),
        });
    }
    require_background_process_provenance(
        input.pid,
        &registered.provenance,
        "palyra.process.input",
    )?;
    let status =
        background_process_runtime_status(input.pid).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.input failed to inspect pid {} before stdin write: {error}",
                input.pid
            ),
        })?;
    if !status.alive() {
        mark_background_process_stopped(&registered.identity);
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.input pid {} is not alive", input.pid),
        });
    }

    let (stdin_state, capabilities, lifetime_mode, expected_provenance) = {
        let processes =
            registered_background_processes().lock().map_err(|error| SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "background process registry lock poisoned for pid {}: {error}",
                    input.pid
                ),
            })?;
        let process = processes.get(&input.pid).ok_or_else(|| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.input requires a pid returned by a live palyra.process.run background result; pid {} is not registered",
                input.pid
            ),
        })?;
        if !process.active {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!("palyra.process.input pid {} is no longer active", input.pid),
            });
        }
        if !process.capabilities.stdin {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.input pid {} was not started with stdin capability",
                    input.pid
                ),
            });
        }
        let stdin_state = process.stdin.clone().ok_or_else(|| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.input pid {} has no writable stdin handle", input.pid),
        })?;
        (stdin_state, process.capabilities, process.lifetime_mode, process.provenance.clone())
    };
    let stdin_handle = Arc::clone(&stdin_state);
    let mut stdin_guard = stdin_state.lock().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("background stdin lock poisoned for pid {}: {error}", input.pid),
    })?;
    if stdin_guard.events_written >= PROCESS_STDIN_MAX_EVENTS {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.input pid {} exceeded {PROCESS_STDIN_MAX_EVENTS} stdin writes",
                input.pid
            ),
        });
    }
    if stdin_guard.bytes_written.saturating_add(payload.len()) > PROCESS_STDIN_TOTAL_MAX_BYTES {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.input pid {} exceeded {PROCESS_STDIN_TOTAL_MAX_BYTES} total stdin bytes",
                input.pid
            ),
        });
    }
    stdin_guard.stdin.write_all(payload.as_slice()).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "palyra.process.input failed to write stdin to pid {}: {error}",
            input.pid
        ),
    })?;
    stdin_guard.stdin.flush().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "palyra.process.input failed to flush stdin to pid {}: {error}",
            input.pid
        ),
    })?;
    stdin_guard.bytes_written = stdin_guard.bytes_written.saturating_add(payload.len());
    stdin_guard.events_written = stdin_guard.events_written.saturating_add(1);
    let bytes_written_total = stdin_guard.bytes_written;
    let events_written_total = stdin_guard.events_written;
    drop(stdin_guard);
    let still_current = registered_background_processes()
        .lock()
        .map(|processes| {
            processes.get(&input.pid).is_some_and(|process| {
                process.active
                    && process.provenance == expected_provenance
                    && process
                        .stdin
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(current, &stdin_handle))
            })
        })
        .unwrap_or(false);
    if !still_current {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.input pid {} changed ownership while stdin was being written",
                input.pid
            ),
        });
    }

    let output_json = serde_json::to_vec(&json!({
        "exit_code": 0,
        "stdout": format!("pid={} input_delivered=true bytes_written={}\n", input.pid, payload.len()),
        "stderr": "",
        "stdout_truncated": false,
        "stderr_truncated": false,
        "stdout_redacted": false,
        "stderr_redacted": false,
        "duration_ms": 0,
        "pid": input.pid,
        "input_delivered": true,
        "bytes_written": payload.len(),
        "stdin_redacted": true,
        "stdin_redaction_level": "input_redacted",
        "stdin_events_written": events_written_total,
        "stdin_bytes_written": bytes_written_total,
        "process_handle": {
            "kind": "pid",
            "direct_process_pid": input.pid,
            "capabilities": process_handle_capabilities_json(
                capabilities,
                &[],
                lifetime_mode
            ),
        },
        "tier": "builtin",
        "sandbox_backend": "builtin_portable",
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox process input output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

fn parse_process_input_tool_input(
    input_json: &[u8],
) -> Result<ProcessInputToolInput, SandboxProcessRunError> {
    let input = serde_json::from_slice::<ProcessInputToolInput>(input_json).map_err(|error| {
        SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.input invalid JSON: {error}"),
        }
    })?;
    if input.pid == 0 {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.input pid must be greater than 0".to_owned(),
        });
    }
    Ok(input)
}

fn validate_process_stdin_payload(pid: u32, payload: &[u8]) -> Result<(), SandboxProcessRunError> {
    if payload.is_empty() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.input pid {pid} input must not be empty"),
        });
    }
    if payload.len() > PROCESS_STDIN_INPUT_MAX_BYTES {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.input pid {pid} input exceeds {PROCESS_STDIN_INPUT_MAX_BYTES} bytes"
            ),
        });
    }
    if payload.contains(&0) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.input pid {pid} input contains a NUL byte"),
        });
    }
    Ok(())
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessSendKeysToolInput {
    pid: u32,
    keys: Vec<ProcessSendKeyInput>,
    #[serde(default)]
    allow_stdin_fallback: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessSendKeyInput {
    key: String,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    repeat: Option<u8>,
}

/// Sends bounded, allowlisted key actions to an interactive background process.
///
/// # Errors
/// Returns `InvalidInput` for malformed schemas, disallowed keys, unregistered
/// PIDs, inactive processes, or stdin fallback attempts without a writable
/// handle. Unsupported PTY availability is reported as a degraded JSON result
/// unless `allow_stdin_fallback=true` permits the known-key stdin fallback.
pub(crate) fn send_keys_to_background_process(
    input_json: &[u8],
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let input = parse_process_send_keys_tool_input(input_json)?;
    let payload = process_send_keys_payload(&input)?;
    let snapshot = registered_background_process("palyra.process.send_keys", input.pid)?;
    if !snapshot.active {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.send_keys pid {} is no longer active", input.pid),
        });
    }
    require_background_process_provenance(
        input.pid,
        &snapshot.provenance,
        "palyra.process.send_keys",
    )?;
    let status =
        background_process_runtime_status(input.pid).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.send_keys failed to inspect pid {} before key send: {error}",
                input.pid
            ),
        })?;
    if !status.alive() {
        mark_background_process_stopped(&snapshot.identity);
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.send_keys pid {} is not alive", input.pid),
        });
    }
    if !snapshot.capabilities.pty && !input.allow_stdin_fallback {
        return send_keys_degraded_output(
            input.pid,
            payload.len(),
            snapshot.capabilities,
            snapshot.lifetime_mode,
            "pty_backend_unavailable",
            "PTY is not available in this process runner; retry with allow_stdin_fallback=true only when ordinary stdin key bytes are acceptable for the target process.",
        );
    }
    if !snapshot.capabilities.pty && !snapshot.capabilities.stdin {
        return send_keys_degraded_output(
            input.pid,
            payload.len(),
            snapshot.capabilities,
            snapshot.lifetime_mode,
            "stdin_fallback_unavailable",
            "PTY is unavailable and this process was not started with a writable stdin fallback handle.",
        );
    }

    let input_text =
        String::from_utf8(payload.clone()).map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!("palyra.process.send_keys generated invalid UTF-8 payload: {error}"),
        })?;
    let stdin_input = serde_json::to_vec(&json!({
        "pid": input.pid,
        "input": input_text,
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize palyra.process.send_keys stdin fallback: {error}"),
    })?;
    write_background_process_stdin(stdin_input.as_slice())?;
    let frame = background_terminal_frame_snapshot(snapshot.output_monitor.as_ref());
    send_keys_success_output(
        input.pid,
        payload.len(),
        snapshot.capabilities,
        snapshot.lifetime_mode,
        !snapshot.capabilities.pty,
        frame,
    )
}

fn parse_process_send_keys_tool_input(
    input_json: &[u8],
) -> Result<ProcessSendKeysToolInput, SandboxProcessRunError> {
    let input =
        serde_json::from_slice::<ProcessSendKeysToolInput>(input_json).map_err(|error| {
            SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!("palyra.process.send_keys invalid JSON: {error}"),
            }
        })?;
    if input.pid == 0 {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.send_keys pid must be greater than 0".to_owned(),
        });
    }
    if input.keys.is_empty() || input.keys.len() > PROCESS_SEND_KEYS_MAX_ACTIONS {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.send_keys keys must contain 1..={PROCESS_SEND_KEYS_MAX_ACTIONS} actions"
            ),
        });
    }
    Ok(input)
}

fn process_send_keys_payload(
    input: &ProcessSendKeysToolInput,
) -> Result<Vec<u8>, SandboxProcessRunError> {
    let mut payload = Vec::new();
    for action in &input.keys {
        let repeat = action.repeat.unwrap_or(1);
        if repeat == 0 || repeat > PROCESS_SEND_KEYS_MAX_REPEAT {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.send_keys repeat must be 1..={PROCESS_SEND_KEYS_MAX_REPEAT}"
                ),
            });
        }
        let bytes = process_send_key_bytes(action)?;
        for _ in 0..repeat {
            payload.extend_from_slice(bytes.as_slice());
        }
        if payload.len() > PROCESS_STDIN_INPUT_MAX_BYTES {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.send_keys payload exceeds {PROCESS_STDIN_INPUT_MAX_BYTES} bytes"
                ),
            });
        }
    }
    Ok(payload)
}

fn process_send_key_bytes(action: &ProcessSendKeyInput) -> Result<Vec<u8>, SandboxProcessRunError> {
    let key = action.key.trim().to_ascii_lowercase();
    if key == "text" {
        let text = action.text.as_deref().ok_or_else(|| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.send_keys key='text' requires text".to_owned(),
        })?;
        if text.is_empty() || text.len() > PROCESS_SEND_KEYS_TEXT_MAX_BYTES {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.send_keys text must contain 1..={PROCESS_SEND_KEYS_TEXT_MAX_BYTES} bytes"
                ),
            });
        }
        if text.chars().any(char::is_control) {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: "palyra.process.send_keys text must not contain control characters"
                    .to_owned(),
            });
        }
        return Ok(text.as_bytes().to_vec());
    }
    if action.text.is_some() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.send_keys text is only allowed with key='text'".to_owned(),
        });
    }
    let bytes = match key.as_str() {
        "enter" => b"\n".as_slice(),
        "tab" => b"\t".as_slice(),
        "backspace" => b"\x08".as_slice(),
        "escape" => b"\x1b".as_slice(),
        "ctrl_c" => b"\x03".as_slice(),
        "ctrl_d" => b"\x04".as_slice(),
        "up" => b"\x1b[A".as_slice(),
        "down" => b"\x1b[B".as_slice(),
        "right" => b"\x1b[C".as_slice(),
        "left" => b"\x1b[D".as_slice(),
        _ => {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.send_keys unsupported key '{}'",
                    action.key.trim()
                ),
            });
        }
    };
    Ok(bytes.to_vec())
}

fn background_terminal_frame_snapshot(monitor: Option<&BackgroundOutputMonitor>) -> Value {
    let snapshot = monitor.map(BackgroundOutputMonitor::snapshot);
    let Some((stdout, stderr)) = snapshot else {
        return json!({
            "available": false,
            "reason_code": "terminal_frame_unavailable",
        });
    };
    json!({
        "available": true,
        "stdout": terminal_frame_stream_snapshot("stdout", stdout),
        "stderr": terminal_frame_stream_snapshot("stderr", stderr),
    })
}

fn terminal_frame_stream_snapshot(stream_name: &str, capture: StreamCapture) -> Value {
    let redacted = redacted_process_output(capture.bytes.as_slice());
    json!({
        "stream": stream_name,
        "bytes": capture.bytes.len(),
        "truncated": capture.truncated,
        "redacted": redacted.redacted,
        "redaction_reasons": redacted.redaction_reasons,
        "tail": process_text_suffix(redacted.text.as_str(), PROCESS_TERMINAL_FRAME_TEXT_BYTES),
        "read_error": capture.read_error,
    })
}

fn send_keys_degraded_output(
    pid: u32,
    payload_bytes: usize,
    capabilities: BackgroundProcessHandleCapabilities,
    lifetime_mode: BackgroundLifetimeMode,
    reason_code: &str,
    note: &str,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    send_keys_output_json(
        json!({
            "sent": false,
            "degraded": true,
            "degraded_reason": reason_code,
            "note": note,
        }),
        pid,
        payload_bytes,
        capabilities,
        lifetime_mode,
        false,
        Value::Null,
    )
}

fn send_keys_success_output(
    pid: u32,
    payload_bytes: usize,
    capabilities: BackgroundProcessHandleCapabilities,
    lifetime_mode: BackgroundLifetimeMode,
    fallback_used: bool,
    terminal_frame: Value,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    send_keys_output_json(
        json!({
            "sent": true,
            "degraded": false,
            "degraded_reason": Value::Null,
            "note": Value::Null,
        }),
        pid,
        payload_bytes,
        capabilities,
        lifetime_mode,
        fallback_used,
        terminal_frame,
    )
}

fn send_keys_output_json(
    status: Value,
    pid: u32,
    payload_bytes: usize,
    capabilities: BackgroundProcessHandleCapabilities,
    lifetime_mode: BackgroundLifetimeMode,
    fallback_used: bool,
    terminal_frame: Value,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let output_json = serde_json::to_vec(&json!({
        "exit_code": 0,
        "stdout": if status.get("sent").and_then(Value::as_bool) == Some(true) {
            format!("pid={pid} keys_sent=true bytes={payload_bytes}\n")
        } else {
            format!("pid={pid} keys_sent=false degraded_reason={}\n", status.get("degraded_reason").and_then(Value::as_str).unwrap_or("unknown"))
        },
        "stderr": "",
        "stdout_truncated": false,
        "stderr_truncated": false,
        "stdout_redacted": false,
        "stderr_redacted": false,
        "duration_ms": 0,
        "pid": pid,
        "keys_sent": status.get("sent").and_then(Value::as_bool).unwrap_or(false),
        "payload_bytes": payload_bytes,
        "keys_redacted": true,
        "keys_redaction_level": "input_redacted",
        "fallback_used": fallback_used,
        "degraded": status.get("degraded").and_then(Value::as_bool).unwrap_or(false),
        "degraded_reason": status.get("degraded_reason").cloned().unwrap_or(Value::Null),
        "note": status.get("note").cloned().unwrap_or(Value::Null),
        "process_handle": {
            "kind": "pid",
            "direct_process_pid": pid,
            "capabilities": process_handle_capabilities_json(capabilities, &[], lifetime_mode),
        },
        "terminal_frame": terminal_frame,
        "tier": "builtin",
        "sandbox_backend": "builtin_portable",
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox process send_keys output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

/// Probes direct-pid and process-tree liveness for a background process.
///
/// # Errors
///
/// Returns the underlying OS error when liveness cannot be determined (e.g. permission errors
/// on Unix `kill(pid, 0)`, or a failed job-object query while the direct pid is already gone).
pub(crate) fn background_process_runtime_status(
    pid: u32,
) -> io::Result<BackgroundProcessRuntimeStatus> {
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
fn background_process_runtime_status_from_windows_job(
    pid: u32,
    expected: &ProcessProvenance,
    job: &WindowsBackgroundJob,
) -> io::Result<BackgroundProcessRuntimeStatus> {
    let direct_pid_alive = match current_process_start_token(pid)? {
        Some(start_token) if start_token == expected.start_token => process_id_is_alive(pid)?,
        Some(_) | None => false,
    };
    let tracked_process_count = job.active_process_count()?;
    Ok(BackgroundProcessRuntimeStatus {
        direct_pid_alive,
        process_tree_alive: tracked_process_count > 0,
        tracked_process_count: Some(tracked_process_count),
    })
}

#[cfg(windows)]
fn wait_for_windows_background_process_inactive(
    pid: u32,
    expected: &ProcessProvenance,
    job: &WindowsBackgroundJob,
    max_wait: Duration,
) -> io::Result<Option<BackgroundProcessRuntimeStatus>> {
    let started_at = Instant::now();
    loop {
        let status = background_process_runtime_status_from_windows_job(pid, expected, job)?;
        if !status.alive() {
            return Ok(Some(status));
        }
        if started_at.elapsed() >= max_wait {
            return Ok(None);
        }
        thread::sleep(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS));
    }
}

#[cfg(windows)]
fn background_process_runtime_status_for_identity(
    identity: &BackgroundProcessIdentity,
) -> io::Result<BackgroundProcessRuntimeStatus> {
    let job = identity.windows_job.as_ref().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!(
                "background process {} has no exact Windows Job Object capability",
                identity.pid
            ),
        )
    })?;
    background_process_runtime_status_from_windows_job(identity.pid, &identity.provenance, job)
}

#[cfg(not(windows))]
fn background_process_runtime_status_for_identity(
    identity: &BackgroundProcessIdentity,
) -> io::Result<BackgroundProcessRuntimeStatus> {
    background_process_runtime_status(identity.pid)
}

#[cfg(windows)]
fn background_process_tree_status(
    pid: u32,
    direct_pid_alive: bool,
) -> io::Result<(bool, Option<u32>)> {
    match windows_background_job_active_process_count(pid) {
        Some(Ok(active_count)) => Ok((active_count > 0, Some(active_count))),
        // A failed job query is only fatal when the direct pid is also gone; otherwise the
        // direct pid still gives a truthful (if tree-blind) liveness answer.
        Some(Err(error)) if !direct_pid_alive => Err(error),
        Some(Err(_)) | None => Ok((direct_pid_alive, None)),
    }
}

#[cfg(unix)]
fn background_process_tree_status(
    pid: u32,
    _direct_pid_alive: bool,
) -> io::Result<(bool, Option<u32>)> {
    Ok((unix_process_group_is_alive(pid)?, None))
}

#[cfg(not(any(unix, windows)))]
fn background_process_tree_status(
    _pid: u32,
    direct_pid_alive: bool,
) -> io::Result<(bool, Option<u32>)> {
    Ok((direct_pid_alive, None))
}

#[cfg(any(test, not(any(unix, windows))))]
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

/// Waits for an ownership-root PID to be reaped in cross-module regression tests.
#[cfg(test)]
pub(crate) fn wait_for_background_process_reap_for_test(pid: u32, max_wait: Duration) -> bool {
    wait_for_process_not_alive(pid, max_wait)
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
) -> Result<StreamCapture, SandboxProcessRunError> {
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
        // Read one byte beyond the remaining budget purely to detect truncation; the extra
        // byte is dropped below and never reaches the output.
        Read::by_ref(&mut file).take((remaining + 1) as u64).read_to_end(&mut chunk).map_err(
            |error| SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "palyra.process.run builtin '{command}' failed to read '{}': {error}",
                    file_path.display()
                ),
            },
        )?;
        if chunk.len() > remaining {
            output.extend_from_slice(&chunk[..remaining]);
            truncated = true;
            break;
        }
        output.extend_from_slice(chunk.as_slice());
    }

    if truncated {
        output.extend_from_slice(format!("\n... truncated after {max_bytes} bytes\n").as_bytes());
    }
    Ok(StreamCapture { bytes: output, truncated, read_error: None })
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
        let target = resolve_scoped_path(workspace_root, cwd, directory, false)?;
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
    if input.command.chars().any(char::is_whitespace)
        && !command_is_existing_executable_path(input.command.as_str())
    {
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
    validate_process_prepend_path_shape(input.prepend_path.as_slice())?;
    if let Some(timeout_ms) = input.timeout_ms {
        if timeout_ms == 0 {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: "palyra.process.run timeout_ms must be greater than 0".to_owned(),
            });
        }
    }
    if input.stdin_requested() && !input.background {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.run stdin=true requires background=true".to_owned(),
        });
    }
    if input.pty && !input.background {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.run pty=true requires background=true".to_owned(),
        });
    }
    if input.port_hints.len() > MAX_PROCESS_PORT_HINTS {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.run port_hints supports at most {MAX_PROCESS_PORT_HINTS} entries"
            ),
        });
    }
    validate_process_env_profile(input.env_profile_id.as_deref())?;
    validate_process_watch_patterns(input.watch_patterns.as_slice())?;
    validate_process_elevated_intent(input)?;
    validate_process_facade_mapping(input)?;
    Ok(())
}

fn validate_process_env_profile(
    env_profile_id: Option<&str>,
) -> Result<(), SandboxProcessRunError> {
    let Some(env_profile_id) = env_profile_id.map(str::trim).filter(|value| !value.is_empty())
    else {
        return Ok(());
    };
    if env_profile_id.len() > MAX_ENV_PROFILE_ID_LENGTH
        || env_profile_id.contains('\0')
        || !env_profile_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':'))
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: "palyra.process.run env_profile_id must be a non-empty profile identifier using ASCII letters, digits, '.', '-', '_' or ':'"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_process_watch_patterns(
    patterns: &[palyra_common::process_runner_input::ProcessWatchPattern],
) -> Result<(), SandboxProcessRunError> {
    if patterns.len() > MAX_WATCH_PATTERNS {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.run watch_patterns supports at most {MAX_WATCH_PATTERNS} entries"
            ),
        });
    }
    let mut names = BTreeSet::new();
    for pattern in patterns {
        let name = pattern.name.trim();
        if name.is_empty()
            || name.len() > MAX_WATCH_PATTERN_NAME_LENGTH
            || name.contains('\0')
            || !name.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.'))
        {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message:
                    "palyra.process.run watch_patterns[].name must be a bounded ASCII identifier"
                        .to_owned(),
            });
        }
        if !names.insert(name.to_ascii_lowercase()) {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!("palyra.process.run watch pattern name '{name}' is duplicated"),
            });
        }
        let value = pattern.pattern.trim();
        if value.is_empty() || value.len() > MAX_WATCH_PATTERN_LENGTH || value.contains('\0') {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.run watch pattern '{}' must be non-empty and at most {MAX_WATCH_PATTERN_LENGTH} bytes",
                    name
                ),
            });
        }
    }
    Ok(())
}

fn validate_process_elevated_intent(
    input: &ProcessRunnerInput,
) -> Result<(), SandboxProcessRunError> {
    if !process_input_has_elevated_intent(input) || input.elevated_intent {
        return Ok(());
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: "palyra.process.run detected privilege escalation intent; set elevated_intent=true so the approval and audit path records the elevated posture"
            .to_owned(),
    })
}

fn validate_process_facade_mapping(
    input: &ProcessRunnerInput,
) -> Result<(), SandboxProcessRunError> {
    let Some(mapping) = input.facade_mapping.as_ref() else {
        return Ok(());
    };
    if mapping.original_tool_name == "palyra.exec.run"
        && mapping.canonical_tool_name == "palyra.process.run"
    {
        return Ok(());
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: "palyra.process.run facade_mapping must map palyra.exec.run to palyra.process.run"
            .to_owned(),
    })
}

fn process_input_has_elevated_intent(input: &ProcessRunnerInput) -> bool {
    let command = Path::new(input.command.as_str())
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(input.command.as_str())
        .trim()
        .to_ascii_lowercase();
    matches!(command.as_str(), "sudo" | "su" | "doas" | "runas" | "pkexec")
        || input.args.iter().any(|arg| {
            let normalized = arg.trim().to_ascii_lowercase();
            matches!(normalized.as_str(), "sudo" | "su" | "doas" | "runas" | "pkexec")
                || normalized.starts_with("sudo ")
                || normalized.starts_with("doas ")
        })
}

fn validate_background_lifetime_mode(
    input: &ProcessRunnerInput,
) -> Result<(), SandboxProcessRunError> {
    let lifetime_mode = input.effective_lifetime_mode();
    if lifetime_mode.is_detached_handoff() && !input.background {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.run lifetime_mode='{}' requires background=true",
                lifetime_mode.as_str()
            ),
        });
    }
    Ok(())
}

fn validate_background_registration_fence(
    input: &ProcessRunnerInput,
    registration_fence: Option<&BackgroundProcessRegistrationFence>,
) -> Result<(), SandboxProcessRunError> {
    if !input.background {
        return Ok(());
    }
    if input.background && registration_fence.is_none() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: "local background process execution requires a host-owned durable registration fence"
                .to_owned(),
        });
    }
    if input.effective_lifetime_mode().is_detached_handoff() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "local background lifetime_mode='{}' is unavailable until durable detached process handoff is implemented",
                input.effective_lifetime_mode().as_str()
            ),
        });
    }
    Ok(())
}

fn validate_process_prepend_path_shape(paths: &[String]) -> Result<(), SandboxProcessRunError> {
    if paths.len() > MAX_PREPEND_PATH_COUNT {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::InvalidInput,
            message: format!(
                "palyra.process.run prepend_path supports at most {MAX_PREPEND_PATH_COUNT} entries"
            ),
        });
    }

    for path in paths {
        if path.trim().is_empty() || path.trim() != path {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: "palyra.process.run prepend_path entries must be non-empty paths without leading or trailing whitespace"
                    .to_owned(),
            });
        }
        if path.len() > MAX_PREPEND_PATH_LENGTH {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: format!(
                    "palyra.process.run prepend_path entry exceeds {MAX_PREPEND_PATH_LENGTH} characters"
                ),
            });
        }
        if path.contains('\0') {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::InvalidInput,
                message: "palyra.process.run prepend_path entry contains a NUL byte".to_owned(),
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
    let key = key.to_ascii_uppercase();
    if matches!(
        key.as_str(),
        "PATH"
            | "PATHEXT"
            | "HOME"
            | "USERPROFILE"
            | "HOMEDRIVE"
            | "HOMEPATH"
            | "APPDATA"
            | "LOCALAPPDATA"
            | "XDG_CONFIG_HOME"
            | "XDG_CACHE_HOME"
            | "XDG_DATA_HOME"
            | "HTTP_PROXY"
            | "HTTPS_PROXY"
            | "ALL_PROXY"
            | "NO_PROXY"
            | "NETRC"
            | "CURL_HOME"
            | "GIT_CONFIG_GLOBAL"
            | "GIT_CONFIG_SYSTEM"
            | "GIT_ASKPASS"
            | "GIT_SSH"
            | "GIT_SSH_COMMAND"
            | "AWS_CONFIG_FILE"
            | "AWS_SHARED_CREDENTIALS_FILE"
            | "GOOGLE_APPLICATION_CREDENTIALS"
            | "KUBECONFIG"
            | "DOCKER_CONFIG"
            | "NPM_CONFIG_USERCONFIG"
            | "NODE_DISABLE_COMPILE_CACHE"
            | "PIP_CONFIG_FILE"
            | "REQUESTS_CA_BUNDLE"
            | "SSL_CERT_FILE"
            | "SSL_CERT_DIR"
            | "SSH_AUTH_SOCK"
            | "PALYRA_CONFIG"
            | "PALYRA_STATE_ROOT"
            | "PALYRA_HOME"
            | "PALYRA_CLI_PROFILE"
            | "PALYRA_CLI_PROFILES_PATH"
            | "PALYRA_VAULT_DIR"
    ) {
        return true;
    }
    key.starts_with("LD_")
        || key.starts_with("DYLD_")
        || key.ends_with("_PROXY")
        || key.starts_with("NPM_CONFIG_")
        || key.starts_with("YARN_")
        || key.starts_with("PIP_")
        || key.starts_with("AWS_")
        || key.starts_with("GOOGLE_")
        || key.starts_with("GIT_CONFIG_")
}

fn redact_env_key_for_error(key: &str) -> String {
    key.chars().take(MAX_ENV_KEY_LENGTH).collect()
}

fn process_runner_command_with_args_message(command: &str) -> String {
    if command_has_path_separator(command) {
        return format!(
            "palyra.process.run command contains whitespace but does not resolve to an executable path: {command:?}. Use an exact executable path in command without quotes and put executable arguments in args. Do not split executable paths at spaces."
        );
    }
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

fn command_is_existing_executable_path(command: &str) -> bool {
    command_has_path_separator(command) && Path::new(command.trim()).is_file()
}

fn command_has_path_separator(command: &str) -> bool {
    command.contains('/') || command.contains('\\')
}

// Catches the common model mistake of passing one whole command line as a single arg (e.g.
// args=["node -e ..."]); such strings would otherwise reach the child as a literal argument
// and fail confusingly, or worse, be re-tokenized by .cmd shims on Windows.
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
            "palyra.process.run args must be an array of executable arguments, not a single command-line string; got args=[{arg:?}]. Use command={:?} and split each argument into its own args entry, for example args=[\"scripts/check.js\"] for a Node script file.",
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

// Allowlist matching accepts either the raw command or its normalized basename (extension
// stripped, lowercased) so "node", "node.exe", and a full path to node all match one entry.
// Full executable paths are reserved for host-access mode; sandboxed runs must use bare names
// so the runner controls resolution.
fn validate_allowed_executable(
    policy: &SandboxProcessRunnerPolicy,
    command: &str,
) -> Result<(), SandboxProcessRunError> {
    let normalized = normalize_process_executable_token(command);
    if command_has_path_separator(command) {
        if !process_runner_accepts_host_path_fields(policy) {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                message: "sandbox denied: executable paths require host path access; use a bare executable name for workspace-only sandboxed execution"
                    .to_owned(),
            });
        }
        validate_allowed_executable_name(policy, command, normalized.as_str())?;
        validate_allowed_interpreter(policy, command, normalized.as_str())?;
        return Ok(());
    }
    validate_allowed_executable_name(policy, command, normalized.as_str())?;
    validate_allowed_interpreter(policy, command, normalized.as_str())
}

fn validate_allowed_executable_name(
    policy: &SandboxProcessRunnerPolicy,
    command: &str,
    normalized: &str,
) -> Result<(), SandboxProcessRunError> {
    if normalized.is_empty() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "sandbox denied: command must resolve to an executable name".to_owned(),
        });
    }
    if !policy.allowed_executables.iter().any(|allowed| {
        allowed.eq_ignore_ascii_case(command) || allowed.eq_ignore_ascii_case(normalized)
    }) && !policy.allowed_executables.iter().any(|allowed| allowed.trim() == "*")
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

fn validate_allowed_interpreter(
    policy: &SandboxProcessRunnerPolicy,
    command: &str,
    normalized: &str,
) -> Result<(), SandboxProcessRunError> {
    if process_executable_is_interpreter(normalized) && !policy.allow_interpreters {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: interpreter executable '{command}' requires explicit process runner allow_interpreters=true"
            ),
        });
    }
    Ok(())
}

// Name-based process termination (pkill, taskkill /IM, Stop-Process -Name, and PowerShell
// pipelines that emulate them) can kill unrelated host processes, so only PID-scoped cleanup
// is allowed through.
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
    normalize_process_executable_token(command)
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

// Interpreters get a second scan beyond ordinary argument scoping because their arguments can
// embed absolute paths inside source text (e.g. open('/etc/passwd') passed as inline code):
// interpreter-level shell-eval flags are denied, and any absolute-path-like substring must
// itself resolve inside the workspace.
fn validate_interpreter_argument_guardrails(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    if !process_executable_is_interpreter(command.trim()) {
        return Ok(());
    }

    if interpreter_args_contain_blocked_eval_flag(command, args) {
        return Err(interpreter_shell_eval_denied_error(command));
    }

    for (index, argument) in args.iter().enumerate() {
        if argument_is_non_path_option_assignment(argument.as_str()) {
            continue;
        }
        if let Some(previous) = index.checked_sub(1).and_then(|previous| args.get(previous)) {
            if command_option_consumes_non_path_value(command, previous.as_str()) {
                continue;
            }
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

#[cfg(test)]
fn validate_host_interpreter_argument_guardrails(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    let host_roots = host_access_roots();
    validate_host_interpreter_argument_guardrails_with_roots(
        workspace_root,
        cwd,
        command,
        args,
        host_roots.as_slice(),
    )
}

// Mirror of validate_interpreter_argument_guardrails with host-root scoping instead of
// workspace-only scoping; keep the two in lockstep when changing either.
fn validate_host_interpreter_argument_guardrails_with_roots(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
    host_roots: &[PathBuf],
) -> Result<(), SandboxProcessRunError> {
    if host_command_bridges_unscoped_namespace(command) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "sandbox denied: host namespace bridge '{command}' cannot enforce Windows approved-root authority inside the nested runtime; use an enforceable Tier C sandbox or a dedicated nested-runtime authority"
            ),
        });
    }
    if !process_executable_is_interpreter(command.trim()) {
        return Ok(());
    }

    if interpreter_args_contain_blocked_eval_flag(command, args) {
        return Err(interpreter_shell_eval_denied_error(command));
    }

    for (index, argument) in args.iter().enumerate() {
        if argument_is_non_path_option_assignment(argument.as_str()) {
            continue;
        }
        if let Some(previous) = index.checked_sub(1).and_then(|previous| args.get(previous)) {
            if command_option_consumes_non_path_value(command, previous.as_str()) {
                continue;
            }
        }
        if !contains_embedded_absolute_path(argument.as_str()) {
            continue;
        }
        if interpreter_absolute_path_argument_stays_in_host_scope(
            workspace_root,
            cwd,
            argument.as_str(),
            host_roots,
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

fn host_command_bridges_unscoped_namespace(command: &str) -> bool {
    if !cfg!(windows) {
        return false;
    }
    // These Windows launchers cross into a separate WSL filesystem, where
    // relative paths and HOME no longer inherit the approved Windows roots.
    matches!(normalized_process_command_name(command).as_str(), "wsl" | "bash")
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
    host_roots: &[PathBuf],
) -> Result<bool, SandboxProcessRunError> {
    let trimmed = argument.trim();
    if trimmed.is_empty() {
        return Ok(false);
    }

    if let Some(file_url_path) = parse_file_url_path(trimmed)? {
        return Ok(resolve_host_access_path_with_roots(
            workspace_root,
            cwd,
            file_url_path.as_str(),
            false,
            host_roots,
        )
        .is_ok());
    }

    if let Some(value) = option_assignment_value(trimmed) {
        return interpreter_absolute_path_argument_stays_in_host_scope(
            workspace_root,
            cwd,
            value,
            host_roots,
        );
    }

    if let Some(value) = option_compact_value(trimmed) {
        return interpreter_absolute_path_argument_stays_in_host_scope(
            workspace_root,
            cwd,
            value,
            host_roots,
        );
    }

    if let Some(stays_in_host_scope) = interpreter_path_list_argument_stays_in_host_scope(
        workspace_root,
        cwd,
        trimmed,
        host_roots,
    )? {
        return Ok(stays_in_host_scope);
    }

    if !token_looks_like_absolute_path(trimmed) {
        return Ok(false);
    }

    Ok(resolve_host_access_path_with_roots(workspace_root, cwd, trimmed, false, host_roots).is_ok())
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
    host_roots: &[PathBuf],
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
            resolve_host_access_path_with_roots(
                workspace_root,
                cwd,
                file_url_path.as_str(),
                false,
                host_roots,
            )
        } else {
            resolve_host_access_path_with_roots(workspace_root, cwd, component, false, host_roots)
        };
        if resolved.is_err() {
            return Ok(Some(false));
        }
    }

    Ok(saw_absolute_path.then_some(true))
}

fn interpreter_path_list_components(raw: &str) -> Vec<&str> {
    raw.split(|ch| interpreter_embedded_path_delimiter(ch) || (!cfg!(windows) && ch == ':'))
        .map(str::trim)
        .filter(|component| !component.is_empty())
        .collect()
}

fn interpreter_shell_eval_denied_error(command: &str) -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "sandbox denied: interpreter command '{}' cannot use shell-eval flags (-c/--command/--eval, or node -e/-p); write a workspace script and run it as a script file instead, for example command='pwsh', args=['-NoProfile','-File','scripts/check.ps1'] on Windows, command='bash', args=['scripts/check.sh'] when bash is available, or command='node', args=['scripts/check.js'] for Node",
            command
        ),
    }
}

// Splits on whitespace and common code punctuation so absolute paths quoted inside inline
// source (open('/etc/passwd'), require("/x"), arrays, blocks) still surface as tokens.
fn contains_embedded_absolute_path(raw: &str) -> bool {
    raw.split(interpreter_embedded_path_delimiter).any(token_or_path_list_contains_absolute_path)
}

fn interpreter_embedded_path_delimiter(ch: char) -> bool {
    ch.is_whitespace()
        || matches!(ch, '"' | '\'' | '`' | ',' | ';' | '(' | ')' | '[' | ']' | '{' | '}')
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

fn token_looks_like_drive_qualified_path(raw: &str) -> bool {
    let bytes = raw.trim().as_bytes();
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

// Distinguishes string-escape leftovers (e.g. a "\n" fragment split out of inline source code)
// from genuine Windows root-relative paths that also start with a backslash.
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

#[cfg(test)]
fn resolve_host_working_directory(
    workspace_root: &Path,
    cwd: Option<&str>,
) -> Result<PathBuf, SandboxProcessRunError> {
    let host_roots = host_access_roots();
    let path_env = BTreeMap::new();
    resolve_host_working_directory_with_roots(workspace_root, cwd, host_roots.as_slice(), &path_env)
}

fn resolve_host_working_directory_with_roots(
    workspace_root: &Path,
    cwd: Option<&str>,
    host_roots: &[PathBuf],
    path_env: &BTreeMap<String, PathBuf>,
) -> Result<PathBuf, SandboxProcessRunError> {
    let cwd_value = cwd.unwrap_or(".");
    if cwd_value.contains('\0') {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner denied cwd with embedded NUL byte".to_owned(),
        });
    }
    let expanded_cwd = expand_host_access_safe_env_path(cwd_value, path_env)?;
    let resolved_cwd = expanded_cwd.as_ref().map(|path| path.to_string_lossy().to_string());
    let cwd_for_resolution = resolved_cwd.as_deref().unwrap_or(cwd_value);
    let resolved = resolve_host_access_path_with_roots(
        workspace_root,
        workspace_root,
        cwd_for_resolution,
        true,
        host_roots,
    )?;
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

// Validation-only wrapper: runs the same rewriter that builds the spawn argv and discards the
// result, so validation and execution can never disagree about which arguments are paths.
fn validate_argument_workspace_scope(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    let _ = rewrite_arguments_to_scoped_paths(workspace_root, cwd, command, args)?;
    Ok(())
}

#[cfg(test)]
fn validate_host_argument_scope(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
) -> Result<(), SandboxProcessRunError> {
    let host_roots = host_access_roots();
    validate_host_argument_scope_with_roots(
        workspace_root,
        cwd,
        command,
        args,
        host_roots.as_slice(),
    )
}

fn validate_host_argument_scope_with_roots(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    args: &[String],
    host_roots: &[PathBuf],
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
            || command_arg_is_non_path_value(command, args, index)
        {
            index = index.saturating_add(1);
            continue;
        }
        validate_host_argument_path_scope(workspace_root, cwd, command, arg.as_str(), host_roots)?;
        index = index.saturating_add(1);
    }
    Ok(())
}

#[cfg(all(test, windows))]
fn validate_host_command_path_scope(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
) -> Result<(), SandboxProcessRunError> {
    let host_roots = host_access_roots();
    validate_host_command_path_scope_with_roots(workspace_root, cwd, command, host_roots.as_slice())
}

fn validate_host_command_path_scope_with_roots(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    host_roots: &[PathBuf],
) -> Result<(), SandboxProcessRunError> {
    if !command_has_path_separator(command) {
        return Ok(());
    }
    let executable =
        resolve_host_executable_path_with_roots(workspace_root, cwd, command, host_roots)?;
    if !executable.is_file() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "host process runner executable path '{}' is not a regular file",
                executable.display()
            ),
        });
    }
    Ok(())
}

fn validate_host_argument_path_scope(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    arg: &str,
    host_roots: &[PathBuf],
) -> Result<(), SandboxProcessRunError> {
    if let Some(file_url_path) = parse_file_url_path(arg)? {
        let _ = resolve_host_access_path_with_roots(
            workspace_root,
            cwd,
            file_url_path.as_str(),
            false,
            host_roots,
        )?;
        return Ok(());
    }
    if let Some(value) = option_assignment_value(arg) {
        return validate_host_argument_path_scope(
            workspace_root,
            cwd,
            command,
            value.trim(),
            host_roots,
        );
    }
    if let Some(value) = option_compact_scoped_value(arg, cwd) {
        return validate_host_argument_path_scope(workspace_root, cwd, command, value, host_roots);
    }
    if !argument_requires_path_validation(arg) {
        return Ok(());
    }
    let _ = resolve_host_access_path_with_roots(workspace_root, cwd, arg, false, host_roots).map_err(|error| {
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

// Walks the argv once, classifying each argument: known non-path values and opaque tokens pass
// through untouched, while syntactically path-shaped values, file URLs, compact path options, and
// virtual workspace aliases are resolved through the workspace boundary.
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
        if command_arg_is_non_path_value(command, args, index) {
            rewritten.push(arg.clone());
            index = index.saturating_add(1);
            continue;
        }
        if let Some(file_url_path) = parse_file_url_path(arg.as_str())? {
            let scoped = resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)?;
            let scoped = child_process_path(scoped.as_path());
            rewritten.push(scoped_file_url_argument(scoped.as_path())?);
            index = index.saturating_add(1);
            continue;
        }
        if let Some(value) = option_assignment_value(arg.as_str()) {
            let value = value.trim();
            if let Some(file_url_path) = parse_file_url_path(value)? {
                let scoped =
                    resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)?;
                let scoped = child_process_path(scoped.as_path());
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
                child_process_path(scoped.as_path()).to_string_lossy().as_ref(),
            ));
            index = index.saturating_add(1);
            continue;
        }
        if let Some(value) = option_compact_scoped_value(arg.as_str(), cwd) {
            if let Some(file_url_path) = parse_file_url_path(value)? {
                let scoped =
                    resolve_scoped_path(workspace_root, cwd, file_url_path.as_str(), false)?;
                let scoped = child_process_path(scoped.as_path());
                let scoped = scoped_file_url_argument(scoped.as_path())?;
                rewritten.push(replace_option_compact_value(arg.as_str(), scoped.as_str()));
                index = index.saturating_add(1);
                continue;
            }
            // `option_compact_scoped_value` also recognizes plain relative
            // names that exist below cwd. Resolve those too so a symlink such
            // as `-Cbackup` cannot bypass canonical containment merely because
            // its value contains no slash.
            let scoped = resolve_scoped_path(workspace_root, cwd, value, false)?;
            rewritten.push(replace_option_compact_value(
                arg.as_str(),
                child_process_path(scoped.as_path()).to_string_lossy().as_ref(),
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
        rewritten.push(child_process_path(scoped.as_path()).to_string_lossy().to_string());
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
        || arg
            .trim()
            .split_once('=')
            .is_some_and(|(option, value)| network_listener_value_is_non_path(option, value))
}

fn option_consumes_non_path_value(arg: &str) -> bool {
    matches!(
        arg.trim().to_ascii_lowercase().as_str(),
        "--test-name-pattern" | "--testnamepattern" | "--grep" | "--grep-invert"
    )
}

fn command_option_consumes_non_path_value(command: &str, arg: &str) -> bool {
    option_consumes_non_path_value(arg)
        || python_module_option_consumes_non_path_value(command, arg)
        || windows_acl_option_consumes_non_path_value(command, arg)
        || git_option_consumes_non_path_value(command, arg)
}

fn python_module_option_consumes_non_path_value(command: &str, arg: &str) -> bool {
    let command = normalize_process_executable_token(command);
    matches!(command.as_str(), "python" | "python3" | "py") && arg.trim().eq_ignore_ascii_case("-m")
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

fn git_option_consumes_non_path_value(command: &str, arg: &str) -> bool {
    if normalize_process_executable_token(command) != "git" {
        return false;
    }
    let arg = arg.trim();
    matches!(arg, "-b" | "-c" | "-m")
        || matches!(arg.to_ascii_lowercase().as_str(), "--create" | "--orphan" | "--message")
}

fn command_positional_arg_is_non_path_value(command: &str, arg: &str) -> bool {
    let command = normalize_process_executable_token(command);
    matches!(command.as_str(), "sleep") && is_sleep_duration_literal(arg)
}

fn command_arg_is_non_path_value(command: &str, args: &[String], index: usize) -> bool {
    let Some(arg) = args.get(index) else {
        return false;
    };
    command_positional_arg_is_non_path_value(command, arg.as_str())
        || python_module_invocation_arg_is_non_path_value(command, args, index)
        || git_invocation_arg_is_non_path_value(command, args, index)
        || npm_invocation_arg_is_non_path_value(command, args, index)
        || network_listener_arg_is_non_path_value(args, index)
}

fn network_listener_arg_is_non_path_value(args: &[String], index: usize) -> bool {
    let Some(value) = args.get(index) else {
        return false;
    };
    args.get(index.saturating_sub(1))
        .is_some_and(|option| network_listener_value_is_non_path(option, value))
}

fn network_listener_value_is_non_path(option: &str, value: &str) -> bool {
    let option = option.trim().to_ascii_lowercase();
    if option == "--port" {
        return value.trim().parse::<u16>().is_ok();
    }
    matches!(
        option.as_str(),
        "--host" | "--hostname" | "--bind" | "--listen" | "--address" | "--addr"
    ) && maybe_extract_bare_host(value, true).is_some()
}

fn git_invocation_arg_is_non_path_value(command: &str, args: &[String], index: usize) -> bool {
    if normalize_process_executable_token(command) != "git" {
        return false;
    }
    let Some(subcommand_index) = args.iter().position(|arg| !arg.trim().starts_with('-')) else {
        return false;
    };
    if index == subcommand_index {
        return true;
    }
    if args[..index].iter().any(|arg| arg.trim() == "--") {
        return false;
    }
    let current = args[index].trim();
    if current.starts_with('.')
        || token_looks_like_absolute_path(current)
        || token_looks_like_drive_qualified_path(current)
    {
        return false;
    }
    args.get(subcommand_index).is_some_and(|subcommand| {
        matches!(
            subcommand.trim().to_ascii_lowercase().as_str(),
            "branch"
                | "checkout"
                | "diff"
                | "log"
                | "rev-list"
                | "rev-parse"
                | "show"
                | "status"
                | "switch"
                | "tag"
        )
    })
}

fn npm_invocation_arg_is_non_path_value(command: &str, args: &[String], index: usize) -> bool {
    if normalize_process_executable_token(command) != "npm" {
        return false;
    }
    let Some(arg) = args.get(index).map(|arg| arg.trim()) else {
        return false;
    };
    if index == 0 {
        return matches!(
            arg.to_ascii_lowercase().as_str(),
            "run" | "run-script" | "test" | "start" | "stop" | "restart" | "exec"
        );
    }
    index == 1
        && args.first().is_some_and(|subcommand| {
            matches!(subcommand.trim().to_ascii_lowercase().as_str(), "run" | "run-script")
        })
        && !arg.is_empty()
        && arg.chars().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':'))
}

fn python_module_invocation_arg_is_non_path_value(
    command: &str,
    args: &[String],
    index: usize,
) -> bool {
    let command = normalize_process_executable_token(command);
    if !matches!(command.as_str(), "python" | "python3" | "py") {
        return false;
    }
    let Some(module_flag_index) = args.iter().position(|arg| arg.trim().eq_ignore_ascii_case("-m"))
    else {
        return false;
    };
    if index == module_flag_index.saturating_add(1) {
        return true;
    }
    let Some(module_name) = args.get(module_flag_index.saturating_add(1)).map(|arg| arg.trim())
    else {
        return false;
    };
    if !matches!(module_name, "http.server" | "http_server") {
        return false;
    }
    let Some(arg) = args.get(index).map(|arg| arg.trim()) else {
        return false;
    };
    if index == module_flag_index.saturating_add(2) && arg.parse::<u16>().is_ok() {
        return true;
    }
    args.get(index.saturating_sub(1))
        .map(|previous| previous.trim())
        .is_some_and(|previous| matches!(previous, "--bind" | "-b" | "--protocol"))
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
        "find" => is_find_windows_switch(arg.as_str()),
        "icacls" => matches!(
            arg.as_str(),
            "/C" | "/L"
                | "/Q"
                | "/T"
                | "/?"
                | "/INHERITANCE:E"
                | "/INHERITANCE:D"
                | "/INHERITANCE:R"
        ),
        "whoami" => matches!(arg.as_str(), "/ALL"),
        _ => false,
    }
}

fn is_find_windows_switch(arg: &str) -> bool {
    matches!(arg, "/C" | "/V" | "/N" | "/I" | "/OFFLINE" | "/?")
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
    let value = raw_option_compact_value(arg)?;
    compact_option_value_looks_like_path(value).then_some(value)
}

fn option_compact_scoped_value<'a>(arg: &'a str, cwd: &Path) -> Option<&'a str> {
    let value = raw_option_compact_value(arg)?;
    (compact_option_value_looks_like_path(value)
        || compact_option_value_exists_relative_to(value, cwd))
    .then_some(value)
}

// Extracts the value glued onto a short option (e.g. "-Cpath" -> "path"). Classification stays
// separate because a plain relative value may need filesystem context to distinguish it from a
// flag cluster.
fn raw_option_compact_value(arg: &str) -> Option<&str> {
    let trimmed = arg.trim();
    if !trimmed.starts_with('-') || trimmed.starts_with("--") || is_builtin_list_flag(trimmed) {
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

fn compact_option_value_looks_like_path(value: &str) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    let normalized = trimmed.replace('\\', "/");
    trimmed.starts_with('.')
        || token_looks_like_absolute_path(trimmed)
        || normalized.contains('/')
        || normalized.starts_with("workspace/")
        || normalized.to_ascii_lowercase().starts_with("file://")
}

fn compact_option_value_exists_relative_to(value: &str, base: &Path) -> bool {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return false;
    }
    if trimmed.contains('\0') {
        return true;
    }
    Path::new(trimmed).is_relative() && base.join(trimmed).try_exists().unwrap_or(true)
}

fn argument_requires_path_validation(arg: &str) -> bool {
    let trimmed = arg.trim();
    if trimmed.is_empty() || trimmed.starts_with('-') || is_builtin_list_flag(trimmed) {
        return false;
    }
    if token_looks_like_absolute_path(trimmed) || token_looks_like_drive_qualified_path(trimmed) {
        return true;
    }
    match reqwest::Url::parse(trimmed) {
        Ok(url) => url.scheme().eq_ignore_ascii_case("file"),
        Err(_) => {
            let normalized = trimmed.replace('\\', "/");
            trimmed.starts_with('.') || normalized.contains('/')
        }
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

// Resolves `raw` to a path proven to stay under `workspace_root`. Traversal components are
// rejected before any filesystem access, and the scope check runs on the canonicalized form so
// symlinks cannot smuggle the path outside the workspace. For non-existent targets the nearest
// existing ancestor is canonicalized and checked instead (non-existent components cannot be
// symlinks), which lets mkdir-style builtins validate paths they are about to create.
// This is path admission for arbitrary child-process arguments, not a lease on a filesystem
// object. Runtime sandboxing must still contain hostile concurrent filesystem mutation because
// ordinary child processes open these paths themselves after validation.
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
    if token_looks_like_drive_qualified_path(raw) && !token_looks_like_absolute_path(raw) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "sandbox denied: drive-relative paths are not allowed".to_owned(),
        });
    }
    let candidate = if let Some(suffix) = virtual_workspace_path_suffix(raw) {
        workspace_root.join(normalize_virtual_workspace_suffix(workspace_root, suffix))
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

    // Existing targets return the canonical form; not-yet-existing targets return the joined
    // candidate so callers can create them at the exact path that was scope-checked.
    if candidate.exists() {
        Ok(inspected)
    } else {
        Ok(candidate)
    }
}

// Host-access counterpart of `resolve_scoped_path`: the scope check accepts the workspace plus
// explicitly approved `host_roots`, and additionally refuses protected OS paths outright.
fn resolve_host_access_path_with_roots(
    workspace_root: &Path,
    base: &Path,
    raw: &str,
    must_exist: bool,
    host_roots: &[PathBuf],
) -> Result<PathBuf, SandboxProcessRunError> {
    if let Some(suffix) = named_virtual_workspace_path_suffix(raw) {
        return resolve_scoped_path(
            workspace_root,
            workspace_root,
            suffix.to_string_lossy().as_ref(),
            must_exist,
        );
    }
    if bare_virtual_workspace_root_alias(raw) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner denied bare workspace-root alias in host-access mode; use '/workspace' for the workspace root".to_owned(),
        });
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
    // Re-checked after the join as defense in depth against any join/normalization surprises.
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

    ensure_host_access_path_allowed(workspace_root, inspected.as_path(), host_roots)?;
    if candidate.exists() {
        Ok(inspected)
    } else {
        Ok(candidate)
    }
}

fn ensure_host_access_path_allowed(
    workspace_root: &Path,
    inspected: &Path,
    host_roots: &[PathBuf],
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
    if path_starts_with_case_aware(inspected, workspace_root) {
        return Ok(());
    }
    if let Some(root) =
        host_roots.iter().find(|root| path_starts_with_case_aware(inspected, root.as_path()))
    {
        ensure_host_access_path_components_private(root.as_path(), inspected)?;
        return Ok(());
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "host process runner path '{}' is outside workspace and explicitly approved OS roots",
            inspected.display()
        ),
    })
}

fn ensure_host_access_path_components_private(
    root: &Path,
    inspected: &Path,
) -> Result<(), SandboxProcessRunError> {
    #[cfg(unix)]
    {
        ensure_unix_host_access_path_component_private(root, "approved OS root")?;
        let Ok(relative) = inspected.strip_prefix(root) else {
            return Ok(());
        };
        let mut current = root.to_path_buf();
        for component in relative.components() {
            match component {
                Component::CurDir => {}
                Component::Normal(name) => {
                    current.push(name);
                    ensure_unix_host_access_path_component_private(
                        current.as_path(),
                        "approved OS path component",
                    )?;
                }
                Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                    return Err(SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                        message: format!(
                            "host process runner denied invalid approved OS path component '{}'",
                            inspected.display()
                        ),
                    });
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = root;
        let _ = inspected;
    }
    Ok(())
}

#[cfg(unix)]
fn ensure_unix_host_access_path_component_private(
    path: &Path,
    label: &str,
) -> Result<(), SandboxProcessRunError> {
    let metadata = fs::metadata(path).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "host process runner denied {label} '{}' because metadata could not be inspected: {error}",
            path.display()
        ),
    })?;
    if unix_host_access_metadata_is_private(&metadata) {
        return Ok(());
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "host process runner denied {label} '{}' because it is not owned by the current user or is writable by group/other",
            path.display()
        ),
    })
}

#[cfg(unix)]
fn unix_host_access_metadata_is_private(metadata: &fs::Metadata) -> bool {
    const UNSAFE_WRITE_BITS: u32 = 0o022;
    // SAFETY: `geteuid` reads the current process credentials and has no preconditions.
    let current_uid = unsafe { libc::geteuid() };
    metadata.uid() == current_uid && metadata.permissions().mode() & UNSAFE_WRITE_BITS == 0
}

fn host_access_root_is_private(path: &Path) -> bool {
    #[cfg(unix)]
    {
        fs::metadata(path).is_ok_and(|metadata| unix_host_access_metadata_is_private(&metadata))
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        true
    }
}

// Executables (unlike data paths) may additionally live under Program Files, so installed
// tools remain launchable; the protected-OS-path denial still applies to data arguments.
fn ensure_host_executable_path_allowed(
    workspace_root: &Path,
    inspected: &Path,
    host_roots: &[PathBuf],
) -> Result<(), SandboxProcessRunError> {
    if path_starts_with_case_aware(inspected, workspace_root)
        || host_roots.iter().any(|root| path_starts_with_case_aware(inspected, root.as_path()))
        || windows_program_files_path(inspected)
    {
        return Ok(());
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "host process runner executable path '{}' is outside workspace, explicitly configured OS roots, and installed-program roots",
            inspected.display()
        ),
    })
}

fn windows_program_files_path(path: &Path) -> bool {
    #[cfg(windows)]
    {
        let normalized = path.to_string_lossy().replace('\\', "/").to_ascii_lowercase();
        normalized.contains(":/program files/") || normalized.contains(":/program files (x86)/")
    }
    #[cfg(not(windows))]
    {
        let _ = path;
        false
    }
}

fn host_access_roots() -> Vec<PathBuf> {
    // `palyra.fs.os_file` has its own audited user-profile policy. Process execution must never
    // inherit HOME or USERPROFILE implicitly because a child could bypass that policy entirely.
    let mut roots = Vec::new();
    if let Some(configured_roots) = configured_user_host_roots() {
        for root in configured_roots {
            push_canonical_host_root(&mut roots, root);
        }
    }
    roots
}

fn host_access_path_env_for_input(input: &ProcessRunnerInput) -> BTreeMap<String, PathBuf> {
    let mut path_env = BTreeMap::new();
    for key in HOST_ACCESS_SAFE_PALYRA_ENV_KEYS {
        let Some(value) = input.env.get(*key).filter(|value| !value.trim().is_empty()) else {
            continue;
        };
        let Ok(canonical) = fs::canonicalize(Path::new(value)) else {
            continue;
        };
        if canonical.is_dir() {
            path_env.insert((*key).to_owned(), canonical);
        }
    }
    path_env
}

fn resolve_process_prepend_path_entries(
    policy: &SandboxProcessRunnerPolicy,
    input: &ProcessRunnerInput,
    workspace_root: &Path,
    cwd: &Path,
) -> Result<Vec<PathBuf>, SandboxProcessRunError> {
    if input.prepend_path.is_empty() {
        return Ok(Vec::new());
    }
    match process_runner_effective_path_access_mode(policy) {
        PathAccessMode::ApprovedRoots => {
            let host_roots = host_access_roots();
            let path_env = host_access_path_env_for_input(input);
            return input
                .prepend_path
                .iter()
                .map(|path| {
                    let expanded = expand_host_access_safe_env_path(path.as_str(), &path_env)?;
                    let resolved_path =
                        expanded.as_ref().map(|path| path.to_string_lossy().to_string());
                    let raw = resolved_path.as_deref().unwrap_or(path.as_str());
                    let resolved = resolve_host_access_path_with_roots(
                        workspace_root,
                        cwd,
                        raw,
                        true,
                        host_roots.as_slice(),
                    )?;
                    require_prepend_path_directory(path.as_str(), resolved)
                })
                .collect();
        }
        PathAccessMode::WorkspaceOnly => {}
    }

    input
        .prepend_path
        .iter()
        .map(|path| {
            let resolved = resolve_scoped_path(workspace_root, cwd, path.as_str(), true)?;
            require_prepend_path_directory(path.as_str(), resolved)
        })
        .collect()
}

fn require_prepend_path_directory(
    raw: &str,
    path: PathBuf,
) -> Result<PathBuf, SandboxProcessRunError> {
    if path.is_dir() {
        return Ok(path);
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!("palyra.process.run prepend_path entry '{raw}' is not a directory"),
    })
}

fn configured_user_host_roots() -> Option<Vec<PathBuf>> {
    let value = std::env::var_os(PALYRA_OS_FILE_ROOTS_ENV)?;
    let roots = std::env::split_paths(&value)
        .filter(|path| !path.as_os_str().is_empty())
        .collect::<Vec<_>>();
    if roots.is_empty() {
        None
    } else {
        Some(roots)
    }
}

fn push_canonical_host_root(roots: &mut Vec<PathBuf>, root: PathBuf) {
    let Ok(canonical) = fs::canonicalize(root.as_path()) else {
        return;
    };
    if !canonical.is_dir() {
        return;
    }
    if !host_access_root_is_private(canonical.as_path()) {
        return;
    }
    if !roots.iter().any(|existing| same_path_case_aware(existing.as_path(), canonical.as_path())) {
        roots.push(canonical);
    }
}

// Deny-list of OS locations host-access mode must never touch even though they may sit under
// an approved root. The substring form (":/windows" etc.) intentionally matches every drive
// letter, not just C:.
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

// Windows paths are case-insensitive, so a pure `starts_with` would wrongly deny e.g.
// `c:\users\...` against a root recorded as `C:\Users\...`; Unix stays strictly case-exact.
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

// Maps the virtual aliases models commonly emit ("/", "/workspace", "workspace/...") onto the
// real workspace root so sandboxed runs behave as if the workspace were the filesystem root.
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

// Collapses "/workspace/<root-name>/..." to "/workspace/..." when the first suffix component
// repeats the active workspace directory name; models frequently double up the alias.
fn normalize_virtual_workspace_suffix(workspace_root: &Path, suffix: PathBuf) -> PathBuf {
    let mut components = suffix.components();
    let Some(Component::Normal(first)) = components.next() else {
        return suffix;
    };
    let Some(root_name) = workspace_root.file_name() else {
        return suffix;
    };
    if !path_component_equals(first, root_name) {
        return suffix;
    }
    components.as_path().to_path_buf()
}

fn path_component_equals(left: &std::ffi::OsStr, right: &std::ffi::OsStr) -> bool {
    #[cfg(windows)]
    {
        left.to_string_lossy().eq_ignore_ascii_case(&right.to_string_lossy())
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

// Host-access variant of `virtual_workspace_path_suffix` that deliberately excludes the bare
// "/" alias: on the host, "/" is a real filesystem root and must not be remapped.
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

fn bare_virtual_workspace_root_alias(raw: &str) -> bool {
    let trimmed = raw.trim();
    !trimmed.is_empty() && trimmed.replace('\\', "/") == "/"
}

fn rewrite_host_access_process_args(
    args: &[String],
    workspace_root: &Path,
    path_env: &BTreeMap<String, PathBuf>,
) -> Result<Vec<String>, SandboxProcessRunError> {
    args.iter()
        .map(|arg| {
            let expanded = rewrite_host_access_safe_env_arg(arg.as_str(), path_env)?;
            rewrite_host_virtual_workspace_arg(expanded.as_str(), workspace_root)
        })
        .collect()
}

#[cfg(test)]
fn rewrite_host_virtual_workspace_args(
    args: &[String],
    workspace_root: &Path,
) -> Result<Vec<String>, SandboxProcessRunError> {
    rewrite_host_access_process_args(args, workspace_root, &BTreeMap::new())
}

fn rewrite_host_access_safe_env_arg(
    arg: &str,
    path_env: &BTreeMap<String, PathBuf>,
) -> Result<String, SandboxProcessRunError> {
    let trimmed = arg.trim();
    if host_access_arg_starts_with_supported_path_env(trimmed) {
        if let Some(path) = expand_host_access_safe_env_path(trimmed, path_env)? {
            return Ok(path.to_string_lossy().to_string());
        }
    }

    if let Some((name, value)) = trimmed.split_once('=') {
        let value = value.trim();
        if name.starts_with('-') && host_access_arg_starts_with_supported_path_env(value) {
            let Some(path) = expand_host_access_safe_env_path(value, path_env)? else {
                return Ok(arg.to_owned());
            };
            return Ok(format!("{name}={}", path.to_string_lossy()));
        }
    }

    Ok(arg.to_owned())
}

fn host_access_arg_starts_with_supported_path_env(arg: &str) -> bool {
    HOST_ACCESS_SAFE_PALYRA_ENV_KEYS.iter().any(|key| {
        let windows_prefix = format!("%{key}%");
        if arg.starts_with(windows_prefix.as_str()) {
            return true;
        }
        let braced_prefix = format!("${{{key}}}");
        if arg.starts_with(braced_prefix.as_str()) {
            return true;
        }
        let bare_prefix = format!("${key}");
        if !arg.starts_with(bare_prefix.as_str()) {
            return false;
        }
        arg[bare_prefix.len()..]
            .chars()
            .next()
            .is_none_or(|ch| !(ch.is_ascii_alphanumeric() || ch == '_'))
    })
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
    if bare_virtual_workspace_root_alias(raw) {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner denied bare workspace-root alias in host-access mode; use '/workspace' for the workspace root".to_owned(),
        });
    }
    if named_virtual_workspace_path_suffix(raw).is_none() {
        return Ok(None);
    }

    let resolved = resolve_scoped_path(workspace_root, workspace_root, raw, false)?;
    Ok(Some(resolved.to_string_lossy().to_string()))
}

fn expand_host_access_safe_env_path(
    raw: &str,
    path_env: &BTreeMap<String, PathBuf>,
) -> Result<Option<PathBuf>, SandboxProcessRunError> {
    let Some((key, suffix)) = host_access_path_env_prefix(raw)? else {
        return Ok(None);
    };
    let Some(base) = path_env.get(key) else {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: format!(
                "host process runner path references unset or unsupported environment variable '{key}'"
            ),
        });
    };
    append_host_access_path_env_suffix(base.clone(), suffix).map(Some)
}

fn host_access_path_env_prefix(path: &str) -> Result<Option<(&str, &str)>, SandboxProcessRunError> {
    if let Some(rest) = path.strip_prefix('%') {
        let Some(end) = rest.find('%') else {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                message: "host process runner path has malformed %VAR% environment prefix"
                    .to_owned(),
            });
        };
        let key = &rest[..end];
        validate_host_access_path_env_key(key)?;
        return Ok(Some((key, &rest[end + 1..])));
    }
    if let Some(rest) = path.strip_prefix("${") {
        let Some(end) = rest.find('}') else {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                message: "host process runner path has malformed ${VAR} environment prefix"
                    .to_owned(),
            });
        };
        let key = &rest[..end];
        validate_host_access_path_env_key(key)?;
        return Ok(Some((key, &rest[end + 1..])));
    }
    if let Some(rest) = path.strip_prefix('$') {
        let key_len = rest
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_alphanumeric() || *ch == '_')
            .map(|(index, ch)| index + ch.len_utf8())
            .last()
            .unwrap_or(0);
        if key_len == 0 {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                message: "host process runner path has malformed $VAR environment prefix"
                    .to_owned(),
            });
        }
        let key = &rest[..key_len];
        validate_host_access_path_env_key(key)?;
        return Ok(Some((key, &rest[key_len..])));
    }
    Ok(None)
}

fn validate_host_access_path_env_key(key: &str) -> Result<(), SandboxProcessRunError> {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner path environment variable name is empty".to_owned(),
        });
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner path environment variable name must use ASCII letters, digits, or underscores".to_owned(),
        });
    }
    Ok(())
}

fn append_host_access_path_env_suffix(
    mut base: PathBuf,
    suffix: &str,
) -> Result<PathBuf, SandboxProcessRunError> {
    let relative_suffix = suffix.trim_start_matches(['/', '\\']);
    if relative_suffix.is_empty() {
        return Ok(base);
    }
    for segment in relative_suffix.split(['/', '\\']) {
        if segment.is_empty() {
            continue;
        }
        if segment == "." || segment == ".." || segment.contains(':') {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                message: "host process runner environment path suffix must stay relative to the expanded root".to_owned(),
            });
        }
        if segment.chars().any(char::is_control) {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                message: "host process runner path contains unsupported characters".to_owned(),
            });
        }
        base.push(segment);
    }
    Ok(base)
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

// Best-effort preflight extraction of every host the invocation appears to target: explicit
// requested_egress_hosts, URL arguments, --host=value style assignments, values following
// host-hint flags, and URL/host-shaped env values. Loopback values used by listen/bind flags are
// removed because they describe a local socket target rather than outbound traffic. This is a
// heuristic deny gate, not runtime isolation; strict mode layers backend-enforced network
// isolation on top.
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
    for (key, value) in &input.env {
        collect_hosts_from_token(&mut hosts, value, is_env_host_hint_key(key))?;
    }
    let loopback_listen_hosts = collect_loopback_listen_hosts(input);
    hosts.retain(|host| !loopback_listen_hosts.iter().any(|listen_host| listen_host == host));
    Ok(hosts)
}

fn collect_loopback_listen_hosts(input: &ProcessRunnerInput) -> Vec<String> {
    let mut hosts = Vec::new();
    for (index, arg) in input.args.iter().enumerate() {
        if let Some((key, value)) = arg.split_once('=') {
            if is_listen_host_hint_key(key) {
                push_loopback_listen_host(&mut hosts, value);
            }
            continue;
        }
        if is_listen_host_hint_key(arg.as_str()) {
            if let Some(value) = input.args.get(index.saturating_add(1)) {
                push_loopback_listen_host(&mut hosts, value);
            }
        }
    }
    hosts
}

fn is_listen_host_hint_key(raw: &str) -> bool {
    matches!(
        raw.trim().trim_start_matches('-').to_ascii_lowercase().as_str(),
        "host" | "hostname" | "bind" | "listen" | "address" | "addr"
    )
}

fn push_loopback_listen_host(hosts: &mut Vec<String>, raw: &str) {
    let candidate = raw.trim().trim_matches(['"', '\'']).trim_end_matches('.');
    let candidate = split_host_and_port(candidate)
        .filter(|(_, port)| port.chars().all(|ch| ch.is_ascii_digit()))
        .map_or(candidate, |(host, _)| host);
    let normalized = candidate.to_ascii_lowercase();
    let is_loopback = normalized == "localhost"
        || normalized
            .parse::<std::net::Ipv4Addr>()
            .map(|address| address.is_loopback())
            .unwrap_or(false);
    if is_loopback && !hosts.iter().any(|host| host == &normalized) {
        hosts.push(normalized);
    }
}

fn validate_requested_egress_hosts_require_enforcement(
    input: &ProcessRunnerInput,
) -> Result<(), SandboxProcessRunError> {
    if input.requested_egress_hosts.is_empty() {
        return Ok(());
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::EgressDenied,
        message: "sandbox denied: requested_egress_hosts is only usable when tool_call.process_runner.egress_enforcement_mode='preflight'; current egress_enforcement_mode is none. Omit requested_egress_hosts for ordinary local commands, use palyra.http.fetch or browser tools for network retrieval, or ask an operator to enable process-runner preflight egress checks.".to_owned(),
    })
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

fn resolve_host_executable_path_with_roots(
    workspace_root: &Path,
    base: &Path,
    raw: &str,
    host_roots: &[PathBuf],
) -> Result<PathBuf, SandboxProcessRunError> {
    if raw.contains('\0') {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "host process runner denied executable path with embedded NUL byte".to_owned(),
        });
    }
    let raw_path = Path::new(raw);
    let uses_parent_dir =
        raw_path.components().any(|component| matches!(component, Component::ParentDir));
    for candidate in host_executable_path_candidates(workspace_root, base, raw_path) {
        if !candidate.exists() {
            continue;
        }
        let executable =
            fs::canonicalize(candidate.as_path()).map_err(|error| SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                message: format!(
                    "host process runner denied invalid executable path '{}': {error}",
                    candidate.display()
                ),
            })?;
        if uses_parent_dir && !path_starts_with_case_aware(executable.as_path(), workspace_root) {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
                message: format!(
                    "host process runner denied executable path traversal outside workspace for '{raw}'"
                ),
            });
        }
        ensure_host_executable_path_allowed(workspace_root, executable.as_path(), host_roots)?;
        return Ok(executable);
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!("host process runner executable path '{raw}' does not exist"),
    })
}

fn resolve_host_process_program_with_roots(
    workspace_root: &Path,
    cwd: &Path,
    command: &str,
    host_roots: &[PathBuf],
    trusted_path: &OsStr,
    require_trusted_resolution: bool,
    allow_cwd_resolution: bool,
) -> Result<PathBuf, SandboxProcessRunError> {
    if command_has_path_separator(command) {
        return resolve_host_executable_path_with_roots(workspace_root, cwd, command, host_roots);
    }
    resolve_tier_b_process_program(
        command,
        cwd,
        trusted_path,
        require_trusted_resolution,
        allow_cwd_resolution,
    )
}

fn host_executable_path_candidates(
    workspace_root: &Path,
    base: &Path,
    raw_path: &Path,
) -> Vec<PathBuf> {
    if raw_path.is_absolute() {
        return vec![raw_path.to_path_buf()];
    }
    let base_candidate = base.join(raw_path);
    let workspace_candidate = workspace_root.join(raw_path);
    if same_path_case_aware(base_candidate.as_path(), workspace_candidate.as_path()) {
        vec![base_candidate]
    } else {
        vec![base_candidate, workspace_candidate]
    }
}

// Outside a host-hint context a bare token only counts as a host when it carries a numeric
// port (host:443); requiring the port keeps ordinary words and file names out of egress checks.
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
                | "registry"
                | "url"
                | "uri"
                | "domain"
                | "proxy"
                | "address"
                | "addr"
        )
    })
}

fn is_env_host_hint_key(raw: &str) -> bool {
    is_host_hint_key(raw)
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

    let sample_hosts =
        requested_hosts.iter().take(3).map(String::as_str).collect::<Vec<_>>().join(", ");
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
        // Match only on label boundaries: suffix "corp.local" must allow "api.corp.local" but
        // never "evilcorp.local".
        let bare_suffix = suffix.trim_start_matches('.');
        let dotted_suffix = format!(".{bare_suffix}");
        host.eq_ignore_ascii_case(bare_suffix) || host.ends_with(dotted_suffix.as_str())
    })
}

fn managed_process_fault(
    fault_injection: &crate::qa_fault_injection::QaFaultRuntime,
    point_id: &'static str,
    actor: &'static str,
) -> Result<QaFaultDirective, SandboxProcessRunError> {
    fault_injection.checkpoint(point_id, actor).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("qa_fault.managed_process_checkpoint_failed: {error}"),
    })
}

fn apply_managed_process_fault_without_child(
    fault_injection: &crate::qa_fault_injection::QaFaultRuntime,
    point_id: &'static str,
    actor: &'static str,
) -> Result<(), SandboxProcessRunError> {
    match managed_process_fault(fault_injection, point_id, actor)? {
        QaFaultDirective::Continue => Ok(()),
        QaFaultDirective::Activate(directive) => match directive.activation.action.clone() {
            QaFaultAction::Timeout => {
                fault_injection.record_immediate_recovery(&directive).map_err(|error| {
                    SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::RuntimeFailure,
                        message: format!("qa_fault.managed_process_recovery_failed: {error}"),
                    }
                })?;
                Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::TimedOut,
                    message: format!(
                        "qa_fault.managed_process_timeout: activation={}",
                        directive.activation.id
                    ),
                })
            }
            QaFaultAction::TerminateProcess => {
                fault_injection.record_immediate_recovery(&directive).map_err(|error| {
                    SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::RuntimeFailure,
                        message: format!("qa_fault.managed_process_recovery_failed: {error}"),
                    }
                })?;
                #[cfg(feature = "qa-fault-injection")]
                fault_injection.terminate_process();
                #[cfg(not(feature = "qa-fault-injection"))]
                Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message:
                        "qa_fault.feature_disabled: terminate directive reached a feature-off build"
                            .to_owned(),
                })
            }
            action => Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "qa_fault.managed_process_action_unsupported: {}",
                    action.kind().as_str()
                ),
            }),
        },
    }
}

#[cfg(unix)]
fn apply_managed_process_fault_with_unix_supervisor(
    fault_injection: &crate::qa_fault_injection::QaFaultRuntime,
    point_id: &'static str,
    actor: &'static str,
    child: ManagedChildGuard,
    supervisor_control: &UnixProcessSupervisorControl,
    registration_identity: Option<&BackgroundProcessIdentity>,
) -> Result<ManagedChildGuard, SandboxProcessRunError> {
    match managed_process_fault(fault_injection, point_id, actor)? {
        QaFaultDirective::Continue => Ok(child),
        QaFaultDirective::Activate(directive) => match directive.activation.action.clone() {
            QaFaultAction::TerminateProcess => {
                let recovery_class = match point_id {
                    "managed_process.after_effect_before_ack" => {
                        QaFaultRecoveryClass::OutcomeUnknown
                    }
                    "managed_process.after_ack_before_transition" => {
                        QaFaultRecoveryClass::CleanupSucceeded
                    }
                    _ => {
                        return Err(SandboxProcessRunError {
                            kind: SandboxProcessRunErrorKind::RuntimeFailure,
                            message: format!(
                                "qa_fault.managed_process_recovery_unclassified: {point_id}"
                            ),
                        });
                    }
                };
                terminate_unix_supervised_background_child(
                    child,
                    supervisor_control,
                    registration_identity,
                    "fault injection",
                )?;
                fault_injection
                    .record_verified_recovery(
                        &directive,
                        recovery_class,
                        "qa_fault.managed_process_owned_tree_cleanup_verified",
                    )
                    .map_err(|error| SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::RuntimeFailure,
                        message: format!("qa_fault.managed_process_recovery_failed: {error}"),
                    })?;
                #[cfg(feature = "qa-fault-injection")]
                fault_injection.terminate_process();
                #[cfg(not(feature = "qa-fault-injection"))]
                Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message:
                        "qa_fault.feature_disabled: terminate directive reached a feature-off build"
                            .to_owned(),
                })
            }
            action => Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "qa_fault.managed_process_action_unsupported: {}",
                    action.kind().as_str()
                ),
            }),
        },
    }
}

fn apply_managed_process_fault_with_child(
    fault_injection: &crate::qa_fault_injection::QaFaultRuntime,
    point_id: &'static str,
    actor: &'static str,
    child: ManagedChildGuard,
) -> Result<ManagedChildGuard, SandboxProcessRunError> {
    match managed_process_fault(fault_injection, point_id, actor)? {
        QaFaultDirective::Continue => Ok(child),
        QaFaultDirective::Activate(directive) => match directive.activation.action.clone() {
            QaFaultAction::TerminateProcess => {
                let recovery_class = match point_id {
                    "managed_process.after_effect_before_ack" => {
                        QaFaultRecoveryClass::OutcomeUnknown
                    }
                    "managed_process.after_ack_before_transition" => {
                        QaFaultRecoveryClass::CleanupSucceeded
                    }
                    _ => {
                        return Err(SandboxProcessRunError {
                            kind: SandboxProcessRunErrorKind::RuntimeFailure,
                            message: format!(
                                "qa_fault.managed_process_recovery_unclassified: {point_id}"
                            ),
                        });
                    }
                };
                terminate_background_child(child)?;
                fault_injection
                    .record_verified_recovery(
                        &directive,
                        recovery_class,
                        "qa_fault.managed_process_owned_tree_cleanup_verified",
                    )
                    .map_err(|error| SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::RuntimeFailure,
                        message: format!("qa_fault.managed_process_recovery_failed: {error}"),
                    })?;
                #[cfg(feature = "qa-fault-injection")]
                fault_injection.terminate_process();
                #[cfg(not(feature = "qa-fault-injection"))]
                Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message:
                        "qa_fault.feature_disabled: terminate directive reached a feature-off build"
                            .to_owned(),
                })
            }
            action => Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "qa_fault.managed_process_action_unsupported: {}",
                    action.kind().as_str()
                ),
            }),
        },
    }
}

fn apply_managed_process_fault_after_verified_cleanup(
    fault_injection: &crate::qa_fault_injection::QaFaultRuntime,
    point_id: &'static str,
    actor: &'static str,
) -> Result<(), SandboxProcessRunError> {
    match managed_process_fault(fault_injection, point_id, actor)? {
        QaFaultDirective::Continue => Ok(()),
        QaFaultDirective::Activate(directive) => {
            let recovery_class = QaFaultRecoveryClass::CleanupSucceeded;
            match directive.activation.action.clone() {
                QaFaultAction::Timeout => {
                    fault_injection
                        .record_verified_recovery(
                            &directive,
                            recovery_class,
                            "qa_fault.managed_process_owned_tree_cleanup_verified",
                        )
                        .map_err(|error| SandboxProcessRunError {
                            kind: SandboxProcessRunErrorKind::RuntimeFailure,
                            message: format!("qa_fault.managed_process_recovery_failed: {error}"),
                        })?;
                    Err(SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::TimedOut,
                        message: format!(
                            "qa_fault.managed_process_timeout: activation={}",
                            directive.activation.id
                        ),
                    })
                }
                QaFaultAction::TerminateProcess => {
                    fault_injection
                        .record_verified_recovery(
                            &directive,
                            recovery_class,
                            "qa_fault.managed_process_owned_tree_cleanup_verified",
                        )
                        .map_err(|error| SandboxProcessRunError {
                            kind: SandboxProcessRunErrorKind::RuntimeFailure,
                            message: format!("qa_fault.managed_process_recovery_failed: {error}"),
                        })?;
                    #[cfg(feature = "qa-fault-injection")]
                    fault_injection.terminate_process();
                    #[cfg(not(feature = "qa-fault-injection"))]
                    Err(SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::RuntimeFailure,
                        message: "qa_fault.feature_disabled: terminate directive reached a feature-off build"
                            .to_owned(),
                    })
                }
                action => Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!(
                        "qa_fault.managed_process_action_unsupported: {}",
                        action.kind().as_str()
                    ),
                }),
            }
        }
    }
}

fn execute_process(
    request: ForegroundProcessExecutionRequest<'_>,
) -> Result<ProcessExecutionCapture, SandboxProcessRunError> {
    let ForegroundProcessExecutionRequest {
        policy,
        input,
        workspace_root,
        cwd,
        timeout,
        cancellation_requested,
        progress_sink,
        fault_injection,
    } = request;
    let mut command = build_process_command(policy, input, workspace_root, cwd)?;
    configure_child_process_group(&mut command);
    configure_background_child_suspended(&mut command);
    command.stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped());
    if !process_runner_allows_host_access(policy) {
        attach_resource_limits_unix(&mut command, policy);
    }

    apply_managed_process_fault_without_child(
        fault_injection,
        "managed_process.before_effect",
        "foreground",
    )?;
    let child = command.spawn().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: process_spawn_failed_message(policy, input, cwd, &error),
    })?;
    // Windows children start suspended so the kill-on-close job owns the full descendant tree
    // before user code can run and before a post-spawn fault may terminate the daemon.
    #[cfg(windows)]
    let child = prepare_windows_background_child(ManagedChildGuard::new(child))?;
    #[cfg(not(windows))]
    let child = ManagedChildGuard::new(child);
    let mut child = apply_managed_process_fault_with_child(
        fault_injection,
        "managed_process.after_effect_before_ack",
        "foreground",
        child,
    )?;
    let capture = capture_child_output(
        &mut child,
        timeout,
        policy.max_output_bytes as usize,
        cancellation_requested,
        progress_sink,
    );
    let cleanup = terminate_background_child(child);
    if let Err(cleanup_error) = cleanup {
        return Err(match capture {
            Ok(_) => cleanup_error,
            Err(capture_error) => SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "{}; owned-tree cleanup verification also failed: {}",
                    capture_error.message, cleanup_error.message
                ),
            },
        });
    }
    apply_managed_process_fault_after_verified_cleanup(
        fault_injection,
        "managed_process.during_cleanup",
        "foreground",
    )?;
    if capture.is_ok() {
        apply_managed_process_fault_after_verified_cleanup(
            fault_injection,
            "managed_process.after_ack_before_transition",
            "foreground",
        )?;
    }
    capture
}

fn process_spawn_failed_message(
    policy: &SandboxProcessRunnerPolicy,
    input: &ProcessRunnerInput,
    cwd: &Path,
    error: &io::Error,
) -> String {
    let prepend_path_state =
        if input.prepend_path.is_empty() { "not_provided" } else { "provided" };
    let failure_class = process_spawn_failure_class(error);
    format!(
        "sandbox process spawn failed for command '{}' (failure_class={}): {error}. Runtime={}. \
        Lookup used cwd='{}', prepend_path={prepend_path_state}, and the daemon sanitized PATH, \
        not the interactive shell PATH. Install the executable on the trusted process-runner PATH \
        or use an exact executable path allowed by process_runner.allowed_executables.",
        input.command,
        failure_class.as_str(),
        process_runner_executor_name(policy),
        cwd.display()
    )
}

// Makes the child its own process-group leader so kill(-pid) can later terminate the whole
// tree (see terminate_unix_process_group) instead of just the direct child.
#[cfg(unix)]
fn configure_child_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_child_process_group(_command: &mut Command) {}

// Managed runtimes retain the direct child as their ownership root. A new session makes both
// the process-group and session anchors equal that PID, matching provenance verification.
#[cfg(unix)]
fn configure_managed_stdio_process_ownership(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    // SAFETY: this closure runs after fork and before exec and calls only async-signal-safe
    // `setsid`; failure aborts the spawn before any unowned runtime code can execute.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_managed_stdio_process_ownership(_command: &mut Command) {}

// Windows has no pre-exec hook that can atomically attach a new process to a job. Starting the
// initial thread suspended closes that ownership gap: user code cannot create descendants before
// the daemon has installed and registered the kill-on-close job boundary.
#[cfg(windows)]
fn configure_background_child_suspended(command: &mut Command) {
    command.creation_flags(CREATE_SUSPENDED);
}

#[cfg(not(windows))]
fn configure_background_child_suspended(_command: &mut Command) {}

#[cfg(unix)]
fn freeze_unix_supervisor_launch_spec(
    command: &Command,
    policy: &SandboxProcessRunnerPolicy,
    apply_resource_limits: bool,
    lifetime: Duration,
) -> Result<UnixSupervisorLaunchSpec, SandboxProcessRunError> {
    let program = command.get_program().to_os_string();
    if !Path::new(&program).is_absolute() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: "sandbox background target plan requires an absolute executable".to_owned(),
        });
    }
    let args = command.get_args().map(OsStr::to_os_string).collect::<Vec<_>>();
    let cwd =
        command.get_current_dir().map(Path::to_path_buf).ok_or_else(|| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: "sandbox background target plan omitted its working directory".to_owned(),
        })?;
    let mut environment = Vec::new();
    for (key, value) in command.get_envs() {
        let Some(value) = value else {
            continue;
        };
        environment.push((key.to_os_string(), value.to_os_string()));
    }
    let limits = apply_resource_limits.then_some(UnixSupervisorLimits {
        cpu_time_limit_ms: policy.cpu_time_limit_ms,
        memory_limit_bytes: policy.memory_limit_bytes,
    });
    Ok(UnixSupervisorLaunchSpec {
        program,
        args,
        cwd,
        environment,
        limits,
        lifetime_ms: u64::try_from(lifetime.as_millis()).unwrap_or(u64::MAX),
    })
}

#[cfg(unix)]
fn spawn_unix_supervised_background_child(
    target_command: &Command,
    policy: &SandboxProcessRunnerPolicy,
    capabilities: BackgroundProcessHandleCapabilities,
    lifetime: Duration,
    apply_resource_limits: bool,
) -> Result<PreparedUnixSupervisedBackgroundChild, SandboxProcessRunError> {
    let launch_spec = freeze_unix_supervisor_launch_spec(
        target_command,
        policy,
        apply_resource_limits,
        lifetime,
    )?;
    let current_executable = std::env::current_exe().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: format!("failed to resolve trusted palyrad supervisor executable: {error}"),
    })?;
    let selected_executable_sha256 =
        sha256_file_bounded(current_executable.as_path()).map_err(|error| {
            SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::SpawnFailed,
                message: format!("failed to hash trusted palyrad supervisor executable: {error}"),
            }
        })?;
    let (mut supervisor, control) = UnixProcessSupervisorControl::prepare(current_executable)
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::SpawnFailed,
            message: format!("failed to prepare trusted Unix process supervisor: {error}"),
        })?;
    supervisor
        .stdin(if capabilities.stdin { Stdio::piped() } else { Stdio::null() })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let child = supervisor.spawn().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: format!("failed to spawn trusted Unix process supervisor: {error}"),
    })?;
    let child = ManagedChildGuard::new_reap_only(child);
    if let Err(error) = control.await_ready(child.id()) {
        let readiness_error = SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("trusted Unix process supervisor readiness failed: {error}"),
        };
        return Err(settle_unready_unix_supervisor_failure(child, control, readiness_error));
    }
    let observed_executable_sha256 = match current_process_executable_sha256(child.id()) {
        Ok(observed) => observed,
        Err(_) => {
            let identity_error = SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: "trusted Unix process supervisor image identity could not be verified"
                    .to_owned(),
            };
            return Err(settle_unready_unix_supervisor_failure(child, control, identity_error));
        }
    };
    if observed_executable_sha256 != selected_executable_sha256 {
        let identity_error = SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: "trusted Unix process supervisor image identity mismatch".to_owned(),
        };
        return Err(settle_unready_unix_supervisor_failure(child, control, identity_error));
    }
    if let Err(error) = control.set_launch_spec(launch_spec) {
        let plan_error = SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("trusted Unix process supervisor launch plan failed: {error}"),
        };
        return Err(settle_unready_unix_supervisor_failure(child, control, plan_error));
    }
    Ok(PreparedUnixSupervisedBackgroundChild {
        child,
        control: Arc::new(control),
        supervisor_executable_sha256: selected_executable_sha256,
    })
}

#[cfg(unix)]
fn settle_unready_unix_supervisor_failure(
    mut child: ManagedChildGuard,
    control: UnixProcessSupervisorControl,
    original: SandboxProcessRunError,
) -> SandboxProcessRunError {
    drop(control);
    match child.wait_for_exit(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS)) {
        Ok(Some(_)) => original,
        Ok(None) => SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "{}; unregistered trusted supervisor did not exit after control shutdown",
                original.message
            ),
        },
        Err(error) => SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "{}; unregistered trusted supervisor reap failed: {error}",
                original.message
            ),
        },
    }
}

#[cfg(unix)]
fn release_unix_supervised_background_target(
    child: ManagedChildGuard,
    control: Arc<UnixProcessSupervisorControl>,
) -> Result<UnixSupervisedBackgroundChild, (ManagedChildGuard, SandboxProcessRunError)> {
    match control.start_target() {
        Ok(target_pid) => Ok(UnixSupervisedBackgroundChild { child, control, target_pid }),
        Err(error) => Err((
            child,
            SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!("trusted Unix process supervisor failed to start target: {error}"),
            },
        )),
    }
}

fn spawn_background_process(
    request: BackgroundProcessSpawnRequest<'_>,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let BackgroundProcessSpawnRequest {
        policy,
        input,
        workspace_root,
        cwd,
        process_risk,
        lifetime,
        max_lifetime,
        auto_background_reason,
        lifetime_mode,
        registration_fence,
        fault_injection,
    } = request;
    #[cfg(unix)]
    let command = build_process_command(policy, input, workspace_root, cwd)?;
    #[cfg(not(unix))]
    let mut command = build_process_command(policy, input, workspace_root, cwd)?;
    let capabilities = BackgroundProcessHandleCapabilities::from_input(input);
    #[cfg(windows)]
    {
        configure_child_process_group(&mut command);
        configure_background_child_suspended(&mut command);
        command
            .stdin(if capabilities.stdin { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        if !process_runner_allows_host_access(policy) {
            attach_resource_limits_unix(&mut command, policy);
        }
    }

    // Every startup wait below is bounded by this budget, which reserves a slice of the
    // lifetime for returning process metadata before the tool-call timeout fires; otherwise a
    // slow startup could eat the whole window and the caller would never learn the pid.
    let lifetime_ms = lifetime.as_millis() as u64;
    let startup_budget = background_process_startup_metadata_budget(lifetime)
        .ok_or_else(|| background_process_startup_budget_expired_error(input, lifetime_ms))?;
    apply_managed_process_fault_without_child(
        fault_injection,
        "managed_process.before_effect",
        "background",
    )?;
    #[cfg(windows)]
    let mut child = {
        let child = command.spawn().map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::SpawnFailed,
            message: process_spawn_failed_message(policy, input, cwd, &error),
        })?;
        bind_windows_background_child(ManagedChildGuard::new(child))?
    };
    #[cfg(unix)]
    let PreparedUnixSupervisedBackgroundChild {
        mut child,
        control: unix_supervisor_control,
        supervisor_executable_sha256,
    } = spawn_unix_supervised_background_child(
        &command,
        policy,
        capabilities,
        lifetime,
        !process_runner_allows_host_access(policy),
    )?;
    #[cfg(not(any(unix, windows)))]
    let mut child = {
        configure_child_process_group(&mut command);
        configure_background_child_suspended(&mut command);
        command
            .stdin(if capabilities.stdin { Stdio::piped() } else { Stdio::null() })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        ManagedChildGuard::new(command.spawn().map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::SpawnFailed,
            message: process_spawn_failed_message(policy, input, cwd, &error),
        })?)
    };
    // The advertised lifetime starts once the ownership root exists. Fault handling, target
    // release, Windows owner binding/resume, and registry contention must not give a live tree
    // unmetered runtime.
    let started_at = Instant::now();
    let pid = child.id();
    #[cfg(windows)]
    let windows_job_bound = true;
    #[cfg(not(windows))]
    let windows_job_bound = false;
    #[cfg(unix)]
    let registration_control = Arc::clone(&unix_supervisor_control);
    #[cfg(unix)]
    let provenance_result = capture_background_process_provenance_with_executable_sha256(
        pid,
        Some(supervisor_executable_sha256.as_str()),
    );
    #[cfg(not(unix))]
    let provenance_result = capture_background_process_provenance(pid);
    let provenance = match provenance_result {
        Ok(provenance) => provenance,
        Err(error) => {
            #[cfg(unix)]
            terminate_unix_supervised_background_child(
                child,
                unix_supervisor_control.as_ref(),
                None,
                "provenance capture failure",
            )?;
            #[cfg(not(unix))]
            terminate_background_child(child)?;
            return Err(error);
        }
    };
    let stdin = capabilities.stdin.then(|| child.child_mut().stdin.take()).flatten();
    if capabilities.stdin && stdin.is_none() {
        #[cfg(unix)]
        terminate_unix_supervised_background_child(
            child,
            unix_supervisor_control.as_ref(),
            None,
            "stdin capture failure",
        )?;
        #[cfg(not(unix))]
        terminate_background_child(child)?;
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "sandbox background process failed to open stdin handle for command '{}'",
                input.command
            ),
        });
    }
    let registration_identity = match register_background_process_pid(
        pid,
        capabilities,
        lifetime_mode,
        provenance.clone(),
        stdin,
        #[cfg(unix)]
        Some(registration_control),
    ) {
        Ok(identity) => identity,
        Err(error) => {
            #[cfg(unix)]
            terminate_unix_supervised_background_child(
                child,
                unix_supervisor_control.as_ref(),
                None,
                "registry insertion failure",
            )?;
            #[cfg(not(unix))]
            terminate_background_child(child)?;
            return Err(error);
        }
    };
    let registration_request = BackgroundProcessRegistrationRequest {
        pid,
        provenance: provenance.clone(),
        lifetime_ms,
        lifetime_mode,
    };
    if let Some(registration_fence) = registration_fence {
        if let Err(error) = registration_fence(registration_request) {
            #[cfg(unix)]
            terminate_unix_supervised_background_child(
                child,
                unix_supervisor_control.as_ref(),
                Some(&registration_identity),
                "durable registration failure",
            )?;
            #[cfg(not(unix))]
            settle_background_registration_failure(child, &registration_identity)?;
            return Err(error);
        }
    }
    #[cfg(windows)]
    if let Err(resume_error) = resume_windows_background_child(&child) {
        let message = if windows_background_startup_was_superseded(
            &registration_identity,
            startup_budget.saturating_sub(started_at.elapsed()),
        ) {
            format!(
                "sandbox background process {pid} startup was superseded by verified Windows job termination before resume acknowledgement: {resume_error}"
            )
        } else {
            format!(
                "sandbox background process {pid} could not resume after Windows job ownership was established: {resume_error}"
            )
        };
        let error =
            SandboxProcessRunError { kind: SandboxProcessRunErrorKind::RuntimeFailure, message };
        settle_background_registration_failure(child, &registration_identity)?;
        return Err(error);
    }
    #[cfg(unix)]
    let (released_child, unix_supervisor_control, target_pid) =
        match release_unix_supervised_background_target(child, Arc::clone(&unix_supervisor_control))
        {
            Ok(released) => (released.child, released.control, released.target_pid),
            Err((child, error)) => {
                terminate_unix_supervised_background_child(
                    child,
                    unix_supervisor_control.as_ref(),
                    Some(&registration_identity),
                    "target start failure",
                )?;
                return Err(error);
            }
        };
    #[cfg(unix)]
    {
        child = released_child;
        if let Err(error) = attach_background_target_pid(&registration_identity, target_pid) {
            terminate_unix_supervised_background_child(
                child,
                unix_supervisor_control.as_ref(),
                Some(&registration_identity),
                "target metadata attachment failure",
            )?;
            return Err(error);
        }
    }
    #[cfg(not(unix))]
    let target_pid = pid;
    #[cfg(unix)]
    {
        child = apply_managed_process_fault_with_unix_supervisor(
            fault_injection,
            "managed_process.after_effect_before_ack",
            "background",
            child,
            unix_supervisor_control.as_ref(),
            Some(&registration_identity),
        )?;
    }
    #[cfg(not(unix))]
    {
        child = apply_managed_process_fault_with_child(
            fault_injection,
            "managed_process.after_effect_before_ack",
            "background",
            child,
        )?;
    }
    let output_monitor = match start_background_output_monitor(
        child.child_mut(),
        policy.max_output_bytes as usize,
    ) {
        Ok(output_monitor) => output_monitor,
        Err(error) => {
            #[cfg(unix)]
            terminate_unix_supervised_background_child(
                child,
                unix_supervisor_control.as_ref(),
                Some(&registration_identity),
                "output monitor startup failure",
            )?;
            #[cfg(not(unix))]
            settle_background_registration_failure(child, &registration_identity)?;
            return Err(error);
        }
    };
    if let Err(error) =
        attach_background_output_monitor(&registration_identity, output_monitor.clone())
    {
        #[cfg(unix)]
        terminate_unix_supervised_background_child(
            child,
            unix_supervisor_control.as_ref(),
            Some(&registration_identity),
            "output monitor attachment failure",
        )?;
        #[cfg(not(unix))]
        settle_background_registration_failure(child, &registration_identity)?;
        return Err(error);
    }
    let Some(startup_check_wait) = bounded_background_process_wait(
        startup_budget,
        started_at.elapsed(),
        Duration::from_millis(BACKGROUND_STARTUP_CHECK_MS),
    ) else {
        #[cfg(unix)]
        terminate_unix_supervised_background_child(
            child,
            unix_supervisor_control.as_ref(),
            Some(&registration_identity),
            "startup budget expiry",
        )?;
        #[cfg(not(unix))]
        settle_background_registration_failure(child, &registration_identity)?;
        return Err(background_process_startup_budget_expired_error(input, lifetime_ms));
    };
    thread::sleep(startup_check_wait);
    if remaining_background_process_lifetime(startup_budget, started_at.elapsed()).is_none() {
        #[cfg(unix)]
        terminate_unix_supervised_background_child(
            child,
            unix_supervisor_control.as_ref(),
            Some(&registration_identity),
            "startup budget expiry",
        )?;
        #[cfg(not(unix))]
        settle_background_registration_failure(child, &registration_identity)?;
        return Err(background_process_startup_budget_expired_error(input, lifetime_ms));
    }
    let startup_status = match child.try_wait() {
        Ok(status) => status,
        Err(error) => {
            let error = SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "sandbox background process startup check failed for command '{}': {error}",
                    input.command
                ),
            };
            #[cfg(unix)]
            terminate_unix_supervised_background_child(
                child,
                unix_supervisor_control.as_ref(),
                Some(&registration_identity),
                "startup status failure",
            )?;
            #[cfg(not(unix))]
            settle_background_registration_failure(child, &registration_identity)?;
            return Err(error);
        }
    };
    if let Some(status) = startup_status {
        let startup_output_drain = bounded_background_process_wait(
            startup_budget,
            started_at.elapsed(),
            Duration::from_millis(BACKGROUND_STARTUP_OUTPUT_DRAIN_MS),
        )
        .unwrap_or(Duration::ZERO);
        let (stdout, stderr) = output_monitor.snapshot_after_startup_drain(startup_output_drain);
        #[cfg(windows)]
        let startup_was_superseded = windows_background_startup_was_superseded(
            &registration_identity,
            startup_budget.saturating_sub(started_at.elapsed()),
        );
        #[cfg(unix)]
        terminate_unix_supervised_background_child(
            child,
            unix_supervisor_control.as_ref(),
            Some(&registration_identity),
            "startup terminal status",
        )?;
        #[cfg(not(unix))]
        settle_background_registration_failure(child, &registration_identity)?;
        #[cfg(windows)]
        if startup_was_superseded {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "sandbox background process {pid} startup was superseded by verified Windows job termination before startup acknowledgement: {status}"
                ),
            });
        }
        if status.success() {
            release_background_process_tracking_if_stopped(pid);
            return background_launcher_completed_successfully(
                BackgroundLauncherCompletedContext {
                    policy,
                    status,
                    stdout: &stdout,
                    stderr: &stderr,
                    duration: started_at.elapsed(),
                    auto_background_reason,
                    lifetime_mode,
                    process_risk,
                },
            );
        }
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
    if output_monitor.quota_exceeded() {
        #[cfg(unix)]
        terminate_unix_supervised_background_child(
            child,
            unix_supervisor_control.as_ref(),
            Some(&registration_identity),
            "startup output quota",
        )?;
        #[cfg(not(unix))]
        settle_background_registration_failure(child, &registration_identity)?;
        return Err(background_process_output_quota_error(policy, &stdout, &stderr));
    }
    let post_output_exit_check = bounded_background_process_wait(
        startup_budget,
        started_at.elapsed(),
        Duration::from_millis(BACKGROUND_POST_OUTPUT_EXIT_CHECK_MS),
    )
    .unwrap_or(Duration::ZERO);
    // Second exit probe after the output drain: catches commands that print something and then
    // die (e.g. an unknown-subcommand banner), which the first probe is too early to see.
    let post_output_status =
        match wait_for_background_process_exit(&mut child, post_output_exit_check) {
            Ok(status) => status,
            Err(error) => {
                #[cfg(unix)]
                terminate_unix_supervised_background_child(
                    child,
                    unix_supervisor_control.as_ref(),
                    Some(&registration_identity),
                    "post-output status failure",
                )?;
                #[cfg(not(unix))]
                settle_background_registration_failure(child, &registration_identity)?;
                return Err(error);
            }
        };
    if let Some(status) = post_output_status {
        #[cfg(windows)]
        let startup_was_superseded = windows_background_startup_was_superseded(
            &registration_identity,
            startup_budget.saturating_sub(started_at.elapsed()),
        );
        #[cfg(unix)]
        terminate_unix_supervised_background_child(
            child,
            unix_supervisor_control.as_ref(),
            Some(&registration_identity),
            "post-output terminal status",
        )?;
        #[cfg(not(unix))]
        settle_background_registration_failure(child, &registration_identity)?;
        #[cfg(windows)]
        if startup_was_superseded {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "sandbox background process {pid} startup was superseded by verified Windows job termination before startup acknowledgement: {status}"
                ),
            });
        }
        if status.success() {
            release_background_process_tracking_if_stopped(pid);
            return background_launcher_completed_successfully(
                BackgroundLauncherCompletedContext {
                    policy,
                    status,
                    stdout: &stdout,
                    stderr: &stderr,
                    duration: started_at.elapsed(),
                    auto_background_reason,
                    lifetime_mode,
                    process_risk,
                },
            );
        }
        return Err(background_process_startup_failure(input, status, &stdout, &stderr));
    }
    let Some(remaining_lifetime) =
        remaining_background_process_lifetime(lifetime, started_at.elapsed())
    else {
        #[cfg(unix)]
        terminate_unix_supervised_background_child(
            child,
            unix_supervisor_control.as_ref(),
            Some(&registration_identity),
            "lifetime expiry before monitor handoff",
        )?;
        #[cfg(not(unix))]
        settle_background_registration_failure(child, &registration_identity)?;
        return Err(background_process_lifetime_expired_error(input, lifetime_ms));
    };
    #[cfg(unix)]
    let child = apply_managed_process_fault_with_unix_supervisor(
        fault_injection,
        "managed_process.after_ack_before_transition",
        "background",
        child,
        unix_supervisor_control.as_ref(),
        Some(&registration_identity),
    )?;
    #[cfg(not(unix))]
    let child = apply_managed_process_fault_with_child(
        fault_injection,
        "managed_process.after_ack_before_transition",
        "background",
        child,
    )?;
    let monitor_output = output_monitor.clone();
    // The monitor thread owns the child from here; it reaps a natural exit or kills the tree
    // when the remaining lifetime expires, so no background process can outlive its budget.
    let background_fault_injection = fault_injection.clone();
    let monitor =
        thread::Builder::new().name(format!("palyra-process-monitor-{pid}")).spawn(move || {
            monitor_background_child_until_lifetime(
                child,
                monitor_output,
                registration_identity,
                #[cfg(unix)]
                unix_supervisor_control,
                remaining_lifetime,
                background_fault_injection,
            )
        });
    if let Err(error) = monitor {
        let cleanup_error = terminate_retained_background_process(pid, &provenance).err();
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: match cleanup_error {
                Some(cleanup_error) => format!(
                    "failed to start background process monitor for pid {pid}: {error}; exact retained cleanup also failed: {cleanup_error}"
                ),
                None => format!(
                    "failed to start background process monitor for pid {pid}: {error}; exact retained cleanup verified process-tree absence"
                ),
            },
        });
    }

    let RedactedProcessOutputText {
        text: stdout_text,
        redacted: stdout_redacted,
        redaction_reasons: stdout_redaction_reasons,
    } = redacted_process_output(stdout.bytes.as_slice());
    let RedactedProcessOutputText {
        text: stderr_text,
        redacted: stderr_redacted,
        redaction_reasons: stderr_redaction_reasons,
    } = redacted_process_output(stderr.bytes.as_slice());
    let max_lifetime_ms = max_lifetime.as_millis() as u64;
    let requested_lifetime_ms = input.timeout_ms;
    let durable_handoff = lifetime_mode.is_detached_handoff();
    let ports = infer_background_handoff_ports(input, stdout_text.as_str(), stderr_text.as_str());
    let handoff = if durable_handoff {
        background_handoff_metadata(
            input,
            pid,
            cleanup.clone(),
            ports.as_slice(),
            lifetime_ms,
            max_lifetime_ms,
            lifetime_mode,
        )
    } else {
        Value::Null
    };
    let background_lifetime_adjustment_reason =
        background_lifetime_adjustment_reason(requested_lifetime_ms, lifetime_ms);
    let background_lifetime_adjusted = background_lifetime_adjustment_reason.is_some();
    let background_lifetime_adjustment_note =
        background_lifetime_adjustment_note(background_lifetime_adjustment_reason);
    let auto_backgrounded = auto_background_reason.is_some();
    let notification = process_completion_notification_projection(
        Some(input),
        ProcessCompletionState::Subscribed,
        Some(pid),
        stdout_text.as_str(),
        stderr_text.as_str(),
    );
    let output_json = serde_json::to_vec(&json!({
        "exit_code": Value::Null,
        "stdout": stdout_text,
        "stderr": stderr_text,
        "stdout_truncated": stdout.truncated,
        "stderr_truncated": stderr.truncated,
        "stdout_redacted": stdout_redacted,
        "stderr_redacted": stderr_redacted,
        "stdout_redaction_reasons": stdout_redaction_reasons,
        "stderr_redaction_reasons": stderr_redaction_reasons,
        "background_output_note": "stdout/stderr are bounded startup snapshots captured during the startup check, not command completion output; use an explicit fixed port if a dynamic port is not printed here",
        "duration_ms": 0,
        "background": true,
        "auto_backgrounded": auto_backgrounded,
        "auto_background_reason": auto_background_reason,
        "foreground_request_backgrounded": auto_backgrounded,
        "lifetime_mode": lifetime_mode.as_str(),
        "run_owned_lifetime": !durable_handoff,
        "durable_handoff": durable_handoff,
        "background_risk_posture": background_lifetime_risk_posture(durable_handoff),
        "run_lifecycle_note": background_run_lifecycle_note(durable_handoff),
        "started": true,
        "completed": false,
        "startup_success": true,
        "process_state": "running",
        "pid": pid,
        "ports": ports,
        "requested_lifetime_ms": requested_lifetime_ms,
        "lifetime_ms": lifetime_ms,
        "max_lifetime_ms": max_lifetime_ms,
        "min_background_lifetime_ms": MIN_BACKGROUND_PROCESS_LIFETIME_MS,
        "background_lifetime_adjusted": background_lifetime_adjusted,
        "background_lifetime_adjustment_reason": background_lifetime_adjustment_reason,
        "background_lifetime_note": format!(
            "{}{}",
            background_lifetime_adjustment_note,
            background_lifetime_note(durable_handoff, lifetime_ms, max_lifetime_ms)
        ),
        "target_pid": target_pid,
        "process_handle": {
            "kind": if cfg!(unix) { "unix_process_supervisor" } else { "pid" },
            "ownership_root_pid": pid,
            "direct_process_pid": pid,
            "target_pid": target_pid,
            "process_tree": cfg!(any(unix, windows)),
            "windows_job_object": windows_job_bound,
            "provenance": provenance,
            "capabilities": process_handle_capabilities_json(capabilities, ports.as_slice(), lifetime_mode),
            "identity_note": "pid identifies the ownership root, not necessarily the user target; lifecycle operations require the matching start token, executable digest, owner nonce, and process-group or Job Object identity"
        },
        "cleanup": cleanup,
        "handoff": handoff,
        "runtime_request": process_runtime_request_projection(Some(input)),
        "notification": notification,
        "verification_hint": background_verification_hint(ports.as_slice(), durable_handoff),
        "tier": policy.tier.as_str(),
        "sandbox_backend": process_runner_executor_name(policy),
        "process_risk": process_risk,
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox background process output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

fn background_lifetime_adjustment_reason(
    requested_lifetime_ms: Option<u64>,
    lifetime_ms: u64,
) -> Option<&'static str> {
    let requested = requested_lifetime_ms?;
    if lifetime_ms > requested {
        Some("raised_to_minimum_background_lifetime")
    } else if lifetime_ms < requested {
        Some("capped_by_effective_background_max_lifetime")
    } else {
        None
    }
}

fn background_lifetime_adjustment_note(reason: Option<&str>) -> &'static str {
    match reason {
        Some("raised_to_minimum_background_lifetime") => {
            "Requested timeout_ms was below the safe background minimum and was raised for local app/browser reliability. "
        }
        Some("capped_by_effective_background_max_lifetime") => {
            "Requested timeout_ms exceeded the effective background maximum and was capped by the operator-configured tool execution timeout or runtime hard cap. "
        }
        Some(_) => "Requested timeout_ms was normalized by background lifetime policy. ",
        None => "",
    }
}

fn background_run_lifecycle_note(durable_handoff: bool) -> &'static str {
    if durable_handoff {
        return "This background process is detached from terminal run cleanup and may keep running after the final answer. It remains bounded by auto_kill_after_ms and should be handed to the user or verifier with cleanup.portable_stop_command.";
    }
    "This background process is owned by the current agent run. Palyra automatically stops run-owned background processes when the run reaches a terminal state, so do not tell the user this PID or server will keep running after the final answer unless you explicitly stopped it first or requested a detached lifetime mode."
}

fn background_lifetime_note(
    durable_handoff: bool,
    lifetime_ms: u64,
    max_lifetime_ms: u64,
) -> String {
    if durable_handoff {
        return format!(
            "Palyra will not stop this detached background process at terminal run cleanup, but it remains bounded: the runtime monitor will auto-terminate it after {lifetime_ms}ms unless cleanup.portable_stop_command stops it first. Set timeout_ms up to {max_lifetime_ms}ms within the operator-configured tool execution timeout for longer verifier handoff windows."
        );
    }
    format!(
        "Palyra will auto-terminate this run-owned background process after {lifetime_ms}ms or when the current agent run reaches a terminal state, whichever happens first; omit timeout_ms for the default long-lived background server window, set timeout_ms up to {max_lifetime_ms}ms within the operator-configured tool execution timeout for long browser verification loops, and use cleanup.portable_stop_command when finished."
    )
}

fn background_lifetime_risk_posture(durable_handoff: bool) -> Value {
    json!({
        "blocks_execution": false,
        "requires_user_approval": false,
        "satisfied_by": "out_of_box_process_runner_policy",
        "detached_handoff_requested": durable_handoff,
        "note": if durable_handoff {
            "Detached background lifetimes run out of the box and return a bounded cleanup handoff; preserve the returned cleanup handle for the user or verifier."
        } else {
            "Run-owned background lifetimes run out of the box and are cleaned up automatically at terminal run cleanup."
        },
    })
}

fn background_handoff_metadata(
    input: &ProcessRunnerInput,
    pid: u32,
    cleanup: Value,
    ports: &[u16],
    lifetime_ms: u64,
    max_lifetime_ms: u64,
    lifetime_mode: BackgroundLifetimeMode,
) -> Value {
    json!({
        "kind": "background_process",
        "lifetime_mode": lifetime_mode.as_str(),
        "pid": pid,
        "ports": ports,
        "ports_source": if ports.is_empty() {
            "no explicit local port was inferred from args or startup output; verify the service with an explicit probe"
        } else {
            "inferred from args or bounded startup output; verify readiness with an explicit HTTP/browser probe"
        },
        "start_command": redacted_process_start_command(input),
        "stop_command": cleanup.pointer("/portable_stop_command").cloned().unwrap_or(Value::Null),
        "status_command": cleanup.pointer("/portable_status_command").cloned().unwrap_or(Value::Null),
        "cleanup_handle": cleanup,
        "auto_kill_after_ms": lifetime_ms,
        "max_lifetime_ms": max_lifetime_ms,
        "run_cleanup_behavior": "not_registered_for_terminal_run_cleanup",
        "verification_hint": background_verification_hint(ports, true),
        "handoff_note": "This detached process may remain alive after the final answer; include the stop_command and any verified URL/port in the final answer."
    })
}

fn background_verification_hint(ports: &[u16], durable_handoff: bool) -> &'static str {
    match (ports.is_empty(), durable_handoff) {
        (true, true) => {
            "No port was inferred; explicitly probe the service readiness before handoff and include cleanup.stop_command."
        }
        (false, true) => {
            "Probe the inferred port before final handoff and include cleanup.stop_command with the verified URL."
        }
        (true, false) => {
            "No port was inferred; use an explicit probe before relying on this run-owned background process."
        }
        (false, false) => {
            "Probe the inferred port before browser or API verification; this process is run-owned and will be cleaned up automatically."
        }
    }
}

fn redacted_process_start_command(input: &ProcessRunnerInput) -> Value {
    json!({
        "command": redacted_process_command_token(input.command.as_str()),
        "args": input
            .args
            .iter()
            .map(|arg| redacted_process_command_token(arg.as_str()))
            .collect::<Vec<_>>(),
        "cwd": input.cwd.as_deref().unwrap_or("/workspace"),
        "env": {
            "omitted": true,
            "provided_key_count": input.env.len(),
        },
    })
}

fn redacted_process_command_token(raw: &str) -> String {
    redacted_process_output_text(raw).text
}

fn infer_background_handoff_ports(
    input: &ProcessRunnerInput,
    stdout_text: &str,
    stderr_text: &str,
) -> Vec<u16> {
    let mut ports = BTreeSet::new();
    ports.extend(input.port_hints.iter().copied());
    collect_ports_from_args(input.args.as_slice(), &mut ports);
    collect_ports_from_text(stdout_text, &mut ports);
    collect_ports_from_text(stderr_text, &mut ports);
    ports.into_iter().collect()
}

fn process_handle_capabilities_json(
    capabilities: BackgroundProcessHandleCapabilities,
    ports: &[u16],
    lifetime_mode: BackgroundLifetimeMode,
) -> Value {
    json!({
        "stdin": capabilities.stdin,
        "pty_requested": capabilities.pty_requested,
        "pty": capabilities.pty,
        "signals": capabilities.signals,
        "background": capabilities.background,
        "port_hints": ports,
        "lifetime_mode": lifetime_mode.as_str(),
        "pty_degraded_reason": if capabilities.pty_requested && !capabilities.pty {
            json!("pty_backend_unavailable")
        } else {
            Value::Null
        },
    })
}

fn collect_ports_from_args(args: &[String], ports: &mut BTreeSet<u16>) {
    let mut index = 0usize;
    while index < args.len() {
        let arg = args[index].trim();
        if matches!(arg, "--port" | "-p" | "--listen-port") {
            if let Some(next) = args.get(index.saturating_add(1)) {
                push_port_candidate(ports, next.as_str());
            }
            index = index.saturating_add(2);
            continue;
        }
        if let Some((key, value)) = arg.split_once('=') {
            if matches!(key, "--port" | "port" | "PORT" | "--listen-port") {
                push_port_candidate(ports, value);
            }
        }
        collect_ports_from_local_endpoint_token(arg, ports);
        index = index.saturating_add(1);
    }
}

fn collect_ports_from_text(text: &str, ports: &mut BTreeSet<u16>) {
    for token in text.split_whitespace() {
        let trimmed = token.trim_matches(|character: char| {
            matches!(character, '"' | '\'' | '(' | ')' | '[' | ']' | '{' | '}' | ',' | ';')
        });
        collect_ports_from_local_endpoint_token(trimmed, ports);
        if let Some((key, value)) = trimmed.split_once('=') {
            if key.eq_ignore_ascii_case("port") || key.eq_ignore_ascii_case("PORT") {
                push_port_candidate(ports, value);
            }
        }
    }
}

fn collect_ports_from_local_endpoint_token(token: &str, ports: &mut BTreeSet<u16>) {
    let normalized = token.to_ascii_lowercase();
    for marker in ["localhost:", "127.0.0.1:", "0.0.0.0:", "[::1]:"] {
        let Some(index) = normalized.find(marker) else {
            continue;
        };
        let start = index.saturating_add(marker.len());
        push_port_candidate(ports, &normalized[start..]);
    }
}

fn push_port_candidate(ports: &mut BTreeSet<u16>, raw: &str) {
    let digits = raw
        .trim()
        .trim_start_matches(':')
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() || digits.len() > 5 {
        return;
    }
    if let Ok(port) = digits.parse::<u16>() {
        if port > 0 {
            ports.insert(port);
        }
    }
}

fn background_launcher_completed_successfully(
    context: BackgroundLauncherCompletedContext<'_>,
) -> Result<SandboxProcessRunSuccess, SandboxProcessRunError> {
    let BackgroundLauncherCompletedContext {
        policy,
        status,
        stdout,
        stderr,
        duration,
        auto_background_reason,
        lifetime_mode,
        process_risk,
    } = context;
    let RedactedProcessOutputText {
        text: stdout_text,
        redacted: stdout_redacted,
        redaction_reasons: stdout_redaction_reasons,
    } = redacted_process_output(stdout.bytes.as_slice());
    let RedactedProcessOutputText {
        text: stderr_text,
        redacted: stderr_redacted,
        redaction_reasons: stderr_redaction_reasons,
    } = redacted_process_output(stderr.bytes.as_slice());
    let auto_backgrounded = auto_background_reason.is_some();
    if auto_backgrounded && matches!(lifetime_mode, BackgroundLifetimeMode::RunOwned) {
        let reason = auto_background_reason.unwrap_or("recognized_dev_server");
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "auto-backgrounded dev server exited before a trackable process was available: \
error_code=untracked_auto_background_service auto_background_reason={reason} \
run_owned_lifetime=false. Start the server as a direct long-running foreground process, or request \
background=true only for commands whose direct child remains alive and can be stopped by \
terminal run cleanup."
            ),
        });
    }
    let output_json = serde_json::to_vec(&json!({
        "exit_code": status.code(),
        "stdout": stdout_text,
        "stderr": stderr_text,
        "stdout_preview": redacted_process_output_preview(stdout.bytes.as_slice()),
        "stderr_preview": redacted_process_output_preview(stderr.bytes.as_slice()),
        "stdout_truncated": stdout.truncated,
        "stderr_truncated": stderr.truncated,
        "stdout_redacted": stdout_redacted,
        "stderr_redacted": stderr_redacted,
        "stdout_redaction_reasons": stdout_redaction_reasons,
        "stderr_redaction_reasons": stderr_redaction_reasons,
        "background_output_note": "stdout/stderr are bounded startup snapshots captured before the direct launcher exited successfully; the launcher process tree was terminated because no trackable background process remained",
        "duration_ms": duration.as_millis() as u64,
        "background": true,
        "auto_backgrounded": auto_backgrounded,
        "auto_background_reason": auto_background_reason,
        "foreground_request_backgrounded": auto_backgrounded,
        "lifetime_mode": lifetime_mode.as_str(),
        "run_owned_lifetime": false,
        "durable_handoff": false,
        "run_lifecycle_note": "The direct background launcher exited successfully before a trackable process was available. Palyra terminated the launcher process tree and is not tracking a run-owned child process.",
        "started": true,
        "completed": true,
        "launcher_completed_successfully": true,
        "startup_success": true,
        "process_state": "completed",
        "tracked_pid": Value::Null,
        "pid": Value::Null,
        "cleanup": Value::Null,
        "note": "direct launcher exited successfully; launcher process tree was terminated because no trackable background process remained",
        "tier": policy.tier.as_str(),
        "sandbox_backend": process_runner_executor_name(policy),
        "process_risk": process_risk,
    }))
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("failed to serialize sandbox background launcher output JSON: {error}"),
    })?;
    Ok(SandboxProcessRunSuccess { output_json })
}

fn wait_for_background_process_exit(
    child: &mut ManagedChildGuard,
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

fn background_process_output_quota_error(
    policy: &SandboxProcessRunnerPolicy,
    stdout: &StreamCapture,
    stderr: &StreamCapture,
) -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::QuotaExceeded,
        message: format!(
            "sandbox background process exceeded output quota (max_output_bytes={}) and was terminated; process_output_summary={}",
            policy.max_output_bytes,
            process_output_diagnostic_summary(stdout, stderr)
        ),
    }
}

fn background_process_startup_failure(
    input: &ProcessRunnerInput,
    status: ExitStatus,
    stdout: &StreamCapture,
    stderr: &StreamCapture,
) -> SandboxProcessRunError {
    let failure_class = process_exit_failure_class(status);
    let stdout_preview = redacted_process_failure_preview(stdout.bytes.as_slice());
    let stderr_preview = redacted_process_failure_preview(stderr.bytes.as_slice());
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "sandbox background process exited before startup check (failure_class={}, code={}) for command '{}', stdout_bytes={}, stdout_truncated={}, stderr_bytes={}, stderr_truncated={}; stdout_preview={stdout_preview:?}; stderr_preview={stderr_preview:?}; use the cwd field instead of command-line cwd flags, verify the server command, and probe the expected port before browser navigation",
            failure_class.as_str(),
            status.code().unwrap_or(-1),
            input.command,
            stdout.bytes.len(),
            stdout.truncated,
            stderr.bytes.len(),
            stderr.truncated,
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
        Arc::clone(&quota_triggered),
        Arc::clone(&stderr_capture),
    );
    Ok(BackgroundOutputMonitor { stdout: stdout_capture, stderr: stderr_capture, quota_triggered })
}

// Background variant of spawn_capture_reader: publishes incrementally into a shared capture
// so startup snapshots can be taken while the process keeps running, instead of returning the
// buffer once at join time.
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
                        quota_triggered.store(true, Ordering::Release);
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

#[cfg(windows)]
fn terminate_owned_background_process_tree(pid: u32) -> io::Result<()> {
    windows_background_job(pid)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!("background process {pid} has no owned Windows job object"),
            )
        })?
        .terminate()
}

#[cfg(unix)]
fn terminate_owned_background_process_tree(pid: u32) -> io::Result<()> {
    match terminate_unix_process_group(pid) {
        Ok(()) => Ok(()),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(not(any(unix, windows)))]
fn terminate_owned_background_process_tree(pid: u32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("background process tree ownership is unavailable for pid {pid}"),
    ))
}

#[cfg(windows)]
fn owned_background_process_tree_is_alive(pid: u32) -> io::Result<bool> {
    windows_background_job_active_process_count(pid)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!("background process {pid} has no owned Windows job object"),
            )
        })?
        .map(|active_count| active_count > 0)
}

#[cfg(unix)]
fn owned_background_process_tree_is_alive(pid: u32) -> io::Result<bool> {
    unix_process_group_is_alive(pid)
}

#[cfg(not(any(unix, windows)))]
fn owned_background_process_tree_is_alive(pid: u32) -> io::Result<bool> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("background process tree ownership is unavailable for pid {pid}"),
    ))
}

fn terminate_owned_background_process_tree_for_identity(
    pid: u32,
    identity: Option<&BackgroundProcessIdentity>,
) -> io::Result<()> {
    #[cfg(windows)]
    if let Some(identity) = identity {
        return identity
            .windows_job
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("background process {pid} has no exact Windows Job Object capability"),
                )
            })?
            .terminate();
    }
    let _ = identity;
    terminate_owned_background_process_tree(pid)
}

fn owned_background_process_tree_is_alive_for_identity(
    pid: u32,
    identity: Option<&BackgroundProcessIdentity>,
) -> io::Result<bool> {
    #[cfg(windows)]
    if let Some(identity) = identity {
        return identity
            .windows_job
            .as_ref()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!("background process {pid} has no exact Windows Job Object capability"),
                )
            })?
            .active_process_count()
            .map(|active_count| active_count > 0);
    }
    let _ = identity;
    owned_background_process_tree_is_alive(pid)
}

fn wait_for_owned_background_tree_inactive_for_identity(
    pid: u32,
    identity: Option<&BackgroundProcessIdentity>,
    max_wait: Duration,
) -> io::Result<bool> {
    let started_at = Instant::now();
    loop {
        if !owned_background_process_tree_is_alive_for_identity(pid, identity)? {
            return Ok(true);
        }
        if started_at.elapsed() >= max_wait {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS));
    }
}

fn request_background_child_termination(
    child: &mut ManagedChildGuard,
    identity: Option<&BackgroundProcessIdentity>,
) -> io::Result<()> {
    #[cfg(windows)]
    if let Some(identity) = identity {
        terminate_owned_background_process_tree_for_identity(child.id(), Some(identity))?;
        child.note_owned_tree_termination_requested();
        return Ok(());
    }
    let _ = identity;
    child.request_termination()
}

fn release_verified_background_process_tracking_exact(identity: &BackgroundProcessIdentity) {
    mark_background_process_stopped(identity);
    #[cfg(windows)]
    if !background_process_cleanup_authority_retained_exact(identity) {
        if let Some(job) = identity.windows_job.as_ref() {
            remove_windows_background_job_exact(identity.pid, job);
        }
    }
}

fn release_verified_background_process_tracking(pid: u32) {
    if let Ok(snapshot) = registered_background_process("release tracking", pid) {
        release_verified_background_process_tracking_exact(&snapshot.identity);
    } else {
        #[cfg(windows)]
        remove_windows_background_job(pid);
    }
}

#[cfg(not(unix))]
fn settle_background_registration_failure(
    child: ManagedChildGuard,
    identity: &BackgroundProcessIdentity,
) -> Result<(), SandboxProcessRunError> {
    match terminate_background_child_exact(child, identity) {
        Ok(()) => Ok(()),
        Err(cleanup_error) => {
            let process_alive = background_process_runtime_status_for_identity(identity)
                .map(BackgroundProcessRuntimeStatus::alive)
                .unwrap_or(true);
            // The registration callback may already have used and released the exact platform
            // ownership anchor. Suppress the duplicate cleanup failure only when that callback
            // also proved the ownership domain absent; otherwise losing the anchor remains fatal.
            if !process_alive && !background_process_cleanup_authority_retained_exact(identity) {
                mark_background_process_stopped(identity);
                Ok(())
            } else {
                Err(cleanup_error)
            }
        }
    }
}

fn terminate_background_child(mut child: ManagedChildGuard) -> Result<(), SandboxProcessRunError> {
    terminate_background_child_with_identity(&mut child, None)
}

#[cfg(not(unix))]
fn terminate_background_child_exact(
    mut child: ManagedChildGuard,
    identity: &BackgroundProcessIdentity,
) -> Result<(), SandboxProcessRunError> {
    terminate_background_child_with_identity(&mut child, Some(identity))
}

fn terminate_background_child_with_identity(
    child: &mut ManagedChildGuard,
    identity: Option<&BackgroundProcessIdentity>,
) -> Result<(), SandboxProcessRunError> {
    let pid = child.id();
    match child.try_wait() {
        Ok(Some(_)) => {
            terminate_owned_background_process_tree_for_identity(pid, identity).map_err(
                |error| SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!(
                        "sandbox background descendant cleanup failed for reaped pid {pid}: {error}"
                    ),
                },
            )?;
        }
        Ok(None) => {
            request_background_child_termination(child, identity).map_err(|error| {
                SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!(
                        "sandbox background process cleanup failed for pid {pid}: {error}"
                    ),
                }
            })?;
        }
        Err(wait_error) => {
            terminate_owned_background_process_tree_for_identity(pid, identity).map_err(
                |termination_error| {
                SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!(
                        "sandbox background wait probe failed for pid {pid}: {wait_error}; owned-tree termination also failed: {termination_error}"
                    ),
                }
            },
            )?;
            child.note_owned_tree_termination_requested();
        }
    }
    let direct_exit = child
        .wait_for_exit(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS))
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!("sandbox background process cleanup failed for pid {pid}: {error}"),
        })?;
    if direct_exit.is_none() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "sandbox background process {pid} did not exit within the bounded cleanup window"
            ),
        });
    }
    let tree_inactive = wait_for_owned_background_tree_inactive_for_identity(
        pid,
        identity,
        Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS),
    )
    .map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!("sandbox background tree verification failed for pid {pid}: {error}"),
    })?;
    if !tree_inactive {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "sandbox background process tree {pid} remained active after bounded cleanup"
            ),
        });
    }
    if let Some(identity) = identity {
        release_verified_background_process_tracking_exact(identity);
    } else {
        release_verified_background_process_tracking(pid);
    }
    Ok(())
}

#[cfg(unix)]
fn terminate_unix_supervised_background_child(
    mut child: ManagedChildGuard,
    supervisor_control: &UnixProcessSupervisorControl,
    registration_identity: Option<&BackgroundProcessIdentity>,
    reason: &str,
) -> Result<(), SandboxProcessRunError> {
    let pid = child.id();
    supervisor_control.terminate().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "trusted Unix process supervisor cleanup failed for pid {pid} ({reason}): {error}"
        ),
    })?;
    let direct_exit = child
        .wait_for_exit(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS))
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "trusted Unix process supervisor reap failed for pid {pid} ({reason}): {error}"
            ),
        })?;
    if direct_exit.is_none() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "trusted Unix process supervisor pid {pid} did not exit after acknowledged cleanup ({reason})"
            ),
        });
    }
    if let Some(identity) = registration_identity {
        mark_background_process_stopped_after_unix_cleanup(identity);
    }
    Ok(())
}

fn monitor_background_child_until_lifetime(
    mut child: ManagedChildGuard,
    output_monitor: BackgroundOutputMonitor,
    registration_identity: BackgroundProcessIdentity,
    #[cfg(unix)] supervisor_control: Arc<UnixProcessSupervisorControl>,
    lifetime: Duration,
    fault_injection: crate::qa_fault_injection::QaFaultRuntime,
) {
    let pid = child.id();
    let started_at = Instant::now();
    let mut direct_exit_status = None;
    loop {
        if output_monitor.quota_exceeded() {
            #[cfg(unix)]
            let cleanup = terminate_unix_supervised_background_child(
                child,
                supervisor_control.as_ref(),
                Some(&registration_identity),
                "output quota",
            );
            #[cfg(not(unix))]
            let cleanup = terminate_background_child_exact(child, &registration_identity);
            if let Err(error) = cleanup {
                warn!(error = ?error, pid, "background process output-quota cleanup failed");
            } else {
                record_background_process_terminal_state(
                    &registration_identity,
                    "failed",
                    "output_quota_exceeded",
                    None,
                );
                warn!(
                    pid,
                    reason_code = "process.output_quota_exceeded",
                    "background process exceeded output quota and was terminated"
                );
            }
            return;
        }
        if direct_exit_status.is_none() {
            match child.try_wait() {
                Ok(Some(status)) => {
                    direct_exit_status = Some(status);
                }
                Ok(None) => {}
                Err(error) => {
                    warn!(error = ?error, pid, "background process wait failed; forcing bounded cleanup");
                    #[cfg(unix)]
                    let cleanup = terminate_unix_supervised_background_child(
                        child,
                        supervisor_control.as_ref(),
                        Some(&registration_identity),
                        "wait failure",
                    );
                    #[cfg(not(unix))]
                    let cleanup = terminate_background_child_exact(child, &registration_identity);
                    if let Err(cleanup_error) = cleanup {
                        warn!(error = ?cleanup_error, pid, "background process wait-failure cleanup failed");
                    } else {
                        record_background_process_terminal_state(
                            &registration_identity,
                            "failed",
                            "wait_failed",
                            None,
                        );
                    }
                    return;
                }
            }
        }
        if let Some(status) = direct_exit_status {
            #[cfg(unix)]
            {
                match supervisor_control.terminate() {
                    Ok(()) => {
                        mark_background_process_stopped_after_unix_cleanup(&registration_identity);
                        record_background_process_terminal_state(
                            &registration_identity,
                            if status.success() { "exited" } else { "failed" },
                            if status.success() { "completed" } else { "nonzero_exit" },
                            status.code(),
                        );
                    }
                    Err(error) => {
                        warn!(
                            error = ?error,
                            pid,
                            "trusted Unix supervisor exited without consumable cleanup acknowledgement; retaining process tracking"
                        );
                    }
                }
                return;
            }
            #[cfg(not(unix))]
            match owned_background_process_tree_is_alive_for_identity(
                pid,
                Some(&registration_identity),
            ) {
                Ok(false) => {
                    release_verified_background_process_tracking_exact(&registration_identity);
                    record_background_process_terminal_state(
                        &registration_identity,
                        if status.success() { "exited" } else { "failed" },
                        if status.success() { "completed" } else { "nonzero_exit" },
                        status.code(),
                    );
                    return;
                }
                Ok(true) => {}
                Err(error) => {
                    warn!(error = ?error, pid, "owned background tree status failed; forcing bounded cleanup");
                    let cleanup = terminate_background_child_exact(child, &registration_identity);
                    if let Err(cleanup_error) = cleanup {
                        warn!(error = ?cleanup_error, pid, "background status-failure cleanup failed");
                    }
                    return;
                }
            }
        }

        let elapsed = started_at.elapsed();
        if elapsed >= lifetime {
            #[cfg(unix)]
            let cleanup = terminate_unix_supervised_background_child(
                child,
                supervisor_control.as_ref(),
                Some(&registration_identity),
                "lifetime expiry",
            );
            #[cfg(not(unix))]
            let cleanup = terminate_background_child_exact(child, &registration_identity);
            match cleanup {
                Ok(()) => {
                    record_background_process_terminal_state(
                        &registration_identity,
                        "lifetime_expired",
                        "lifetime_expired",
                        None,
                    );
                    if let Err(error) = apply_managed_process_fault_after_verified_cleanup(
                        &fault_injection,
                        "managed_process.during_cleanup",
                        "background",
                    ) {
                        warn!(error = ?error, pid, "background process cleanup fault adapter returned an error");
                    }
                }
                Err(error) => {
                    warn!(error = ?error, pid, "background process cleanup verification failed");
                }
            }
            return;
        }

        let remaining = lifetime.saturating_sub(elapsed);
        thread::sleep(remaining.min(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS)));
    }
}

#[cfg(windows)]
fn bind_windows_background_child(
    child: ManagedChildGuard,
) -> Result<ManagedChildGuard, SandboxProcessRunError> {
    bind_windows_background_child_with_operation(child, bind_child_to_windows_background_job)
}

#[cfg(windows)]
fn bind_windows_background_child_with_operation<Bind>(
    mut child: ManagedChildGuard,
    bind: Bind,
) -> Result<ManagedChildGuard, SandboxProcessRunError>
where
    Bind: FnOnce(&Child, u32) -> io::Result<()>,
{
    let pid = child.id();
    if let Err(bind_error) = bind(child.child(), pid) {
        let cleanup = child
            .terminate_and_reap(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS))
            .map(|status| status.is_some());
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "sandbox background process {pid} could not bind its required Windows job object before resume: {bind_error}; bounded direct-exit verification: {cleanup:?}"
            ),
        });
    }
    Ok(child)
}

#[cfg(windows)]
fn resume_windows_background_child(child: &ManagedChildGuard) -> io::Result<()> {
    resume_suspended_windows_process(child.id())
}

// A successful exact Job Object termination distinguishes terminal cleanup from an ordinary
// startup exit; only a subsequently inactive ownership domain may supersede startup.
#[cfg(windows)]
fn windows_background_startup_was_superseded(
    identity: &BackgroundProcessIdentity,
    max_wait: Duration,
) -> bool {
    let Some(job) = identity.windows_job.as_deref() else {
        return false;
    };
    if !job.termination_was_requested_and_succeeded() {
        return false;
    }
    let status = wait_for_windows_background_process_inactive(
        identity.pid,
        &identity.provenance,
        job,
        max_wait,
    )
    .ok()
    .flatten();
    windows_background_startup_cleanup_is_authoritative(true, status)
}

#[cfg(windows)]
fn windows_background_startup_cleanup_is_authoritative(
    termination_succeeded: bool,
    status: Option<BackgroundProcessRuntimeStatus>,
) -> bool {
    termination_succeeded && status.is_some_and(|status| !status.alive())
}

#[cfg(windows)]
fn prepare_windows_background_child(
    child: ManagedChildGuard,
) -> Result<ManagedChildGuard, SandboxProcessRunError> {
    prepare_windows_background_child_with_operations(
        child,
        bind_child_to_windows_background_job,
        resume_suspended_windows_process,
    )
}

#[cfg(windows)]
fn prepare_windows_background_child_with_operations<Bind, Resume>(
    mut child: ManagedChildGuard,
    bind: Bind,
    resume: Resume,
) -> Result<ManagedChildGuard, SandboxProcessRunError>
where
    Bind: FnOnce(&Child, u32) -> io::Result<()>,
    Resume: FnOnce(u32) -> io::Result<()>,
{
    let pid = child.id();
    if let Err(bind_error) = bind(child.child(), pid) {
        let cleanup = child
            .terminate_and_reap(Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS))
            .map(|status| status.is_some());
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "sandbox background process {pid} could not bind its required Windows job object before resume: {bind_error}; bounded direct-exit verification: {cleanup:?}"
            ),
        });
    }

    if let Err(resume_error) = resume(pid) {
        let cleanup = terminate_background_child(child);
        remove_windows_background_job(pid);
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "sandbox background process {pid} could not resume after Windows job ownership was established: {resume_error}; bounded owned-tree cleanup verification: {cleanup:?}"
            ),
        });
    }

    Ok(child)
}

#[cfg(windows)]
fn bind_child_to_windows_background_job(child: &Child, pid: u32) -> io::Result<()> {
    bind_child_to_windows_background_job_with_register(child, pid, register_windows_background_job)
}

#[cfg(windows)]
fn bind_child_to_windows_background_job_with_register<Register>(
    child: &Child,
    pid: u32,
    register: Register,
) -> io::Result<()>
where
    Register: FnOnce(u32, Arc<WindowsBackgroundJob>) -> io::Result<()>,
{
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

    register(pid, Arc::new(job))
}

#[cfg(windows)]
pub(crate) fn resume_suspended_windows_process(pid: u32) -> io::Result<()> {
    // SAFETY: TH32CS_SNAPTHREAD ignores the process-id parameter and returns an owned snapshot
    // handle on success.
    let snapshot_handle = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
    let snapshot =
        WindowsOwnedHandle::new(snapshot_handle, "thread snapshot").map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to snapshot threads before resuming process {pid}: {error}"),
            )
        })?;
    let resume_result = resume_suspended_windows_process_from_snapshot(snapshot.get(), pid);
    combine_windows_operation_and_close_result(resume_result, snapshot.close(), "thread snapshot")
}

#[cfg(windows)]
fn resume_suspended_windows_process_from_snapshot(snapshot: HANDLE, pid: u32) -> io::Result<()> {
    let mut entry = THREADENTRY32 {
        dwSize: std::mem::size_of::<THREADENTRY32>() as u32,
        ..THREADENTRY32::default()
    };
    // SAFETY: `snapshot` is a live ToolHelp snapshot and `entry` is a writable structure whose
    // required size field is initialized.
    if unsafe { Thread32First(snapshot, &mut entry) } == 0 {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!("failed to enumerate the initial suspended thread for process {pid}: {error}"),
        ));
    }

    let mut resumed_threads = 0_u32;
    loop {
        if entry.th32OwnerProcessID == pid {
            resume_suspended_windows_thread(pid, entry.th32ThreadID)?;
            resumed_threads = resumed_threads.saturating_add(1);
        }

        // SAFETY: the snapshot and output structure remain valid for the full enumeration.
        if unsafe { Thread32Next(snapshot, &mut entry) } != 0 {
            continue;
        }
        // SAFETY: GetLastError has no preconditions and is read immediately after Thread32Next.
        let error_code = unsafe { GetLastError() };
        if error_code != ERROR_NO_MORE_FILES {
            return Err(io::Error::new(
                io::Error::from_raw_os_error(error_code as i32).kind(),
                format!(
                    "thread enumeration failed while resuming process {pid}: {}",
                    io::Error::from_raw_os_error(error_code as i32)
                ),
            ));
        }
        break;
    }

    if resumed_threads == 0 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no suspended thread was found for background process {pid}"),
        ));
    }
    Ok(())
}

#[cfg(windows)]
fn resume_suspended_windows_thread(pid: u32, thread_id: u32) -> io::Result<()> {
    // SAFETY: the access mask requests only resume rights, handle inheritance is disabled, and
    // `thread_id` came from the live ToolHelp snapshot.
    let thread_handle = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, thread_id) };
    let thread =
        WindowsOwnedHandle::new(thread_handle, "background process thread").map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("failed to open suspended thread {thread_id} for process {pid}: {error}"),
            )
        })?;

    // SAFETY: `thread` owns a live handle opened with THREAD_SUSPEND_RESUME access.
    let previous_suspend_count = unsafe { ResumeThread(thread.get()) };
    let resume_result = if previous_suspend_count == u32::MAX {
        let error = io::Error::last_os_error();
        Err(io::Error::new(
            error.kind(),
            format!("failed to resume thread {thread_id} for process {pid}: {error}"),
        ))
    } else if previous_suspend_count != 1 {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "thread {thread_id} for process {pid} had unexpected suspend count {previous_suspend_count}; expected exactly one CREATE_SUSPENDED hold"
            ),
        ))
    } else {
        Ok(())
    };

    combine_windows_operation_and_close_result(
        resume_result,
        thread.close(),
        "background process thread",
    )
}

#[cfg(windows)]
fn combine_windows_operation_and_close_result(
    operation: io::Result<()>,
    close: io::Result<()>,
    handle_kind: &str,
) -> io::Result<()> {
    match (operation, close) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(close_error)) => Err(io::Error::new(
            close_error.kind(),
            format!("failed to close {handle_kind} handle: {close_error}"),
        )),
        (Err(error), Err(close_error)) => Err(io::Error::other(format!(
            "{error}; failed to close {handle_kind} handle: {close_error}"
        ))),
    }
}

#[cfg(windows)]
fn create_windows_background_job() -> io::Result<WindowsBackgroundJob> {
    // SAFETY: null security attributes and an unnamed job object are valid inputs.
    let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
    if !windows_handle_is_valid(handle) {
        return Err(io::Error::last_os_error());
    }

    let job = WindowsBackgroundJob { handle, termination_succeeded: Mutex::new(false) };
    let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
    // Kill-on-close means the OS tears the whole tree down if the daemon exits or the last job
    // handle is dropped, so orphaned background trees cannot survive a daemon crash.
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

// Win32 failure sentinels are inconsistent (CreateJobObjectW returns NULL, file APIs return
// INVALID_HANDLE_VALUE), so both are rejected.
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
            if let Some(existing) = jobs.get(&pid) {
                if existing.active_process_count()? > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!(
                            "background process {pid} already owns an active Windows Job Object"
                        ),
                    ));
                }
            }
            jobs.insert(pid, job);
            Ok(())
        }
        Err(error) => Err(io::Error::other(format!(
            "windows background job registry lock poisoned for pid {pid}: {error}"
        ))),
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
fn windows_background_job(pid: u32) -> Option<Arc<WindowsBackgroundJob>> {
    match windows_background_jobs().lock() {
        Ok(jobs) => jobs.get(&pid).cloned(),
        Err(_) => None,
    }
}

#[cfg(windows)]
fn remove_windows_background_job(pid: u32) {
    if let Ok(mut jobs) = windows_background_jobs().lock() {
        jobs.remove(&pid);
    }
}

#[cfg(any(windows, test))]
fn remove_arc_registry_entry_if_same<T>(
    entries: &mut HashMap<u32, Arc<T>>,
    pid: u32,
    expected: &Arc<T>,
) -> bool {
    if !entries.get(&pid).is_some_and(|current| Arc::ptr_eq(current, expected)) {
        return false;
    }
    entries.remove(&pid);
    true
}

#[cfg(windows)]
fn remove_windows_background_job_exact(pid: u32, expected: &Arc<WindowsBackgroundJob>) -> bool {
    windows_background_jobs()
        .lock()
        .map(|mut jobs| remove_arc_registry_entry_if_same(&mut jobs, pid, expected))
        .unwrap_or(false)
}

/// Releases platform-specific process-tree tracking once a caller has verified the tree is
/// inactive.
#[cfg(windows)]
pub(crate) fn release_background_process_tracking_if_stopped(pid: u32) {
    let Ok(snapshot) = registered_background_process("release tracking", pid) else {
        return;
    };
    release_background_process_tracking_if_stopped_exact(&snapshot.identity);
}

#[cfg(windows)]
fn release_background_process_tracking_if_stopped_exact(identity: &BackgroundProcessIdentity) {
    if background_process_runtime_status_for_identity(identity)
        .map(|status| !status.alive())
        .unwrap_or(false)
    {
        mark_background_process_stopped(identity);
        if !background_process_cleanup_authority_retained_exact(identity) {
            if let Some(job) = identity.windows_job.as_ref() {
                remove_windows_background_job_exact(identity.pid, job);
            }
        }
    }
}

/// Releases platform-specific process-tree tracking once a caller has verified the tree is
/// inactive.
#[cfg(not(windows))]
pub(crate) fn release_background_process_tracking_if_stopped(pid: u32) {
    let Ok(snapshot) = registered_background_process("release tracking", pid) else {
        return;
    };
    release_background_process_tracking_if_stopped_exact(&snapshot.identity);
}

#[cfg(not(windows))]
fn release_background_process_tracking_if_stopped_exact(identity: &BackgroundProcessIdentity) {
    if background_process_runtime_status(identity.pid)
        .map(|status| !status.alive())
        .unwrap_or(false)
    {
        mark_background_process_stopped(identity);
    }
}

#[cfg(unix)]
fn registered_unix_supervisor_control(
    pid: u32,
    expected: &ProcessProvenance,
    require_retained: bool,
) -> io::Result<Arc<UnixProcessSupervisorControl>> {
    let processes = registered_background_processes().lock().map_err(|error| {
        io::Error::other(format!(
            "background process registry lock poisoned for pid {pid}: {error}"
        ))
    })?;
    let process = processes.get(&pid).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!("background process {pid} has no registered Unix supervisor capability"),
        )
    })?;
    if process.provenance != *expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("background process {pid} Unix supervisor provenance does not match"),
        ));
    }
    if require_retained && !process.cleanup_authority_retained {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("background process {pid} cleanup authority is not retained"),
        ));
    }
    process.supervisor_control.clone().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("background process {pid} has no exact Unix supervisor capability"),
        )
    })
}

#[cfg(unix)]
fn terminate_registered_unix_supervisor(pid: u32, expected: &ProcessProvenance) -> io::Result<()> {
    let supervisor_control = registered_unix_supervisor_control(pid, expected, false)?;
    match verify_background_process_provenance(pid, expected) {
        ProcessProvenanceDisposition::Match => supervisor_control.terminate(),
        ProcessProvenanceDisposition::Missing | ProcessProvenanceDisposition::Unsupported => {
            // The exact control capability, not the reusable numeric PID, is the authority here.
            // It may consume an autonomous cleanup acknowledgement after the supervisor exits.
            supervisor_control.terminate()
        }
        ProcessProvenanceDisposition::Mismatch => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("background process {pid} provenance no longer matches"),
        )),
    }
}

/// Terminates a retained process tree and returns its verified inactive status.
///
/// This recovery path is used when the process launched successfully but its durable lease could
/// not be committed. It requires the exact in-memory ownership anchor and never falls back to a
/// PID-only signal.
///
/// # Errors
/// Returns an error when provenance no longer matches, termination fails, or ownership-domain
/// absence cannot be established within the bounded cleanup window.
pub(crate) fn terminate_retained_background_process(
    pid: u32,
    expected: &ProcessProvenance,
) -> io::Result<BackgroundProcessRuntimeStatus> {
    require_background_process_cleanup_authority(pid, expected)?;
    let registration = registered_background_process("retained cleanup", pid).map_err(|error| {
        io::Error::other(format!("failed to capture retained process identity: {}", error.message))
    })?;
    if registration.provenance != *expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("background process {pid} cleanup identity no longer matches"),
        ));
    }
    let current_status = background_process_runtime_status_for_identity(&registration.identity)?;
    // Cleanup can race the runner's own startup/monitor failure path. The retained exact registry
    // entry remains the authority, but proven ownership-domain absence makes a second signal both
    // unnecessary and unsafe if the numeric pid or process-group id is reused before settlement.
    if !current_status.alive() {
        mark_background_process_stopped(&registration.identity);
        return Ok(current_status);
    }
    #[cfg(test)]
    if forced_retained_background_cleanup_failures()
        .lock()
        .map_err(|error| {
            io::Error::other(format!("forced cleanup registry lock poisoned: {error}"))
        })?
        .remove(&pid)
    {
        return Err(io::Error::other(format!(
            "forced retained background cleanup failure for pid {pid}"
        )));
    }
    #[cfg(windows)]
    let retained_windows_job = registration.identity.windows_job.clone().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::Unsupported,
            format!("background process {pid} has no exact Windows Job Object capability"),
        )
    })?;
    #[cfg(unix)]
    let supervisor_control = registered_unix_supervisor_control(pid, expected, true)?;
    #[cfg(unix)]
    {
        supervisor_control.terminate()?;
        if !wait_for_owned_background_tree_inactive_for_identity(
            pid,
            Some(&registration.identity),
            Duration::from_millis(RETAINED_BACKGROUND_TERMINATION_WAIT_MS),
        )? {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "trusted Unix process supervisor group {pid} remained active after acknowledged cleanup"
                ),
            ));
        }
    }
    #[cfg(not(unix))]
    {
        terminate_owned_background_process_tree_for_identity(pid, Some(&registration.identity))?;
        if !wait_for_owned_background_tree_inactive_for_identity(
            pid,
            Some(&registration.identity),
            Duration::from_millis(RETAINED_BACKGROUND_TERMINATION_WAIT_MS),
        )? {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("background process tree {pid} remained active after bounded cleanup"),
            ));
        }
    }
    #[cfg(windows)]
    let status = wait_for_windows_background_process_inactive(
        pid,
        expected,
        &retained_windows_job,
        Duration::from_millis(RETAINED_BACKGROUND_TERMINATION_WAIT_MS),
    )?
    .ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "background process {pid} remained directly alive after verified ownership-domain cleanup"
            ),
        )
    })?;
    #[cfg(unix)]
    let status = BackgroundProcessRuntimeStatus {
        direct_pid_alive: false,
        process_tree_alive: false,
        tracked_process_count: Some(0),
    };
    #[cfg(not(any(unix, windows)))]
    let status = {
        let status = background_process_runtime_status(pid)?;
        if status.alive() {
            return Err(io::Error::other(format!(
                "background process tree {pid} remained active after verified ownership-domain cleanup"
            )));
        }
        status
    };
    #[cfg(unix)]
    mark_background_process_stopped_after_unix_cleanup(&registration.identity);
    #[cfg(not(unix))]
    mark_background_process_stopped(&registration.identity);
    Ok(status)
}

/// Terminates the exact registered process tree rooted at `pid` (Windows).
///
/// # Errors
/// Returns an error when durable provenance no longer matches or the retained Job Object cannot
/// terminate the tree. PID-based `taskkill` is deliberately excluded from this authority path.
#[cfg(windows)]
pub(crate) fn terminate_background_process_tree_exact(
    pid: u32,
    expected: &ProcessProvenance,
) -> io::Result<()> {
    let registration = registered_background_process("exact Windows tree termination", pid)
        .map_err(|error| io::Error::new(io::ErrorKind::NotFound, error.message))?;
    if registration.provenance != *expected {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("background process {pid} provenance no longer matches"),
        ));
    }
    registration
        .identity
        .windows_job
        .as_ref()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!("background process {pid} has no exact Windows Job Object capability"),
            )
        })?
        .terminate()
}

#[cfg(windows)]
fn terminate_windows_process_tree(pid: u32) -> io::Result<()> {
    let pid_arg = pid.to_string();
    // taskkill is resolved from the Win32 system directory with a cleared environment so a
    // poisoned PATH (or PATH-resolved shim) can never substitute the termination tool.
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

    // Grow-and-retry: GetSystemDirectoryW reports the required length when the buffer is too
    // small; the hard cap keeps a misbehaving API from forcing unbounded allocation.
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

/// Terminates the exact registered Unix target through its trusted supervisor capability.
///
/// # Errors
/// Returns an error when registered provenance differs, a live supervisor identity mismatches, or
/// the exact supervisor capability cannot acknowledge verified cleanup. Once the supervisor is
/// absent or live identity cannot be probed, only that retained capability may consume an autonomous
/// cleanup acknowledgement; no direct-PID or raw process-group fallback is permitted.
#[cfg(unix)]
pub(crate) fn terminate_background_process_tree_exact(
    pid: u32,
    expected: &ProcessProvenance,
) -> io::Result<()> {
    terminate_registered_unix_supervisor(pid, expected)
}

/// Fallback for platforms without a supported termination mechanism: always fails so callers
/// surface the gap instead of silently believing a process was stopped.
///
/// # Errors
///
/// Always returns [`io::ErrorKind::Unsupported`].
#[cfg(not(any(unix, windows)))]
pub(crate) fn terminate_background_process_tree_exact(
    pid: u32,
    expected: &ProcessProvenance,
) -> io::Result<()> {
    let _ = (pid, expected);
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "portable background process termination is unsupported on this platform",
    ))
}

#[cfg(unix)]
fn terminate_unix_process_group(pid: u32) -> io::Result<()> {
    let process_group_id = unix_pid_from_u32(pid)?;
    // A negative pid addresses the whole process group; the child was made its own group
    // leader at spawn, so its pid doubles as the pgid.
    // SAFETY: kill(2) is safe to call with any pid value; errors are handled below.
    let result = unsafe { libc::kill(-process_group_id, libc::SIGKILL) };
    if result == 0 {
        return Ok(());
    }
    Err(io::Error::last_os_error())
}

/// Reports whether an owned Unix process group has any live members.
///
/// # Errors
/// Returns an error when the group id is invalid or the bounded Linux proof cannot stabilize.
#[cfg(target_os = "linux")]
pub(crate) fn unix_process_group_is_alive(pid: u32) -> io::Result<bool> {
    let process_group_id = unix_pid_from_u32(pid)?;
    if !linux_process_group_signal_probe(process_group_id)? {
        return Ok(false);
    }
    // `/proc` enumeration is not atomic. Require the same PID/start-time identities twice so a
    // live member that forks and exits during one traversal cannot make that traversal decisive.
    let mut exited_members_candidate = None;
    for _ in 0..MAX_LINUX_PROCESS_GROUP_SNAPSHOT_ATTEMPTS {
        match linux_process_group_snapshot(process_group_id)? {
            LinuxProcessGroupSnapshot::LiveMember => return Ok(true),
            LinuxProcessGroupSnapshot::ExitedMembers(exited_members) => {
                if exited_members_candidate.as_ref() == Some(&exited_members) {
                    return Ok(false);
                }
                exited_members_candidate = Some(exited_members);
            }
            LinuxProcessGroupSnapshot::NoMembers => {
                exited_members_candidate = None;
                if !linux_process_group_signal_probe(process_group_id)? {
                    return Ok(false);
                }
            }
        }
    }
    Err(io::Error::other("Linux process-group liveness snapshot did not stabilize"))
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, PartialEq, Eq)]
enum LinuxProcessGroupSnapshot {
    LiveMember,
    ExitedMembers(Vec<LinuxExitedProcessIdentity>),
    NoMembers,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct LinuxExitedProcessIdentity {
    pid: u32,
    start_token: u64,
}

#[cfg(target_os = "linux")]
fn linux_process_group_snapshot(
    process_group_id: libc::pid_t,
) -> io::Result<LinuxProcessGroupSnapshot> {
    let mut process_count = 0_usize;
    let mut exited_members = Vec::new();
    for entry in fs::read_dir("/proc")? {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) if linux_process_vanished(&error) => continue,
            Err(error) => return Err(error),
        };
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
            .filter(|pid| *pid > 0)
        else {
            continue;
        };
        process_count = process_count.saturating_add(1);
        if process_count > MAX_LINUX_PROCESS_COUNT {
            return Err(io::Error::other("Linux process snapshot exceeded bounded capacity"));
        }
        let Some(stat) = read_linux_process_stat(pid)? else {
            continue;
        };
        if stat.process_group_id != Some(process_group_id) {
            continue;
        }
        if matches!(stat.state, b'Z' | b'X' | b'x') {
            exited_members.push(LinuxExitedProcessIdentity { pid, start_token: stat.start_token });
        } else {
            return Ok(LinuxProcessGroupSnapshot::LiveMember);
        }
    }
    if exited_members.is_empty() {
        Ok(LinuxProcessGroupSnapshot::NoMembers)
    } else {
        exited_members.sort_unstable();
        Ok(LinuxProcessGroupSnapshot::ExitedMembers(exited_members))
    }
}

#[cfg(target_os = "linux")]
fn linux_process_group_signal_probe(process_group_id: libc::pid_t) -> io::Result<bool> {
    // Signal 0 probes the owned process group without changing it. A missing group is the only
    // successful cleanup proof; permission and other failures stay fail-closed.
    // SAFETY: kill(2) with signal 0 has no side effect and the return value is checked.
    let result = unsafe { libc::kill(-process_group_id, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(false);
    }
    Err(error)
}

/// Reports whether an owned Unix process group has any live members.
///
/// # Errors
/// Returns an error when the group id is invalid or the operating-system probe fails.
#[cfg(all(unix, not(any(target_os = "linux", target_os = "macos"))))]
pub(crate) fn unix_process_group_is_alive(pid: u32) -> io::Result<bool> {
    let process_group_id = unix_pid_from_u32(pid)?;
    // Signal 0 is the best available portable Unix probe outside the Linux and Darwin snapshots.
    // SAFETY: kill(2) with signal 0 has no side effect and the return value is checked.
    let result = unsafe { libc::kill(-process_group_id, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(false);
    }
    Err(error)
}

/// Reports whether an owned Unix process group has any live members.
///
/// # Errors
/// Returns an error when the group id is invalid or the bounded Darwin snapshot cannot stabilize.
#[cfg(target_os = "macos")]
pub(crate) fn unix_process_group_is_alive(pid: u32) -> io::Result<bool> {
    let process_group_id = unix_pid_from_u32(pid)?;
    let mut process_ids: Vec<libc::pid_t> = vec![0; MAX_MACOS_BACKGROUND_PROCESS_GROUP_MEMBERS];
    let buffer_size =
        i32::try_from(process_ids.len().saturating_mul(std::mem::size_of::<libc::pid_t>()))
            .map_err(|_| io::Error::other("macOS process-group buffer exceeds libproc limits"))?;

    for _ in 0..MAX_MACOS_PROCESS_GROUP_SNAPSHOT_ATTEMPTS {
        process_ids.fill(0);
        // libproc collapses a failed __proc_info call into a zero result. Clear and capture the
        // thread-local errno around the call so an authoritative empty snapshot remains distinct
        // from a hidden operating-system error.
        // SAFETY: __error returns this thread's writable errno slot, and `process_ids` is a writable
        // buffer of exactly `buffer_size` bytes for the validated positive Darwin process group.
        let (raw_count, error_number) = unsafe {
            let error_slot = libc::__error();
            *error_slot = 0;
            let count = macos_proc_listpgrppids(
                process_group_id,
                process_ids.as_mut_ptr().cast(),
                buffer_size,
            );
            (count, *error_slot)
        };
        let count = match classify_macos_process_group_list_result(
            raw_count,
            error_number,
            process_ids.len(),
        )? {
            MacosProcessGroupListResult::Empty => return Ok(false),
            MacosProcessGroupListResult::Members(count) => count,
        };

        let mut snapshot_changed = false;
        for process_id in process_ids.iter().copied().take(count) {
            if process_id <= 0 {
                snapshot_changed = true;
                continue;
            }
            let expected_process_id = u32::try_from(process_id)
                .map_err(|_| io::Error::other("macOS process id is invalid"))?;
            let mut information = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
            let information_size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
                .map_err(|_| io::Error::other("macOS process information buffer is invalid"))?;
            // SAFETY: `information` is the exact writable PROC_PIDTBSDINFO ABI buffer and remains
            // live for the duration of the call.
            let read = unsafe {
                macos_proc_pidinfo(
                    process_id,
                    libc::PROC_PIDTBSDINFO,
                    // Group enumeration includes zombproc; the nonzero argument tells Darwin's
                    // BSDINFO lookup to search that list instead of reporting a stable ESRCH.
                    MACOS_PROC_PIDINFO_INCLUDE_ZOMBIES,
                    information.as_mut_ptr().cast(),
                    information_size,
                )
            };
            if read <= 0 {
                let error = io::Error::last_os_error();
                if matches!(error.raw_os_error(), Some(libc::ESRCH) | Some(libc::ENOENT)) {
                    snapshot_changed = true;
                    continue;
                }
                return Err(error);
            }
            if read != information_size {
                return Err(io::Error::other("macOS process information response was incomplete"));
            }
            // SAFETY: proc_pidinfo reported that it initialized the complete fixed-size structure.
            let information = unsafe { information.assume_init() };
            if information.pbi_pid != expected_process_id || information.pbi_pgid != pid {
                snapshot_changed = true;
                continue;
            }
            // Zombies have exited and released their descriptors, but Darwin may retain them in
            // the process group until the parent reaps them.
            if information.pbi_status != libc::SZOMB {
                return Ok(true);
            }
        }
        if !snapshot_changed {
            return Ok(false);
        }
    }

    Err(io::Error::other("macOS process-group liveness snapshot did not stabilize"))
}

#[cfg(any(target_os = "macos", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MacosProcessGroupListResult {
    Empty,
    Members(usize),
}

#[cfg(any(target_os = "macos", test))]
fn classify_macos_process_group_list_result(
    raw_count: i32,
    error_number: i32,
    capacity: usize,
) -> io::Result<MacosProcessGroupListResult> {
    if raw_count <= 0 {
        if raw_count == 0 && error_number == 0 {
            return Ok(MacosProcessGroupListResult::Empty);
        }
        let error_number = if error_number == 0 { libc::EIO } else { error_number };
        return Err(io::Error::from_raw_os_error(error_number));
    }
    let count = usize::try_from(raw_count)
        .map_err(|_| io::Error::other("macOS process-group count is invalid"))?;
    if count >= capacity {
        return Err(io::Error::other("bounded macOS process-group capacity exceeded"));
    }
    Ok(MacosProcessGroupListResult::Members(count))
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct MacProcessUniqueInfo {
    _executable_uuid: [u8; 16],
    unique_id: u64,
    _parent_unique_id: u64,
    id_version: i32,
    _original_parent_id_version: i32,
    _reserved_2: u64,
    _reserved_3: u64,
}

#[cfg(target_os = "macos")]
const _: () = assert!(std::mem::size_of::<MacProcessUniqueInfo>() == 56);

#[cfg(target_os = "macos")]
#[link(name = "proc")]
unsafe extern "C" {
    #[link_name = "proc_listpgrppids"]
    fn macos_proc_listpgrppids(
        process_group_id: libc::pid_t,
        buffer: *mut std::ffi::c_void,
        buffer_size: i32,
    ) -> i32;
    #[link_name = "proc_pidinfo"]
    fn macos_proc_pidinfo(
        process_id: libc::pid_t,
        flavor: i32,
        argument: u64,
        buffer: *mut std::ffi::c_void,
        buffer_size: i32,
    ) -> i32;
    #[link_name = "proc_pidpath"]
    fn macos_proc_pidpath(
        process_id: libc::pid_t,
        buffer: *mut std::ffi::c_void,
        buffer_size: u32,
    ) -> i32;
}

#[cfg(windows)]
fn process_id_is_alive(pid: u32) -> io::Result<bool> {
    palyra_common::windows_security::process_is_alive(pid)
}

#[cfg(unix)]
fn process_id_is_alive(pid: u32) -> io::Result<bool> {
    let process_id = unix_pid_from_u32(pid)?;
    // Signal 0 performs the permission and existence checks without delivering anything.
    // SAFETY: kill(2) with signal 0 never affects the target; errors are handled below.
    let result = unsafe { libc::kill(process_id, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    // ESRCH means "no such process" (dead); other errors (e.g. EPERM) mean the pid exists but
    // is not ours, so they propagate instead of being misread as "stopped".
    if error.raw_os_error() == Some(libc::ESRCH) {
        return Ok(false);
    }
    Err(error)
}

#[cfg(unix)]
fn unix_pid_from_u32(pid: u32) -> io::Result<libc::pid_t> {
    Ok(unix_pid_i32_from_u32(pid)? as libc::pid_t)
}

#[cfg(any(unix, test))]
fn unix_pid_i32_from_u32(pid: u32) -> io::Result<i32> {
    let process_id = i32::try_from(pid).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("pid {pid} exceeds Unix pid_t range"))
    })?;
    if process_id <= 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("pid {pid} is not a positive Unix pid"),
        ));
    }
    Ok(process_id)
}

#[cfg(not(any(unix, windows)))]
fn process_id_is_alive(pid: u32) -> io::Result<bool> {
    let _ = pid;
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "portable process status is unsupported on this platform",
    ))
}

fn terminate_child_process_tree(child: &mut Child) -> io::Result<()> {
    let pid = child.id();
    match terminate_owned_background_process_tree(pid) {
        Ok(()) => Ok(()),
        Err(tree_error) => child.kill().map_err(|direct_error| {
            io::Error::other(format!(
                "failed to terminate process tree or direct child {pid}: tree termination failed: {tree_error}; direct child kill failed: {direct_error}"
            ))
        }),
    }
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
    #[cfg(unix)]
    {
        let _ = pid;
        Value::Null
    }
    #[cfg(not(any(unix, windows)))]
    {
        json!({
            "command": "kill",
            "args": ["-TERM", pid.to_string()],
        })
    }
}

fn background_cleanup_note() -> &'static str {
    #[cfg(windows)]
    {
        "Use cleanup.portable_stop_command to terminate the direct process and its descendants; manual_command is a platform fallback if the run fails before automatic lifetime cleanup runs."
    }
    #[cfg(unix)]
    {
        "Use cleanup.portable_stop_command; the registered Unix pid is a trusted supervisor anchor and must not be signalled as the target cleanup group. Cleanup covers processes that remain in the supervised target process group; descendants that deliberately leave it are outside this portable ownership domain."
    }
    #[cfg(not(any(unix, windows)))]
    {
        "Use cleanup.portable_stop_command to terminate the registered ownership root; manual_command is a platform fallback if automatic lifetime cleanup fails."
    }
}

// Requested lifetimes below the floor are raised (short timeouts copied from foreground habits
// would kill dev servers mid-verification); everything is then capped by the operator execution
// timeout and the runtime hard maximum. The adjustment is reported back to the caller via
// background_lifetime_adjustment_reason.
fn background_process_lifetime(timeout_ms: Option<u64>, execution_timeout: Duration) -> Duration {
    let lifetime_limit = background_process_lifetime_limit(execution_timeout);
    let default_lifetime = Duration::from_millis(DEFAULT_BACKGROUND_PROCESS_LIFETIME_MS);
    let minimum_lifetime = Duration::from_millis(MIN_BACKGROUND_PROCESS_LIFETIME_MS);
    match timeout_ms.map(Duration::from_millis) {
        Some(requested) if requested < minimum_lifetime && lifetime_limit > requested => {
            minimum_lifetime.min(lifetime_limit)
        }
        Some(requested) => requested.min(lifetime_limit),
        None => default_lifetime.min(lifetime_limit),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BackgroundProcessLifetimeApprovalMetadata {
    pub(crate) requested_lifetime_ms: Option<u64>,
    pub(crate) effective_lifetime_ms: u64,
    pub(crate) max_lifetime_ms: u64,
    pub(crate) min_background_lifetime_ms: u64,
    pub(crate) adjusted: bool,
    pub(crate) adjustment_reason: Option<&'static str>,
}

pub(crate) fn background_process_lifetime_approval_metadata(
    timeout_ms: Option<u64>,
    execution_timeout: Duration,
) -> BackgroundProcessLifetimeApprovalMetadata {
    let effective_lifetime_ms =
        background_process_lifetime(timeout_ms, execution_timeout).as_millis() as u64;
    let adjustment_reason =
        background_lifetime_adjustment_reason(timeout_ms, effective_lifetime_ms);
    BackgroundProcessLifetimeApprovalMetadata {
        requested_lifetime_ms: timeout_ms,
        effective_lifetime_ms,
        max_lifetime_ms: background_process_lifetime_limit(execution_timeout).as_millis() as u64,
        min_background_lifetime_ms: MIN_BACKGROUND_PROCESS_LIFETIME_MS,
        adjusted: adjustment_reason.is_some(),
        adjustment_reason,
    }
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

// Fail-closed platform gate: sandboxed (non-host-access) execution requires enforceable
// CPU/memory quotas, and macOS lacks a reliable total-memory rlimit (RLIMIT_AS is advisory
// there), so the runner refuses rather than running with silently weaker limits.
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
    let prepend_path = resolve_process_prepend_path_entries(policy, input, workspace_root, cwd)?;
    let allow_cwd_resolution = process_runner_allows_cwd_process_resolution(policy);
    // Unix supervisors freeze an immutable launch spec before durable registration, so a
    // background target cannot defer executable resolution until exec.
    let require_trusted_resolution = !prepend_path.is_empty() || (cfg!(unix) && input.background);
    if process_runner_allows_host_access(policy) {
        let host_roots = host_access_roots();
        let trusted_path = host_access_path();
        let program = resolve_host_process_program_with_roots(
            workspace_root,
            cwd,
            input.command.as_str(),
            host_roots.as_slice(),
            OsStr::new(&trusted_path),
            require_trusted_resolution,
            allow_cwd_resolution,
        )?;
        let path_env = host_access_path_env_for_input(input);
        let args =
            rewrite_host_access_process_args(input.args.as_slice(), workspace_root, &path_env)?;
        let mut command = build_tier_b_process_command(program.as_path(), args.as_slice(), cwd)?;
        configure_host_access_process_environment(
            &mut command,
            input.command.as_str(),
            program.as_path(),
            workspace_root,
        )?;
        apply_process_path_prepend(&mut command, prepend_path.as_slice())?;
        apply_process_env_overrides(&mut command, input);
        configure_node_runtime_environment(&mut command);
        configure_wsl_path_env_bridge(&mut command, input.command.as_str(), program.as_path());
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
            TierCCommandRequest { command: input.command.clone(), args: scoped_args };
        let mut plan = build_tier_c_command_plan(&tier_c_policy, &tier_c_request)
            .map_err(map_tier_c_backend_error)?;
        apply_tier_c_inner_path_prepend(&mut plan, prepend_path.as_slice())?;
        let mut command = Command::new(plan.program);
        let current_dir = child_process_path(cwd);
        command
            .args(plan.args)
            .current_dir(current_dir.as_path())
            .env_clear()
            .env("PATH", sandbox_process_path())
            .env("LANG", "C")
            .env("LC_ALL", "C");
        apply_process_env_overrides(&mut command, input);
        configure_node_runtime_environment(&mut command);
        return Ok(command);
    }

    let program = resolve_tier_b_process_program(
        input.command.as_str(),
        cwd,
        OsStr::new(sandbox_process_path()),
        require_trusted_resolution,
        allow_cwd_resolution,
    )?;
    let mut command = build_tier_b_process_command(program.as_path(), scoped_args.as_slice(), cwd)?;
    configure_tier_b_process_environment(
        &mut command,
        input.command.as_str(),
        program.as_path(),
        policy,
    )?;
    apply_process_path_prepend(&mut command, prepend_path.as_slice())?;
    apply_process_env_overrides(&mut command, input);
    configure_node_runtime_environment(&mut command);
    configure_wsl_path_env_bridge(&mut command, input.command.as_str(), program.as_path());
    Ok(command)
}

fn process_runner_allows_cwd_process_resolution(policy: &SandboxProcessRunnerPolicy) -> bool {
    policy.allowed_executables.iter().any(|allowed| allowed.trim() == "*")
}

fn apply_process_path_prepend(
    command: &mut Command,
    prepend_path: &[PathBuf],
) -> Result<(), SandboxProcessRunError> {
    if prepend_path.is_empty() {
        return Ok(());
    }
    let existing_path = command_env_value_os(command, "PATH");
    let joined = join_process_path_prepend(prepend_path, existing_path.as_deref())?;
    command.env("PATH", joined);
    Ok(())
}

fn join_process_path_prepend(
    prepend_path: &[PathBuf],
    existing_path: Option<&OsStr>,
) -> Result<OsString, SandboxProcessRunError> {
    let mut entries = prepend_path.to_vec();
    if let Some(existing_path) = existing_path.filter(|value| !value.is_empty()) {
        entries.extend(std::env::split_paths(existing_path));
    }
    std::env::join_paths(entries).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::InvalidInput,
        message: format!("palyra.process.run prepend_path could not be joined into PATH: {error}"),
    })
}

fn apply_tier_c_inner_path_prepend(
    plan: &mut TierCCommandPlan,
    prepend_path: &[PathBuf],
) -> Result<(), SandboxProcessRunError> {
    if prepend_path.is_empty() {
        return Ok(());
    }
    let Some(path_value_index) = tier_c_plan_inner_path_index(plan.args.as_slice()) else {
        return Ok(());
    };
    let existing_path = OsStr::new(plan.args[path_value_index].as_str());
    let joined = join_process_path_prepend(prepend_path, Some(existing_path))?;
    plan.args[path_value_index] = joined.to_string_lossy().into_owned();
    Ok(())
}

fn tier_c_plan_inner_path_index(args: &[String]) -> Option<usize> {
    let command_separator = args.iter().position(|arg| arg == "--").unwrap_or(args.len());
    let mut index = 0;
    while index + 2 < command_separator {
        if args[index] == "--setenv" && args[index + 1] == "PATH" {
            return Some(index + 2);
        }
        index += 1;
    }
    None
}

fn command_env_value_os(command: &Command, requested_key: &str) -> Option<OsString> {
    command.get_envs().find_map(|(key, value)| {
        if env_key_matches(key, requested_key) {
            value.map(OsString::from)
        } else {
            None
        }
    })
}

#[cfg(windows)]
fn env_key_matches(key: &OsStr, requested_key: &str) -> bool {
    key.to_string_lossy().eq_ignore_ascii_case(requested_key)
}

#[cfg(not(windows))]
fn env_key_matches(key: &OsStr, requested_key: &str) -> bool {
    key == OsStr::new(requested_key)
}

// Applied after computed defaults so validated task-specific settings win. Runtime-owned
// hardening values are reapplied by the caller after this function returns.
fn apply_process_env_overrides(command: &mut Command, input: &ProcessRunnerInput) {
    for (key, value) in &input.env {
        command.env(key, value);
    }
}

#[cfg(windows)]
fn configure_wsl_path_env_bridge(command: &mut Command, process_command: &str, program: &Path) {
    if !windows_program_is_wsl_launcher(process_command, program) {
        return;
    }
    let mut entries = command_env_value(command, "WSLENV")
        .map(|value| {
            value
                .split(':')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    for key in HOST_ACCESS_SAFE_PALYRA_ENV_KEYS {
        if command_env_value(command, key).is_some() {
            ensure_wslenv_path_entry(&mut entries, key);
        }
    }
    if !entries.is_empty() {
        command.env("WSLENV", entries.join(":"));
    }
}

#[cfg(not(windows))]
fn configure_wsl_path_env_bridge(_command: &mut Command, _process_command: &str, _program: &Path) {}

#[cfg(windows)]
fn command_env_value(command: &Command, requested_key: &str) -> Option<String> {
    command.get_envs().find_map(|(key, value)| {
        if !key.to_string_lossy().eq_ignore_ascii_case(requested_key) {
            return None;
        }
        value.map(|value| value.to_string_lossy().into_owned())
    })
}

#[cfg(windows)]
fn ensure_wslenv_path_entry(entries: &mut Vec<String>, requested_key: &str) {
    for entry in entries.iter_mut() {
        let matches_key =
            entry.split('/').next().is_some_and(|key| key.eq_ignore_ascii_case(requested_key));
        if !matches_key {
            continue;
        }
        let has_path_flag = entry
            .split_once('/')
            .is_some_and(|(_, flags)| flags.chars().any(|flag| matches!(flag, 'p' | 'P')));
        if !has_path_flag {
            if entry.contains('/') {
                entry.push('p');
            } else {
                entry.push_str("/p");
            }
        }
        return;
    }
    entries.push(format!("{requested_key}/p"));
}

#[cfg(windows)]
fn windows_program_is_wsl_launcher(process_command: &str, program: &Path) -> bool {
    let command = normalized_process_command_name(process_command);
    if matches!(command.as_str(), "bash" | "wsl") {
        return true;
    }
    program
        .file_stem()
        .and_then(|stem| stem.to_str())
        .is_some_and(|stem| stem.eq_ignore_ascii_case("bash") || stem.eq_ignore_ascii_case("wsl"))
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

// Tier-B children start from an empty environment plus a deterministic minimum (fixed PATH,
// LANG/LC_ALL=C); nothing from the daemon environment leaks through implicitly.
fn configure_tier_b_process_environment(
    command: &mut Command,
    process_command: &str,
    program: &Path,
    policy: &SandboxProcessRunnerPolicy,
) -> Result<(), SandboxProcessRunError> {
    command.env_clear();
    #[cfg(windows)]
    {
        configure_windows_tier_b_process_environment(command, program, policy)?;
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
    )?;
    configure_node_runtime_environment(command);
    Ok(())
}

fn configure_host_access_process_environment(
    command: &mut Command,
    process_command: &str,
    program: &Path,
    workspace_root: &Path,
) -> Result<(), SandboxProcessRunError> {
    configure_host_access_safe_environment(command, workspace_root)?;
    configure_workspace_python_environment(command, process_command, workspace_root)?;
    if !is_palyra_cli_program(program) {
        return Ok(());
    }
    // The daemon may run with PALYRA_CLI_PROFILE set but without the profiles-path companion
    // (e.g. desktop supervisor launch). A child `palyra` CLI would then fail to resolve the
    // profile, so either re-derive the desktop profiles path from the state root or drop the
    // dangling profile selector entirely.
    if !should_repair_palyra_cli_profile_env(
        std::env::var_os(PALYRA_CLI_PROFILE_ENV).as_deref(),
        std::env::var_os(PALYRA_CLI_PROFILES_PATH_ENV).as_deref(),
    ) {
        return Ok(());
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
    Ok(())
}

// Host-access children also start from env_clear, then copy only the allowlisted keys; this is
// what keeps daemon admin tokens and provider keys out of unsandboxed processes (test-pinned).
fn configure_host_access_safe_environment(
    command: &mut Command,
    workspace_root: &Path,
) -> Result<(), SandboxProcessRunError> {
    command.env_clear();
    for key in HOST_ACCESS_SAFE_ENV_KEYS {
        copy_env_if_present(command, key);
    }
    for key in HOST_ACCESS_SAFE_PALYRA_ENV_KEYS {
        copy_env_if_present(command, key);
    }
    #[cfg(windows)]
    configure_windows_host_access_safe_environment(command, workspace_root)?;
    #[cfg(not(windows))]
    configure_unix_host_access_safe_environment(command, workspace_root)?;
    configure_node_runtime_environment(command);
    Ok(())
}

// Node would otherwise persist an on-disk compile cache under the (scrubbed or redirected)
// user profile, causing writes outside the workspace and nondeterministic startup behavior.
fn configure_node_runtime_environment(command: &mut Command) {
    command.env(NODE_DISABLE_COMPILE_CACHE_ENV, "1");
}

fn child_process_path(path: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        windows_deverbatim_path_string(path)
            .map(PathBuf::from)
            .unwrap_or_else(|| path.to_path_buf())
    }
    #[cfg(not(windows))]
    {
        path.to_path_buf()
    }
}

fn copy_env_if_present(command: &mut Command, key: &str) {
    if let Some(value) = std::env::var_os(key).filter(|value| !value.is_empty()) {
        command.env(key, value);
    }
}

#[cfg(windows)]
fn configure_windows_host_access_safe_environment(
    command: &mut Command,
    workspace_root: &Path,
) -> Result<(), SandboxProcessRunError> {
    for key in WINDOWS_HOST_ACCESS_SAFE_ENV_KEYS {
        copy_env_if_present(command, key);
    }
    let temp_root = process_runner_child_temp_root(workspace_root)?;
    command
        .env("PATH", host_access_path())
        .env("TEMP", temp_root.as_path())
        .env("TMP", temp_root.as_path());
    Ok(())
}

#[cfg(windows)]
const WINDOWS_HOST_ACCESS_SAFE_ENV_KEYS: &[&str] = &[
    "APPDATA",
    "COMSPEC",
    "LOCALAPPDATA",
    "PATHEXT",
    "PROGRAMDATA",
    "ProgramFiles",
    "ProgramFiles(x86)",
    "ProgramW6432",
    "SystemDrive",
    "SystemRoot",
    "USERDOMAIN",
    "USERNAME",
    "USERPROFILE",
    "WINDIR",
];

#[cfg(not(windows))]
fn configure_unix_host_access_safe_environment(
    command: &mut Command,
    workspace_root: &Path,
) -> Result<(), SandboxProcessRunError> {
    let temp_root = process_runner_child_temp_root(workspace_root)?;
    command.env("PATH", host_access_path()).env("TMPDIR", temp_root.as_path());
    Ok(())
}

fn host_access_path() -> String {
    std::env::var_os("PATH")
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string_lossy().into_owned())
        .unwrap_or_else(|| sandbox_process_path().to_owned())
}

// Pins pip user installs and caches under runtime-owned state, not the target checkout.
fn configure_workspace_python_environment(
    command: &mut Command,
    process_command: &str,
    workspace_root: &Path,
) -> Result<(), SandboxProcessRunError> {
    let Some(environment) = workspace_python_environment(process_command, workspace_root)? else {
        return Ok(());
    };
    command
        .env("PYTHONUSERBASE", environment.user_base)
        .env("PIP_CACHE_DIR", environment.pip_cache)
        .env("PYTHONNOUSERSITE", "1")
        .env("PIP_DISABLE_PIP_VERSION_CHECK", "1");
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WorkspacePythonEnvironment {
    user_base: PathBuf,
    pip_cache: PathBuf,
}

fn workspace_python_environment(
    process_command: &str,
    workspace_root: &Path,
) -> Result<Option<WorkspacePythonEnvironment>, SandboxProcessRunError> {
    if !is_python_runtime_command(process_command) {
        return Ok(None);
    }

    let environment_root = process_runner_python_environment_root(workspace_root)?;
    Ok(Some(WorkspacePythonEnvironment {
        user_base: environment_root.join(PYTHON_USER_BASE_DIR),
        pip_cache: environment_root.join(PIP_CACHE_DIR),
    }))
}

fn process_runner_python_environment_root(
    workspace_root: &Path,
) -> Result<PathBuf, SandboxProcessRunError> {
    let workspace_key = process_runner_workspace_cache_key(workspace_root);
    let mut environment_parent = process_runner_runtime_root()?;
    ensure_private_process_runner_directory(environment_parent.as_path())?;
    for component in PROCESS_RUNNER_PYTHON_ENV_RELATIVE_PATH
        .iter()
        .copied()
        .chain(std::iter::once(workspace_key.as_str()))
    {
        environment_parent.push(component);
        ensure_private_process_runner_directory(environment_parent.as_path())?;
    }
    for _ in 0..PROCESS_RUNNER_PYTHON_ENV_CREATE_ATTEMPTS {
        let nonce = process_owner_nonce().map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run failed to generate an isolated Python environment name: {error}"
            ),
        })?;
        let environment_root = environment_parent.join(format!("run-{nonce}"));
        match fs::create_dir(environment_root.as_path()) {
            Ok(()) => {
                ensure_private_process_runner_directory(environment_root.as_path())?;
                return Ok(environment_root);
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => {
                return Err(SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!(
                        "palyra.process.run failed to create isolated Python environment: {error}"
                    ),
                });
            }
        }
    }
    Err(SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message:
            "palyra.process.run could not allocate a unique isolated Python environment directory"
                .to_owned(),
    })
}

fn process_runner_child_temp_root(
    workspace_root: &Path,
) -> Result<PathBuf, SandboxProcessRunError> {
    let workspace_key = process_runner_workspace_cache_key(workspace_root);
    let mut temp_root = process_runner_runtime_root()?;
    ensure_private_process_runner_directory(temp_root.as_path())?;
    for component in PROCESS_RUNNER_TEMP_RELATIVE_PATH
        .iter()
        .copied()
        .chain(std::iter::once(workspace_key.as_str()))
    {
        temp_root.push(component);
        ensure_private_process_runner_directory(temp_root.as_path())?;
    }
    Ok(temp_root)
}

fn ensure_private_process_runner_directory(path: &Path) -> Result<(), SandboxProcessRunError> {
    if let Ok(metadata) = fs::symlink_metadata(path) {
        if process_runner_directory_is_link(&metadata) {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "palyra.process.run rejected child temp directory {} because it is a symbolic link or reparse point",
                    path.display()
                ),
            });
        }
        if !metadata.is_dir() {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "palyra.process.run rejected child temp path {} because it is not a directory",
                    path.display()
                ),
            });
        }
    }
    ensure_owner_only_dir(path).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "palyra.process.run failed to secure child temp directory {}: {error}",
            path.display()
        ),
    })?;
    let metadata = fs::symlink_metadata(path).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: format!(
            "palyra.process.run failed to verify child temp directory {}: {error}",
            path.display()
        ),
    })?;
    if process_runner_directory_is_link(&metadata) || !metadata.is_dir() {
        return Err(SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run rejected child temp directory {} after security hardening",
                path.display()
            ),
        });
    }
    #[cfg(unix)]
    {
        // SAFETY: `geteuid` only reads current process credentials and has no preconditions.
        let current_uid = unsafe { libc::geteuid() };
        if metadata.uid() != current_uid || metadata.permissions().mode() & 0o077 != 0 {
            return Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: format!(
                    "palyra.process.run rejected child temp directory {} because it is not owner-only",
                    path.display()
                ),
            });
        }
    }
    Ok(())
}

fn process_runner_directory_is_link(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;

        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    false
}

fn process_runner_runtime_root() -> Result<PathBuf, SandboxProcessRunError> {
    if let Some(state_root) = std::env::var_os(PALYRA_STATE_ROOT_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|path| child_process_path(path.as_path()))
    {
        return Ok(state_root);
    }
    default_state_root()
        .map(|path| child_process_path(path.as_path()))
        .map_err(|error| SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: format!(
                "palyra.process.run could not resolve a safe state root for process-runner runtime state: {error}; set {PALYRA_STATE_ROOT_ENV}"
            ),
        })
}

fn process_runner_workspace_cache_key(workspace_root: &Path) -> String {
    let mut hasher = DefaultHasher::new();
    child_process_path(workspace_root).hash(&mut hasher);
    format!("workspace-{:016x}", hasher.finish())
}

#[cfg(test)]
fn join_relative_components(root: &Path, components: &[&str]) -> PathBuf {
    components.iter().fold(root.to_path_buf(), |path, component| path.join(component))
}

fn is_python_runtime_command(process_command: &str) -> bool {
    let command = normalized_process_command_name(process_command);
    matches!(command.as_str(), "py" | "pip" | "pip3")
        || command == "python"
        || command == "python3"
        || command.starts_with("python3.")
        || is_versioned_pip_command(command.as_str())
}

fn is_versioned_pip_command(command: &str) -> bool {
    let Some(version) = command.strip_prefix("pip") else {
        return false;
    };
    let Some((major, minor)) = version.split_once('.') else {
        return false;
    };
    !major.is_empty()
        && !minor.is_empty()
        && major.chars().all(|ch| ch.is_ascii_digit())
        && minor.chars().all(|ch| ch.is_ascii_digit())
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

fn resolve_tier_b_process_program(
    command: &str,
    cwd: &Path,
    trusted_path: &OsStr,
    require_trusted_resolution: bool,
    allow_cwd_resolution: bool,
) -> Result<PathBuf, SandboxProcessRunError> {
    if command_has_path_separator(command) {
        return Ok(PathBuf::from(command));
    }

    #[cfg(windows)]
    {
        let resolved = resolve_windows_process_program(
            command,
            cwd,
            trusted_path,
            allow_cwd_resolution && !require_trusted_resolution,
        )?
        .map(canonicalize_trusted_process_program)
        .transpose()?;
        if require_trusted_resolution {
            resolved.ok_or_else(|| trusted_process_program_not_found_error(command))
        } else {
            Ok(resolved.unwrap_or_else(|| PathBuf::from(command)))
        }
    }
    #[cfg(not(windows))]
    {
        let _ = cwd;
        let _ = allow_cwd_resolution;
        if !require_trusted_resolution {
            return Ok(PathBuf::from(command));
        }
        process_program_candidates_from_path(command, trusted_path)
            .into_iter()
            .next()
            .map(canonicalize_trusted_process_program)
            .transpose()?
            .ok_or_else(|| trusted_process_program_not_found_error(command))
    }
}

fn canonicalize_trusted_process_program(
    candidate: PathBuf,
) -> Result<PathBuf, SandboxProcessRunError> {
    fs::canonicalize(candidate.as_path()).map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "sandbox denied: trusted process-runner executable '{}' could not be resolved: {error}",
            candidate.display()
        ),
    })
}

fn trusted_process_program_not_found_error(command: &str) -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "sandbox denied: executable '{command}' was not found on the trusted process-runner PATH; prepend_path only affects the child environment and cannot select the executable"
        ),
    }
}

#[cfg(not(windows))]
fn process_program_candidates_from_path(command: &str, path: &OsStr) -> Vec<PathBuf> {
    let command_path = Path::new(command);
    if command_path.components().count() != 1 {
        return Vec::new();
    }
    std::env::split_paths(path)
        .map(|directory| directory.join(command))
        .filter(|candidate| candidate.is_file())
        .collect()
}

// Resolves a bare command on Windows. Restrictive allowlists prefer trusted PATH
// candidates and reject current-directory shims; wildcard policies keep
// workspace-cwd lookup for intentionally broad host-access workflows.
#[cfg(windows)]
fn resolve_windows_process_program(
    command: &str,
    cwd: &Path,
    trusted_path: &OsStr,
    allow_cwd_resolution: bool,
) -> Result<Option<PathBuf>, SandboxProcessRunError> {
    let command_path = Path::new(command);
    if command_path.components().count() != 1 {
        return Ok(None);
    }

    let cwd_candidate = windows_command_candidates(command)
        .into_iter()
        .map(|candidate| cwd.join(candidate))
        .find(|candidate| candidate.is_file());
    if allow_cwd_resolution {
        return Ok(cwd_candidate.or_else(|| {
            windows_program_candidates_from_path_env(command, trusted_path).into_iter().next()
        }));
    }
    if let Some(trusted_candidate) =
        windows_program_candidates_from_path_env(command, trusted_path).into_iter().next()
    {
        return Ok(Some(trusted_candidate));
    }
    if let Some(cwd_candidate) = cwd_candidate {
        return Err(windows_cwd_process_program_denied_error(command, cwd_candidate.as_path()));
    }
    Ok(None)
}

#[cfg(windows)]
fn windows_cwd_process_program_denied_error(
    command: &str,
    candidate: &Path,
) -> SandboxProcessRunError {
    SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
        message: format!(
            "sandbox denied: Windows command '{command}' resolved to current-directory shim '{}'; use an exact executable path allowed by process_runner.allowed_executables or configure a trusted PATH executable",
            candidate.display()
        ),
    }
}

#[cfg(windows)]
fn windows_program_candidates_from_path_env(command: &str, path: &OsStr) -> Vec<PathBuf> {
    let command_path = Path::new(command);
    if command_path.components().count() != 1 {
        return Vec::new();
    }

    let candidates = windows_command_candidates(command);
    std::env::split_paths(path)
        .flat_map(|directory| candidates.iter().map(move |candidate| directory.join(candidate)))
        .filter(|candidate| candidate.is_file())
        .collect()
}

// Expands a bare command name into the PATHEXT candidate list (npm -> npm.COM, npm.EXE, ...)
// because std::process does not emulate cmd.exe extension resolution.
#[cfg(windows)]
fn windows_command_candidates(command: &str) -> Vec<String> {
    let has_extension = Path::new(command).extension().is_some();
    if has_extension {
        return vec![command.to_owned()];
    }

    windows_command_candidates_from_pathext(
        command,
        WINDOWS_DEFAULT_PATH_EXTENSIONS.join(";").as_str(),
    )
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
    // .cmd/.bat scripts cannot be spawned directly by CreateProcess, so they are dispatched
    // through cmd.exe with a fully controlled command line: /D skips AutoRun registry commands,
    // /S plus the outer quotes pins cmd's quote parsing, and every argument is validated and
    // quoted by windows_cmd_wrapper_command_line to prevent metacharacter injection.
    if windows_program_requires_cmd_wrapper(program) {
        let mut command = Command::new(windows_command_processor()?);
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
fn windows_command_processor() -> Result<PathBuf, SandboxProcessRunError> {
    trusted_windows_system32_dir().map(|system32| system32.join("cmd.exe")).map_err(|error| {
        SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::SpawnFailed,
            message: format!(
                "sandbox denied: trusted Windows command processor could not be resolved: {error}"
            ),
        }
    })
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

// Strips \\?\ and \\.\ verbatim prefixes (and rewrites \\?\UNC\) because cmd.exe and many
// child tools reject verbatim paths that std::fs::canonicalize produces on Windows.
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

// Quoting alone cannot make cmd.exe safe: `%var%` expands and `!var!` may expand (delayed
// expansion) even inside double quotes, and an embedded quote would re-open parsing. Those
// characters are therefore rejected outright; the remaining text is quoted with `^` escaped.
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
) -> Result<(), SandboxProcessRunError> {
    let system32 = trusted_windows_system32_dir().map_err(|error| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: format!(
            "sandbox denied: trusted Windows system directory could not be resolved: {error}"
        ),
    })?;
    let windows_dir = system32.parent().ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::SpawnFailed,
        message: "sandbox denied: trusted Windows system directory has no parent".to_owned(),
    })?;
    let temp_root = process_runner_child_temp_root(policy.workspace_root.as_path())?;
    command
        .env("PATH", windows_tier_b_process_path(program, system32.as_path()))
        .env("TEMP", temp_root.as_path())
        .env("TMP", temp_root.as_path())
        .env("COMSPEC", system32.join("cmd.exe"))
        .env("PATHEXT", WINDOWS_DEFAULT_PATH_EXTENSIONS.join(";"))
        .env("SystemRoot", windows_dir)
        .env("WINDIR", windows_dir)
        .env("LANG", "C")
        .env("LC_ALL", "C");
    Ok(())
}

// The selected program may need a colocated runtime (for example npm.cmd -> node.exe), but
// directories belonging only to other allowlisted commands must not become ambient child authority.
#[cfg(windows)]
fn windows_tier_b_process_path(program: &Path, system32: &Path) -> String {
    let mut directories = vec![system32.to_path_buf()];
    if let Some(windows_dir) = system32.parent() {
        push_unique_windows_path(&mut directories, windows_dir.to_path_buf());
        push_unique_windows_path(&mut directories, system32.join("WindowsPowerShell").join("v1.0"));
    }
    if let Some(parent) = program.parent() {
        push_unique_windows_path(&mut directories, parent.to_path_buf());
    }
    std::env::join_paths(directories)
        .map(|path| path.to_string_lossy().into_owned())
        .unwrap_or_else(|_| system32.to_string_lossy().into_owned())
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
    // SAFETY: the pre_exec closure runs in the forked child before exec. Its success path only
    // calls async-signal-safe syscalls (getrusage, setrlimit) on plain copied values; the error
    // path allocates for the message, which is acceptable because the child aborts exec anyway.
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
    // Hard limit == soft limit so the child cannot raise its own quota back up.
    let rlimit = libc::rlimit { rlim_cur: limit, rlim_max: limit };
    // SAFETY: `rlimit` is a valid initialized struct that outlives the call.
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
    // SAFETY: `usage` is a writable rusage-sized buffer; getrusage fully initializes it on the
    // success path checked below.
    let result = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: getrusage returned 0, so the buffer is initialized.
    let usage = unsafe { usage.assume_init() };
    Ok(timeval_micros(usage.ru_utime).saturating_add(timeval_micros(usage.ru_stime)))
}

// RLIMIT_CPU counts the process's total CPU time, so any CPU already attributed to the forked
// child image when the limit is applied must be added on top of the requested budget; setting
// the raw budget would silently shrink the child's effective quota.
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
    child: &mut ManagedChildGuard,
    timeout: Duration,
    max_output_bytes: usize,
    cancellation_requested: Option<Arc<AtomicBool>>,
    progress_sink: Option<ProcessProgressSink>,
) -> Result<ProcessExecutionCapture, SandboxProcessRunError> {
    let stdout = child.child_mut().stdout.take().ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: "sandbox process stdout pipe is unavailable".to_owned(),
    })?;
    let stderr = child.child_mut().stderr.take().ok_or_else(|| SandboxProcessRunError {
        kind: SandboxProcessRunErrorKind::RuntimeFailure,
        message: "sandbox process stderr pipe is unavailable".to_owned(),
    })?;

    let quota_triggered = Arc::new(AtomicBool::new(false));
    let remaining_budget = Arc::new(AtomicUsize::new(max_output_bytes));
    let started_at = Instant::now();
    let progress_monitor = progress_sink.as_ref().map(|_| ProcessProgressMonitor::new(started_at));
    let stdout_reader = spawn_capture_reader(
        stdout,
        Arc::clone(&remaining_budget),
        Arc::clone(&quota_triggered),
        progress_monitor.as_ref().map(ProcessProgressMonitor::stdout_capture),
    );
    let stderr_reader = spawn_capture_reader(
        stderr,
        Arc::clone(&remaining_budget),
        Arc::clone(&quota_triggered),
        progress_monitor.as_ref().map(ProcessProgressMonitor::stderr_capture),
    );

    // Poll loop semantics: cancellation, output quota, and timeout each request a single
    // tree-wide kill (guarded by `termination_requested`), but the loop keeps running until the
    // child actually exits so the reader threads can drain the pipes and report truthful
    // truncation state. All three flags are sticky and reported with that priority by the
    // caller (cancelled > timed_out > quota_exceeded).
    let mut timed_out = false;
    let mut quota_exceeded = false;
    let mut cancelled = false;
    let mut termination_requested_at = None;
    let mut last_progress_emitted_at = None;
    let mut last_progress_stdout_bytes = 0_usize;
    let mut last_progress_stderr_bytes = 0_usize;
    let exit_status = loop {
        maybe_emit_process_progress(
            progress_sink.as_ref(),
            progress_monitor.as_ref(),
            child.id(),
            started_at,
            &mut last_progress_emitted_at,
            &mut last_progress_stdout_bytes,
            &mut last_progress_stderr_bytes,
        );
        if cancellation_requested
            .as_ref()
            .is_some_and(|requested| requested.load(Ordering::Relaxed))
        {
            cancelled = true;
            if termination_requested_at.is_none() {
                child.request_termination().map_err(|error| SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!("sandbox process cancellation cleanup failed: {error}"),
                })?;
                termination_requested_at = Some(Instant::now());
            }
        }
        if quota_triggered.load(Ordering::Relaxed) {
            quota_exceeded = true;
            if termination_requested_at.is_none() {
                child.request_termination().map_err(|error| SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!("sandbox process quota cleanup failed: {error}"),
                })?;
                termination_requested_at = Some(Instant::now());
            }
        }
        if started_at.elapsed() > timeout {
            timed_out = true;
            if termination_requested_at.is_none() {
                child.request_termination().map_err(|error| SandboxProcessRunError {
                    kind: SandboxProcessRunErrorKind::RuntimeFailure,
                    message: format!("sandbox process timeout cleanup failed: {error}"),
                })?;
                termination_requested_at = Some(Instant::now());
            }
        }

        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) => {
                if termination_requested_at.is_some_and(|requested_at| {
                    requested_at.elapsed() >= Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS)
                }) {
                    return Err(SandboxProcessRunError {
                        kind: SandboxProcessRunErrorKind::RuntimeFailure,
                        message: "sandbox process did not exit within the bounded cleanup window"
                            .to_owned(),
                    });
                }
                thread::sleep(Duration::from_millis(CAPTURE_POLL_INTERVAL_MS));
            }
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
    // Re-check after joining the readers: a stream may have hit the budget between the last
    // loop iteration and process exit.
    quota_exceeded = quota_exceeded
        || quota_triggered.load(Ordering::Relaxed)
        || stdout.truncated
        || stderr.truncated;
    Ok(ProcessExecutionCapture {
        exit_status,
        stdout,
        stderr,
        cancelled,
        timed_out,
        quota_exceeded,
        duration_ms: started_at.elapsed().as_millis() as u64,
    })
}

fn maybe_emit_process_progress(
    progress_sink: Option<&ProcessProgressSink>,
    progress_monitor: Option<&ProcessProgressMonitor>,
    pid: u32,
    started_at: Instant,
    last_progress_emitted_at: &mut Option<Instant>,
    last_progress_stdout_bytes: &mut usize,
    last_progress_stderr_bytes: &mut usize,
) {
    let (Some(progress_sink), Some(progress_monitor)) = (progress_sink, progress_monitor) else {
        return;
    };
    if started_at.elapsed() < Duration::from_millis(PROCESS_PROGRESS_MIN_ELAPSED_MS) {
        return;
    }
    if last_progress_emitted_at.is_some_and(|emitted_at| {
        emitted_at.elapsed() < Duration::from_millis(PROCESS_PROGRESS_INTERVAL_MS)
    }) {
        return;
    }

    let elapsed_ms = elapsed_millis_u64(started_at);
    let event = progress_monitor.snapshot(pid, elapsed_ms);
    let output_changed = event.stdout_bytes as usize != *last_progress_stdout_bytes
        || event.stderr_bytes as usize != *last_progress_stderr_bytes;
    if last_progress_emitted_at.is_some() && !output_changed {
        progress_sink(event.clone());
        *last_progress_emitted_at = Some(Instant::now());
        return;
    }

    *last_progress_stdout_bytes = event.stdout_bytes as usize;
    *last_progress_stderr_bytes = event.stderr_bytes as usize;
    progress_sink(event);
    *last_progress_emitted_at = Some(Instant::now());
}

// Reader threads stop consuming once the shared budget is exhausted; combined with the kill in
// the capture loop this bounds both memory use and how long a chatty child can keep running.
fn spawn_capture_reader<R>(
    mut reader: R,
    remaining_budget: Arc<AtomicUsize>,
    quota_triggered: Arc<AtomicBool>,
    progress: Option<ProcessProgressStreamCapture>,
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
                        if let Some(progress) = progress.as_ref() {
                            progress.record_bytes(&buffer[..granted]);
                        }
                    }
                    if granted < read_count {
                        truncated = true;
                        quota_triggered.store(true, Ordering::Release);
                        if let Some(progress) = progress.as_ref() {
                            progress.mark_truncated();
                        }
                        break;
                    }
                }
                Err(error) => {
                    if let Some(progress) = progress.as_ref() {
                        progress.record_read_error(error.to_string());
                    }
                    return StreamCapture { bytes, truncated, read_error: Some(error.to_string()) };
                }
            }
        }
        StreamCapture { bytes, truncated, read_error: None }
    })
}

// CAS loop over the budget shared by the stdout and stderr readers, so `max_output_bytes` caps
// the combined capture: each reader keeps at most the bytes it could atomically reserve.
// Relaxed ordering suffices because the counter is the only shared state being coordinated.
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
        collections::{BTreeMap, HashMap},
        ffi::OsString,
        fs, io,
        path::{Path, PathBuf},
        process::{Command, Stdio},
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc, Mutex, OnceLock,
        },
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use palyra_common::process_runner_input::ProcessWatchStream;
    #[cfg(feature = "qa-fault-injection")]
    use palyra_common::qa_fault_injection::{
        parse_qa_fault_evidence_sidecar_ndjson, QaFaultAction, QaFaultActivation,
        QaFaultEvidenceSidecarRecord, QaFaultInjectionPlan, QaFaultLaunchDocument,
        QaFaultRecoveryClass, QA_FAULT_INJECTION_PLAN_FORMAT,
        QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION, QA_FAULT_LAUNCH_SCHEMA_VERSION,
        QA_FAULT_TERMINATE_EXIT_CODE,
    };
    use palyra_sandbox::TierCCommandPlan;
    use serde_json::Value;

    use super::unix_pid_i32_from_u32;
    use super::{
        apply_tier_c_inner_path_prepend, build_process_command, builtin_list_directory_stdout,
        canonical_workspace_root, collect_requested_egress_hosts, command_env_value_os,
        command_option_consumes_non_path_value, cpu_rlimit_seconds_from_usage_micros,
        host_access_roots, is_host_allowlisted, maybe_emit_process_progress,
        process_completion_notification_projection, process_failure_message,
        process_failure_output_json, process_output_diagnostic_summary, process_progress_tail,
        process_runner_command_with_args_message, process_runtime_request_projection,
        process_success_output_json, process_watch_events_projection,
        redacted_process_output_preview, redacted_process_output_text,
        resolve_host_executable_path_with_roots, resolve_host_working_directory,
        resolve_host_working_directory_with_roots, resolve_scoped_path, resolve_working_directory,
        rewrite_arguments_to_scoped_paths, rewrite_host_access_process_args,
        rewrite_host_virtual_workspace_args, run_constrained_process,
        run_constrained_process_with_fault_injection, same_path_case_aware,
        tier_c_plan_inner_path_index, validate_allowed_executable,
        validate_argument_workspace_scope, validate_cmd_invocation_shape,
        validate_host_argument_scope, validate_host_argument_scope_with_roots,
        validate_host_interpreter_argument_guardrails,
        validate_host_interpreter_argument_guardrails_with_roots, validate_input_shape,
        validate_interpreter_argument_guardrails, validate_no_embedded_command_line_arg,
        validate_process_env_overrides, validate_process_prepend_path_shape,
        validate_process_termination_scope, validate_runtime_egress_enforcement,
        BackgroundLifetimeMode, EgressEnforcementMode, ManagedChildGuard, PathAccessMode,
        ProcessCompletionState, ProcessProgressMonitor, ProcessProgressSink, ProcessRunnerInput,
        ProcessSuccessOutputJsonInput, SandboxProcessRunError, SandboxProcessRunErrorKind,
        SandboxProcessRunnerPolicy, SandboxProcessRunnerTier, StreamCapture,
        MAX_PREPEND_PATH_COUNT, MAX_WATCH_PATTERNS, NODE_DISABLE_COMPILE_CACHE_ENV,
        PALYRA_OS_FILE_ROOTS_ENV, PROCESS_PROGRESS_MIN_ELAPSED_MS,
    };
    #[cfg(not(target_os = "macos"))]
    use super::{
        run_constrained_process_with_cancellation, BACKGROUND_MONITOR_POLL_MS,
        BACKGROUND_TERMINATION_WAIT_MS,
    };
    #[cfg(windows)]
    use super::{validate_host_command_path_scope, windows_program_files_path};
    #[cfg(not(target_os = "macos"))]
    use std::sync::atomic::AtomicBool;

    #[cfg(not(target_os = "macos"))]
    const BACKGROUND_TEST_EXECUTION_TIMEOUT_MS: u64 = 10_000;
    #[cfg(not(target_os = "macos"))]
    const BACKGROUND_TEST_SCRIPT_SLEEP_SECS: u64 = 8;
    const MANAGED_CHILD_GUARD_TEST_ENV: &str = "PALYRA_MANAGED_CHILD_GUARD_TEST_CHILD";
    const MANAGED_CHILD_GUARD_MARKER_ENV: &str = "PALYRA_MANAGED_CHILD_GUARD_TEST_MARKER";
    const MANAGED_CHILD_GUARD_GATE_ENV: &str = "PALYRA_MANAGED_CHILD_GUARD_TEST_GATE";
    const MANAGED_CHILD_GUARD_STARTED_ENV: &str = "PALYRA_MANAGED_CHILD_GUARD_TEST_STARTED";
    const MANAGED_CHILD_GUARD_LAUNCHER_STARTED_ENV: &str =
        "PALYRA_MANAGED_CHILD_GUARD_TEST_LAUNCHER_STARTED";
    #[cfg(feature = "qa-fault-injection")]
    const MANAGED_PROCESS_FAULT_TEST_MODE_ENV: &str = "PALYRA_MANAGED_PROCESS_FAULT_TEST_MODE";
    #[cfg(feature = "qa-fault-injection")]
    const MANAGED_PROCESS_FAULT_TEST_ROOT_ENV: &str = "PALYRA_MANAGED_PROCESS_FAULT_TEST_ROOT";
    static PROCESS_ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    fn empty_process_risk_report() -> palyra_common::process_risk::ProcessRiskReport {
        palyra_common::process_risk::ProcessRiskReport {
            schema_version: 1,
            policy: "advisory_only".to_owned(),
            execution_allowed: true,
            blocks_execution: false,
            requires_user_approval: false,
            highest_severity: palyra_common::process_risk::ProcessRiskSeverity::Low,
            target_runtime: None,
            findings: Vec::new(),
        }
    }

    struct ScopedEnvVar {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: impl Into<OsString>) -> Self {
            let previous = std::env::var_os(key);
            std::env::set_var(key, value.into());
            Self { key, previous }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.previous.as_ref() {
                Some(previous) => std::env::set_var(self.key, previous),
                None => std::env::remove_var(self.key),
            }
        }
    }

    #[test]
    fn unix_pid_i32_from_u32_rejects_values_outside_pid_t_range() {
        assert_eq!(unix_pid_i32_from_u32(1).expect("pid 1 should fit"), 1_i32);
        let error = unix_pid_i32_from_u32(u32::MAX).expect_err("oversized pid should be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(
            error.to_string().contains("exceeds Unix pid_t range"),
            "error should explain the rejected pid range"
        );
    }

    #[test]
    fn managed_child_guard_drop_terminates_and_reaps_the_child() {
        match std::env::var(MANAGED_CHILD_GUARD_TEST_ENV).ok().as_deref() {
            Some("sleep") => {
                thread::sleep(Duration::from_secs(5));
                fs::write(
                    std::env::var_os(MANAGED_CHILD_GUARD_MARKER_ENV)
                        .expect("managed child marker path should be provided"),
                    b"child completed without being reaped",
                )
                .expect("managed child marker should be writable");
                return;
            }
            Some("exit") => return,
            _ => {}
        }

        let marker_root = unique_temp_dir("managed-child-guard");
        fs::create_dir_all(marker_root.as_path()).expect("marker root should be created");
        let marker = marker_root.join("completed.txt");
        let mut command =
            Command::new(std::env::current_exe().expect("current test executable should resolve"));
        command
            .args([
                "--exact",
                "sandbox_runner::tests::managed_child_guard_drop_terminates_and_reaps_the_child",
                "--nocapture",
            ])
            .env(MANAGED_CHILD_GUARD_TEST_ENV, "sleep")
            .env(MANAGED_CHILD_GUARD_MARKER_ENV, marker.as_os_str())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        super::configure_child_process_group(&mut command);
        let child = command.spawn().expect("managed child should spawn");
        let child_pid = child.id();
        let termination_probe = Arc::new(AtomicUsize::new(0));
        let started_at = Instant::now();

        drop(ManagedChildGuard::with_termination_probe(child, Arc::clone(&termination_probe)));

        assert!(
            started_at.elapsed() < Duration::from_secs(4),
            "guard drop waited for the child's natural completion instead of terminating it"
        );
        assert_eq!(termination_probe.load(Ordering::SeqCst), 1);
        assert!(
            super::wait_for_process_not_alive(child_pid, Duration::from_secs(1)),
            "guard drop must leave no live child process"
        );
        assert!(!marker.exists(), "terminated child must not reach its completion marker");
        fs::remove_dir_all(marker_root).expect("marker root should be removable after reap");
    }

    #[cfg(feature = "qa-fault-injection")]
    #[test]
    fn managed_process_post_spawn_fault_verifies_cleanup_before_exit() {
        match std::env::var(MANAGED_PROCESS_FAULT_TEST_MODE_ENV).ok().as_deref() {
            Some("process") => {
                thread::sleep(Duration::from_secs(30));
                return;
            }
            Some("adapter") => run_managed_process_fault_adapter_child(),
            _ => {}
        }

        let temporary = tempfile::tempdir().expect("managed-process fault root should be created");
        let output = Command::new(
            std::env::current_exe().expect("current test executable should resolve"),
        )
        .args([
            "--exact",
            "sandbox_runner::tests::managed_process_post_spawn_fault_verifies_cleanup_before_exit",
            "--nocapture",
        ])
        .env(MANAGED_PROCESS_FAULT_TEST_MODE_ENV, "adapter")
        .env(MANAGED_PROCESS_FAULT_TEST_ROOT_ENV, temporary.path())
        .output()
        .expect("managed-process fault adapter child should launch");
        assert_eq!(
            output.status.code(),
            Some(QA_FAULT_TERMINATE_EXIT_CODE),
            "adapter stderr: {}",
            String::from_utf8_lossy(output.stderr.as_slice())
        );

        let pid = fs::read_to_string(temporary.path().join("process.pid"))
            .expect("adapter should persist the owned process pid")
            .trim()
            .parse::<u32>()
            .expect("owned process pid should be numeric");
        assert!(
            super::wait_for_process_not_alive(pid, Duration::from_secs(5)),
            "managed process {pid} must be reaped before recovery evidence is accepted"
        );

        let plan = managed_process_fault_test_plan();
        let launch = managed_process_fault_test_launch(temporary.path(), &plan);
        let evidence = fs::read(temporary.path().join("evidence.ndjson"))
            .expect("managed-process fault evidence should be readable");
        let parsed = parse_qa_fault_evidence_sidecar_ndjson(evidence.as_slice(), &launch, &plan)
            .expect("managed-process fault evidence should validate");
        assert!(matches!(
            parsed.records().last(),
            Some(QaFaultEvidenceSidecarRecord::RecoveryRecorded(record))
                if record.activation_id == "managed-process-post-spawn"
                    && record.recovery_class == QaFaultRecoveryClass::OutcomeUnknown
                    && record.reason_code
                        == "qa_fault.managed_process_owned_tree_cleanup_verified"
        ));
    }

    #[cfg(feature = "qa-fault-injection")]
    fn run_managed_process_fault_adapter_child() -> ! {
        let root = PathBuf::from(
            std::env::var_os(MANAGED_PROCESS_FAULT_TEST_ROOT_ENV)
                .expect("managed-process fault root should be provided"),
        );
        let plan = managed_process_fault_test_plan();
        let launch = managed_process_fault_test_launch(root.as_path(), &plan);
        let runtime = crate::qa_fault_injection::QaFaultRuntime::active_for_test(
            plan,
            launch,
            root.join("evidence.ndjson"),
        )
        .expect("managed-process fault runtime should initialize");

        let mut command =
            Command::new(std::env::current_exe().expect("current test executable should resolve"));
        command
            .args([
                "--exact",
                "sandbox_runner::tests::managed_process_post_spawn_fault_verifies_cleanup_before_exit",
                "--nocapture",
            ])
            .env(MANAGED_PROCESS_FAULT_TEST_MODE_ENV, "process")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        super::configure_child_process_group(&mut command);
        super::configure_background_child_suspended(&mut command);
        let child = command.spawn().expect("managed test process should spawn");
        #[cfg(windows)]
        let child = super::prepare_windows_background_child(ManagedChildGuard::new(child))
            .expect("managed test process should bind to an owned Windows job");
        #[cfg(not(windows))]
        let child = ManagedChildGuard::new(child);
        fs::write(root.join("process.pid"), child.id().to_string())
            .expect("managed test process pid should be persisted");

        let _ = super::apply_managed_process_fault_with_child(
            &runtime,
            "managed_process.after_effect_before_ack",
            "managed-process-test",
            child,
        );
        panic!("terminate fault must exit after verified process-tree cleanup")
    }

    #[cfg(feature = "qa-fault-injection")]
    fn managed_process_fault_test_plan() -> QaFaultInjectionPlan {
        QaFaultInjectionPlan {
            schema_version: QA_FAULT_INJECTION_PLAN_SCHEMA_VERSION,
            format: QA_FAULT_INJECTION_PLAN_FORMAT.to_owned(),
            seed: 20260711,
            activations: vec![QaFaultActivation {
                id: "managed-process-post-spawn".to_owned(),
                point_id: "managed_process.after_effect_before_ack".to_owned(),
                actor: Some("managed-process-test".to_owned()),
                occurrence: 1,
                action: QaFaultAction::TerminateProcess,
            }],
        }
    }

    #[cfg(feature = "qa-fault-injection")]
    fn managed_process_fault_test_launch(
        root: &Path,
        plan: &QaFaultInjectionPlan,
    ) -> QaFaultLaunchDocument {
        QaFaultLaunchDocument {
            schema_version: QA_FAULT_LAUNCH_SCHEMA_VERSION,
            launch_id: "managed-process-fault-launch".to_owned(),
            plan_path: root.join("plan.json").to_string_lossy().into_owned(),
            plan_sha256: plan.canonical_sha256().expect("managed-process plan should hash"),
            capability_sha256: "a".repeat(64),
            evidence_path: root.join("evidence.ndjson").to_string_lossy().into_owned(),
            expires_at_unix_ms: i64::MAX,
        }
    }

    #[test]
    fn managed_child_guard_never_terminates_after_observing_direct_exit() {
        let mut command =
            Command::new(std::env::current_exe().expect("current test executable should resolve"));
        command
            .args([
                "--exact",
                "sandbox_runner::tests::managed_child_guard_drop_terminates_and_reaps_the_child",
                "--nocapture",
            ])
            .env(MANAGED_CHILD_GUARD_TEST_ENV, "exit")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        super::configure_child_process_group(&mut command);
        let child = command.spawn().expect("short managed child should spawn");
        let termination_probe = Arc::new(AtomicUsize::new(0));
        let mut child =
            ManagedChildGuard::with_termination_probe(child, Arc::clone(&termination_probe));
        let started_at = Instant::now();
        loop {
            if child.try_wait().expect("short child wait should succeed").is_some() {
                break;
            }
            assert!(started_at.elapsed() < Duration::from_secs(5));
            thread::sleep(Duration::from_millis(5));
        }

        drop(child);

        assert_eq!(
            termination_probe.load(Ordering::SeqCst),
            0,
            "observed exit must disarm every numerical termination path"
        );
    }

    #[test]
    fn background_guard_cleans_descendants_after_launcher_exit() {
        match std::env::var(MANAGED_CHILD_GUARD_TEST_ENV).ok().as_deref() {
            Some("descendant") => {
                fs::write(
                    std::env::var_os(MANAGED_CHILD_GUARD_STARTED_ENV)
                        .expect("descendant start marker should be provided"),
                    b"started",
                )
                .expect("descendant start marker should be writable");
                thread::sleep(Duration::from_secs(5));
                fs::write(
                    std::env::var_os(MANAGED_CHILD_GUARD_MARKER_ENV)
                        .expect("descendant completion marker should be provided"),
                    b"descendant survived cleanup",
                )
                .expect("descendant completion marker should be writable");
                return;
            }
            Some("launcher") => {
                if let Some(marker) = std::env::var_os(MANAGED_CHILD_GUARD_LAUNCHER_STARTED_ENV) {
                    fs::write(marker, b"launcher started")
                        .expect("launcher start marker should be writable");
                }
                let gate = PathBuf::from(
                    std::env::var_os(MANAGED_CHILD_GUARD_GATE_ENV)
                        .expect("launcher gate should be provided"),
                );
                let gate_wait = Instant::now();
                while !gate.exists() {
                    assert!(gate_wait.elapsed() < Duration::from_secs(5));
                    thread::sleep(Duration::from_millis(5));
                }
                let mut descendant = Command::new(
                    std::env::current_exe().expect("current test executable should resolve"),
                )
                .args([
                    "--exact",
                    "sandbox_runner::tests::background_guard_cleans_descendants_after_launcher_exit",
                    "--nocapture",
                ])
                .env(MANAGED_CHILD_GUARD_TEST_ENV, "descendant")
                .env(
                    MANAGED_CHILD_GUARD_STARTED_ENV,
                    std::env::var_os(MANAGED_CHILD_GUARD_STARTED_ENV)
                        .expect("descendant start marker should be forwarded"),
                )
                .env(
                    MANAGED_CHILD_GUARD_MARKER_ENV,
                    std::env::var_os(MANAGED_CHILD_GUARD_MARKER_ENV)
                        .expect("descendant completion marker should be forwarded"),
                )
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("descendant should spawn");
                thread::spawn(move || {
                    let _ = descendant.wait();
                });
                let started = PathBuf::from(
                    std::env::var_os(MANAGED_CHILD_GUARD_STARTED_ENV)
                        .expect("descendant start marker should be available"),
                );
                let start_wait = Instant::now();
                while !started.exists() {
                    assert!(start_wait.elapsed() < Duration::from_secs(5));
                    thread::sleep(Duration::from_millis(5));
                }
                return;
            }
            _ => {}
        }

        let root = unique_temp_dir("managed-background-descendant");
        fs::create_dir_all(root.as_path()).expect("background marker root should be created");
        let gate = root.join("gate");
        let started = root.join("started");
        let completed = root.join("completed");
        let mut command =
            Command::new(std::env::current_exe().expect("current test executable should resolve"));
        command
            .args([
                "--exact",
                "sandbox_runner::tests::background_guard_cleans_descendants_after_launcher_exit",
                "--nocapture",
            ])
            .env(MANAGED_CHILD_GUARD_TEST_ENV, "launcher")
            .env(MANAGED_CHILD_GUARD_GATE_ENV, gate.as_os_str())
            .env(MANAGED_CHILD_GUARD_STARTED_ENV, started.as_os_str())
            .env(MANAGED_CHILD_GUARD_MARKER_ENV, completed.as_os_str())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        super::configure_child_process_group(&mut command);
        super::configure_background_child_suspended(&mut command);
        let child = command.spawn().expect("background launcher should spawn");
        #[cfg(windows)]
        let mut child = super::prepare_windows_background_child(ManagedChildGuard::new(child))
            .expect("background launcher should bind to an owned job and resume");
        #[cfg(not(windows))]
        let mut child = ManagedChildGuard::new(child);
        fs::write(gate.as_path(), b"release").expect("launcher gate should open");
        let exit_wait = Instant::now();
        loop {
            if child.try_wait().expect("launcher wait should succeed").is_some() {
                break;
            }
            assert!(exit_wait.elapsed() < Duration::from_secs(5));
            thread::sleep(Duration::from_millis(5));
        }
        assert!(started.exists(), "launcher must start its descendant before exiting");

        super::terminate_background_child(child)
            .expect("owned background tree should terminate and verify inactive");

        assert!(!completed.exists(), "descendant must not survive verified tree cleanup");
        fs::remove_dir_all(root).expect("background marker root should be removable");
    }

    #[cfg(windows)]
    #[test]
    fn windows_background_startup_failures_keep_process_tree_owned() {
        fn injected_bind_failure(_child: &std::process::Child, pid: u32) -> io::Result<()> {
            // If CREATE_SUSPENDED regresses, this window lets the launcher leave deterministic
            // evidence before the injected bind failure triggers cleanup.
            thread::sleep(Duration::from_millis(250));
            Err(io::Error::other(format!("injected Windows job binding failure for pid {pid}")))
        }

        for failure_stage in ["bind", "register", "resume"] {
            let root = unique_temp_dir(&format!("windows-background-{failure_stage}-failure"));
            fs::create_dir_all(root.as_path()).expect("background marker root should be created");
            let gate = root.join("gate");
            let launcher_started = root.join("launcher-started");
            let descendant_started = root.join("descendant-started");
            let descendant_completed = root.join("descendant-completed");
            fs::write(gate.as_path(), b"already open").expect("launcher gate should be writable");

            let mut command = Command::new(
                std::env::current_exe().expect("current test executable should resolve"),
            );
            command
                .args([
                    "--exact",
                    "sandbox_runner::tests::background_guard_cleans_descendants_after_launcher_exit",
                    "--nocapture",
                ])
                .env(MANAGED_CHILD_GUARD_TEST_ENV, "launcher")
                .env(MANAGED_CHILD_GUARD_GATE_ENV, gate.as_os_str())
                .env(
                    MANAGED_CHILD_GUARD_LAUNCHER_STARTED_ENV,
                    launcher_started.as_os_str(),
                )
                .env(MANAGED_CHILD_GUARD_STARTED_ENV, descendant_started.as_os_str())
                .env(MANAGED_CHILD_GUARD_MARKER_ENV, descendant_completed.as_os_str())
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            super::configure_background_child_suspended(&mut command);
            let child = command.spawn().expect("suspended background launcher should spawn");
            let child = ManagedChildGuard::new(child);
            let pid = child.id();
            let launcher_started_after_resume = launcher_started.clone();
            let descendant_started_after_resume = descendant_started.clone();

            let preparation = match failure_stage {
                "bind" => super::prepare_windows_background_child_with_operations(
                    child,
                    injected_bind_failure,
                    super::resume_suspended_windows_process,
                ),
                "register" => super::prepare_windows_background_child_with_operations(
                    child,
                    |child, pid| {
                        super::bind_child_to_windows_background_job_with_register(
                            child,
                            pid,
                            |registered_pid, job| {
                                // This matches a registry error after assignment: the
                                // unregistered Arc is the final kill-on-close owner of the
                                // still-suspended job.
                                drop(job);
                                Err(io::Error::other(format!(
                                    "injected Windows job registry failure for pid {registered_pid}"
                                )))
                            },
                        )
                    },
                    super::resume_suspended_windows_process,
                ),
                "resume" => super::prepare_windows_background_child_with_operations(
                    child,
                    super::bind_child_to_windows_background_job,
                    move |pid| {
                        super::resume_suspended_windows_process(pid)?;
                        // Wait for deterministic proof that both launcher and descendant ran.
                        // Returning the injected error only then exercises partial-resume tree
                        // cleanup without relying on scheduler timing in loaded Windows CI.
                        let marker_wait = Instant::now();
                        while !launcher_started_after_resume.exists()
                            || !descendant_started_after_resume.exists()
                        {
                            if marker_wait.elapsed() >= Duration::from_secs(5) {
                                return Err(io::Error::new(
                                    io::ErrorKind::TimedOut,
                                    format!(
                                        "process {pid} did not produce partial-resume markers before the test deadline"
                                    ),
                                ));
                            }
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(io::Error::other(format!(
                            "injected Windows thread resume failure for pid {pid}"
                        )))
                    },
                ),
                _ => unreachable!("test enumerates every Windows startup failure stage"),
            };
            let error = match preparation {
                Ok(child) => {
                    drop(child);
                    panic!("injected {failure_stage} failure should reject startup");
                }
                Err(error) => error,
            };

            assert!(
                error.message.contains("could not"),
                "failure should retain startup-stage context: {}",
                error.message
            );
            assert!(
                !super::process_id_is_alive(pid)
                    .expect("direct process liveness probe should succeed"),
                "failed startup must leave no live direct process"
            );
            assert!(
                super::windows_background_job(pid).is_none(),
                "failed startup must release any registered job handle"
            );
            if failure_stage != "resume" {
                assert!(
                    !launcher_started.exists(),
                    "launcher code must not run before Windows ownership is established"
                );
                assert!(
                    !descendant_started.exists(),
                    "pre-resume failure must not create a descendant"
                );
            } else {
                assert!(
                    launcher_started.exists(),
                    "partial-resume case must prove that launcher code executed"
                );
                assert!(
                    descendant_started.exists(),
                    "partial-resume case must create a descendant before cleanup"
                );
            }
            assert!(
                !descendant_completed.exists(),
                "failed startup must not leave a descendant completion marker"
            );
            fs::remove_dir_all(root).expect("background marker root should be removable");
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_background_job_termination_retries_after_operation_failure() {
        let job = super::create_windows_background_job()
            .expect("empty kill-on-close job should be created");
        let attempts = AtomicUsize::new(0);

        let first_error = job
            .terminate_with(|_| {
                attempts.fetch_add(1, Ordering::SeqCst);
                Err(io::Error::other("injected TerminateJobObject failure"))
            })
            .expect_err("first termination operation should fail");
        assert!(first_error.to_string().contains("injected TerminateJobObject failure"));

        job.terminate_with(|_| {
            attempts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .expect("termination should retry after the failed operation");
        job.terminate_with(|_| -> io::Result<()> {
            panic!("successful termination must make later calls idempotent")
        })
        .expect("idempotent termination should preserve the successful result");

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[cfg(windows)]
    #[test]
    fn windows_startup_cleanup_classification_requires_verified_inactivity() {
        let inactive = super::BackgroundProcessRuntimeStatus {
            direct_pid_alive: false,
            process_tree_alive: false,
            tracked_process_count: Some(0),
        };
        let active = super::BackgroundProcessRuntimeStatus {
            direct_pid_alive: true,
            process_tree_alive: true,
            tracked_process_count: Some(1),
        };

        assert!(super::windows_background_startup_cleanup_is_authoritative(true, Some(inactive)));
        assert!(!super::windows_background_startup_cleanup_is_authoritative(false, Some(inactive)));
        assert!(!super::windows_background_startup_cleanup_is_authoritative(true, Some(active)));
        assert!(!super::windows_background_startup_cleanup_is_authoritative(true, None));
    }

    #[cfg(not(target_os = "macos"))]
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
            path_access_mode: PathAccessMode::WorkspaceOnly,
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

    fn process_runner_input(
        command: &str,
        args: &[&str],
        timeout_ms: Option<u64>,
    ) -> ProcessRunnerInput {
        ProcessRunnerInput {
            command: command.to_owned(),
            args: args.iter().map(|arg| (*arg).to_owned()).collect(),
            cwd: None,
            env: BTreeMap::new(),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
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
        policy.path_access_mode = PathAccessMode::ApprovedRoots;
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
    fn resolve_host_working_directory_allows_configured_os_roots() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-host-cwd");
        let outside = unique_temp_dir("outside-host-cwd");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let configured_env =
            std::env::join_paths([outside.as_os_str()]).expect("root path should join");
        let _configured_roots = ScopedEnvVar::set(PALYRA_OS_FILE_ROOTS_ENV, configured_env);
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let canonical_outside =
            fs::canonicalize(outside.as_path()).expect("outside cwd should canonicalize");

        let resolved = resolve_host_working_directory(
            canonical_workspace.as_path(),
            Some(canonical_outside.to_string_lossy().as_ref()),
        )
        .expect("host access should allow explicitly configured OS cwd");

        assert_eq!(resolved, canonical_outside);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_access_roots_require_explicit_configuration() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-explicit-host-roots");
        let home = unique_temp_dir("unconfigured-home-host-root");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(home.as_path()).expect("home fixture should be created");
        let ssh_dir = home.join(".ssh");
        fs::create_dir_all(ssh_dir.as_path()).expect("ssh fixture directory should be created");
        let secret = ssh_dir.join("id_ed25519");
        fs::write(secret.as_path(), b"private key fixture")
            .expect("secret fixture should be written");
        let _configured_roots = ScopedEnvVar::set(PALYRA_OS_FILE_ROOTS_ENV, "");
        let _userprofile = ScopedEnvVar::set("USERPROFILE", home.as_os_str());
        let _home = ScopedEnvVar::set("HOME", home.as_os_str());
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");

        assert!(
            host_access_roots().is_empty(),
            "HOME and USERPROFILE must not become implicit process roots"
        );
        let argument_error = validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "cat",
            &[secret.display().to_string()],
        )
        .expect_err("process arguments outside explicit roots must be denied");
        assert_eq!(argument_error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let interpreter_error = validate_host_interpreter_argument_guardrails(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "python",
            &[secret.display().to_string()],
        )
        .expect_err("interpreter paths outside explicit roots must be denied");
        assert_eq!(interpreter_error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(home.as_path());
    }

    #[test]
    fn resolve_host_working_directory_expands_safe_env_root() {
        let workspace = unique_temp_dir("workspace-host-env-cwd");
        let outside = unique_temp_dir("outside-host-env-cwd");
        let nested = outside.join("nested");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(nested.as_path()).expect("env cwd directory should be created");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let canonical_outside =
            fs::canonicalize(outside.as_path()).expect("outside root should canonicalize");
        let canonical_nested =
            fs::canonicalize(nested.as_path()).expect("nested cwd should canonicalize");
        let host_roots = vec![canonical_outside.clone()];
        let path_env =
            BTreeMap::from([("PALYRA_E2E_OS_ROOT".to_owned(), canonical_outside.clone())]);

        let resolved = resolve_host_working_directory_with_roots(
            canonical_workspace.as_path(),
            Some("%PALYRA_E2E_OS_ROOT%\\nested"),
            host_roots.as_slice(),
            &path_env,
        )
        .expect("host access cwd should expand safe launch-context path env roots");

        assert_eq!(resolved, canonical_nested);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[cfg(unix)]
    #[test]
    fn host_access_rejects_writable_approved_root_components() {
        use std::os::unix::fs::PermissionsExt as _;

        let workspace = unique_temp_dir("workspace-host-writable-root-deny");
        let outside = unique_temp_dir("outside-host-writable-root-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        fs::set_permissions(outside.as_path(), fs::Permissions::from_mode(0o777))
            .expect("outside directory permissions should be set");
        let script = outside.join("helper.py");
        fs::write(script.as_path(), b"print('unsafe')\n").expect("helper should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let canonical_outside =
            fs::canonicalize(outside.as_path()).expect("outside root should canonicalize");
        let canonical_script =
            fs::canonicalize(script.as_path()).expect("outside script should canonicalize");

        let error = validate_host_argument_scope_with_roots(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "python",
            &[canonical_script.display().to_string()],
            &[canonical_outside],
        )
        .expect_err("group/other-writable approved root must be denied");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("writable by group/other"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_access_allows_configured_script_argument() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-host-script-arg");
        let outside = unique_temp_dir("outside-host-script-arg");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let configured_env =
            std::env::join_paths([outside.as_os_str()]).expect("root path should join");
        let _configured_roots = ScopedEnvVar::set(PALYRA_OS_FILE_ROOTS_ENV, configured_env);
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
        .expect("host access should allow interpreter scripts under configured OS roots");
        validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_outside.as_path(),
            "node",
            args.as_slice(),
        )
        .expect("host access should allow absolute script args under configured OS roots");

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_access_argument_validation_rejects_bare_virtual_workspace_root_alias() {
        let workspace = unique_temp_dir("workspace-host-root-alias-validation-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");

        for args in [vec!["/".to_owned()], vec!["--root=/".to_owned()]] {
            let error = validate_host_argument_scope_with_roots(
                canonical_workspace.as_path(),
                canonical_workspace.as_path(),
                "node",
                args.as_slice(),
                &[],
            )
            .expect_err("bare root aliases must not validate as host-access workspace paths");

            assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
            assert!(error.message.contains("bare workspace-root alias"), "{}", error.message);
        }

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_interpreter_guardrails_reject_node_eval_with_route_literals() {
        let workspace = unique_temp_dir("workspace-host-node-inline-route");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![
            "-e".to_owned(),
            "const fs = require('fs'); const route = '/settings'; const t = fs.readFileSync('app.js', 'utf8'); console.log(route, t.length);".to_owned(),
        ];

        let error = validate_host_interpreter_argument_guardrails(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect_err("host node eval must stay blocked even when route strings look harmless");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_interpreter_guardrails_reject_node_eval_absolute_fs_path_outside_roots() {
        let workspace = unique_temp_dir("workspace-host-node-inline-path-deny");
        let outside = unique_temp_dir("outside-host-node-inline-path-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let outside_file = outside.join("secret.txt");
        fs::write(outside_file.as_path(), b"secret").expect("outside fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let outside_path = outside_file.to_string_lossy().replace('\\', "/");
        let args =
            vec!["-p".to_owned(), format!("require('fs').readFileSync('{outside_path}', 'utf8')")];

        let error = validate_host_interpreter_argument_guardrails_with_roots(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
            std::slice::from_ref(&canonical_workspace),
        )
        .expect_err("host node eval must be denied before source path heuristics");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn host_access_rejects_relative_wsl_execution_outside_windows_root_authority() {
        let workspace = unique_temp_dir("workspace-host-wsl-relative-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args =
            vec!["--exec".to_owned(), "sh".to_owned(), "scripts/configure-user.sh".to_owned()];

        let error = validate_host_interpreter_argument_guardrails_with_roots(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "wsl.exe",
            args.as_slice(),
            std::slice::from_ref(&canonical_workspace),
        )
        .expect_err("WSL must not inherit Windows approved-root authority");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("nested runtime"), "{}", error.message);
        let _ = fs::remove_dir_all(workspace.as_path());
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
        let expected_script = super::child_process_path(
            canonical_workspace.join("e2e-file-workflow").join("test.js").as_path(),
        )
        .to_string_lossy()
        .to_string();
        let expected_directory =
            super::child_process_path(canonical_workspace.join("e2e-file-workflow").as_path())
                .to_string_lossy()
                .to_string();
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
    fn rewrite_arguments_to_scoped_paths_preserves_builtin_list_flag_cluster() {
        let workspace = unique_temp_dir("workspace-rewrite-list-flag-cluster");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("a"), b"not an option value")
            .expect("flag-cluster sentinel file should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-la".to_owned()];

        let rewritten = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "ls",
            args.as_slice(),
        )
        .expect("builtin list flag cluster should not be interpreted as a path value");

        assert_eq!(rewritten, args);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn resolve_scoped_path_collapses_repeated_active_workspace_name() {
        let parent = unique_temp_dir("workspace-virtual-active-parent");
        let workspace = parent.join("S091_shell_profile");
        fs::create_dir_all(workspace.join("scripts")).expect("workspace directory should exist");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");

        let resolved_root = resolve_scoped_path(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "/workspace/S091_shell_profile",
            true,
        )
        .expect("virtual active workspace alias should resolve to the root itself");
        assert_eq!(resolved_root, canonical_workspace);

        let resolved_nested = resolve_scoped_path(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "/workspace/S091_shell_profile/scripts/helper.ps1",
            false,
        )
        .expect("virtual active workspace alias should preserve nested suffix");
        assert_eq!(resolved_nested, canonical_workspace.join("scripts").join("helper.ps1"));

        let _ = fs::remove_dir_all(parent.as_path());
    }

    #[test]
    fn node_eval_flags_are_not_non_path_option_exemptions() {
        let workspace = unique_temp_dir("workspace-node-eval-arg-rewrite");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-e".to_owned(), "console.log('PALYRA_PROCESS_OK')".to_owned()];

        assert!(!command_option_consumes_non_path_value("node", "-e"));
        assert!(!command_option_consumes_non_path_value("nodejs", "-p"));
        let error = validate_interpreter_argument_guardrails(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect_err("node eval flags must be blocked before argument rewriting");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn node_aliases_require_interpreter_opt_in() {
        let workspace = unique_temp_dir("workspace-node-alias-policy");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let policy =
            sandbox_policy_with_allowed_executables(canonical_workspace, vec!["nodejs".to_owned()]);

        for command in ["nodejs", "nodejs.exe"] {
            let error = validate_allowed_executable(&policy, command)
                .expect_err("Node aliases must require explicit interpreter opt-in");
            assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
            assert!(error.message.contains("allow_interpreters=true"), "{}", error.message);
        }

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn rewrite_arguments_to_scoped_paths_preserves_python_module_names() {
        let workspace = unique_temp_dir("workspace-python-module-arg-rewrite");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-m".to_owned(), "http.server".to_owned(), "0".to_owned()];

        let rewritten = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "python",
            args.as_slice(),
        )
        .expect("python module names after -m should remain literal arguments");

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
    fn rewrite_arguments_to_scoped_paths_preserves_opaque_node_and_git_arguments() {
        let workspace = unique_temp_dir("workspace-opaque-process-arguments");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let cases = [
            ("node", vec!["--experimental-default-type=module", "verify-config.mjs"]),
            ("git", vec!["status", "--untracked-files=all"]),
            ("git", vec!["branch", "e2e/local-commit-smoke"]),
            ("git", vec!["switch", "-c", "e2e/local-commit-smoke"]),
            ("git", vec!["show", "HEAD:README.md"]),
            ("git", vec!["diff", "--unified=0"]),
            ("git", vec!["log", "--date=short"]),
        ];

        for (command, raw_args) in cases {
            let args = raw_args.into_iter().map(str::to_owned).collect::<Vec<_>>();
            let rewritten = rewrite_arguments_to_scoped_paths(
                canonical_workspace.as_path(),
                canonical_workspace.as_path(),
                command,
                args.as_slice(),
            )
            .expect("opaque command arguments should remain valid");
            assert_eq!(rewritten, args, "command {command} must preserve argv exactly");
        }

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn rewrite_arguments_to_scoped_paths_still_rejects_relative_traversal() {
        let workspace = unique_temp_dir("workspace-process-argument-traversal");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["../outside/script.mjs".to_owned()];

        let error = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            args.as_slice(),
        )
        .expect_err("relative traversal must still pass through workspace scoping");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let git_args = vec!["diff".to_owned(), "../outside/secret.txt".to_owned()];
        let git_error = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "git",
            git_args.as_slice(),
        )
        .expect_err("Git pathspec traversal must still pass through workspace scoping");
        assert_eq!(git_error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let drive_relative_args = vec!["C:outside.txt".to_owned()];
        let drive_relative_error = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "node",
            drive_relative_args.as_slice(),
        )
        .expect_err("drive-relative paths must fail closed");
        assert_eq!(drive_relative_error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn destructive_process_proposals_require_approval_before_execution() {
        let workspace = unique_temp_dir("workspace-process-risk-approval");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("cleanup.py"), b"import shutil\nshutil.rmtree('generated')\n")
            .expect("cleanup script should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let policy =
            sandbox_policy_with_allowed_executables(canonical_workspace, vec!["git".to_owned()]);

        assert!(super::process_runner_input_requires_user_approval(
            &policy,
            br#"{"command":"git","args":["clean","-ffd","-x","--","generated"]}"#
        ));
        assert!(super::process_runner_input_requires_user_approval(
            &policy,
            br#"{"command":"python","args":["cleanup.py"]}"#
        ));
        assert!(!super::process_runner_input_requires_user_approval(
            &policy,
            br#"{"command":"git","args":["status","--untracked-files=all"]}"#
        ));

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn rewrite_arguments_to_scoped_paths_preserves_npm_script_and_network_values() {
        let workspace = unique_temp_dir("workspace-npm-network-values");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = ["run", "dev", "--", "--host", "127.0.0.1", "--port", "5173"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();

        let rewritten = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "npm",
            args.as_slice(),
        )
        .expect("npm lifecycle and syntactic network values should remain non-path arguments");

        assert_eq!(rewritten, args);
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn rewrite_arguments_to_scoped_paths_rejects_path_shaped_network_values() {
        let workspace = unique_temp_dir("workspace-network-path-value");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = ["run", "dev", "--", "--host", "../outside"]
            .into_iter()
            .map(str::to_owned)
            .collect::<Vec<_>>();

        let error = rewrite_arguments_to_scoped_paths(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "npm",
            args.as_slice(),
        )
        .expect_err("path-shaped network values must still pass workspace scoping");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
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
        let expected_workspace = super::child_process_path(canonical_workspace.as_path());
        let expected_script = super::child_process_path(
            canonical_workspace.join("e2e-file-workflow").join("test.js").as_path(),
        )
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
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
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

        assert_eq!(args[0], expected_workspace.to_string_lossy());
        assert_eq!(args[1], format!("--config={expected_script}"));

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[cfg(unix)]
    #[test]
    fn build_process_command_resolves_unix_background_program_absolutely() {
        let workspace = unique_temp_dir("workspace-unix-background-program");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let mut input = process_runner_input("sh", &["-c", "exit 0"], Some(1_000));
        input.background = true;
        let policies = [
            sandbox_policy_with_allowed_executables(
                canonical_workspace.clone(),
                vec!["sh".to_owned()],
            ),
            host_access_policy(canonical_workspace.clone()),
        ];

        for policy in policies {
            let command = build_process_command(
                &policy,
                &input,
                canonical_workspace.as_path(),
                canonical_workspace.as_path(),
            )
            .expect("Unix background command should resolve through the trusted path");

            assert!(
                Path::new(command.get_program()).is_absolute(),
                "Unix supervisor target must be absolute: {}",
                command.get_program().to_string_lossy()
            );
        }

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
    fn host_access_rejects_bare_virtual_workspace_root_alias_args() {
        let workspace = unique_temp_dir("workspace-host-root-alias-arg-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");

        for args in [vec!["/".to_owned()], vec!["--root=/".to_owned()]] {
            let error = rewrite_host_access_process_args(
                args.as_slice(),
                canonical_workspace.as_path(),
                &BTreeMap::new(),
            )
            .expect_err("bare root aliases must not be forwarded to host-access processes");

            assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
            assert!(error.message.contains("bare workspace-root alias"), "{}", error.message);
        }

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_access_rewrites_safe_env_path_args() {
        let workspace = unique_temp_dir("workspace-host-env-arg");
        let outside = unique_temp_dir("outside-host-env-arg");
        let nested = outside.join("fixtures");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(nested.as_path()).expect("env arg directory should be created");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let canonical_outside =
            fs::canonicalize(outside.as_path()).expect("outside root should canonicalize");
        let expected_fixture =
            canonical_outside.join("fixtures").join("provider.toml").to_string_lossy().to_string();
        let path_env =
            BTreeMap::from([("PALYRA_E2E_OS_ROOT".to_owned(), canonical_outside.clone())]);
        let args = vec![
            "%PALYRA_E2E_OS_ROOT%/fixtures/provider.toml".to_owned(),
            "--config=%PALYRA_E2E_OS_ROOT%\\fixtures\\provider.toml".to_owned(),
        ];

        let rewritten = rewrite_host_access_process_args(
            args.as_slice(),
            canonical_workspace.as_path(),
            &path_env,
        )
        .expect("host access should rewrite safe path env args");

        assert_eq!(rewritten[0], expected_fixture);
        assert_eq!(rewritten[1], format!("--config={expected_fixture}"));

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_access_preserves_unsupported_env_literal_args() {
        let workspace = unique_temp_dir("workspace-host-env-literal-arg");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let args = vec![
            "$HOME".to_owned(),
            "--message=$PALYRA_NOT_A_PATH_ROOT".to_owned(),
            "%PALYRA_NOT_A_PATH_ROOT%\\fixture.txt".to_owned(),
        ];

        let rewritten = rewrite_host_access_process_args(
            args.as_slice(),
            canonical_workspace.as_path(),
            &BTreeMap::new(),
        )
        .expect("unsupported env literals should not be treated as safe path env prefixes");

        assert_eq!(rewritten, args);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_access_rejects_traversal_in_safe_env_path_args() {
        let workspace = unique_temp_dir("workspace-host-env-arg-traversal");
        let outside = unique_temp_dir("outside-host-env-arg-traversal");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("env root should be created");
        let canonical_workspace = fs::canonicalize(workspace.as_path())
            .expect("workspace root should canonicalize for host access");
        let canonical_outside =
            fs::canonicalize(outside.as_path()).expect("outside root should canonicalize");
        let path_env = BTreeMap::from([("PALYRA_E2E_OS_ROOT".to_owned(), canonical_outside)]);
        let args = vec!["%PALYRA_E2E_OS_ROOT%/../secret.txt".to_owned()];

        let error = rewrite_host_access_process_args(
            args.as_slice(),
            canonical_workspace.as_path(),
            &path_env,
        )
        .expect_err("safe path env suffix traversal must be denied");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("suffix must stay relative"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
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
    fn validate_argument_scopes_allow_windows_find_text_search_switches() {
        let workspace = unique_temp_dir("workspace-find-switches");
        fs::create_dir_all(workspace.join("data")).expect("workspace directory should be created");
        fs::write(workspace.join("data").join("events.jsonl"), "{}\n")
            .expect("fixture file should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args =
            vec!["/c".to_owned(), "/v".to_owned(), String::new(), "data\\events.jsonl".to_owned()];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "find",
            args.as_slice(),
        )
        .expect("Windows find switches should not be treated as absolute paths");
        validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "find",
            args.as_slice(),
        )
        .expect("host access should also treat Windows find switches as non-path switches");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn validate_argument_scopes_allow_icacls_help_switch() {
        let workspace = unique_temp_dir("workspace-icacls-help");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["/?".to_owned()];

        validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "icacls",
            args.as_slice(),
        )
        .expect("icacls help switch should not be treated as an absolute path");
        validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "icacls",
            args.as_slice(),
        )
        .expect("host access should also treat icacls help as a switch");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn validate_host_argument_scope_allows_icacls_grant_principal() {
        let workspace = unique_temp_dir("workspace-icacls-grant");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let target = workspace.join("palyra-e2e-helper.exe");
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
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        let error = validate_no_embedded_command_line_arg(&input)
            .expect_err("single command-line args should be rejected before execution");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("split each argument"), "{}", error.message);
    }

    #[test]
    fn process_runner_command_path_hint_does_not_split_program_files_paths() {
        let message =
            process_runner_command_with_args_message(r"C:\Program Files\Git\bin\bash.exe -lc");

        assert!(message.contains("exact executable path"), "{message}");
        assert!(message.contains("put executable arguments in args"), "{message}");
        assert!(
            !message.contains("command=\"C:\\Program\""),
            "hint must not split a Program Files path: {message}"
        );
    }

    #[test]
    #[cfg(windows)]
    fn validate_host_command_scope_allows_executable_path_with_spaces() {
        let workspace = unique_temp_dir("workspace-command-path-spaces");
        let tools = workspace.join("tools with spaces");
        fs::create_dir_all(tools.as_path()).expect("tools directory should be created");
        let executable = tools.join("palyra-helper.exe");
        fs::write(executable.as_path(), "fake exe").expect("executable fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let policy = host_access_policy(canonical_workspace.clone());
        let input = ProcessRunnerInput {
            command: executable.display().to_string(),
            args: vec!["--version".to_owned()],
            cwd: None,
            env: BTreeMap::new(),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        validate_input_shape(&input)
            .expect("existing executable path with spaces should not look like embedded args");
        validate_allowed_executable(&policy, input.command.as_str())
            .expect("wildcard host policy should allow executable path by basename");
        validate_host_command_path_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            input.command.as_str(),
        )
        .expect("host command executable path should be validated separately from args");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn resolve_host_executable_allows_workspace_relative_command_with_cwd() {
        let workspace = unique_temp_dir("workspace-relative-command-cwd");
        let executable = workspace.join("repo").join(".venv").join("Scripts").join("python.exe");
        fs::create_dir_all(executable.parent().expect("executable should have parent"))
            .expect("venv scripts directory should be created");
        fs::write(executable.as_path(), "fake exe").expect("executable fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let cwd = fs::canonicalize(canonical_workspace.join("repo"))
            .expect("repo cwd should canonicalize");
        let expected =
            fs::canonicalize(executable.as_path()).expect("executable should canonicalize");

        let resolved = resolve_host_executable_path_with_roots(
            canonical_workspace.as_path(),
            cwd.as_path(),
            "repo/.venv/Scripts/python.exe",
            &[],
        )
        .expect("workspace-relative executable should resolve from workspace root when cwd lookup misses");

        assert_eq!(resolved, expected);
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn resolve_host_executable_allows_parent_dir_inside_workspace() {
        let workspace = unique_temp_dir("workspace-parent-command");
        let executable = workspace.join(".venv").join("Scripts").join("python.exe");
        fs::create_dir_all(executable.parent().expect("executable should have parent"))
            .expect("venv scripts directory should be created");
        fs::write(executable.as_path(), "fake exe").expect("executable fixture should be written");
        fs::create_dir_all(workspace.join("repo")).expect("repo directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let cwd = fs::canonicalize(canonical_workspace.join("repo"))
            .expect("repo cwd should canonicalize");
        let expected =
            fs::canonicalize(executable.as_path()).expect("executable should canonicalize");

        let resolved = resolve_host_executable_path_with_roots(
            canonical_workspace.as_path(),
            cwd.as_path(),
            "../.venv/Scripts/python.exe",
            &[],
        )
        .expect("parent-dir executable should be allowed when it remains inside workspace");

        assert_eq!(resolved, expected);
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn resolve_host_executable_rejects_parent_dir_outside_workspace() {
        let root = unique_temp_dir("workspace-parent-command-outside");
        let workspace = root.join("workspace");
        let outside = root.join("outside");
        let executable = outside.join("tool.exe");
        fs::create_dir_all(workspace.join("repo")).expect("repo directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        fs::write(executable.as_path(), "fake exe").expect("outside executable should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let cwd = fs::canonicalize(canonical_workspace.join("repo"))
            .expect("repo cwd should canonicalize");

        let error = resolve_host_executable_path_with_roots(
            canonical_workspace.as_path(),
            cwd.as_path(),
            "../../outside/tool.exe",
            &[],
        )
        .expect_err("parent-dir executable outside workspace should be denied");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(
            error.message.contains("outside workspace"),
            "error should explain traversal boundary: {}",
            error.message
        );
        let _ = fs::remove_dir_all(root.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn host_access_process_command_uses_resolved_workspace_command_path_with_cwd() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-relative-command-spawn");
        let executable = workspace.join("repo").join(".venv").join("Scripts").join("python.exe");
        fs::create_dir_all(executable.parent().expect("executable should have parent"))
            .expect("venv scripts directory should be created");
        fs::write(executable.as_path(), "fake exe").expect("executable fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let policy = host_access_policy(canonical_workspace.clone());
        let input = ProcessRunnerInput {
            command: "repo/.venv/Scripts/python.exe".to_owned(),
            args: vec!["--version".to_owned()],
            cwd: Some("repo".to_owned()),
            env: BTreeMap::new(),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };
        let cwd =
            resolve_host_working_directory(canonical_workspace.as_path(), input.cwd.as_deref())
                .expect("host cwd should resolve");
        let expected =
            fs::canonicalize(executable.as_path()).expect("executable should canonicalize");

        let command =
            build_process_command(&policy, &input, canonical_workspace.as_path(), cwd.as_path())
                .expect("host-access command should build with resolved executable path");

        assert_eq!(PathBuf::from(command.get_program()), expected);
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn windows_program_files_paths_are_allowed_for_command_executables() {
        assert!(windows_program_files_path(Path::new(r"C:\Program Files\Git\bin\bash.exe")));
        assert!(windows_program_files_path(Path::new(r"C:\Program Files (x86)\Example\tool.exe")));
    }

    #[test]
    fn process_runner_env_overrides_accept_fixture_keys_and_reject_reserved_runtime_keys() {
        let mut env = BTreeMap::new();
        env.insert("PALYRA_E2E_HOME".to_owned(), "/tmp/palyra-e2e-home".to_owned());
        validate_process_env_overrides(&env).expect("fixture env keys should be accepted");

        for reserved_key in [
            "PATH",
            "HTTPS_PROXY",
            "HOME",
            "AWS_SHARED_CREDENTIALS_FILE",
            "npm_config_registry",
            "node_disable_compile_cache",
            "LD_AUDIT",
            "ld_debug_output",
            "DYLD_FRAMEWORK_PATH",
        ] {
            let mut env = BTreeMap::new();
            env.insert(reserved_key.to_owned(), "https://blocked.example".to_owned());
            let error = validate_process_env_overrides(&env)
                .expect_err("sensitive env overrides must be reserved");
            assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
            assert!(error.message.contains("reserved by the runtime"), "{}", error.message);
        }
    }

    #[test]
    fn process_runner_prepend_path_shape_is_bounded() {
        let too_many = vec!["tools/bin".to_owned(); MAX_PREPEND_PATH_COUNT + 1];
        let error = validate_process_prepend_path_shape(too_many.as_slice())
            .expect_err("oversized prepend_path should be rejected");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);

        let error = validate_process_prepend_path_shape(&[" tools/bin".to_owned()])
            .expect_err("whitespace-padded prepend_path should be rejected");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
    }

    #[test]
    fn process_runner_notification_request_projects_watch_events() {
        let mut input = process_runner_input("npm", &["run", "dev"], None);
        input.background = true;
        input.notify_on_complete = true;
        input.watch_patterns.push(palyra_common::process_runner_input::ProcessWatchPattern {
            name: "ready".to_owned(),
            pattern: "Local:".to_owned(),
            stream: ProcessWatchStream::Stdout,
            notify_once: true,
        });
        input.env_profile_id = Some("web-dev".to_owned());

        let notification = process_completion_notification_projection(
            Some(&input),
            ProcessCompletionState::Subscribed,
            Some(42),
            "VITE ready\nLocal: http://127.0.0.1:5173\n",
            "",
        );

        assert_eq!(
            notification.pointer("/completion/state").and_then(Value::as_str),
            Some("subscribed")
        );
        assert_eq!(
            notification.pointer("/watch/events/0/name").and_then(Value::as_str),
            Some("ready")
        );
        assert!(
            notification
                .pointer("/watch/events/0/pattern_sha256")
                .and_then(Value::as_str)
                .is_some_and(|value| value.len() == 64),
            "watch projection must carry only a pattern hash: {notification}"
        );
        let runtime_request = process_runtime_request_projection(Some(&input));
        assert_eq!(
            runtime_request.pointer("/env_profile/daemon_env_inherited").and_then(Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn process_watch_pattern_spam_degrades_to_completion_only() {
        let mut input = process_runner_input("node", &["server.js"], None);
        input.watch_patterns.push(palyra_common::process_runner_input::ProcessWatchPattern {
            name: "spam".to_owned(),
            pattern: "ready".to_owned(),
            stream: ProcessWatchStream::Both,
            notify_once: true,
        });
        let stdout = "ready\n".repeat(32);

        let watch = process_watch_events_projection(&input, stdout.as_str(), "");

        assert_eq!(watch.get("state").and_then(Value::as_str), Some("degraded_completion_only"));
        assert_eq!(watch.get("rate_limited").and_then(Value::as_bool), Some(true));
        assert_eq!(watch.get("events").and_then(Value::as_array).map(Vec::len), Some(0));
    }

    #[test]
    fn process_runner_input_rejects_implicit_elevated_intent() {
        let input = process_runner_input("sudo", &["id"], None);
        let error =
            validate_input_shape(&input).expect_err("sudo must require explicit elevated_intent");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("elevated_intent=true"), "{}", error.message);
    }

    #[test]
    fn process_runner_watch_pattern_limits_are_enforced() {
        let mut input = process_runner_input("node", &["server.js"], None);
        input.watch_patterns = (0..=MAX_WATCH_PATTERNS)
            .map(|index| palyra_common::process_runner_input::ProcessWatchPattern {
                name: format!("ready{index}"),
                pattern: "ready".to_owned(),
                stream: ProcessWatchStream::Both,
                notify_once: true,
            })
            .collect();

        let error = validate_input_shape(&input)
            .expect_err("too many watch patterns should fail before spawn");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("watch_patterns supports at most"), "{}", error.message);
    }

    #[test]
    fn process_runner_facade_mapping_is_validated_and_projected() {
        let mut input = process_runner_input("pwd", &[], None);
        input.facade_mapping =
            Some(palyra_common::process_runner_input::ProcessRunnerFacadeMapping {
                original_tool_name: "palyra.exec.run".to_owned(),
                canonical_tool_name: "palyra.process.run".to_owned(),
            });

        validate_input_shape(&input).expect("canonical facade mapping should be accepted");
        let projection = process_runtime_request_projection(Some(&input));

        assert_eq!(
            projection.pointer("/facade_mapping/original_tool_name").and_then(Value::as_str),
            Some("palyra.exec.run")
        );
        assert_eq!(
            projection.pointer("/facade_mapping/canonical_tool_name").and_then(Value::as_str),
            Some("palyra.process.run")
        );

        input.facade_mapping.as_mut().expect("facade mapping should exist").canonical_tool_name =
            "palyra.tool_program.run".to_owned();
        let error = validate_input_shape(&input)
            .expect_err("facade mapping must not target a different execution path");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("facade_mapping"), "{}", error.message);
    }

    #[test]
    fn tier_c_prepend_path_updates_inner_path_only() {
        let workspace = unique_temp_dir("workspace-tier-c-inner-prepend-path");
        let toolchain_bin = workspace.join("toolchain").join("bin");
        let trusted_bin = workspace.join("trusted").join("bin");
        fs::create_dir_all(toolchain_bin.as_path()).expect("toolchain bin should exist");
        fs::create_dir_all(trusted_bin.as_path()).expect("trusted bin should exist");
        let canonical_toolchain_bin =
            fs::canonicalize(toolchain_bin.as_path()).expect("toolchain bin should canonicalize");
        let canonical_trusted_bin =
            fs::canonicalize(trusted_bin.as_path()).expect("trusted bin should canonicalize");
        let original_path = std::env::join_paths([canonical_trusted_bin.as_path()])
            .expect("test PATH should join")
            .to_string_lossy()
            .into_owned();
        let mut plan = TierCCommandPlan {
            backend: palyra_sandbox::TierCBackendKind::LinuxBubblewrap,
            program: "bwrap".to_owned(),
            args: vec![
                "--clearenv".to_owned(),
                "--setenv".to_owned(),
                "PATH".to_owned(),
                original_path,
                "--".to_owned(),
                "node".to_owned(),
            ],
        };

        apply_tier_c_inner_path_prepend(&mut plan, std::slice::from_ref(&canonical_toolchain_bin))
            .expect("tier C inner PATH should accept validated prepend_path entries");
        let path_index =
            tier_c_plan_inner_path_index(plan.args.as_slice()).expect("PATH setenv should remain");
        let entries =
            std::env::split_paths(&OsString::from(&plan.args[path_index])).collect::<Vec<_>>();

        assert_eq!(plan.program, "bwrap");
        assert!(
            entries.first().is_some_and(|entry| {
                same_path_case_aware(entry.as_path(), canonical_toolchain_bin.as_path())
            }),
            "prepend_path entry should lead the inner sandbox PATH: {entries:?}"
        );
        assert!(
            entries.iter().any(|entry| {
                same_path_case_aware(entry.as_path(), canonical_trusted_bin.as_path())
            }),
            "original trusted PATH entry should be preserved: {entries:?}"
        );
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn sandbox_prepend_path_must_stay_inside_workspace() {
        let workspace = unique_temp_dir("workspace-prepend-path-sandbox");
        let outside = unique_temp_dir("outside-prepend-path-sandbox");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let policy = sandbox_policy_with_allowed_executables(
            canonical_workspace.clone(),
            vec!["node".to_owned()],
        );
        let input = ProcessRunnerInput {
            command: "node".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            prepend_path: vec![outside.to_string_lossy().into_owned()],
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        let error = build_process_command(
            &policy,
            &input,
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
        )
        .expect_err("sandbox prepend_path outside workspace should be denied");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_access_process_environment_prepends_validated_path() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-host-prepend-path");
        let toolchain_bin = workspace.join("toolchain").join("bin");
        fs::create_dir_all(toolchain_bin.as_path()).expect("toolchain bin should exist");
        let helper_name = if cfg!(windows) { "palyra-helper.exe" } else { "palyra-helper" };
        let helper = toolchain_bin.join(helper_name);
        fs::write(helper.as_path(), "fake exe")
            .expect("helper executable fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let canonical_toolchain_bin =
            fs::canonicalize(toolchain_bin.as_path()).expect("toolchain bin should canonicalize");
        let policy = host_access_policy(canonical_workspace.clone());
        let input = ProcessRunnerInput {
            command: helper.to_string_lossy().into_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            prepend_path: vec![toolchain_bin.to_string_lossy().into_owned()],
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        let command = build_process_command(
            &policy,
            &input,
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
        )
        .expect("host access command should build with prepend_path");
        let path = command_env_value_os(&command, "PATH").expect("child PATH should be set");
        let entries = std::env::split_paths(&path).collect::<Vec<_>>();

        assert!(
            entries.first().is_some_and(|entry| {
                same_path_case_aware(entry.as_path(), canonical_toolchain_bin.as_path())
            }),
            "prepend_path entry should lead child PATH: {entries:?}"
        );
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_access_prepend_path_does_not_resolve_selected_bare_command() {
        let workspace = unique_temp_dir("workspace-host-prepend-command-shadow");
        let toolchain_bin = workspace.join("toolchain").join("bin");
        fs::create_dir_all(toolchain_bin.as_path()).expect("toolchain bin should exist");
        let command_name = "palyra-prepend-only-tool";
        let executable_name =
            if cfg!(windows) { format!("{command_name}.exe") } else { command_name.to_owned() };
        let shadow = toolchain_bin.join(executable_name);
        fs::write(shadow.as_path(), "fake exe")
            .expect("shadow executable fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let policy = host_access_policy(canonical_workspace.clone());
        let input = ProcessRunnerInput {
            command: command_name.to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            prepend_path: vec![toolchain_bin.to_string_lossy().into_owned()],
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        let error = build_process_command(
            &policy,
            &input,
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
        )
        .expect_err("prepend_path must not select the executable spawned by the daemon");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(
            error.message.contains("prepend_path only affects the child environment"),
            "{}",
            error.message
        );
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_access_process_environment_drops_runtime_auth_and_profile_env() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-host-env");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let state_root = unique_temp_dir("state-host-env");
        fs::create_dir_all(state_root.as_path()).expect("state root should be created");
        let e2e_home = workspace.join("fixture-home");
        let e2e_os_root = workspace.join("fixture-os-root");
        {
            let _admin_token = ScopedEnvVar::set("PALYRA_ADMIN_TOKEN", "admin-secret");
            let _browser_token =
                ScopedEnvVar::set("PALYRA_BROWSER_SERVICE_AUTH_TOKEN", "browser-secret");
            let _model_key =
                ScopedEnvVar::set("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY", "provider-secret");
            let _cli_profile = ScopedEnvVar::set("PALYRA_CLI_PROFILE", "desktop-local");
            let _cli_profiles_path =
                ScopedEnvVar::set("PALYRA_CLI_PROFILES_PATH", workspace.join("profiles.toml"));
            let _state_root = ScopedEnvVar::set("PALYRA_STATE_ROOT", state_root.as_os_str());
            let _e2e_home = ScopedEnvVar::set("PALYRA_E2E_HOME", e2e_home.as_os_str());
            let _e2e_os_root = ScopedEnvVar::set("PALYRA_E2E_OS_ROOT", e2e_os_root.as_os_str());
            let policy = host_access_policy(workspace.clone());
            let input = ProcessRunnerInput {
                command: "palyra-helper".to_owned(),
                args: Vec::new(),
                cwd: None,
                env: BTreeMap::new(),
                prepend_path: Vec::new(),
                requested_egress_hosts: Vec::new(),
                timeout_ms: None,
                background: false,
                interactive: false,
                stdin: false,
                pty: false,
                port_hints: Vec::new(),
                lifetime_mode: BackgroundLifetimeMode::RunOwned,
                keep_running_after_run: false,
                notify_on_complete: false,
                watch_patterns: Vec::new(),
                env_profile_id: None,
                elevated_intent: false,
                facade_mapping: None,
            };

            let command =
                build_process_command(&policy, &input, workspace.as_path(), workspace.as_path())
                    .expect("host access command should build");
            let env = command
                .get_envs()
                .map(|(key, value)| {
                    (
                        key.to_string_lossy().into_owned(),
                        value.map(|value| value.to_string_lossy().into_owned()),
                    )
                })
                .collect::<BTreeMap<_, _>>();

            for key in [
                "PALYRA_ADMIN_TOKEN",
                "PALYRA_BROWSER_SERVICE_AUTH_TOKEN",
                "PALYRA_MODEL_PROVIDER_OPENAI_API_KEY",
                "PALYRA_CLI_PROFILE",
                "PALYRA_CLI_PROFILES_PATH",
                "PALYRA_STATE_ROOT",
            ] {
                assert!(
                    !env.contains_key(key),
                    "host-access process must not inherit runtime env key {key}"
                );
            }
            assert_eq!(
                env.get("PALYRA_E2E_HOME").and_then(Option::as_deref),
                Some(e2e_home.to_string_lossy().as_ref())
            );
            assert_eq!(
                env.get("PALYRA_E2E_OS_ROOT").and_then(Option::as_deref),
                Some(e2e_os_root.to_string_lossy().as_ref())
            );
            assert_eq!(
                env.get(NODE_DISABLE_COMPILE_CACHE_ENV).and_then(Option::as_deref),
                Some("1")
            );
            assert!(env.contains_key("PATH"), "host-access process should keep a usable PATH");
        }

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(state_root.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn host_access_wsl_bash_bridges_safe_path_env_through_wslenv() {
        let workspace = unique_temp_dir("workspace-host-wsl-env");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let e2e_home = workspace.join("fixture-home");
        let e2e_os_root = workspace.join("fixture-os-root");
        fs::create_dir_all(e2e_home.as_path()).expect("fixture home should exist");
        fs::create_dir_all(e2e_os_root.as_path()).expect("fixture OS root should exist");
        let policy = host_access_policy(workspace.clone());
        let input = ProcessRunnerInput {
            command: "bash".to_owned(),
            args: vec!["scripts/show-env.sh".to_owned()],
            cwd: None,
            env: BTreeMap::from([
                ("PALYRA_E2E_HOME".to_owned(), e2e_home.to_string_lossy().into_owned()),
                ("PALYRA_E2E_OS_ROOT".to_owned(), e2e_os_root.to_string_lossy().into_owned()),
            ]),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        let command =
            build_process_command(&policy, &input, workspace.as_path(), workspace.as_path())
                .expect("host access bash command should build");
        let env = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let wslenv = env
            .get("WSLENV")
            .and_then(Option::as_deref)
            .expect("WSL path env bridge should be configured for bash");

        assert_eq!(
            env.get("PALYRA_E2E_HOME").and_then(Option::as_deref),
            Some(e2e_home.to_string_lossy().as_ref())
        );
        assert!(wslenv.split(':').any(|entry| entry == "PALYRA_E2E_HOME/p"), "{wslenv}");
        assert!(wslenv.split(':').any(|entry| entry == "PALYRA_E2E_OS_ROOT/p"), "{wslenv}");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_access_path_policy_uses_configured_os_file_roots_without_user_or_env_roots() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-host-configured-roots");
        let configured_root = unique_temp_dir("configured-host-root");
        let real_profile_root = std::env::current_dir()
            .expect("current dir should resolve")
            .join("target")
            .join(format!(
                "palyra-real-profile-host-root-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time should be after unix epoch")
                    .as_nanos()
            ));
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(configured_root.as_path()).expect("configured root should be created");
        fs::create_dir_all(real_profile_root.as_path()).expect("profile root should be created");
        let configured_env =
            std::env::join_paths([configured_root.as_os_str()]).expect("root path should join");
        let _configured_roots = ScopedEnvVar::set(PALYRA_OS_FILE_ROOTS_ENV, configured_env);
        let _userprofile = ScopedEnvVar::set("USERPROFILE", real_profile_root.as_os_str());
        let _home = ScopedEnvVar::set("HOME", real_profile_root.as_os_str());
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let allowed_target = configured_root.join("Desktop").join("orders.csv");
        let implicit_target = real_profile_root.join("Desktop").join("orders.csv");

        validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "pwsh",
            &[allowed_target.display().to_string()],
        )
        .expect("configured PALYRA_OS_FILE_ROOTS root should be allowed for host access paths");
        let implicit_error = validate_host_argument_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "pwsh",
            &[implicit_target.display().to_string()],
        )
        .expect_err("configured PALYRA_OS_FILE_ROOTS should replace implicit user roots");
        assert_eq!(implicit_error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let env_root = std::env::current_dir()
            .expect("current dir should resolve")
            .join("target")
            .join(format!(
                "palyra-safe-env-host-root-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system time should be after unix epoch")
                    .as_nanos()
            ));
        fs::create_dir_all(env_root.as_path()).expect("safe env root should be created");
        let env = BTreeMap::from([(
            "PALYRA_E2E_OS_ROOT".to_owned(),
            env_root.to_string_lossy().into_owned(),
        )]);
        validate_process_env_overrides(&env)
            .expect("fixture env key should remain valid child env");
        let host_roots = host_access_roots();
        let env_target = env_root.join("provider.toml");
        let env_error = validate_host_argument_scope_with_roots(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "pwsh",
            &[env_target.display().to_string()],
            host_roots.as_slice(),
        )
        .expect_err("per-call env roots must not authorize host access paths");
        assert_eq!(env_error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(configured_root.as_path());
        let _ = fs::remove_dir_all(real_profile_root.as_path());
        let _ = fs::remove_dir_all(env_root.as_path());
    }

    #[test]
    fn sandboxed_process_environment_disables_node_compile_cache() {
        let workspace = unique_temp_dir("workspace-node-compile-cache-env");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let policy = sandbox_policy_with_allowed_executables(
            canonical_workspace.clone(),
            vec!["node".to_owned()],
        );
        let input = ProcessRunnerInput {
            command: "node".to_owned(),
            args: vec!["--version".to_owned()],
            cwd: None,
            env: BTreeMap::new(),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        let command = build_process_command(
            &policy,
            &input,
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
        )
        .expect("sandboxed node command should build");
        let env = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(env.get(NODE_DISABLE_COMPILE_CACHE_ENV).and_then(Option::as_deref), Some("1"));

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn host_access_process_command_rejects_bare_virtual_workspace_root_alias_args() {
        let workspace = unique_temp_dir("workspace-host-root-alias-build-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let helper_name = if cfg!(windows) { "palyra-helper.exe" } else { "palyra-helper" };
        let helper = workspace.join(helper_name);
        fs::write(helper.as_path(), "fake exe")
            .expect("helper executable fixture should be written");
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let policy = host_access_policy(canonical_workspace.clone());
        let input = ProcessRunnerInput {
            command: helper.to_string_lossy().into_owned(),
            args: vec!["--root=/".to_owned()],
            cwd: None,
            env: BTreeMap::new(),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        let error = build_process_command(
            &policy,
            &input,
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
        )
        .expect_err("host-access command builder must reject bare root aliases");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("bare workspace-root alias"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
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
    fn run_constrained_process_rejects_non_allowlisted_egress_host_from_env_value() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy = sandbox_policy(workspace);
        policy.allowed_egress_hosts = vec!["allowed.example".to_owned()];
        let input = br#"{"command":"uname","args":["--version","https://allowed.example/path"],"env":{"APP_ENDPOINT":"https://blocked.example/api"}}"#;
        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("env URL values should be validated against egress allowlists");
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
    fn run_constrained_process_rejects_unregistered_portable_lifecycle_pid() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy_with_allowed_executables(
            workspace,
            vec!["palyra.process.stop".to_owned(), "palyra.process.status".to_owned()],
        );
        let unregistered_pid = (u32::MAX - 17).to_string();

        for command in ["palyra.process.status", "palyra.process.stop"] {
            let input = serde_json::to_vec(&serde_json::json!({
                "command": command,
                "args": [unregistered_pid],
            }))
            .expect("lifecycle input should serialize");
            let error =
                run_constrained_process(&policy, input.as_slice(), Duration::from_millis(1_000))
                    .expect_err("unregistered host pid must not be accepted");

            assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
            assert!(error.message.contains("not registered"), "{}", error.message);
        }
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
    fn windows_tier_b_environment_ignores_host_profile_and_command_resolution_values() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let _host_values = [
            ScopedEnvVar::set("APPDATA", r"C:\HostProfile\AppData\Roaming"),
            ScopedEnvVar::set("LOCALAPPDATA", r"C:\HostProfile\AppData\Local"),
            ScopedEnvVar::set("USERPROFILE", r"C:\HostProfile"),
            ScopedEnvVar::set("VOLTA_HOME", r"C:\HostProfile\Volta"),
            ScopedEnvVar::set("COMSPEC", r"C:\HostProfile\bin\cmd.exe"),
            ScopedEnvVar::set("PATHEXT", ".EVIL"),
        ];
        let workspace = unique_temp_dir("workspace-tier-b-env");
        let state_root = unique_temp_dir("state-tier-b-env");
        let _state_root = ScopedEnvVar::set(super::PALYRA_STATE_ROOT_ENV, state_root.as_os_str());
        let policy = sandbox_policy_with_allowed_executables(workspace, vec!["node".to_owned()]);
        let mut command = Command::new("node");
        command.env_clear();

        super::configure_windows_tier_b_process_environment(
            &mut command,
            Path::new(r"C:\Tools\node.exe"),
            &policy,
        )
        .expect("tier-B environment should use trusted Windows runtime values");

        let env = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for key in ["APPDATA", "LOCALAPPDATA", "USERPROFILE", "VOLTA_HOME"] {
            assert!(!env.contains_key(key), "{key} must not be inherited");
        }
        assert_ne!(
            env.get("COMSPEC").and_then(Option::as_deref),
            Some(r"C:\HostProfile\bin\cmd.exe")
        );
        assert_eq!(env.get("PATHEXT").and_then(Option::as_deref), Some(".com;.exe;.bat;.cmd"));
    }

    #[test]
    #[cfg(windows)]
    fn windows_tier_b_path_contains_only_system_and_selected_program_directories() {
        let path = super::windows_tier_b_process_path(
            Path::new(r"C:\Tools\Node\node.exe"),
            Path::new(r"C:\Windows\System32"),
        );
        let directories =
            std::env::split_paths(std::ffi::OsStr::new(path.as_str())).collect::<Vec<_>>();

        assert_eq!(
            directories,
            vec![
                PathBuf::from(r"C:\Windows\System32"),
                PathBuf::from(r"C:\Windows"),
                PathBuf::from(r"C:\Windows\System32\WindowsPowerShell\v1.0"),
                PathBuf::from(r"C:\Tools\Node"),
            ]
        );
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
    fn windows_restrictive_resolution_prefers_trusted_path_over_cwd_shim() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let _pathext = ScopedEnvVar::set("PATHEXT", ".EXE;.CMD");
        let workspace = unique_temp_dir("workspace-command-shim");
        let trusted_bin = unique_temp_dir("trusted-command-bin");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(trusted_bin.as_path()).expect("trusted bin should be created");
        fs::write(workspace.join("git.CMD"), b"@echo off\r\necho shim\r\n")
            .expect("workspace shim should be written");
        fs::write(trusted_bin.join("git.EXE"), b"trusted executable placeholder")
            .expect("trusted executable placeholder should be written");
        let trusted_path =
            std::env::join_paths([trusted_bin.as_path()]).expect("trusted PATH should join");

        let resolved = super::resolve_tier_b_process_program(
            "git",
            workspace.as_path(),
            trusted_path.as_os_str(),
            false,
            false,
        )
        .expect("restrictive resolution should pick the trusted executable");
        let expected =
            fs::canonicalize(trusted_bin.join("git.EXE")).expect("trusted executable should exist");

        assert!(
            same_path_case_aware(resolved.as_path(), expected.as_path()),
            "resolved={} expected={}",
            resolved.display(),
            expected.display()
        );
        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(trusted_bin.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn windows_restrictive_resolution_denies_cwd_shim_without_trusted_path() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let _pathext = ScopedEnvVar::set("PATHEXT", ".EXE;.CMD");
        let workspace = unique_temp_dir("workspace-command-shim-denied");
        let trusted_bin = unique_temp_dir("empty-trusted-command-bin");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(trusted_bin.as_path()).expect("trusted bin should be created");
        fs::write(workspace.join("git.CMD"), b"@echo off\r\necho shim\r\n")
            .expect("workspace shim should be written");
        let trusted_path =
            std::env::join_paths([trusted_bin.as_path()]).expect("trusted PATH should join");

        let error = super::resolve_tier_b_process_program(
            "git",
            workspace.as_path(),
            trusted_path.as_os_str(),
            false,
            false,
        )
        .expect_err("restrictive resolution should reject current-directory shims");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("current-directory shim"), "{}", error.message);
        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(trusted_bin.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn windows_wildcard_resolution_allows_cwd_shim() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let _pathext = ScopedEnvVar::set("PATHEXT", ".EXE;.CMD");
        let workspace = unique_temp_dir("workspace-command-shim-wildcard");
        let trusted_bin = unique_temp_dir("wildcard-empty-trusted-command-bin");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(trusted_bin.as_path()).expect("trusted bin should be created");
        fs::write(workspace.join("git.CMD"), b"@echo off\r\necho shim\r\n")
            .expect("workspace shim should be written");
        let trusted_path =
            std::env::join_paths([trusted_bin.as_path()]).expect("trusted PATH should join");

        let resolved = super::resolve_tier_b_process_program(
            "git",
            workspace.as_path(),
            trusted_path.as_os_str(),
            false,
            true,
        )
        .expect("wildcard resolution should preserve cwd shims");
        let expected =
            fs::canonicalize(workspace.join("git.CMD")).expect("workspace shim should exist");

        assert!(
            same_path_case_aware(resolved.as_path(), expected.as_path()),
            "resolved={} expected={}",
            resolved.display(),
            expected.display()
        );
        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(trusted_bin.as_path());
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
    fn windows_process_commands_preserve_validated_verbatim_cwd() {
        for (program, cwd) in [
            (r"C:\Tools\tool.exe", r"\\?\C:\Users\test-user\fixture"),
            (r"C:\Tools\tool.cmd", r"\\?\UNC\server\share\fixture"),
        ] {
            let command = super::build_windows_tier_b_process_command(
                Path::new(program),
                &[],
                Path::new(cwd),
            )
            .expect("Windows process command should be built");

            assert_eq!(command.get_current_dir(), Some(Path::new(cwd)));
        }
    }

    #[test]
    #[cfg(windows)]
    fn windows_tier_b_process_environment_uses_runtime_temp_dirs() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-tier-b-temp");
        let verbatim_workspace = PathBuf::from(format!(r"\\?\{}", workspace.to_string_lossy()));
        let state_root = unique_temp_dir("state-tier-b-temp");
        let _state_root = ScopedEnvVar::set(super::PALYRA_STATE_ROOT_ENV, state_root.as_os_str());
        let policy =
            sandbox_policy_with_allowed_executables(verbatim_workspace, vec!["node".to_owned()]);
        let mut command = Command::new("node");

        super::configure_windows_tier_b_process_environment(
            &mut command,
            Path::new(r"C:\Tools\node.exe"),
            &policy,
        )
        .expect("tier-B environment should configure temp directories");

        let env = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.map(|value| value.to_string_lossy().into_owned()),
                )
            })
            .collect::<BTreeMap<_, _>>();

        let expected_root = super::join_relative_components(
            state_root.as_path(),
            super::PROCESS_RUNNER_TEMP_RELATIVE_PATH,
        );
        let temp = env
            .get("TEMP")
            .and_then(Option::as_deref)
            .map(PathBuf::from)
            .expect("TEMP should be set");
        let tmp = env
            .get("TMP")
            .and_then(Option::as_deref)
            .map(PathBuf::from)
            .expect("TMP should be set");
        assert!(temp.starts_with(expected_root.as_path()), "TEMP={}", temp.display());
        assert!(tmp.starts_with(expected_root.as_path()), "TMP={}", tmp.display());
        assert!(!temp.starts_with(workspace.as_path()), "TEMP must stay outside workspace");
        assert!(!tmp.starts_with(workspace.as_path()), "TMP must stay outside workspace");
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

        // A loaded Windows runner can spend multiple seconds starting cmd.exe and its wrapper.
        // Use the suite's bounded process budget so scheduler delay is not mistaken for a hang.
        let result = run_constrained_process(&policy, input, background_test_execution_timeout())
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
    #[cfg(windows)]
    fn host_access_process_runner_resolves_virtual_workspace_cwd_for_workspace_script() {
        let workspace = unique_temp_dir("workspace-script-verbatim");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("marker.txt"), b"workspace marker\n")
            .expect("workspace marker should be written");
        fs::write(
            workspace.join("verify-helper.cmd"),
            b"@echo off\r\nif not exist marker.txt exit /b 21\r\necho S046_HELPER_OK:%~1\r\n",
        )
        .expect("workspace helper script should be written");
        let verbatim_workspace = PathBuf::from(format!(r"\\?\{}", workspace.to_string_lossy()));
        for input in [
            br#"{"command":"verify-helper.cmd","args":["default"],"timeout_ms":5000}"#.as_slice(),
            br#"{"command":"verify-helper.cmd","args":["alias"],"cwd":"/workspace","timeout_ms":5000}"#
                .as_slice(),
        ] {
            let policy = host_access_policy(verbatim_workspace.clone());
            let result = run_constrained_process(&policy, input, Duration::from_millis(10_000))
                .expect("host-access process runner should resolve cwd before command dispatch");
            let output: serde_json::Value =
                serde_json::from_slice(&result.output_json).expect("output should parse");
            let stdout =
                output.get("stdout").and_then(serde_json::Value::as_str).unwrap_or_default();
            let stderr =
                output.get("stderr").and_then(serde_json::Value::as_str).unwrap_or_default();

            assert!(
                stdout.contains("S046_HELPER_OK") || stderr.contains("S046_HELPER_OK"),
                "workspace script should run from the resolved cwd for input={}, stdout={stdout:?}, stderr={stderr:?}",
                String::from_utf8_lossy(input),
            );
        }

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn workspace_python_environment_keeps_userbase_and_cache_out_of_workspace() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-python-env");
        let state_root = unique_temp_dir("state-python-env");
        let _state_root = ScopedEnvVar::set(super::PALYRA_STATE_ROOT_ENV, state_root.as_os_str());

        let environment = super::workspace_python_environment("python", workspace.as_path())
            .expect("python environment should resolve under state root")
            .expect("python commands should receive workspace-local Python environment");
        let next_environment = super::workspace_python_environment("python", workspace.as_path())
            .expect("second Python environment should resolve under state root")
            .expect("Python commands should receive an isolated environment");

        let expected_root = super::join_relative_components(
            state_root.as_path(),
            super::PROCESS_RUNNER_PYTHON_ENV_RELATIVE_PATH,
        );
        assert!(environment.user_base.starts_with(expected_root.as_path()));
        assert!(environment.pip_cache.starts_with(expected_root.as_path()));
        assert!(next_environment.user_base.starts_with(expected_root.as_path()));
        assert!(!environment.user_base.starts_with(workspace.as_path()));
        assert!(!environment.pip_cache.starts_with(workspace.as_path()));
        assert_ne!(
            environment.user_base.parent(),
            next_environment.user_base.parent(),
            "Python userbase roots must not be reused across process runs"
        );
        assert!(
            environment.user_base.parent().is_some_and(Path::is_dir),
            "isolated Python environment root must exist before spawn"
        );
        assert_eq!(
            environment.user_base.file_name().and_then(|name| name.to_str()),
            Some(super::PYTHON_USER_BASE_DIR)
        );
        assert_eq!(
            environment.pip_cache.file_name().and_then(|name| name.to_str()),
            Some(super::PIP_CACHE_DIR)
        );
    }

    #[test]
    fn process_runner_child_temp_root_uses_state_root_outside_workspace() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-child-temp");
        let state_root = unique_temp_dir("state-child-temp");
        let _state_root = ScopedEnvVar::set(super::PALYRA_STATE_ROOT_ENV, state_root.as_os_str());

        let temp_root = super::process_runner_child_temp_root(workspace.as_path())
            .expect("child temp root should be created under state root");

        let expected_root = super::join_relative_components(
            state_root.as_path(),
            super::PROCESS_RUNNER_TEMP_RELATIVE_PATH,
        );
        assert!(
            temp_root.starts_with(expected_root.as_path()),
            "temp_root={}",
            temp_root.display()
        );
        assert!(!temp_root.starts_with(workspace.as_path()));
        assert!(temp_root.is_dir(), "temp root should exist before process spawn");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;

            assert_eq!(
                fs::metadata(temp_root.as_path())
                    .expect("temp root metadata should resolve")
                    .permissions()
                    .mode()
                    & 0o777,
                0o700,
                "process child temp root must be owner-only"
            );
        }
    }

    #[test]
    #[cfg(unix)]
    fn process_runner_child_temp_root_rejects_symlinked_components() {
        use std::os::unix::fs::symlink;

        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-child-temp-symlink");
        let state_root = unique_temp_dir("state-child-temp-symlink");
        let outside = unique_temp_dir("outside-child-temp-symlink");
        fs::create_dir_all(state_root.as_path()).expect("state root should exist");
        fs::create_dir_all(outside.as_path()).expect("outside directory should exist");
        symlink(outside.as_path(), state_root.join("process-runner"))
            .expect("hostile process-runner symlink should be created");
        let _state_root = ScopedEnvVar::set(super::PALYRA_STATE_ROOT_ENV, state_root.as_os_str());

        let error = super::process_runner_child_temp_root(workspace.as_path())
            .expect_err("symlinked temp-root components must fail closed");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::RuntimeFailure);
        assert!(error.message.contains("symbolic link or reparse point"), "{}", error.message);
        assert!(!outside.join("tmp").exists(), "symlink target must not be mutated");
    }

    #[test]
    fn process_runner_runtime_root_falls_back_to_default_state_root_not_shared_temp() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let default_root_base = unique_temp_dir("state-root-fallback-base");
        fs::create_dir_all(default_root_base.as_path()).expect("default state base should exist");
        let _state_root = ScopedEnvVar::set(super::PALYRA_STATE_ROOT_ENV, "");
        #[cfg(windows)]
        let _local_appdata = ScopedEnvVar::set("LOCALAPPDATA", default_root_base.as_os_str());
        #[cfg(not(windows))]
        let _xdg_state_home = ScopedEnvVar::set("XDG_STATE_HOME", default_root_base.as_os_str());

        let runtime_root =
            super::process_runner_runtime_root().expect("default state root should resolve");
        let shared_temp_fallback = std::env::temp_dir().join("palyra-process-runner");

        assert!(
            runtime_root.starts_with(default_root_base.as_path()),
            "runtime_root={}",
            runtime_root.display()
        );
        assert!(
            !runtime_root.starts_with(shared_temp_fallback.as_path()),
            "runtime root must not use predictable shared temp fallback: {}",
            runtime_root.display()
        );
        let _ = fs::remove_dir_all(default_root_base.as_path());
    }

    #[test]
    fn workspace_python_environment_covers_pip_and_versioned_python_commands() {
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let state_root = unique_temp_dir("state-python-command-detection");
        let _state_root = ScopedEnvVar::set(super::PALYRA_STATE_ROOT_ENV, state_root.as_os_str());

        for command in [
            "python",
            "python3",
            "python3.14",
            "py",
            "pip",
            "pip3",
            "pip3.11",
            "pip3.12",
            "pip3.14.exe",
        ] {
            assert!(
                super::workspace_python_environment(command, Path::new("workspace-root"))
                    .expect("python environment should resolve")
                    .is_some(),
                "{command} should be treated as a Python runtime command"
            );
        }
        for command in ["npm", "pipx", "pip3.local", "pip3.", "pip.11"] {
            assert!(
                super::workspace_python_environment(command, Path::new("workspace-root"))
                    .expect("non-Python command should not require state root")
                    .is_none(),
                "{command} should not receive Python-specific environment"
            );
        }
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
    fn run_constrained_process_output_surfaces_process_risk_metadata() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy_with_allowed_executables(workspace, vec!["echo".to_owned()]);
        let input = br#"{"command":"echo","args":["~/.ssh/id_ed25519"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect("portable echo builtin should execute split command args");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(
            output.pointer("/process_risk/policy").and_then(serde_json::Value::as_str),
            Some("advisory_only")
        );
        assert_eq!(
            output.pointer("/process_risk/blocks_execution").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            output
                .pointer("/process_risk/requires_user_approval")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            output
                .pointer("/process_risk/findings/0/risk_class")
                .and_then(serde_json::Value::as_str),
            Some("credential_namespace_mutation")
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
            "MINIMAX_API_KEY=sk-test-secret-value\nCLIENT_SECRET=abc/def.ghi\npublic_setting=true\n",
        );

        assert!(redacted.redacted, "{redacted:?}");
        assert!(redacted.text.contains("public_setting=true"), "{}", redacted.text);
        assert!(!redacted.text.contains("sk-test-secret-value"), "{}", redacted.text);
        assert!(!redacted.text.contains("abc/def.ghi"), "{}", redacted.text);
        assert!(redacted.text.contains("CLIENT_SECRET=[REDACTED_SECRET]"), "{}", redacted.text);
        assert!(redacted.text.contains("REDACTED"), "{}", redacted.text);
        assert!(
            redacted.redaction_reasons.iter().any(|reason| reason == "auth_or_assignment_secret"),
            "{redacted:?}"
        );
    }

    #[test]
    fn process_output_text_redacts_token_fixture_suffix() {
        let output = "fixture token=a%3Db%3Dc selector=#password\n";
        let redacted = redacted_process_output_text(output);

        assert!(redacted.redacted, "{redacted:?}");
        assert!(!redacted.text.contains("a%3Db%3Dc"), "{}", redacted.text);
        assert!(!redacted.text.contains("selector=#password"), "{}", redacted.text);
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
        assert!(
            output
                .pointer("/streams/stdout/redaction_reasons")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|reasons| {
                    reasons
                        .iter()
                        .any(|reason| reason.as_str() == Some("auth_or_assignment_secret"))
                }),
            "{output}"
        );

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
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-host-mkdir");
        let outside = unique_temp_dir("outside-host-mkdir");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let _userprofile = ScopedEnvVar::set("USERPROFILE", outside.as_os_str());
        let _home = ScopedEnvVar::set("HOME", outside.as_os_str());
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
        assert!(error.message.contains("outside workspace"), "{}", error.message);
        assert!(!target.exists(), "outside-workspace mkdir target must not be created");

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    #[test]
    fn unix_supervisor_registration_failure_never_execs_target() {
        let workspace = unique_temp_dir("workspace-unix-background-registration-fence");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let marker = workspace.join("user-code-executed.marker");
        let mut policy = host_access_policy(workspace.clone());
        policy.allowed_executables = vec!["touch".to_owned()];
        let input = serde_json::to_vec(&serde_json::json!({
            "command": "touch",
            "args": [marker.display().to_string()],
            "background": true,
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS
        }))
        .expect("input should serialize");
        let registration_fence: super::BackgroundProcessRegistrationFence = Arc::new(|_| {
            Err(SandboxProcessRunError {
                kind: SandboxProcessRunErrorKind::RuntimeFailure,
                message: "forced registration failure".to_owned(),
            })
        });

        let error = run_constrained_process_with_fault_injection(
            &policy,
            input.as_slice(),
            background_test_execution_timeout(),
            None,
            None,
            Some(registration_fence),
            crate::qa_fault_injection::QaFaultRuntime::default(),
        )
        .expect_err("Unix target release must stop when durable registration fails");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::RuntimeFailure);
        assert_eq!(error.message, "forced registration failure");
        assert!(!marker.exists(), "background target code must not run before registration");
        let _ = fs::remove_dir_all(workspace.as_path());
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
            output.get("requested_lifetime_ms").and_then(serde_json::Value::as_u64),
            Some(BACKGROUND_TEST_EXECUTION_TIMEOUT_MS)
        );
        assert_eq!(
            output.get("lifetime_ms").and_then(serde_json::Value::as_u64),
            Some(BACKGROUND_TEST_EXECUTION_TIMEOUT_MS)
        );
        assert_eq!(
            output.get("max_lifetime_ms").and_then(serde_json::Value::as_u64),
            Some(BACKGROUND_TEST_EXECUTION_TIMEOUT_MS)
        );
        assert_eq!(
            output.get("min_background_lifetime_ms").and_then(serde_json::Value::as_u64),
            Some(super::MIN_BACKGROUND_PROCESS_LIFETIME_MS)
        );
        assert_eq!(
            output.get("background_lifetime_adjusted").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            output.get("run_owned_lifetime").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert!(output
            .get("run_lifecycle_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("terminal state"));
        assert!(output
            .get("run_lifecycle_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("final answer"));
        assert!(output
            .get("background_lifetime_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("auto-terminate"));
        assert!(output
            .get("background_lifetime_note")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("terminal state"));
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
        #[cfg(windows)]
        assert!(output.pointer("/cleanup/manual_command/command").is_some());
        #[cfg(unix)]
        assert_eq!(output.pointer("/cleanup/manual_command"), Some(&serde_json::Value::Null));
        #[cfg(not(any(unix, windows)))]
        assert_eq!(
            output.pointer("/cleanup/manual_command/command").and_then(serde_json::Value::as_str),
            Some("kill")
        );
        let provenance: palyra_common::runtime_contracts::ProcessProvenance =
            serde_json::from_value(
                output
                    .pointer("/process_handle/provenance")
                    .cloned()
                    .expect("background handle should expose process provenance"),
            )
            .expect("background handle provenance should decode");
        provenance.validate().expect("background handle provenance should validate");
        assert!(!provenance.start_token.is_empty());

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn background_process_registry_tracks_input_capabilities() {
        let pid = 4_000_000_u32.saturating_add(std::process::id());
        let provenance = palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind: if cfg!(windows) {
                palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject
            } else {
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup
            },
            start_token: "test-start-token".to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: "test-owner-nonce".to_owned(),
            ownership_identity_sha256: "b".repeat(64),
        };
        let input = ProcessRunnerInput {
            command: "python".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::new(),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: true,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: true,
            stdin: false,
            pty: false,
            port_hints: vec![5173],
            lifetime_mode: BackgroundLifetimeMode::UntilVerifier,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };
        let capabilities = super::BackgroundProcessHandleCapabilities::from_input(&input);

        super::register_background_process_pid(
            pid,
            capabilities,
            input.effective_lifetime_mode(),
            provenance.clone(),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("registry should accept stdin-capable metadata without a pipe");
        let snapshot = super::registered_background_process("palyra.process.status", pid)
            .expect("registered process should be readable");
        super::mark_current_background_process_stopped(pid);

        assert!(snapshot.active);
        assert!(snapshot.capabilities.stdin);
        assert!(!snapshot.capabilities.pty_requested);
        assert!(!snapshot.capabilities.pty);
        assert!(snapshot.capabilities.signals);
        assert_eq!(snapshot.lifetime_mode, BackgroundLifetimeMode::UntilVerifier);
        assert_eq!(snapshot.provenance, provenance);
    }

    #[test]
    fn stopped_background_process_retains_bounded_output_until_history_release() {
        let pid = 4_010_000_u32.saturating_add(std::process::id());
        let provenance = palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind: if cfg!(windows) {
                palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject
            } else {
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup
            },
            start_token: "captured-output-start-token".to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: "captured-output-owner-nonce".to_owned(),
            ownership_identity_sha256: "b".repeat(64),
        };
        let identity = super::register_background_process_pid(
            pid,
            super::BackgroundProcessHandleCapabilities {
                stdin: false,
                pty_requested: false,
                pty: false,
                signals: true,
                background: true,
            },
            BackgroundLifetimeMode::RunOwned,
            provenance.clone(),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("test process identity should register");
        let stdout = Arc::new(Mutex::new(StreamCapture::from_text("sensitive stdout".to_owned())));
        let stderr = Arc::new(Mutex::new(StreamCapture::from_text("sensitive stderr".to_owned())));
        let stdout_weak = Arc::downgrade(&stdout);
        let stderr_weak = Arc::downgrade(&stderr);
        super::attach_background_output_monitor(
            &identity,
            super::BackgroundOutputMonitor {
                stdout,
                stderr,
                quota_triggered: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            },
        )
        .expect("matching process identity should accept the output monitor");

        super::mark_background_process_stopped(&identity);

        assert!(
            stdout_weak.upgrade().is_some(),
            "the owning run should retain bounded terminal stdout"
        );
        assert!(
            stderr_weak.upgrade().is_some(),
            "the owning run should retain bounded terminal stderr"
        );
        let status = super::background_process_status_by_pid_exact(pid, &provenance)
            .expect("completed process status should remain readable");
        let output: serde_json::Value =
            serde_json::from_slice(&status.output_json).expect("status output should parse");
        assert_eq!(output.get("completed").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(output.get("process_state").and_then(serde_json::Value::as_str), Some("exited"));
        assert_eq!(
            output.pointer("/terminal_frame/stdout/tail").and_then(serde_json::Value::as_str),
            Some("sensitive stdout")
        );
        let mut stale_provenance = provenance.clone();
        stale_provenance.owner_nonce = "reused-pid-owner-nonce".to_owned();
        stale_provenance.ownership_identity_sha256 = "c".repeat(64);
        let stale_error = super::background_process_status_by_pid_exact(pid, &stale_provenance)
            .expect_err("a reused PID with different provenance must fail closed");
        assert_eq!(stale_error.kind, SandboxProcessRunErrorKind::InvalidInput);

        super::release_background_process_history(pid, &provenance);
        assert!(
            stdout_weak.upgrade().is_none(),
            "terminal stdout should be released with the owning run history"
        );
        assert!(
            stderr_weak.upgrade().is_none(),
            "terminal stderr should be released with the owning run history"
        );
    }

    #[test]
    fn unix_stop_proof_requires_recorded_supervisor_acknowledgement() {
        let pid = 4_015_000_u32.saturating_add(std::process::id());
        let provenance = palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind:
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
            start_token: "unix-stop-proof-start-token".to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: "unix-stop-proof-owner-nonce".to_owned(),
            ownership_identity_sha256: "b".repeat(64),
        };
        super::register_background_process_pid(
            pid,
            super::BackgroundProcessHandleCapabilities {
                stdin: false,
                pty_requested: false,
                pty: false,
                signals: true,
                background: true,
            },
            BackgroundLifetimeMode::RunOwned,
            provenance,
            None,
            #[cfg(unix)]
            None,
        )
        .expect("test process identity should register");
        let status = super::BackgroundProcessRuntimeStatus {
            direct_pid_alive: false,
            process_tree_alive: false,
            tracked_process_count: Some(0),
        };

        let before = super::registered_background_process("palyra.process.stop", pid)
            .expect("registered process should be readable");
        assert!(super::process_stop_acknowledgement(pid, &before, status).is_none());

        super::mark_current_background_process_stopped_after_unix_cleanup(pid);
        let after = super::registered_background_process("palyra.process.stop", pid)
            .expect("stopped process should remain readable");
        assert!(super::process_stop_acknowledgement(pid, &after, status).is_some());
    }

    #[test]
    fn cleanup_authority_hold_is_exact_and_fail_closed() {
        let pid = 4_025_000_u32.saturating_add(std::process::id());
        let provenance = palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind: if cfg!(windows) {
                palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject
            } else {
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup
            },
            start_token: "cleanup-hold-start-token".to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: "cleanup-hold-owner-nonce".to_owned(),
            ownership_identity_sha256: "b".repeat(64),
        };
        super::register_background_process_pid(
            pid,
            super::BackgroundProcessHandleCapabilities {
                stdin: false,
                pty_requested: false,
                pty: false,
                signals: true,
                background: true,
            },
            BackgroundLifetimeMode::RunOwned,
            provenance.clone(),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("test process identity should register");

        super::retain_background_process_cleanup_authority(pid, &provenance)
            .expect("matching provenance should retain cleanup authority");
        assert!(super::background_process_cleanup_authority_retained(pid));

        let mut mismatched = provenance.clone();
        mismatched.owner_nonce = "other-owner".to_owned();
        let error = super::retain_background_process_cleanup_authority(pid, &mismatched)
            .expect_err("mismatched provenance must not retain cleanup authority");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(super::background_process_cleanup_authority_retained(pid));

        super::release_background_process_cleanup_authority(pid, &mismatched);
        assert!(super::background_process_cleanup_authority_retained(pid));
        super::release_background_process_cleanup_authority(pid, &provenance);
        assert!(!super::background_process_cleanup_authority_retained(pid));
        super::mark_current_background_process_stopped(pid);
    }

    #[test]
    fn retained_background_process_cleanup_requires_an_explicit_hold() {
        let pid = 4_035_000_u32.saturating_add(std::process::id());
        let provenance = palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind: if cfg!(windows) {
                palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject
            } else {
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup
            },
            start_token: "missing-cleanup-hold-start-token".to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: "missing-cleanup-hold-owner-nonce".to_owned(),
            ownership_identity_sha256: "b".repeat(64),
        };
        let error = super::terminate_retained_background_process(pid, &provenance)
            .expect_err("missing retained authority must fail closed before any signal");
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn retained_background_process_cleanup_terminates_exact_live_tree() {
        #[cfg(windows)]
        let (command, args) = ("ping.exe", serde_json::json!(["-t", "0x7f000001"]));
        #[cfg(not(windows))]
        let (command, args) = ("sleep", serde_json::json!(["60"]));

        let workspace = unique_temp_dir("retained-background-cleanup");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![command.to_owned()]);
        policy.path_access_mode = PathAccessMode::ApprovedRoots;
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": command,
            "args": args,
            "background": true,
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS,
        }))
        .expect("background input should serialize");
        let result =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect("background process should start");
        let output: Value =
            serde_json::from_slice(result.output_json.as_slice()).expect("output should decode");
        let pid = output
            .get("pid")
            .and_then(Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
            .expect("background output should include pid");
        let provenance = super::background_process_provenance_snapshot(pid)
            .expect("live process provenance should exist")
            .provenance;

        super::retain_background_process_cleanup_authority(pid, &provenance)
            .expect("exact cleanup authority should retain");
        let status = super::terminate_retained_background_process(pid, &provenance)
            .expect("retained process tree should terminate");

        assert!(!status.direct_pid_alive());
        assert!(!status.process_tree_alive());
        assert!(super::background_process_cleanup_authority_retained(pid));
        let repeated = super::terminate_retained_background_process(pid, &provenance)
            .expect("already-absent exact retained tree should settle idempotently");
        assert!(!repeated.alive());
        assert!(super::background_process_cleanup_authority_retained(pid));
        super::release_background_process_cleanup_authority(pid, &provenance);
        assert!(!super::background_process_cleanup_authority_retained(pid));
        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn retained_cleanup_authority_prevents_pid_slot_replacement() {
        let pid = 4_045_000_u32.saturating_add(std::process::id());
        let provenance = |start_token: &str| palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind: if cfg!(windows) {
                palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject
            } else {
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup
            },
            start_token: start_token.to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: format!("owner-{start_token}"),
            ownership_identity_sha256: "b".repeat(64),
        };
        let capabilities = super::BackgroundProcessHandleCapabilities {
            stdin: false,
            pty_requested: false,
            pty: false,
            signals: true,
            background: true,
        };
        let first = provenance("retained");
        super::register_background_process_pid(
            pid,
            capabilities,
            BackgroundLifetimeMode::RunOwned,
            first.clone(),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("first process identity should register");
        super::retain_background_process_cleanup_authority(pid, &first)
            .expect("exact cleanup authority should retain");
        super::mark_current_background_process_stopped(pid);

        let error = super::register_background_process_pid(
            pid,
            capabilities,
            BackgroundLifetimeMode::RunOwned,
            provenance("replacement"),
            None,
            #[cfg(unix)]
            None,
        )
        .expect_err("retained cleanup authority must reserve the exact pid slot");
        assert!(error.message.contains("retained cleanup authority"));
        let snapshot = super::registered_background_process("palyra.process.status", pid)
            .expect("retained identity should remain readable");
        assert_eq!(snapshot.provenance, first);

        super::release_background_process_cleanup_authority(pid, &snapshot.provenance);
    }

    #[test]
    fn inactive_registry_entry_can_be_replaced_after_verified_cleanup() {
        let pid = 4_050_000_u32.saturating_add(std::process::id());
        let provenance = |start_token: &str| palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind: if cfg!(windows) {
                palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject
            } else {
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup
            },
            start_token: start_token.to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: format!("owner-{start_token}"),
            ownership_identity_sha256: "b".repeat(64),
        };
        let capabilities = super::BackgroundProcessHandleCapabilities {
            stdin: false,
            pty_requested: false,
            pty: false,
            signals: true,
            background: true,
        };
        super::register_background_process_pid(
            pid,
            capabilities,
            BackgroundLifetimeMode::RunOwned,
            provenance("first"),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("first process identity should register");
        super::mark_current_background_process_stopped(pid);
        super::register_background_process_pid(
            pid,
            capabilities,
            BackgroundLifetimeMode::RunOwned,
            provenance("second"),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("inactive pid slot should accept a new exact identity");

        let snapshot = super::registered_background_process("palyra.process.status", pid)
            .expect("replacement identity should be readable");
        assert_eq!(snapshot.provenance.start_token, "second");
        super::mark_current_background_process_stopped(pid);
    }

    #[test]
    fn stale_windows_job_capability_cannot_remove_pid_replacement() {
        let pid = 4_060_000_u32.saturating_add(std::process::id());
        let stale_job = Arc::new(());
        let replacement_job = Arc::new(());
        let mut jobs = HashMap::from([(pid, Arc::clone(&stale_job))]);

        jobs.insert(pid, Arc::clone(&replacement_job));

        assert!(!super::remove_arc_registry_entry_if_same(&mut jobs, pid, &stale_job));
        assert!(jobs.get(&pid).is_some_and(|job| Arc::ptr_eq(job, &replacement_job)));
        assert!(super::remove_arc_registry_entry_if_same(&mut jobs, pid, &replacement_job));
        assert!(!jobs.contains_key(&pid));
    }

    #[test]
    fn stale_registry_identity_cannot_mutate_replacement() {
        let pid = 4_075_000_u32.saturating_add(std::process::id());
        let provenance = |start_token: &str| palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind: if cfg!(windows) {
                palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject
            } else {
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup
            },
            start_token: start_token.to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: format!("owner-{start_token}"),
            ownership_identity_sha256: "b".repeat(64),
        };
        let capabilities = super::BackgroundProcessHandleCapabilities {
            stdin: false,
            pty_requested: false,
            pty: false,
            signals: true,
            background: true,
        };
        let first = super::register_background_process_pid(
            pid,
            capabilities,
            BackgroundLifetimeMode::RunOwned,
            provenance("first"),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("first process identity should register");
        super::mark_background_process_stopped(&first);
        let second = super::register_background_process_pid(
            pid,
            capabilities,
            BackgroundLifetimeMode::RunOwned,
            provenance("second"),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("replacement process identity should register");

        super::mark_background_process_stopped_after_unix_cleanup(&first);
        let stale_update = super::compare_update_registered_background_process(&first, |process| {
            process.target_pid = Some(7);
        })
        .expect("registry update should not fail");
        assert!(stale_update.is_none());

        let snapshot = super::registered_background_process("palyra.process.status", pid)
            .expect("replacement identity should remain readable");
        assert!(snapshot.active);
        assert!(!snapshot.unix_cleanup_acknowledged);
        assert_eq!(snapshot.provenance.start_token, "second");
        assert!(super::registered_background_processes()
            .lock()
            .expect("registry lock should remain healthy")
            .get(&pid)
            .is_some_and(|process| process.target_pid.is_none()));
        super::mark_background_process_stopped(&second);
    }

    #[test]
    fn mismatched_process_provenance_never_authorizes_lifecycle_actions() {
        let pid = 4_100_000_u32.saturating_add(std::process::id());
        let provenance = palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind: if cfg!(windows) {
                palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject
            } else {
                palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup
            },
            start_token: "registered-start-token".to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: "registered-owner-nonce".to_owned(),
            ownership_identity_sha256: "b".repeat(64),
        };
        super::register_background_process_pid(
            pid,
            super::BackgroundProcessHandleCapabilities {
                stdin: false,
                pty_requested: false,
                pty: false,
                signals: true,
                background: true,
            },
            BackgroundLifetimeMode::RunOwned,
            provenance.clone(),
            None,
            #[cfg(unix)]
            None,
        )
        .expect("test provenance should register");
        super::retain_background_process_cleanup_authority(pid, &provenance)
            .expect("registered provenance should retain cleanup authority");
        let mut mismatched = provenance.clone();
        mismatched.start_token = "recycled-start-token".to_owned();

        assert_eq!(
            super::verify_background_process_provenance(pid, &mismatched),
            palyra_common::runtime_contracts::ProcessProvenanceDisposition::Mismatch
        );
        let error = super::terminate_retained_background_process(pid, &mismatched)
            .expect_err("stale provenance must not authorize retained cleanup");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(super::background_process_cleanup_authority_retained(pid));

        super::release_background_process_cleanup_authority(pid, &provenance);
        super::mark_current_background_process_stopped(pid);
    }

    fn persisted_process_test_provenance(
        ownership_kind: palyra_common::runtime_contracts::ProcessOwnershipKind,
    ) -> palyra_common::runtime_contracts::ProcessProvenance {
        palyra_common::runtime_contracts::ProcessProvenance {
            ownership_kind,
            start_token: "captured-start-token".to_owned(),
            executable_sha256: "a".repeat(64),
            owner_nonce: "captured-owner-nonce".to_owned(),
            ownership_identity_sha256: "b".repeat(64),
        }
    }

    #[test]
    fn persisted_direct_pid_absence_requires_ownership_domain_absence() {
        let provenance = persisted_process_test_provenance(
            palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
        );
        let disposition = super::verify_process_identity_with(
            42,
            &provenance,
            false,
            |_| Ok(None),
            |_| panic!("missing pid must not hash an executable"),
        );

        assert_eq!(
            disposition,
            palyra_common::runtime_contracts::ProcessProvenanceDisposition::Unsupported
        );
    }

    #[cfg(unix)]
    #[test]
    fn managed_stdio_process_establishes_unix_session_anchor() {
        let process = super::spawn_managed_stdio_process(&super::ManagedStdioProcessConfig {
            executable: PathBuf::from("/bin/sh"),
            args: vec!["-c".to_owned(), "read _".to_owned()],
            cwd: PathBuf::from("/"),
            env: BTreeMap::new(),
            generation: 1,
            lease_duration: Duration::from_secs(5),
        })
        .expect("managed stdio process should satisfy provenance admission");
        let pid = process.lease().pid;
        let unix_pid = super::unix_pid_from_u32(pid).expect("managed pid should fit pid_t");

        // SAFETY: both calls inspect the live positive PID retained by `process`.
        let process_group_id = unsafe { libc::getpgid(unix_pid) };
        let session_id = unsafe { libc::getsid(unix_pid) };
        assert_eq!(process_group_id, unix_pid);
        assert_eq!(session_id, unix_pid);

        let report = process.cleanup(false);
        assert_eq!(report.outcome, palyra_common::runtime_contracts::CleanupOutcome::Completed);
    }

    #[cfg(unix)]
    #[test]
    fn persisted_unix_group_with_live_descendant_is_not_absent() {
        use std::os::unix::process::CommandExt;

        let workspace = unique_temp_dir("persisted-process-group-descendant");
        fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        let pid_file = workspace.join("descendant.pid");
        let script = format!(
            "sleep 30 & child=$!; printf '%s' \"$child\" > '{}'; exit 0",
            pid_file.display()
        );
        let mut command = Command::new("sh");
        command.args(["-c", script.as_str()]).process_group(0);
        let mut leader = command.spawn().expect("process-group leader should spawn");
        let group_id = leader.id();
        let status = leader.wait().expect("process-group leader should exit");
        assert!(status.success());
        for _ in 0..100 {
            if pid_file.exists() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let descendant_pid = fs::read_to_string(pid_file.as_path())
            .expect("descendant pid should be recorded")
            .trim()
            .parse::<u32>()
            .expect("descendant pid should parse");

        assert_eq!(
            super::current_process_start_token(group_id).expect("leader lookup should work"),
            None
        );
        assert!(super::unix_process_group_is_alive(group_id)
            .expect("process-group probe should observe the live descendant"));

        super::terminate_unix_process_group(group_id)
            .expect("test process group should terminate through its exact group id");
        assert!(super::wait_for_process_not_alive(descendant_pid, Duration::from_secs(5)));
        let _ = fs::remove_dir_all(workspace);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn persisted_linux_zombie_only_group_is_absent() {
        use std::os::unix::process::CommandExt;

        let mut command = Command::new("sh");
        command.args(["-c", "exit 0"]).process_group(0);
        let mut leader = super::ManagedChildGuard::new_reap_only(
            command.spawn().expect("process-group leader should spawn"),
        );
        let group_id = leader.id();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stat = super::read_linux_process_stat(group_id)
                .expect("zombie process stat lookup should work")
                .expect("unreaped process-group leader should remain visible");
            if stat.state == b'Z' {
                break;
            }
            assert!(Instant::now() < deadline, "process-group leader should become a zombie");
            thread::sleep(Duration::from_millis(10));
        }

        let process_group_id =
            super::unix_pid_from_u32(group_id).expect("process-group id should fit pid_t");
        assert!(
            super::linux_process_group_signal_probe(process_group_id)
                .expect("raw process-group probe should work"),
            "signal-zero must demonstrate why the zombie-aware snapshot is required"
        );
        let status = super::background_process_runtime_status(group_id)
            .expect("zombie-aware runtime status should work");
        assert!(status.direct_pid_alive());
        assert!(!status.process_tree_alive());
        assert!(!status.alive(), "an unreaped zombie is not a live ownership domain");
        assert!(
            super::wait_for_owned_background_tree_inactive_for_identity(
                group_id,
                None,
                Duration::ZERO,
            )
            .expect("zombie-aware ownership-domain wait should work"),
            "acknowledged cleanup may settle before the child owner reaps the supervisor"
        );

        let status = leader
            .wait_for_exit(Duration::from_secs(5))
            .expect("zombie leader reap should work")
            .expect("zombie leader should be waitable");
        assert!(status.success());
    }

    #[test]
    fn macos_process_group_list_result_distinguishes_empty_snapshots_from_errors() {
        assert_eq!(
            super::classify_macos_process_group_list_result(0, 0, 16)
                .expect("a successful empty libproc result should classify"),
            super::MacosProcessGroupListResult::Empty
        );
        assert_eq!(
            super::classify_macos_process_group_list_result(1, libc::EPERM, 16)
                .expect("a positive libproc result should ignore stale errno"),
            super::MacosProcessGroupListResult::Members(1)
        );
        assert_eq!(
            super::classify_macos_process_group_list_result(0, libc::ESRCH, 16)
                .expect_err("a zero libproc result with ESRCH must fail closed")
                .raw_os_error(),
            Some(libc::ESRCH)
        );
        assert_eq!(
            super::classify_macos_process_group_list_result(0, libc::ENOENT, 16)
                .expect_err("a zero libproc result with ENOENT must fail closed")
                .raw_os_error(),
            Some(libc::ENOENT)
        );
        assert_eq!(
            super::classify_macos_process_group_list_result(0, libc::EPERM, 16)
                .expect_err("a zero libproc result with errno must fail closed")
                .raw_os_error(),
            Some(libc::EPERM)
        );
        assert_eq!(
            super::classify_macos_process_group_list_result(-1, 0, 16)
                .expect_err("a negative libproc result must fail closed")
                .raw_os_error(),
            Some(libc::EIO)
        );
        assert!(super::classify_macos_process_group_list_result(16, 0, 16)
            .expect_err("a full libproc buffer must remain ambiguous")
            .to_string()
            .contains("capacity"));
    }

    #[test]
    fn macos_zombie_anchor_requires_a_quiescent_process_group() {
        super::verify_macos_zombie_group_is_quiescent(41, false)
            .expect("a zombie-only process group should remain admissible");
        let error = super::verify_macos_zombie_group_is_quiescent(41, true)
            .expect_err("a live process-group member must deny zombie admission");
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert!(error.to_string().contains("pid 41"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_start_token_includes_unreaped_short_lived_children() {
        let mut command = Command::new("/usr/bin/true");
        super::configure_managed_stdio_process_ownership(&mut command);
        let mut child = ManagedChildGuard::new_reap_only(
            command.spawn().expect("short-lived managed child should spawn"),
        );
        let pid = child.id();
        let process_id = super::unix_pid_from_u32(pid).expect("child pid should fit pid_t");
        let information_size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
            .expect("macOS process information size should fit");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut information = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
            // SAFETY: the exact child remains unreaped and `information` is the fixed writable
            // PROC_PIDTBSDINFO ABI buffer for the validated positive PID.
            let read = unsafe {
                super::macos_proc_pidinfo(
                    process_id,
                    libc::PROC_PIDTBSDINFO,
                    super::MACOS_PROC_PIDINFO_INCLUDE_ZOMBIES,
                    information.as_mut_ptr().cast(),
                    information_size,
                )
            };
            if read == information_size {
                // SAFETY: proc_pidinfo reported a complete fixed-size structure.
                let information = unsafe { information.assume_init() };
                if information.pbi_status == libc::SZOMB {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "short-lived child should become a zombie");
            thread::sleep(Duration::from_millis(10));
        }

        let start_token = super::current_process_start_token(pid)
            .expect("unreaped child identity lookup should succeed")
            .expect("unreaped child should retain its stable identity");
        assert!(start_token.starts_with("macos:"));
        let repeated_start_token = super::current_process_start_token(pid)
            .expect("repeated unreaped child identity lookup should succeed")
            .expect("unreaped child should retain its stable identity");
        assert_eq!(repeated_start_token, start_token);
        super::verify_live_ownership_anchor(pid)
            .expect("unreaped child should retain its exact ownership anchor");

        let status = child
            .wait_for_exit(Duration::from_secs(5))
            .expect("short-lived child reap should succeed")
            .expect("short-lived child should be waitable");
        assert!(status.success());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn managed_stdio_process_admits_short_lived_macos_children() {
        for generation in 1..=32 {
            let process = super::spawn_managed_stdio_process(&super::ManagedStdioProcessConfig {
                executable: PathBuf::from("/usr/bin/true"),
                args: Vec::new(),
                cwd: PathBuf::from("/"),
                env: BTreeMap::new(),
                generation,
                lease_duration: Duration::from_secs(5),
            })
            .expect("short-lived managed child should satisfy provenance admission");
            assert!(process.lease().provenance.start_token.starts_with("macos:"));
            let report = process.cleanup(false);
            assert_eq!(report.outcome, palyra_common::runtime_contracts::CleanupOutcome::Completed);
        }
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn persisted_macos_signaled_zombie_only_group_is_absent() {
        use std::os::unix::process::{CommandExt, ExitStatusExt};

        let mut command = Command::new("sh");
        command.args(["-c", "kill -KILL $$"]).process_group(0);
        let mut leader = super::ManagedChildGuard::new(
            command.spawn().expect("process-group leader should spawn"),
        );
        let group_id = leader.id();
        let process_id =
            super::unix_pid_from_u32(group_id).expect("process-group id should fit pid_t");
        let information_size = i32::try_from(std::mem::size_of::<libc::proc_bsdinfo>())
            .expect("macOS process information size should fit");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let mut information = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
            // SAFETY: `information` is the exact writable PROC_PIDTBSDINFO ABI buffer and the
            // unreaped child PID remains reserved until this test explicitly waits for it.
            let read = unsafe {
                super::macos_proc_pidinfo(
                    process_id,
                    libc::PROC_PIDTBSDINFO,
                    super::MACOS_PROC_PIDINFO_INCLUDE_ZOMBIES,
                    information.as_mut_ptr().cast(),
                    information_size,
                )
            };
            if read == information_size {
                // SAFETY: proc_pidinfo reported that it initialized the complete fixed-size
                // structure.
                let information = unsafe { information.assume_init() };
                if information.pbi_status == libc::SZOMB {
                    break;
                }
            }
            assert!(Instant::now() < deadline, "process-group leader should become a zombie");
            thread::sleep(Duration::from_millis(10));
        }

        assert!(!super::unix_process_group_is_alive(group_id)
            .expect("macOS zombie-aware process-group probe should work"));

        let status = leader
            .wait_for_exit(Duration::from_secs(5))
            .expect("zombie leader reap should work")
            .expect("zombie leader should be waitable");
        assert_eq!(status.signal(), Some(libc::SIGKILL));
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[test]
    fn linux_process_stat_parser_tracks_state_group_and_start_token() {
        let mut stat = b"123 (command with ) spaces ".to_vec();
        stat.push(0xff);
        stat.extend_from_slice(b") Z 1 456 456 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 789\n");

        assert_eq!(
            super::parse_linux_process_stat(123, stat.as_slice())
                .expect("Linux process stat should parse"),
            super::LinuxProcessStat { state: b'Z', process_group_id: Some(456), start_token: 789 }
        );

        let unavailable_group_stat = b"123 (exiting) X 1 -1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 790\n";
        assert_eq!(
            super::parse_linux_process_stat(123, unavailable_group_stat)
                .expect("Linux exiting-process stat should parse"),
            super::LinuxProcessStat { state: b'X', process_group_id: None, start_token: 790 }
        );

        let namespace_hidden_group_stat =
            b"123 (running) S 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 791\n";
        assert_eq!(
            super::parse_linux_process_stat(123, namespace_hidden_group_stat)
                .expect("Linux namespace-hidden process stat should parse"),
            super::LinuxProcessStat { state: b'S', process_group_id: None, start_token: 791 }
        );

        let malformed_group_stat =
            b"123 (malformed) S 1 invalid 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 792\n";
        let error = super::parse_linux_process_stat(123, malformed_group_stat)
            .expect_err("nonnumeric Linux process group should fail");
        assert_eq!(error.to_string(), "Linux process stat process group invalid");
    }

    #[test]
    fn retained_numeric_ownership_anchor_without_direct_identity_fails_closed() {
        let provenance = persisted_process_test_provenance(
            palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
        );
        let disposition = super::verify_process_identity_with(
            42,
            &provenance,
            true,
            |_| Ok(None),
            |_| panic!("missing pid must not hash an executable"),
        );

        assert_eq!(
            disposition,
            palyra_common::runtime_contracts::ProcessProvenanceDisposition::Unsupported
        );
    }

    #[test]
    fn registered_tree_probe_error_never_proves_absence() {
        let disposition = super::registered_process_liveness_disposition(
            palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
            Err(io::Error::new(io::ErrorKind::PermissionDenied, "probe denied")),
            Ok(false),
        );

        assert_eq!(
            disposition,
            Some(palyra_common::runtime_contracts::ProcessProvenanceDisposition::Unsupported)
        );
    }

    #[test]
    fn registered_unix_group_without_direct_identity_never_authorizes_signalling() {
        let disposition = super::registered_process_liveness_disposition(
            palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
            Ok(true),
            Ok(false),
        );

        assert_eq!(
            disposition,
            Some(palyra_common::runtime_contracts::ProcessProvenanceDisposition::Unsupported)
        );
    }

    #[test]
    fn persisted_unix_group_absence_cannot_prove_descendant_absence() {
        let provenance = persisted_process_test_provenance(
            palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
        );
        let disposition = super::verify_process_identity_with(
            42,
            &provenance,
            false,
            |_| Ok(None),
            |_| panic!("missing pid must not hash an executable"),
        );

        assert_eq!(
            disposition,
            palyra_common::runtime_contracts::ProcessProvenanceDisposition::Unsupported
        );
    }

    #[test]
    fn persisted_live_process_identity_mismatch_fails_closed() {
        let provenance = persisted_process_test_provenance(
            palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
        );
        let disposition = super::verify_process_identity_with(
            42,
            &provenance,
            false,
            |_| Ok(Some("recycled-start-token".to_owned())),
            |_| panic!("mismatched start token must stop before executable hashing"),
        );

        assert_eq!(
            disposition,
            palyra_common::runtime_contracts::ProcessProvenanceDisposition::Mismatch
        );
    }

    #[test]
    fn persisted_live_executable_mismatch_fails_closed() {
        let provenance = persisted_process_test_provenance(
            palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
        );
        let disposition = super::verify_process_identity_with(
            42,
            &provenance,
            false,
            |_| Ok(Some(provenance.start_token.clone())),
            |_| Ok("c".repeat(64)),
        );

        assert_eq!(
            disposition,
            palyra_common::runtime_contracts::ProcessProvenanceDisposition::Mismatch
        );
    }

    #[test]
    fn persisted_live_identity_match_cannot_reconstruct_ownership() {
        let provenance = persisted_process_test_provenance(
            palyra_common::runtime_contracts::ProcessOwnershipKind::WindowsJobObject,
        );
        let disposition = super::verify_process_identity_with(
            42,
            &provenance,
            false,
            |_| Ok(Some(provenance.start_token.clone())),
            |_| Ok(provenance.executable_sha256.clone()),
        );

        assert_eq!(
            disposition,
            palyra_common::runtime_contracts::ProcessProvenanceDisposition::Unsupported
        );
    }

    #[test]
    fn persisted_identity_lookup_errors_never_prove_absence() {
        let provenance = persisted_process_test_provenance(
            palyra_common::runtime_contracts::ProcessOwnershipKind::UnixProcessGroup,
        );
        let disposition = super::verify_process_identity_with(
            42,
            &provenance,
            false,
            |_| Err(io::Error::new(io::ErrorKind::PermissionDenied, "denied")),
            |_| panic!("failed identity lookup must not hash an executable"),
        );

        assert_eq!(
            disposition,
            palyra_common::runtime_contracts::ProcessProvenanceDisposition::Unsupported
        );
    }

    #[test]
    fn process_input_rejects_oversized_payload_before_pid_lookup() {
        let oversized = "x".repeat(super::PROCESS_STDIN_INPUT_MAX_BYTES + 1);
        let input = serde_json::to_vec(&serde_json::json!({
            "pid": 42,
            "input": oversized
        }))
        .expect("input should serialize");

        let error = super::write_background_process_stdin(input.as_slice())
            .expect_err("oversized process input should be rejected before registry lookup");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("input exceeds"), "{}", error.message);
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn background_process_accepts_bounded_stdin_input() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-python-background-stdin");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join("stdin_ready.py"),
            "import pathlib, sys, time\nprint('ready', flush=True)\nline = sys.stdin.readline()\npathlib.Path('stdin-result.txt').write_text(line, encoding='utf-8')\ntime.sleep(30)\n",
        )
        .expect("background stdin script should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["stdin_ready.py"],
            "background": true,
            "stdin": true,
            "port_hints": [8787],
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS
        }))
        .expect("input should serialize");

        let result =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect("stdin-capable background process should start");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        let pid = output
            .get("pid")
            .and_then(serde_json::Value::as_u64)
            .expect("background process should return pid") as u32;

        assert_eq!(
            output
                .pointer("/process_handle/capabilities/stdin")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            output
                .pointer("/process_handle/capabilities/pty_requested")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            output.pointer("/process_handle/capabilities/pty").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(
            output
                .pointer("/process_handle/capabilities/signals")
                .and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            output
                .pointer("/process_handle/capabilities/port_hints/0")
                .and_then(serde_json::Value::as_u64),
            Some(8787)
        );

        let input_result = super::write_background_process_stdin(
            serde_json::to_vec(&serde_json::json!({
                "pid": pid,
                "input": "hello from stdin",
                "append_newline": true
            }))
            .expect("stdin input should serialize")
            .as_slice(),
        )
        .expect("stdin input should be delivered to the process");
        let input_output: serde_json::Value =
            serde_json::from_slice(&input_result.output_json).expect("input output should parse");
        assert_eq!(
            input_output.get("input_delivered").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            input_output.get("stdin_redaction_level").and_then(serde_json::Value::as_str),
            Some("input_redacted")
        );

        let result_path = workspace.join("stdin-result.txt");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut delivered = String::new();
        while delivered.replace("\r\n", "\n") != "hello from stdin\n" {
            if Instant::now() >= deadline {
                let _ = super::stop_background_process_by_pid(pid);
                panic!(
                    "stdin-capable background process did not write expected result; latest={delivered:?}"
                );
            }
            if result_path.exists() {
                delivered =
                    fs::read_to_string(result_path.as_path()).unwrap_or_else(|_| String::new());
            }
            thread::sleep(Duration::from_millis(25));
        }

        let _ = super::stop_background_process_by_pid(pid);
        let _ = fs::remove_dir_all(workspace.as_path());
        assert_eq!(delivered.replace("\r\n", "\n"), "hello from stdin\n");
    }

    #[test]
    fn process_send_keys_rejects_raw_escape_text() {
        let input = br#"{"pid":42,"keys":[{"key":"text","text":"\u001b[31m"}],"allow_stdin_fallback":true}"#;

        let error = super::send_keys_to_background_process(input)
            .expect_err("raw escape text should be rejected before PID lookup");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("control characters"), "{}", error.message);
    }

    #[test]
    fn terminal_frame_snapshot_is_bounded_and_redacted() {
        let snapshot = super::terminal_frame_stream_snapshot(
            "stdout",
            StreamCapture {
                bytes: format!("{}\nTOKEN=secret-value", "x".repeat(5_000)).into_bytes(),
                truncated: true,
                read_error: None,
            },
        );

        let tail = snapshot
            .get("tail")
            .and_then(serde_json::Value::as_str)
            .expect("terminal frame tail should be present");
        assert!(tail.len() <= super::PROCESS_TERMINAL_FRAME_TEXT_BYTES);
        assert!(!tail.contains("secret-value"), "{tail}");
        assert_eq!(snapshot.get("truncated").and_then(serde_json::Value::as_bool), Some(true));
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn process_send_keys_reports_degraded_pty_without_fallback() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-python-send-keys-degraded");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join("keys_ready.py"),
            "import time\nprint('ready', flush=True)\ntime.sleep(30)\n",
        )
        .expect("background keys script should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["keys_ready.py"],
            "background": true,
            "pty": true,
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS
        }))
        .expect("input should serialize");

        let result =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect("pty-requested background process should start with degraded metadata");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        let pid = output
            .get("pid")
            .and_then(serde_json::Value::as_u64)
            .expect("background process should return pid") as u32;
        let keys_result = super::send_keys_to_background_process(
            serde_json::to_vec(&serde_json::json!({
                "pid": pid,
                "keys": [{"key": "enter"}]
            }))
            .expect("keys input should serialize")
            .as_slice(),
        )
        .expect("unsupported PTY should return degraded output, not fail");
        let keys_output: serde_json::Value =
            serde_json::from_slice(&keys_result.output_json).expect("keys output should parse");

        let _ = super::stop_background_process_by_pid(pid);
        let _ = fs::remove_dir_all(workspace.as_path());
        assert_eq!(
            output.pointer("/process_handle/capabilities/pty").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert_eq!(keys_output.get("keys_sent").and_then(serde_json::Value::as_bool), Some(false));
        assert_eq!(
            keys_output.get("degraded_reason").and_then(serde_json::Value::as_str),
            Some("pty_backend_unavailable")
        );
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn process_send_keys_uses_stdin_fallback_and_reports_frame() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-python-send-keys-fallback");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join("keys_echo.py"),
            "import pathlib, sys, time\nprint('ready', flush=True)\nline = sys.stdin.readline()\npathlib.Path('keys-result.txt').write_text(line, encoding='utf-8')\nprint('accepted', flush=True)\ntime.sleep(30)\n",
        )
        .expect("background keys script should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["keys_echo.py"],
            "background": true,
            "pty": true,
            "timeout_ms": BACKGROUND_TEST_EXECUTION_TIMEOUT_MS
        }))
        .expect("input should serialize");

        let result =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect("pty-requested background process should start with stdin fallback");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");
        let pid = output
            .get("pid")
            .and_then(serde_json::Value::as_u64)
            .expect("background process should return pid") as u32;
        let keys_result = super::send_keys_to_background_process(
            serde_json::to_vec(&serde_json::json!({
                "pid": pid,
                "keys": [{"key": "text", "text": "hello keys"}, {"key": "enter"}],
                "allow_stdin_fallback": true
            }))
            .expect("keys input should serialize")
            .as_slice(),
        )
        .expect("send_keys should use stdin fallback when explicitly allowed");
        let keys_output: serde_json::Value =
            serde_json::from_slice(&keys_result.output_json).expect("keys output should parse");

        let result_path = workspace.join("keys-result.txt");
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut delivered = String::new();
        while delivered.replace("\r\n", "\n") != "hello keys\n" {
            if Instant::now() >= deadline {
                let _ = super::stop_background_process_by_pid(pid);
                panic!(
                    "send_keys stdin fallback did not write expected result; latest={delivered:?}"
                );
            }
            if result_path.exists() {
                delivered =
                    fs::read_to_string(result_path.as_path()).unwrap_or_else(|_| String::new());
            }
            thread::sleep(Duration::from_millis(25));
        }

        let _ = super::stop_background_process_by_pid(pid);
        let _ = fs::remove_dir_all(workspace.as_path());
        assert_eq!(keys_output.get("keys_sent").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(
            keys_output.get("fallback_used").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            keys_output.pointer("/terminal_frame/available").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(delivered.replace("\r\n", "\n"), "hello keys\n");
    }

    #[test]
    fn run_constrained_process_rejects_detached_background_handoff_until_durable_registration() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy_with_allowed_executables(workspace, vec!["echo".to_owned()]);
        let input =
            br#"{"command":"echo","args":["ok"],"background":true,"lifetime_mode":"detached"}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("detached local background lifetime must fail closed");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("durable detached process handoff"), "{}", error.message);
    }

    #[test]
    fn run_constrained_process_rejects_detached_lifetime_without_background() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let policy = sandbox_policy_with_allowed_executables(workspace, vec!["echo".to_owned()]);
        let input = br#"{"command":"echo","args":["ok"],"lifetime_mode":"detached"}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("detached lifecycle must require background=true");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::InvalidInput);
        assert!(error.message.contains("requires background=true"), "{}", error.message);
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn run_constrained_process_keeps_foreground_python_http_server_foreground_when_available() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-python-http-server-auto-background");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["-m", "http.server", "0"],
            "timeout_ms": 250
        }))
        .expect("input should serialize");

        let error =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect_err("foreground http.server must not be silently backgrounded");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::TimedOut);
        assert!(
            error.message.contains("background=true"),
            "timeout guidance should require an explicit background request: {}",
            error.message
        );

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
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-background-startup-output");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let script = format!(
            "import time\nfor _ in range(20):\n    print('PORT=54321', flush=True)\n    time.sleep(0.05)\ntime.sleep({BACKGROUND_TEST_SCRIPT_SLEEP_SECS})\n"
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
        assert!(
            stdout.lines().any(|line| line == "PORT=54321"),
            "startup stdout should include the port line: {stdout:?}"
        );
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
        #[cfg(unix)]
        assert_eq!(
            stopped_output
                .pointer("/stop_acknowledgement/proof")
                .and_then(serde_json::Value::as_str),
            Some("unix_supervisor_cleanup_acknowledged")
        );

        #[cfg(unix)]
        {
            let repeated_stop = run_constrained_process(
                &policy,
                stop_input.as_slice(),
                background_test_execution_timeout(),
            )
            .expect("repeated portable stop should preserve verified cleanup evidence");
            let repeated_output: serde_json::Value =
                serde_json::from_slice(&repeated_stop.output_json)
                    .expect("repeated stop should parse");
            assert_eq!(
                repeated_output
                    .pointer("/stop_acknowledgement/proof")
                    .and_then(serde_json::Value::as_str),
                Some("unix_supervisor_cleanup_acknowledged")
            );
        }

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
    #[cfg(not(target_os = "macos"))]
    fn run_constrained_process_cancels_foreground_process_tree() {
        #[cfg(windows)]
        let (command, args): (&str, Vec<&str>) = ("ping", vec!["-n", "30", "127.0.0.1"]);
        #[cfg(not(windows))]
        let (command, args): (&str, Vec<&str>) = ("sleep", vec!["30"]);

        if Command::new(command).output().is_err() {
            return;
        }

        let workspace = unique_temp_dir("workspace-foreground-cancel");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let policy = host_access_policy(workspace.clone());
        let cancellation_requested = Arc::new(AtomicBool::new(false));
        let cancellation_for_runner = Arc::clone(&cancellation_requested);
        let input = serde_json::to_vec(&serde_json::json!({
            "command": command,
            "args": args,
            "timeout_ms": 30_000,
        }))
        .expect("input should serialize");

        let started_at = Instant::now();
        let handle = thread::spawn(move || {
            run_constrained_process_with_cancellation(
                &policy,
                input.as_slice(),
                Duration::from_millis(30_000),
                Some(cancellation_for_runner),
            )
        });
        thread::sleep(Duration::from_millis(300));
        cancellation_requested.store(true, Ordering::Relaxed);

        let error = handle
            .join()
            .expect("foreground cancellation worker should not panic")
            .expect_err("cancelled foreground process should return an error");
        let _ = fs::remove_dir_all(workspace.as_path());

        assert_eq!(error.kind, SandboxProcessRunErrorKind::Cancelled);
        assert!(
            started_at.elapsed() < Duration::from_secs(5),
            "foreground cancellation should stop promptly instead of waiting for timeout"
        );
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
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-background-windows-job-child");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let child_pid_path = workspace.join("child.pid");
        fs::write(
            workspace.join("child.py"),
            "import os, pathlib, time\npathlib.Path('child.pid').write_text(str(os.getpid()), encoding='utf-8')\ntime.sleep(30)\n",
        )
        .expect("child script should be written");
        fs::write(
            workspace.join("launcher.py"),
            "import os, subprocess, sys, time\nworkspace = os.path.dirname(__file__)\nchild = subprocess.Popen([sys.executable, os.path.join(workspace, 'child.py')], cwd=workspace, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)\nprint(f'child_pid={child.pid}', flush=True)\ntime.sleep(6)\nos._exit(0)\n",
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

        let child_pid_deadline = Instant::now() + Duration::from_secs(5);
        while !child_pid_path.exists() {
            if Instant::now() >= child_pid_deadline {
                let _ = super::stop_background_process_by_pid(pid);
                let _ = fs::remove_dir_all(workspace.as_path());
                panic!("launcher pid {pid} should start a child process before exiting");
            }
            thread::sleep(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS));
        }
        let child_pid = fs::read_to_string(child_pid_path.as_path())
            .expect("child pid file should be readable")
            .trim()
            .parse::<u32>()
            .expect("child pid should parse");
        assert!(
            super::process_id_is_alive(child_pid).unwrap_or(false),
            "child process {child_pid} should still be alive before launcher exits"
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

        let status_deadline = Instant::now() + Duration::from_secs(3);
        let status = loop {
            let status = super::background_process_status_by_pid(pid)
                .expect("status should inspect the Windows job after direct pid exits");
            let status_output: serde_json::Value =
                serde_json::from_slice(&status.output_json).expect("status output should parse");
            let direct_pid_alive =
                status_output.get("direct_pid_alive").and_then(serde_json::Value::as_bool);
            let process_tree_alive =
                status_output.get("process_tree_alive").and_then(serde_json::Value::as_bool);
            if direct_pid_alive == Some(false) && process_tree_alive == Some(true) {
                break status;
            }
            if Instant::now() >= status_deadline {
                let _ = super::stop_background_process_by_pid(pid);
                let _ = fs::remove_dir_all(workspace.as_path());
                panic!(
                    "job should still report child process {child_pid} as alive: {status_output}"
                );
            }
            thread::sleep(Duration::from_millis(BACKGROUND_MONITOR_POLL_MS));
        };
        let stopped = super::stop_background_process_by_pid(pid);
        let _ = fs::remove_dir_all(workspace.as_path());

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
        assert_eq!(
            stopped_output.get("tracked_process_count").and_then(serde_json::Value::as_u64),
            Some(0),
            "stop should report the post-termination Windows job count before releasing tracking: {stopped_output}"
        );
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn run_constrained_process_accepts_successful_background_launcher() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let workspace = unique_temp_dir("workspace-background-immediate-exit");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join("launcher_ok.py"),
            "print('launcher started external service', flush=True)\n",
        )
        .expect("launcher fixture should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["launcher_ok.py"],
            "background": true,
            "timeout_ms": 60_000
        }))
        .expect("input should serialize");

        let result =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect("successful background launcher should not be reported as failure");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(output.get("background").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(output.get("completed").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(
            output.get("launcher_completed_successfully").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(
            output.get("process_state").and_then(serde_json::Value::as_str),
            Some("completed")
        );
        assert_eq!(output.get("tracked_pid"), Some(&serde_json::Value::Null));
        assert_eq!(
            output.get("run_owned_lifetime").and_then(serde_json::Value::as_bool),
            Some(false)
        );
        assert!(output
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .contains("launcher started external service"));

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(not(target_os = "macos"))]
    fn successful_background_launcher_terminates_spawned_child_process_tree() {
        let Some(python) = ["python3", "python", "py"].into_iter().find(|command| {
            Command::new(command)
                .arg("--version")
                .output()
                .map(|output| output.status.success())
                .unwrap_or(false)
        }) else {
            return;
        };
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-background-success-child-cleanup");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let child_script = workspace.join("child.py");
        let launcher_script = workspace.join("launcher.py");
        let child_pid_path = workspace.join("child.pid");
        fs::write(
            child_script.as_path(),
            format!("import time\ntime.sleep({BACKGROUND_TEST_SCRIPT_SLEEP_SECS})\n"),
        )
        .expect("child script should be written");
        // Keep the fixture silent so the bounded exit probes, rather than pipe-read timing, observe
        // completion. os._exit also removes interpreter teardown from this process-tree contract.
        fs::write(
            launcher_script.as_path(),
            "import os, pathlib, subprocess, sys\nroot = pathlib.Path(__file__).resolve().parent\npid_path = root / 'child.pid'\nchild = root / 'child.py'\nprocess = subprocess.Popen([sys.executable, str(child)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\npid_path.write_text(str(process.pid), encoding='utf-8')\nos._exit(0)\n",
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

        let result =
            run_constrained_process(&policy, input.as_slice(), background_test_execution_timeout())
                .expect("successful background launcher should still report success");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(output.get("background").and_then(serde_json::Value::as_bool), Some(true));
        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(
            output.get("launcher_completed_successfully").and_then(serde_json::Value::as_bool),
            Some(true)
        );
        assert_eq!(output.get("tracked_pid"), Some(&serde_json::Value::Null));

        let child_pid = fs::read_to_string(child_pid_path.as_path())
            .expect("launcher should write child pid before exiting")
            .trim()
            .parse::<u32>()
            .expect("child pid should be numeric");
        assert!(
            super::wait_for_process_not_alive(
                child_pid,
                Duration::from_millis(BACKGROUND_TERMINATION_WAIT_MS)
            ),
            "child pid {child_pid} should be terminated after successful launcher exit"
        );

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
            "import sys\nprint('arbitrary-private-background-output', flush=True)\nsys.exit(1)\n",
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
        assert!(
            error.message.contains("stdout_preview=Some(")
                && error.message.contains("arbitrary-private-background-output"),
            "{}",
            error.message
        );

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
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-background-escaped-child");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let child_script = workspace.join("child.py");
        let launcher_script = workspace.join("launcher.py");
        let child_pid_path = workspace.join("child.pid");
        fs::write(
            child_script.as_path(),
            format!("import time\ntime.sleep({BACKGROUND_TEST_SCRIPT_SLEEP_SECS})\n"),
        )
        .expect("child script should be written");
        fs::write(
            launcher_script.as_path(),
            "import pathlib, subprocess, sys\nroot = pathlib.Path(__file__).resolve().parent\npid_path = root / 'child.pid'\nchild = root / 'child.py'\nprocess = subprocess.Popen([sys.executable, str(child)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\npid_path.write_text(str(process.pid), encoding='utf-8')\nsys.exit(1)\n",
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
                .expect_err("launcher should fail during background startup checks");

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
        #[cfg(unix)]
        {
            assert_eq!(metadata.get("manual_command"), Some(&serde_json::Value::Null));
            assert_eq!(
                metadata.get("process_tree").and_then(serde_json::Value::as_bool),
                Some(true)
            );
            assert_eq!(
                metadata.get("windows_job_object").and_then(serde_json::Value::as_bool),
                Some(false)
            );
        }
        #[cfg(not(any(unix, windows)))]
        {
            assert_eq!(
                metadata.pointer("/manual_command/command").and_then(serde_json::Value::as_str),
                Some("kill")
            );
            assert_eq!(
                metadata.pointer("/manual_command/args/1").and_then(serde_json::Value::as_str),
                Some("1234")
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

        assert_eq!(short, Duration::from_millis(750));
        assert_eq!(execution_capped, Duration::from_millis(750));
        assert_eq!(capped, Duration::from_millis(750));
    }

    #[test]
    fn background_lifetime_adjustment_reason_reports_raise_and_cap() {
        assert_eq!(
            super::background_lifetime_adjustment_reason(Some(60_000), 120_000),
            Some("raised_to_minimum_background_lifetime")
        );
        assert_eq!(
            super::background_lifetime_adjustment_reason(Some(600_000), 120_000),
            Some("capped_by_effective_background_max_lifetime")
        );
        assert_eq!(super::background_lifetime_adjustment_reason(Some(120_000), 120_000), None);
        assert_eq!(super::background_lifetime_adjustment_reason(None, 120_000), None);
        assert!(super::background_lifetime_adjustment_note(Some(
            "capped_by_effective_background_max_lifetime"
        ))
        .contains("was capped"));
    }

    #[test]
    fn background_process_lifetime_raises_short_explicit_timeout_when_cap_permits() {
        let raised =
            super::background_process_lifetime(Some(15_000), Duration::from_millis(180_000));
        let capped_by_execution =
            super::background_process_lifetime(Some(15_000), Duration::from_millis(60_000));

        assert_eq!(raised, Duration::from_millis(super::MIN_BACKGROUND_PROCESS_LIFETIME_MS));
        assert_eq!(capped_by_execution, Duration::from_millis(60_000));
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
    fn run_constrained_process_rejects_allowlisted_node_eval_when_available() {
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

        let error = run_constrained_process(&policy, input, Duration::from_millis(20_000))
            .expect_err("allowlisted node eval must still be rejected");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_executes_explicit_workspace_node_script_when_available() {
        if Command::new("node").arg("--version").output().is_err() {
            return;
        }
        let workspace = unique_temp_dir("workspace-node-script");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(workspace.join("smoke.js"), b"console.log('PALYRA_NODE_SCRIPT_OK');\n")
            .expect("Node smoke script should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["node".to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        let input = br#"{"command":"node","args":["smoke.js"]}"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(20_000))
            .expect("allowlisted Node must execute an explicit workspace script");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("process output should parse");

        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(
            output.get("stdout").and_then(serde_json::Value::as_str),
            Some("PALYRA_NODE_SCRIPT_OK\n")
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_honors_approved_roots_with_preflight_egress() {
        if Command::new("node").arg("--version").output().is_err() {
            return;
        }
        let _guard = PROCESS_ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("process env lock should not be poisoned");
        let workspace = unique_temp_dir("workspace-approved-root-preflight");
        let approved_root = unique_temp_dir("approved-root-preflight");
        let adjacent_root = unique_temp_dir("adjacent-root-preflight");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(approved_root.as_path()).expect("approved root should be created");
        fs::create_dir_all(adjacent_root.as_path()).expect("adjacent root should be created");
        let approved_script = approved_root.join("approved.js");
        let adjacent_script = adjacent_root.join("denied.js");
        fs::write(&approved_script, b"console.log('PALYRA_APPROVED_ROOT_OK');\n")
            .expect("approved Node script should be written");
        fs::write(&adjacent_script, b"console.log('PALYRA_ADJACENT_ROOT_BAD');\n")
            .expect("adjacent Node script should be written");
        let configured_roots =
            std::env::join_paths([approved_root.as_os_str()]).expect("root path should join");
        let _configured_roots = ScopedEnvVar::set(PALYRA_OS_FILE_ROOTS_ENV, configured_roots);
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["node".to_owned()]);
        policy.allow_interpreters = true;
        policy.path_access_mode = PathAccessMode::ApprovedRoots;
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;

        assert!(super::process_runner_allows_host_access(&policy));
        let input = serde_json::to_vec(&serde_json::json!({
            "command": "node",
            "args": [approved_script.to_string_lossy()]
        }))
        .expect("approved-root process input should serialize");
        let result =
            run_constrained_process(&policy, input.as_slice(), Duration::from_millis(20_000))
                .expect("preflight mode should preserve approved-root path authority");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("process output should parse");
        assert_eq!(
            output.get("stdout").and_then(serde_json::Value::as_str),
            Some("PALYRA_APPROVED_ROOT_OK\n")
        );

        let denied_input = serde_json::to_vec(&serde_json::json!({
            "command": "node",
            "args": [adjacent_script.to_string_lossy()]
        }))
        .expect("adjacent-root process input should serialize");
        let error = run_constrained_process(
            &policy,
            denied_input.as_slice(),
            Duration::from_millis(20_000),
        )
        .expect_err("an adjacent unapproved root must remain denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("outside approved host roots"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(approved_root.as_path());
        let _ = fs::remove_dir_all(adjacent_root.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_allows_npm_loopback_dev_server_arguments() {
        if Command::new("npm").arg("--version").output().is_err() {
            return;
        }
        let workspace = unique_temp_dir("workspace-npm-loopback-server");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::write(
            workspace.join("package.json"),
            br#"{"private":true,"scripts":{"dev":"node smoke.js"}}"#,
        )
        .expect("package fixture should be written");
        fs::write(workspace.join("smoke.js"), b"console.log(process.argv.slice(2).join('|'));\n")
            .expect("Node fixture should be written");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["npm".to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::Preflight;
        let input = br#"{
            "command":"npm",
            "args":["run","dev","--","--host","127.0.0.1","--port","5173"],
            "requested_egress_hosts":["127.0.0.1"]
        }"#;

        let result = run_constrained_process(&policy, input, Duration::from_millis(20_000))
            .expect("loopback listen arguments should not require outbound egress authority");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("process output should parse");
        let stdout = output.get("stdout").and_then(serde_json::Value::as_str).unwrap_or_default();
        assert!(stdout.contains("--host|127.0.0.1|--port|5173"), "{stdout}");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_supports_workspace_local_git_workflow_when_available() {
        if Command::new("git").arg("--version").output().is_err() {
            return;
        }
        let workspace = unique_temp_dir("workspace-git-workflow");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        for args in [
            vec!["init", "--quiet"],
            vec!["config", "user.name", "Palyra Test"],
            vec!["config", "user.email", "palyra-test@example.invalid"],
        ] {
            let status = Command::new("git")
                .args(args)
                .current_dir(workspace.as_path())
                .status()
                .expect("Git fixture setup should start");
            assert!(status.success(), "Git fixture setup should succeed");
        }
        fs::write(workspace.join("README.md"), b"initial\n")
            .expect("initial Git fixture should be written");

        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["git".to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        let run_git = |args: &[&str]| {
            let input = serde_json::to_vec(&serde_json::json!({
                "command": "git",
                "args": args
            }))
            .expect("Git process input should serialize");
            let result =
                run_constrained_process(&policy, input.as_slice(), Duration::from_millis(20_000))
                    .expect("allowlisted Git workspace operation should succeed");
            serde_json::from_slice::<serde_json::Value>(&result.output_json)
                .expect("Git process output should parse")
        };

        let status = run_git(&["status", "--short"]);
        assert_eq!(status.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(
            status.get("stdout").and_then(serde_json::Value::as_str),
            Some("?? README.md\n")
        );

        let checkout = run_git(&["checkout", "-b", "e2e-local-commit-smoke"]);
        assert_eq!(checkout.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        fs::write(workspace.join("README.md"), b"initial\nupdated\n")
            .expect("updated Git fixture should be written");
        assert_eq!(
            run_git(&["add", "README.md"]).get("exit_code").and_then(serde_json::Value::as_i64),
            Some(0)
        );
        assert_eq!(
            run_git(&["commit", "-m", "local harness commit"])
                .get("exit_code")
                .and_then(serde_json::Value::as_i64),
            Some(0)
        );
        let show = run_git(&["show", "--stat", "--oneline", "HEAD"]);
        let stdout = show
            .get("stdout")
            .and_then(serde_json::Value::as_str)
            .expect("Git show output should include stdout");
        assert!(stdout.contains("local harness commit"), "{stdout}");
        assert!(stdout.contains("README.md"), "{stdout}");

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_applies_explicit_env_without_shell_wrapper() {
        let Some(python) = ["python3", "python", "py"]
            .into_iter()
            .find(|command| Command::new(command).arg("--version").output().is_ok())
        else {
            return;
        };
        let workspace = unique_temp_dir("workspace-python-env");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        let script = workspace.join("print_env.py");
        fs::write(
            script.as_path(),
            b"import os\nprint(os.environ.get('PALYRA_E2E_HOME', 'missing'))\n",
        )
        .expect("python env fixture should be written");
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["print_env.py"],
            "env": {
                "PALYRA_E2E_HOME": "C:\\Users\\Palo\\AppData\\Local\\Palyra-TestHarness\\home\\S100"
            }
        }))
        .expect("input should serialize");

        let result =
            run_constrained_process(&policy, input.as_slice(), Duration::from_millis(20_000))
                .expect("explicit process env should be applied without shell syntax");
        let output: serde_json::Value =
            serde_json::from_slice(&result.output_json).expect("output should parse");

        assert_eq!(output.get("exit_code").and_then(serde_json::Value::as_i64), Some(0));
        assert_eq!(
            output.get("stdout").and_then(serde_json::Value::as_str),
            Some("C:\\Users\\Palo\\AppData\\Local\\Palyra-TestHarness\\home\\S100\r\n")
        );

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_timeout_terminates_python_process_tree_when_available() {
        let Some(python) = ["python3", "python", "py"]
            .into_iter()
            .find(|command| Command::new(command).arg("--version").output().is_ok())
        else {
            return;
        };
        let workspace = unique_temp_dir("workspace-python-timeout-tree");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec![python.to_owned()]);
        policy.allow_interpreters = true;
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        fs::write(workspace.join("child.py"), b"import time\ntime.sleep(30)\n")
            .expect("python child fixture should be written");
        fs::write(
            workspace.join("parent.py"),
            b"import subprocess, sys, time\n\
              child = subprocess.Popen([sys.executable, 'child.py'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n\
              time.sleep(30)\n",
        )
        .expect("python parent fixture should be written");
        let input = serde_json::to_vec(&serde_json::json!({
            "command": python,
            "args": ["parent.py"],
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
    fn validate_argument_workspace_scope_rejects_compact_short_option_relative_symlink_escape() {
        let workspace = unique_temp_dir("workspace-compact-short-option-symlink");
        let outside = unique_temp_dir("outside-compact-short-option-symlink");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let link_path = workspace.join("backup");
        if let Err(error) = create_directory_symlink(outside.as_path(), link_path.as_path()) {
            eprintln!(
                "skipping compact short option symlink regression because symlink creation failed: {error}"
            );
            let _ = fs::remove_dir_all(workspace.as_path());
            let _ = fs::remove_dir_all(outside.as_path());
            return;
        }
        let canonical_workspace = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-Cbackup".to_owned()];

        let error = validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "tar",
            &args,
        )
        .expect_err("compact short option with relative symlink target must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let nested_args = vec!["-Cbackup/new-output".to_owned()];
        let error = validate_argument_workspace_scope(
            canonical_workspace.as_path(),
            canonical_workspace.as_path(),
            "tar",
            &nested_args,
        )
        .expect_err("compact short option under relative symlink parent must be denied");
        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
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
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
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
    fn collect_requested_egress_hosts_excludes_loopback_listen_targets() {
        let mut input = process_runner_input(
            "npm",
            &[
                "run",
                "dev",
                "--",
                "--host",
                "127.0.0.1",
                "--port",
                "5173",
                "--endpoint",
                "api.example.com",
            ],
            None,
        );
        input.requested_egress_hosts = vec!["127.0.0.1".to_owned()];

        let hosts = collect_requested_egress_hosts(&input)
            .expect("loopback listen host extraction should succeed");

        assert_eq!(hosts, vec!["api.example.com".to_owned()]);
    }

    #[test]
    fn collect_requested_egress_hosts_keeps_remote_host_hints() {
        let mut input =
            process_runner_input("vite", &["--host=preview.example.com", "--port", "5173"], None);
        input.requested_egress_hosts = vec!["localhost".to_owned()];

        let hosts =
            collect_requested_egress_hosts(&input).expect("remote host extraction should succeed");

        assert_eq!(hosts, vec!["localhost".to_owned(), "preview.example.com".to_owned()]);
    }

    #[test]
    fn collect_requested_egress_hosts_extracts_hosts_from_env_values() {
        let input = ProcessRunnerInput {
            command: "uname".to_owned(),
            args: Vec::new(),
            cwd: None,
            env: BTreeMap::from([
                ("APP_ENDPOINT".to_owned(), "https://blocked.example/api".to_owned()),
                ("APP_HOST".to_owned(), "allowed.example:443".to_owned()),
                ("FIXTURE_NAME".to_owned(), "readme.md".to_owned()),
            ]),
            prepend_path: Vec::new(),
            requested_egress_hosts: Vec::new(),
            timeout_ms: None,
            background: false,
            notify_on_complete: false,
            watch_patterns: Vec::new(),
            interactive: false,
            stdin: false,
            pty: false,
            port_hints: Vec::new(),
            lifetime_mode: BackgroundLifetimeMode::RunOwned,
            keep_running_after_run: false,
            env_profile_id: None,
            elevated_intent: false,
            facade_mapping: None,
        };

        let hosts = collect_requested_egress_hosts(&input)
            .expect("env host hint parsing should succeed for valid host values");
        assert!(hosts.iter().any(|host| host == "blocked.example"));
        assert!(hosts.iter().any(|host| host == "allowed.example"));
        assert!(
            !hosts.iter().any(|host| host == "readme.md"),
            "non-host env values should not be collected"
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
                matches!(key, "PATH" | "LANG" | "LC_ALL" | NODE_DISABLE_COMPILE_CACHE_ENV),
                "unexpected environment variable leaked into sandbox process: {line}"
            );
        }
        assert!(stdout.contains("PATH="), "sandbox process should retain deterministic PATH");
        assert!(stdout.contains("LANG=C"), "sandbox process should set LANG=C");
        assert!(stdout.contains("LC_ALL=C"), "sandbox process should set LC_ALL=C");
        assert!(
            stdout.contains("NODE_DISABLE_COMPILE_CACHE=1"),
            "sandbox process should disable Node compile cache"
        );
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
    fn run_constrained_process_runtime_failure_exposes_redacted_child_stderr_preview() {
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
            "runtime failure should remain diagnosable with redacted stderr content: {}",
            error.message
        );
        assert!(
            !error.message.contains(secret_marker),
            "runtime failure message must not leak raw stderr payload"
        );
        assert!(error.message.contains("stderr_preview=Some("), "{}", error.message);
        assert!(
            error.message.contains("<redacted>") || error.message.contains("[REDACTED_SECRET]"),
            "{}",
            error.message
        );
    }

    #[test]
    fn process_stderr_preview_redacts_secret_like_values() {
        let preview = redacted_process_output_preview(
            b"wc: token=abc123: No such file or directory\nnode failed for https://example.com/token/abc123?api_key=qwerty\nfatal: GitHub PAT ghp_12345678901234567890abcdefABCDEF\nnext",
        )
        .expect("preview should be present");

        assert!(preview.contains("<redacted>"), "{preview}");
        assert!(!preview.contains("abc123"), "{preview}");
        assert!(!preview.contains("qwerty"), "{preview}");
        assert!(!preview.contains("ghp_12345678901234567890abcdefABCDEF"), "{preview}");
        assert!(preview.contains("[REDACTED_SECRET]"), "{preview}");
        assert!(preview.contains("node failed"), "{preview}");
    }

    #[test]
    fn process_stderr_preview_redacts_private_key_blocks() {
        let preview = redacted_process_output_preview(
            b"-----BEGIN PRIVATE KEY-----\nMIICUNIQUEPROCESSFAILURESECRET\n-----END PRIVATE KEY-----\n",
        )
        .expect("preview should be present");

        assert!(preview.contains("[REDACTED_SECRET]"), "{preview}");
        assert!(!preview.contains("MIICUNIQUEPROCESSFAILURESECRET"), "{preview}");
        assert!(!preview.contains("PRIVATE KEY"), "{preview}");
    }

    #[test]
    fn process_success_output_summarizes_large_stdout() {
        let stdout = StreamCapture {
            bytes: b"package manager progress line\n".repeat(10_000),
            truncated: false,
            read_error: None,
        };
        let stderr = StreamCapture::default();

        let output = process_success_output_json(ProcessSuccessOutputJsonInput {
            exit_code: 0,
            stdout: &stdout,
            stderr: &stderr,
            duration_ms: 42,
            tier: "b",
            sandbox_backend: "tier_b_in_process",
            process_risk: &empty_process_risk_report(),
            input: None,
        })
        .expect("process output should serialize");
        let rendered = String::from_utf8(output.clone()).expect("output should be utf-8 JSON");
        let parsed: serde_json::Value =
            serde_json::from_slice(output.as_slice()).expect("output should parse");

        assert!(rendered.len() < 16 * 1024, "model-visible JSON should stay small");
        assert!(
            parsed["stdout"].as_str().is_some_and(|value| value.contains("stdout omitted")),
            "large stdout should not stay fully inline: {rendered}"
        );
        assert_eq!(
            parsed.pointer("/streams/stdout/inline_truncated").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert!(
            parsed
                .pointer("/streams/stdout/head")
                .and_then(|v| v.as_str())
                .is_some_and(|value| { value.contains("package manager progress line") }),
            "summary should include head text"
        );
        assert!(
            parsed
                .pointer("/streams/stdout/tail")
                .and_then(|v| v.as_str())
                .is_some_and(|value| { value.contains("package manager progress line") }),
            "summary should include tail text"
        );
    }

    #[test]
    fn process_success_output_omits_binary_stdout() {
        let mut binary = b"API_KEY=secret-value\x00\x7fELF".to_vec();
        binary.extend_from_slice(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        binary.extend_from_slice(&[0; 256]);
        let raw_hex = hex::encode(binary.as_slice());
        let stdout = StreamCapture { bytes: binary, truncated: false, read_error: None };
        let stderr = StreamCapture::default();

        let output = process_success_output_json(ProcessSuccessOutputJsonInput {
            exit_code: 0,
            stdout: &stdout,
            stderr: &stderr,
            duration_ms: 7,
            tier: "b",
            sandbox_backend: "tier_b_in_process",
            process_risk: &empty_process_risk_report(),
            input: None,
        })
        .expect("process output should serialize");
        let rendered = String::from_utf8(output.clone()).expect("output should be utf-8 JSON");
        let parsed: serde_json::Value =
            serde_json::from_slice(output.as_slice()).expect("output should parse");

        assert!(rendered.contains("binary stdout omitted"), "{rendered}");
        assert!(!rendered.contains("\\u0000"), "raw binary escapes must not be model-visible");
        assert_eq!(parsed.pointer("/streams/stdout/binary").and_then(|v| v.as_bool()), Some(true));
        assert_eq!(
            parsed.pointer("/streams/stdout/binary_output_omitted").and_then(|v| v.as_bool()),
            Some(true)
        );
        assert!(parsed.pointer("/streams/stdout/head_hex").is_none());
        assert!(parsed.pointer("/streams/stdout/tail_hex").is_none());
        assert!(!rendered.contains(raw_hex.as_str()), "{rendered}");
        assert!(!rendered.contains("7365637265742d76616c7565"), "{rendered}");
    }

    #[test]
    fn process_progress_omits_binary_tail_bytes() {
        let binary = b"API_KEY=secret-value\x00remaining-binary";

        let tail = process_progress_tail("stdout", binary);

        assert!(tail.contains("binary stdout tail omitted"), "{tail}");
        assert!(!tail.contains("tail_hex"), "{tail}");
        assert!(!tail.contains("7365637265742d76616c7565"), "{tail}");
    }

    #[test]
    fn process_success_output_normalizes_ansi_stdout_as_text() {
        let ansi_table =
            "\r\n\x1b[32;1mName\x1b[0m            \x1b[32;1mMode\x1b[0m\r\nsrc             d----\r\n"
                .repeat(24);
        let stdout =
            StreamCapture { bytes: ansi_table.into_bytes(), truncated: false, read_error: None };
        let stderr = StreamCapture::default();

        let output = process_success_output_json(ProcessSuccessOutputJsonInput {
            exit_code: 0,
            stdout: &stdout,
            stderr: &stderr,
            duration_ms: 11,
            tier: "b",
            sandbox_backend: "tier_b_in_process",
            process_risk: &empty_process_risk_report(),
            input: None,
        })
        .expect("process output should serialize");
        let rendered = String::from_utf8(output.clone()).expect("output should be utf-8 JSON");
        let parsed: serde_json::Value =
            serde_json::from_slice(output.as_slice()).expect("output should parse");
        let stdout = parsed["stdout"].as_str().expect("stdout should be string");

        assert!(!rendered.contains("binary stdout omitted"), "{rendered}");
        assert!(stdout.contains("Name"), "{stdout}");
        assert!(!stdout.contains('\u{1b}'), "{stdout:?}");
        assert_eq!(parsed.pointer("/streams/stdout/binary").and_then(|v| v.as_bool()), Some(false));
        assert_eq!(
            parsed.pointer("/streams/stdout/binary_output_omitted").and_then(|v| v.as_bool()),
            Some(false)
        );
        assert_eq!(
            parsed.pointer("/streams/stdout/encoding").and_then(|v| v.as_str()),
            Some("utf-8")
        );
    }

    #[test]
    fn process_success_output_redacts_ansi_obfuscated_secret() {
        let stdout = StreamCapture {
            bytes: b"MINIMAX_API_\x1b[31mKEY\x1b[0m=sk-\x1b]0;status\x07test-secret-value\n"
                .to_vec(),
            truncated: false,
            read_error: None,
        };
        let stderr = StreamCapture::default();

        let output = process_success_output_json(ProcessSuccessOutputJsonInput {
            exit_code: 0,
            stdout: &stdout,
            stderr: &stderr,
            duration_ms: 3,
            tier: "b",
            sandbox_backend: "tier_b_in_process",
            process_risk: &empty_process_risk_report(),
            input: None,
        })
        .expect("process output should serialize");
        let parsed: serde_json::Value =
            serde_json::from_slice(output.as_slice()).expect("output should parse");
        let stdout = parsed["stdout"].as_str().expect("stdout should be string");
        let stream =
            parsed.pointer("/streams/stdout").expect("stdout stream metadata should be present");

        assert!(stdout.contains("[REDACTED_SECRET]"), "{stdout}");
        assert!(!stdout.contains("sk-test-secret-value"), "{stdout}");
        assert!(!stdout.contains('\u{1b}'), "{stdout:?}");
        assert_eq!(parsed.get("stdout_redacted").and_then(serde_json::Value::as_bool), Some(true));
        assert!(
            stream.get("redaction_reasons").and_then(serde_json::Value::as_array).is_some_and(
                |reasons| {
                    reasons
                        .iter()
                        .any(|reason| reason.as_str() == Some("terminal_control_sequence"))
                }
            ),
            "{stream}"
        );
        assert!(
            !stream
                .get("head")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .contains("sk-test-secret-value"),
            "{stream}"
        );
        assert!(
            !stream
                .get("tail")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .contains("sk-test-secret-value"),
            "{stream}"
        );
    }

    #[test]
    fn process_success_output_decodes_windows_1252_degree_symbol_with_metadata() {
        let stdout = StreamCapture {
            bytes: b"Station 101 mean temperature: -15.5\xb0C\n".to_vec(),
            truncated: false,
            read_error: None,
        };
        let stderr = StreamCapture::default();

        let output = process_success_output_json(ProcessSuccessOutputJsonInput {
            exit_code: 0,
            stdout: &stdout,
            stderr: &stderr,
            duration_ms: 3,
            tier: "b",
            sandbox_backend: "tier_b_in_process",
            process_risk: &empty_process_risk_report(),
            input: None,
        })
        .expect("process output should serialize");
        let parsed: serde_json::Value =
            serde_json::from_slice(output.as_slice()).expect("output should parse");
        let stdout = parsed["stdout"].as_str().expect("stdout should be string");

        assert!(stdout.contains("-15.5°C"), "{stdout}");
        assert!(!stdout.contains('\u{fffd}'), "{stdout}");
        assert_eq!(
            parsed.pointer("/streams/stdout/encoding").and_then(|v| v.as_str()),
            Some("windows-1252")
        );
        assert_eq!(
            parsed.pointer("/streams/stdout/decode_replacement_count").and_then(|v| v.as_u64()),
            Some(0)
        );
    }

    #[test]
    fn process_output_diagnostic_summary_omits_child_content() {
        let mut stdout = b"build progress\n".repeat(2_000);
        let secret_marker = "arbitrary-private-build-context";
        stdout.extend_from_slice(secret_marker.as_bytes());
        let stdout = StreamCapture { bytes: stdout, truncated: false, read_error: None };
        let stderr = StreamCapture::default();

        let summary = process_output_diagnostic_summary(&stdout, &stderr);

        assert!(summary.contains("\"size_bytes\""), "{summary}");
        assert!(summary.contains("\"content_omitted\":true"), "{summary}");
        assert!(!summary.contains(secret_marker), "{summary}");
        assert!(!summary.contains("\"sha256\""), "{summary}");
        assert!(!summary.contains("\"head\""), "{summary}");
        assert!(!summary.contains("\"tail\""), "{summary}");
    }

    #[test]
    fn process_progress_event_reports_tail_and_byte_counts_after_threshold() {
        let started_at =
            Instant::now() - Duration::from_millis(PROCESS_PROGRESS_MIN_ELAPSED_MS + 50);
        let monitor = ProcessProgressMonitor::new(started_at);
        monitor.stdout_capture().record_bytes(b"line one\nline two\nlast visible progress\n");
        monitor.stderr_capture().record_bytes(b"warning tail\n");
        let emitted = Arc::new(Mutex::new(Vec::new()));
        let emitted_for_sink = Arc::clone(&emitted);
        let sink: ProcessProgressSink = Arc::new(move |progress| {
            emitted_for_sink.lock().expect("progress events lock").push(progress);
        });
        let mut last_progress_emitted_at = None;
        let mut last_progress_stdout_bytes = 0;
        let mut last_progress_stderr_bytes = 0;

        maybe_emit_process_progress(
            Some(&sink),
            Some(&monitor),
            1234,
            started_at,
            &mut last_progress_emitted_at,
            &mut last_progress_stdout_bytes,
            &mut last_progress_stderr_bytes,
        );

        let emitted = emitted.lock().expect("progress events lock");
        let progress = emitted.first().expect("progress event should be emitted");
        assert_eq!(progress.pid, 1234);
        assert!(progress.elapsed_ms >= PROCESS_PROGRESS_MIN_ELAPSED_MS);
        assert_eq!(progress.stdout_bytes, 40);
        assert_eq!(progress.stderr_bytes, 13);
        assert!(progress.stdout_tail.contains("last visible progress"), "{progress:?}");
        assert!(progress.stderr_tail.contains("warning tail"), "{progress:?}");
        assert!(progress.last_output_at_ms.is_some(), "{progress:?}");
        assert!(last_progress_emitted_at.is_some());
    }

    #[test]
    fn process_failure_message_exposes_bounded_redacted_stdout_and_stderr() {
        let stdout = StreamCapture {
            bytes: b"arbitrary-private-stdout\naccess_token=stdout-secret".to_vec(),
            truncated: false,
            read_error: None,
        };
        let stderr = StreamCapture {
            bytes: b"arbitrary-private-stderr\ntoken=stderr-secret\n".to_vec(),
            truncated: false,
            read_error: None,
        };

        let message =
            process_failure_message(super::ProcessFailureClass::NonzeroExit, 1, &stdout, &stderr);

        assert!(message.contains("failure_class=nonzero_exit"), "{message}");
        assert!(message.contains("stdout_bytes="), "{message}");
        assert!(message.contains("stderr_bytes="), "{message}");
        assert!(message.contains("stdout_preview=Some("), "{message}");
        assert!(message.contains("stderr_preview=Some("), "{message}");
        assert!(message.contains("arbitrary-private-stdout"), "{message}");
        assert!(message.contains("arbitrary-private-stderr"), "{message}");
        assert!(!message.contains("stdout-secret"), "{message}");
        assert!(!message.contains("stderr-secret"), "{message}");
        assert!(
            message.contains("<redacted>") || message.contains("[REDACTED_SECRET]"),
            "{message}"
        );
    }

    #[test]
    fn process_failure_payload_omits_separated_stderr_secrets() {
        let stdout = StreamCapture::default();
        let stderr = StreamCapture {
            bytes: br#"password: hunter2
api key: qwerty
{"access_token": "abc123"}
"#
            .to_vec(),
            truncated: false,
            read_error: None,
        };
        let error = SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::RuntimeFailure,
            message: process_failure_message(
                super::ProcessFailureClass::NonzeroExit,
                1,
                &stdout,
                &stderr,
            ),
        };

        let payload =
            String::from_utf8(process_failure_output_json(&error, "host_process", "none"))
                .expect("failure output should be UTF-8 JSON");

        for secret in ["hunter2", "qwerty", "abc123"] {
            assert!(!payload.contains(secret), "failure payload leaked {secret}: {payload}");
        }
        assert!(payload.contains("stderr_preview"), "{payload}");
        assert!(
            payload.contains("<redacted>") || payload.contains("[REDACTED_SECRET]"),
            "{payload}"
        );
    }

    #[test]
    fn process_failure_message_omits_secret_suffix_when_marker_precedes_tail() {
        let stdout = StreamCapture { bytes: Vec::new(), truncated: false, read_error: None };
        let mut stderr = b"access_token=".to_vec();
        stderr.extend_from_slice("s".repeat(8 * 1024).as_bytes());
        stderr.extend_from_slice(b"-unique-secret-suffix\n");
        let stderr = StreamCapture { bytes: stderr, truncated: false, read_error: None };

        let message =
            process_failure_message(super::ProcessFailureClass::NonzeroExit, 101, &stdout, &stderr);

        assert!(message.contains("stderr_bytes="), "{message}");
        assert!(message.contains("stderr_preview=Some("), "{message}");
        assert!(!message.contains("stderr_tail="), "{message}");
        assert!(!message.contains("unique-secret-suffix"), "{message}");
        assert!(
            message.contains("<redacted>") || message.contains("[REDACTED_SECRET]"),
            "{message}"
        );
    }

    #[test]
    fn process_failure_message_adds_wsl_no_distribution_hint() {
        let stdout = StreamCapture { bytes: Vec::new(), truncated: false, read_error: None };
        let stderr = StreamCapture {
            bytes: b"The Windows Subsystem for Linux has no installed distributions.\n".to_vec(),
            truncated: false,
            read_error: None,
        };

        let message =
            process_failure_message(super::ProcessFailureClass::NonzeroExit, 1, &stdout, &stderr);

        assert!(message.contains("WSL reports no installed Linux distributions"), "{message}");
        assert!(message.contains("command='pwsh'"), "{message}");
        assert!(message.contains("'-File'"), "{message}");
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
    fn interpreter_guardrails_reject_shell_eval_flags_with_script_file_hint() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args =
            vec!["-NoProfile".to_owned(), "-Command".to_owned(), "Write-Output ok".to_owned()];

        let error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "pwsh",
            args.as_slice(),
        )
        .expect_err("PowerShell inline shell eval must stay blocked");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);
        assert!(error.message.contains("command='pwsh'"), "{}", error.message);
        assert!(error.message.contains("'-File'"), "{}", error.message);
        assert!(error.message.contains("scripts/check.ps1"), "{}", error.message);
    }

    #[test]
    fn interpreter_guardrails_allow_python_module_downstream_config_flag() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![
            "-m".to_owned(),
            "bandit".to_owned(),
            "-c".to_owned(),
            "_test_cache_config.yaml".to_owned(),
            "package".to_owned(),
        ];

        validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "python",
            args.as_slice(),
        )
        .expect("python module application flags after -m <module> should be allowed");
    }

    #[test]
    fn interpreter_guardrails_reject_python_module_eval_command() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![
            "-m".to_owned(),
            "pdb".to_owned(),
            "-c".to_owned(),
            "!__import__('os').system('whoami')".to_owned(),
            "scripts/check.py".to_owned(),
        ];

        let workspace_error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "python",
            args.as_slice(),
        )
        .expect_err("Python module eval commands must stay blocked in workspace mode");
        assert_eq!(workspace_error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(
            workspace_error.message.contains("shell-eval flags"),
            "{}",
            workspace_error.message
        );

        let host_error = validate_host_interpreter_argument_guardrails_with_roots(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "python",
            args.as_slice(),
            std::slice::from_ref(&workspace_root),
        )
        .expect_err("Python module eval commands must stay blocked in host-access mode");
        assert_eq!(host_error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(host_error.message.contains("shell-eval flags"), "{}", host_error.message);
    }

    #[test]
    fn interpreter_guardrails_reject_absolute_python_exe_shell_eval_flags() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec!["-c".to_owned(), "print('blocked')".to_owned()];

        let error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "C:/workspace/.venv/Scripts/python.exe",
            args.as_slice(),
        )
        .expect_err("absolute python.exe interpreter eval must stay blocked");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);
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
    fn process_argument_scope_allows_powershell_file_workspace_script() {
        let workspace = unique_temp_dir("workspace-powershell-file-script");
        let script_dir = workspace.join("scripts");
        fs::create_dir_all(script_dir.as_path()).expect("workspace scripts directory should exist");
        fs::write(script_dir.join("check.ps1"), b"Write-Output 'ok'\n")
            .expect("workspace PowerShell fixture should be written");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args =
            vec!["-NoProfile".to_owned(), "-File".to_owned(), "scripts/check.ps1".to_owned()];

        validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "pwsh",
            args.as_slice(),
        )
        .expect("PowerShell script-file invocation should not be treated as shell eval");
        validate_argument_workspace_scope(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "pwsh",
            args.as_slice(),
        )
        .expect("PowerShell script file should stay inside workspace scope");

        let rewritten = rewrite_arguments_to_scoped_paths(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "pwsh",
            args.as_slice(),
        )
        .expect("PowerShell script path should rewrite to a scoped absolute path");
        assert_eq!(rewritten[0], "-NoProfile");
        assert_eq!(rewritten[1], "-File");
        assert_eq!(
            rewritten[2],
            super::child_process_path(workspace_root.join("scripts").join("check.ps1").as_path())
                .display()
                .to_string()
        );

        let _ = fs::remove_dir_all(workspace.as_path());
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
    fn interpreter_guardrails_reject_inline_node_code_with_relative_paths() {
        let workspace = unique_temp_dir("workspace-node-inline-relative-paths");
        fs::create_dir_all(workspace.join("src").as_path())
            .expect("workspace src directory should be created");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![
            "-e".to_owned(),
            "const fs = require('fs'); const route = '/settings'; const lines = fs.readFileSync('src/reporting.ts', 'utf8').split('\\n'); console.log(route, 'reporting.ts line count:', lines.length);".to_owned(),
        ];

        let error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "node",
            args.as_slice(),
        )
        .expect_err("node inline eval must be blocked even when it only uses relative paths");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn interpreter_guardrails_reject_node_eval_workspace_absolute_fs_path() {
        let workspace = unique_temp_dir("workspace-node-inline-absolute-workspace-path");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let report = workspace.join("report.txt");
        fs::write(report.as_path(), b"ok").expect("workspace fixture should be written");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let report_path = report.to_string_lossy().replace('\\', "/");
        let args =
            vec!["-e".to_owned(), format!("require('fs').readFileSync('{report_path}', 'utf8')")];

        let error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "node",
            args.as_slice(),
        )
        .expect_err("node inline eval must be blocked even for workspace paths");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);

        let _ = fs::remove_dir_all(workspace.as_path());
    }

    #[test]
    fn interpreter_guardrails_reject_node_eval_absolute_fs_path() {
        #[cfg(windows)]
        let outside_path = "C:/Windows/win.ini";
        #[cfg(not(windows))]
        let outside_path = "/etc/passwd";
        let workspace = unique_temp_dir("workspace-node-inline-host-path-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args =
            vec!["-e".to_owned(), format!("require('fs').readFileSync('{outside_path}', 'utf8')")];

        let error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "node",
            args.as_slice(),
        )
        .expect_err("node inline eval must be denied before source path heuristics");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("shell-eval flags"), "{}", error.message);

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
    fn interpreter_guardrails_reject_comma_separated_option_paths_outside_workspace() {
        let workspace = unique_temp_dir("workspace-interpreter-comma-list-deny");
        let outside = unique_temp_dir("outside-interpreter-comma-list-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let inside_component = workspace_root.join("missing");
        let args =
            vec![format!("--allow-read={},{}", inside_component.display(), outside.display())];

        let error = validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "deno",
            args.as_slice(),
        )
        .expect_err("comma-separated interpreter paths must be checked independently");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("absolute path-like substring"));

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(outside.as_path());
    }

    #[test]
    fn host_interpreter_guardrails_reject_comma_separated_paths_outside_approved_roots() {
        let workspace = unique_temp_dir("workspace-host-interpreter-comma-list-deny");
        let approved = unique_temp_dir("approved-host-interpreter-comma-list-deny");
        let outside = unique_temp_dir("outside-host-interpreter-comma-list-deny");
        fs::create_dir_all(workspace.as_path()).expect("workspace directory should be created");
        fs::create_dir_all(approved.as_path()).expect("approved directory should be created");
        fs::create_dir_all(outside.as_path()).expect("outside directory should be created");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let approved_root = approved.canonicalize().expect("approved root should canonicalize");
        let args = vec![format!(
            "--allow-read={},{}",
            approved_root.join("missing").display(),
            outside.display()
        )];

        let error = validate_host_interpreter_argument_guardrails_with_roots(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "deno",
            args.as_slice(),
            &[approved_root],
        )
        .expect_err("comma-separated host paths must stay within approved roots");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::WorkspaceScopeDenied);
        assert!(error.message.contains("outside approved host roots"));

        let _ = fs::remove_dir_all(workspace.as_path());
        let _ = fs::remove_dir_all(approved.as_path());
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
    fn interpreter_guardrails_allow_comma_separated_option_paths_inside_workspace() {
        let workspace = unique_temp_dir("workspace-interpreter-comma-list-allow");
        let inside_one = workspace.join("src");
        let inside_two = workspace.join("fixtures");
        fs::create_dir_all(inside_one.as_path()).expect("first workspace directory should exist");
        fs::create_dir_all(inside_two.as_path()).expect("second workspace directory should exist");
        let workspace_root = canonical_workspace_root(workspace.as_path())
            .expect("workspace root should canonicalize");
        let args = vec![format!("--allow-read={},{}", inside_one.display(), inside_two.display())];

        validate_interpreter_argument_guardrails(
            workspace_root.as_path(),
            workspace_root.as_path(),
            "deno",
            args.as_slice(),
        )
        .expect("comma-separated paths inside the workspace should remain allowed");

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
    fn run_constrained_process_rejects_requested_egress_hosts_in_none_mode() {
        let workspace = std::env::current_dir().expect("workspace current_dir should resolve");
        let mut policy =
            sandbox_policy_with_allowed_executables(workspace, vec!["echo".to_owned()]);
        policy.egress_enforcement_mode = EgressEnforcementMode::None;
        let input = br#"{"command":"echo","args":["ok"],"requested_egress_hosts":["localhost"]}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("explicit requested_egress_hosts must fail closed in none mode");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::EgressDenied);
        assert!(
            error.message.contains("requested_egress_hosts")
                && error.message.contains("egress_enforcement_mode is none"),
            "{}",
            error.message
        );
        assert!(
            error.message.contains("palyra.http.fetch")
                && error.message.contains("browser tools")
                && error.message.contains("enable process-runner preflight"),
            "{}",
            error.message
        );
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
    fn process_spawn_failed_message_explains_daemon_path_lookup() {
        let workspace = PathBuf::from("workspace-root");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["openssl".to_owned()]);
        let input = process_runner_input("openssl", &["version"], None);
        let error = io::Error::new(io::ErrorKind::NotFound, "program not found");

        let message =
            super::process_spawn_failed_message(&policy, &input, workspace.as_path(), &error);

        assert!(message.contains("Runtime=sandbox_tier_b"));
        assert!(message.contains("failure_class=command_not_found"));
        assert!(message.contains("prepend_path=not_provided"));
        assert!(message.contains("daemon sanitized PATH"));
        assert!(message.contains("not the interactive shell PATH"));
        assert!(message.contains("process_runner.allowed_executables"));
    }

    #[test]
    fn process_failure_output_json_classifies_actionable_failures() {
        let timeout = super::SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::TimedOut,
            message: "sandbox process timed out".to_owned(),
        };
        let timeout_output: serde_json::Value = serde_json::from_slice(
            super::process_failure_output_json(&timeout, "sandbox_tier_b", "workspace_only")
                .as_slice(),
        )
        .expect("timeout failure output should parse");
        assert_eq!(timeout_output["failure_class"], "timed_out");
        assert_eq!(timeout_output["cleanup_policy"]["status"], "process_tree_terminated");

        let denied = super::SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::WorkspaceScopeDenied,
            message: "sandbox denied workspace escape".to_owned(),
        };
        assert_eq!(
            super::process_failure_class(&denied),
            super::ProcessFailureClass::SandboxDenied
        );
        let egress = super::SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::EgressDenied,
            message: "egress denied".to_owned(),
        };
        assert_eq!(super::process_failure_class(&egress), super::ProcessFailureClass::EgressDenied);
        let output_limit = super::SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::QuotaExceeded,
            message: "output quota exceeded".to_owned(),
        };
        assert_eq!(
            super::process_failure_class(&output_limit),
            super::ProcessFailureClass::OutputLimit
        );
        let killed = super::SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::Cancelled,
            message: "process cancelled".to_owned(),
        };
        assert_eq!(super::process_failure_class(&killed), super::ProcessFailureClass::Killed);

        let spawn = super::SandboxProcessRunError {
            kind: SandboxProcessRunErrorKind::SpawnFailed,
            message:
                "sandbox process spawn failed (failure_class=permission_denied): access is denied"
                    .to_owned(),
        };
        assert_eq!(
            super::process_failure_class(&spawn),
            super::ProcessFailureClass::PermissionDenied
        );
    }

    #[test]
    #[cfg(windows)]
    fn run_constrained_process_rejects_docker_posix_target_on_windows_host() {
        let workspace = unique_temp_dir("docker-posix-target");
        fs::create_dir_all(workspace.as_path()).expect("workspace should be created");
        fs::write(workspace.join("Dockerfile"), b"FROM debian:bookworm\nWORKDIR /app\n")
            .expect("Dockerfile should be written");
        let policy =
            sandbox_policy_with_allowed_executables(workspace.clone(), vec!["openssl".to_owned()]);
        let input = br#"{"command":"openssl","args":["req","-newkey","rsa:2048","-nodes","-keyout","server.key","-out","server.csr"]}"#;

        let error = run_constrained_process(&policy, input, Duration::from_millis(1_000))
            .expect_err("Docker/Linux POSIX target work must fail before Windows host spawn");

        assert_eq!(error.kind, SandboxProcessRunErrorKind::RuntimeFailure);
        assert!(error.message.contains("error_code=target_runtime_unsupported"));
        assert!(error.message.contains("target_runtime=docker"));
        assert!(error.message.contains("host_runtime=windows"));
        assert!(
            error.message.contains("POSIX file-mode semantics"),
            "error should explain the Linux permission semantic gap"
        );
        assert!(
            !workspace.join("server.key").exists(),
            "rejection should happen before host key material is created"
        );
        fs::remove_dir_all(workspace.as_path()).expect("workspace should be cleaned up");
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
