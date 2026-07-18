//! Trusted Unix ownership-root supervisor for durably registered background processes.
//!
//! The parent registers this process and its process group before any target data is released;
//! a bounded private protocol then starts and monitors the untrusted target. Cleanup proves
//! absence of the target process group; descendants that deliberately leave it are not adopted.

#[cfg(test)]
use std::os::unix::process::ExitStatusExt;
use std::{
    collections::HashSet,
    env,
    ffi::{OsStr, OsString},
    io::{self, Read, Write},
    net::Shutdown,
    os::{
        fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd, RawFd},
        unix::{
            ffi::{OsStrExt, OsStringExt},
            net::UnixStream,
            process::CommandExt,
        },
    },
    path::PathBuf,
    process::{Child, Command},
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

pub(crate) const INTERNAL_PROCESS_SUPERVISOR_ARG: &str = "--palyra-internal-process-supervisor";
const INTERNAL_CLEANUP_HELPER_ARG: &str = "--palyra-internal-process-cleanup-helper";
const INTERNAL_TARGET_LAUNCHER_ARG: &str = "--palyra-internal-process-target-launcher";
#[cfg(test)]
const TEST_PROCESS_SUPERVISOR_ENV: &str = "PALYRA_TEST_INTERNAL_PROCESS_SUPERVISOR";
#[cfg(test)]
const TEST_PROCESS_SUPERVISOR_MODE: &str = "supervisor";
#[cfg(test)]
const TEST_CLEANUP_HELPER_MODE: &str = "cleanup-helper";
#[cfg(test)]
const TEST_TARGET_LAUNCHER_MODE: &str = "target-launcher";
#[cfg(test)]
const TEST_PROCESS_SUPERVISOR_HELPER: &str =
    "unix_process_supervisor::tests::hidden_process_supervisor_helper";
#[cfg(test)]
const TEST_MARKER_ROOT_ENV: &str = "PALYRA_TEST_INTERNAL_SUPERVISOR_MARKER_ROOT";
#[cfg(test)]
const TEST_FAIL_NEXT_CLEANUP_ENV: &str = "PALYRA_TEST_INTERNAL_SUPERVISOR_FAIL_NEXT_CLEANUP";
#[cfg(test)]
const TEST_TARGET_MODE_BLOCK: &str = "block";
#[cfg(test)]
const TEST_TARGET_MODE_SPAWN_DESCENDANT: &str = "spawn-descendant";
#[cfg(test)]
const TEST_TARGET_MODE_SPAWN_DESCENDANT_AND_EXIT: &str = "spawn-descendant-and-exit";
#[cfg(test)]
const TEST_TARGET_MODE_DESCENDANT: &str = "descendant";
#[cfg(test)]
const TEST_TARGET_NATURAL_EXIT_CODE: i32 = 23;

const CONTROL_FD: RawFd = 3;
const MIN_DUPLICATED_FD: libc::c_int = 5;
const PROTOCOL_VERSION: u8 = 1;
const FRAME_HEADER_LEN: usize = 8;
const MAX_FRAME_PAYLOAD_BYTES: usize = 1024 * 1024;
const MAX_STRING_BYTES: usize = 64 * 1024;
const MAX_ARGUMENTS: usize = 512;
const MAX_ENVIRONMENT_ENTRIES: usize = 64;
#[cfg(not(test))]
const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
// Full process-tree regression tests can start slowly on saturated CI hosts.
#[cfg(test)]
const CONTROL_TIMEOUT: Duration = Duration::from_secs(120);
const CLEANUP_WAIT_TIMEOUT: Duration = Duration::from_secs(5);
const MONITOR_POLL_INTERVAL: Duration = Duration::from_millis(10);
const INTERNAL_FAILURE_EXIT_CODE: i32 = 70;

const HELPER_READY: u8 = 0xA1;
const HELPER_START: u8 = 0xA2;
const HELPER_STARTED: u8 = 0xA3;
const HELPER_EXEC_FAILED: u8 = 0xA4;
const HELPER_CLEANUP: u8 = 0xC1;
const HELPER_CLEANUP_COMPLETE: u8 = 0xC2;
const HELPER_CLEANUP_FAILED: u8 = 0xC3;
const HELPER_CANCEL_UNSTARTED: u8 = 0xCA;

const EXEC_STAGE_LAUNCHER_SPAWN: u8 = 1;
const EXEC_STAGE_LAUNCHER_READY: u8 = 2;
const EXEC_STAGE_SPEC_TRANSFER: u8 = 3;
const EXEC_STAGE_TARGET_EXEC: u8 = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum MessageType {
    Ready = 1,
    Spec = 2,
    SpecOk = 3,
    Start = 4,
    Started = 5,
    ExecFailed = 6,
    Terminate = 7,
    CleanupComplete = 8,
    CleanupFailed = 9,
}

impl MessageType {
    fn parse(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::Ready),
            2 => Ok(Self::Spec),
            3 => Ok(Self::SpecOk),
            4 => Ok(Self::Start),
            5 => Ok(Self::Started),
            6 => Ok(Self::ExecFailed),
            7 => Ok(Self::Terminate),
            8 => Ok(Self::CleanupComplete),
            9 => Ok(Self::CleanupFailed),
            _ => Err(invalid_data("supervisor protocol message type is unknown")),
        }
    }

    const fn fixed_payload_len(self) -> Option<usize> {
        match self {
            Self::Ready | Self::Started => Some(4),
            Self::Spec => None,
            Self::SpecOk
            | Self::Start
            | Self::Terminate
            | Self::CleanupComplete
            | Self::CleanupFailed => Some(0),
            Self::ExecFailed => Some(5),
        }
    }
}

#[derive(Debug)]
struct Frame {
    kind: MessageType,
    payload: Vec<u8>,
}

/// Target-only resource limits installed after the supervisor has been durably registered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnixSupervisorLimits {
    pub(crate) cpu_time_limit_ms: u64,
    pub(crate) memory_limit_bytes: u64,
}

/// Frozen, sanitized target launch plan transferred only after durable ownership registration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnixSupervisorLaunchSpec {
    pub(crate) program: OsString,
    pub(crate) args: Vec<OsString>,
    pub(crate) cwd: PathBuf,
    pub(crate) environment: Vec<(OsString, OsString)>,
    pub(crate) limits: Option<UnixSupervisorLimits>,
    pub(crate) lifetime_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParentState {
    Prepared,
    Ready,
    SpecAccepted,
    StartInFlight,
    Started,
    CleanupRequired,
    Terminated,
}

#[derive(Debug)]
struct ParentControlInner {
    stream: UnixStream,
    state: ParentState,
}

/// Parent-side capability controlling the exact trusted supervisor instance.
#[derive(Debug)]
pub(crate) struct UnixProcessSupervisorControl {
    inner: Mutex<ParentControlInner>,
}

impl UnixProcessSupervisorControl {
    /// Prepares a hidden supervisor command and a private parent control capability.
    ///
    /// # Errors
    /// Returns an error when the trusted executable, socket pair, descriptor duplication, or
    /// command setup cannot be established.
    pub(crate) fn prepare(current_executable: PathBuf) -> io::Result<(Command, Self)> {
        let metadata = std::fs::metadata(current_executable.as_path())?;
        if !metadata.is_file() {
            return Err(invalid_input("trusted supervisor executable is not a regular file"));
        }
        let (parent_stream, child_stream) = UnixStream::pair()?;
        set_fd_cloexec(parent_stream.as_raw_fd())?;
        let child_fd = duplicate_fd_cloexec(child_stream.as_raw_fd(), MIN_DUPLICATED_FD)?;
        drop(child_stream);

        let mut command = Command::new(current_executable);
        command.env_clear().current_dir("/");
        configure_supervisor_invocation(&mut command);
        let mut inherited_fd = Some(child_fd);
        // SAFETY: the closure runs after fork and before exec. It uses only async-signal-safe
        // descriptor/process-group syscalls; the one-shot descriptor is consumed by the only spawn.
        unsafe {
            command.pre_exec(move || {
                if libc::setsid() < 0 {
                    return Err(io::Error::last_os_error());
                }
                install_inherited_fd(&mut inherited_fd, CONTROL_FD)
            });
        }
        Ok((
            command,
            Self {
                inner: Mutex::new(ParentControlInner {
                    stream: parent_stream,
                    state: ParentState::Prepared,
                }),
            },
        ))
    }

    /// Waits for the exact supervisor PID and process-group anchor to become ready.
    ///
    /// # Errors
    /// Returns an error on timeout, malformed protocol data, PID mismatch, or missing group anchor.
    pub(crate) fn await_ready(&self, expected_pid: u32) -> io::Result<()> {
        let mut inner = lock_parent_control(&self.inner)?;
        require_parent_state(inner.state, ParentState::Prepared)?;
        let frame = read_frame_expected(&inner.stream, &[MessageType::Ready], deadline())?;
        let reported_pid = decode_pid(frame.payload.as_slice())?;
        if reported_pid != expected_pid {
            return Err(invalid_data("supervisor ready pid mismatch"));
        }
        let expected_pid = pid_t_from_u32(expected_pid)?;
        // SAFETY: getpgid accepts a positive process id and has no pointer arguments.
        let process_group = unsafe { libc::getpgid(expected_pid) };
        if process_group < 0 {
            return Err(io::Error::last_os_error());
        }
        if process_group != expected_pid {
            return Err(invalid_data("supervisor process-group anchor mismatch"));
        }
        // SAFETY: getsid accepts a positive process id and has no pointer arguments.
        let session = unsafe { libc::getsid(expected_pid) };
        if session < 0 {
            return Err(io::Error::last_os_error());
        }
        if session != expected_pid {
            return Err(invalid_data("supervisor session anchor mismatch"));
        }
        inner.state = ParentState::Ready;
        Ok(())
    }

    /// Transfers one validated frozen target plan and waits for supervisor acceptance.
    ///
    /// # Errors
    /// Returns an error when the plan violates protocol bounds or the supervisor rejects/fails to
    /// acknowledge it within the bounded control deadline.
    pub(crate) fn set_launch_spec(&self, spec: UnixSupervisorLaunchSpec) -> io::Result<()> {
        let payload = encode_launch_spec(&spec)?;
        let mut inner = lock_parent_control(&self.inner)?;
        require_parent_state(inner.state, ParentState::Ready)?;
        let operation_deadline = deadline();
        write_frame(&inner.stream, MessageType::Spec, payload.as_slice(), operation_deadline)?;
        let _ = read_frame_expected(&inner.stream, &[MessageType::SpecOk], operation_deadline)?;
        inner.state = ParentState::SpecAccepted;
        Ok(())
    }

    /// Releases the target only after the caller has durably registered the supervisor root.
    ///
    /// # Errors
    /// Returns an error when control fails, the response is malformed, or target exec fails. Exec
    /// errors expose only a fixed stage and raw errno.
    pub(crate) fn start_target(&self) -> io::Result<u32> {
        let mut inner = lock_parent_control(&self.inner)?;
        require_parent_state(inner.state, ParentState::SpecAccepted)?;
        inner.state = ParentState::StartInFlight;
        let operation_deadline = deadline();
        if let Err(error) = write_frame(&inner.stream, MessageType::Start, &[], operation_deadline)
        {
            inner.state = ParentState::CleanupRequired;
            return Err(error);
        }
        let frame = match read_frame_expected(
            &inner.stream,
            &[MessageType::Started, MessageType::ExecFailed, MessageType::CleanupFailed],
            operation_deadline,
        ) {
            Ok(frame) => frame,
            Err(error) => {
                inner.state = ParentState::CleanupRequired;
                return Err(error);
            }
        };
        match frame.kind {
            MessageType::Started => {
                let target_pid = decode_pid(frame.payload.as_slice())?;
                inner.state = ParentState::Started;
                Ok(target_pid)
            }
            MessageType::ExecFailed => {
                inner.state = ParentState::Terminated;
                let (stage, errno) = decode_exec_failure(frame.payload.as_slice())?;
                Err(io::Error::other(format!(
                    "supervisor target exec failed at stage {stage} with errno {errno}"
                )))
            }
            MessageType::CleanupFailed => {
                inner.state = ParentState::CleanupRequired;
                Err(io::Error::other(
                    "supervisor could not prove cleanup after target start failed",
                ))
            }
            _ => Err(invalid_data("supervisor start response is invalid")),
        }
    }

    /// Requests ordered cleanup and waits for the exact supervisor to acknowledge completion.
    ///
    /// Repeated requests are idempotent after a prior acknowledgement. If autonomous cleanup won
    /// the race, the queued acknowledgement is consumed even when the peer already closed.
    /// A timeout or cleanup failure keeps the capability live so the caller can retain ownership
    /// and retry.
    ///
    /// # Errors
    /// Returns an error when the terminate frame cannot be delivered, the response is malformed,
    /// cleanup fails, or the bounded acknowledgement deadline expires.
    pub(crate) fn terminate(&self) -> io::Result<()> {
        let mut inner = lock_parent_control(&self.inner)?;
        if inner.state == ParentState::Terminated {
            return Ok(());
        }
        match inner.state {
            ParentState::Prepared => {
                return Err(invalid_input("supervisor is not ready for termination"));
            }
            ParentState::Ready => {
                return Err(invalid_input("supervisor launch plan is not accepted"));
            }
            ParentState::SpecAccepted
            | ParentState::StartInFlight
            | ParentState::Started
            | ParentState::CleanupRequired => {}
            ParentState::Terminated => return Ok(()),
        }
        let operation_deadline = deadline();
        if let Err(write_error) =
            write_frame(&inner.stream, MessageType::Terminate, &[], operation_deadline)
        {
            return match read_frame_expected(
                &inner.stream,
                &[MessageType::CleanupComplete],
                operation_deadline,
            ) {
                Ok(_) => {
                    inner.state = ParentState::Terminated;
                    Ok(())
                }
                Err(read_error) => {
                    inner.state = ParentState::CleanupRequired;
                    Err(io::Error::other(format!(
                        "supervisor terminate delivery failed: {write_error}; cleanup acknowledgement was unavailable: {read_error}"
                    )))
                }
            };
        }
        let frame = match read_frame_expected(
            &inner.stream,
            &[MessageType::CleanupComplete, MessageType::CleanupFailed],
            operation_deadline,
        ) {
            Ok(frame) => frame,
            Err(error) => {
                inner.state = ParentState::CleanupRequired;
                return Err(error);
            }
        };
        match frame.kind {
            MessageType::CleanupComplete => {
                inner.state = ParentState::Terminated;
                Ok(())
            }
            MessageType::CleanupFailed => {
                inner.state = ParentState::CleanupRequired;
                Err(io::Error::other("supervisor reported incomplete process cleanup"))
            }
            _ => Err(invalid_data("supervisor cleanup response is invalid")),
        }
    }
}

#[cfg(not(test))]
fn configure_supervisor_invocation(command: &mut Command) {
    command.arg(INTERNAL_PROCESS_SUPERVISOR_ARG);
}

#[cfg(test)]
fn configure_supervisor_invocation(command: &mut Command) {
    configure_test_hidden_invocation(command, TEST_PROCESS_SUPERVISOR_MODE);
}

#[cfg(not(test))]
fn configure_cleanup_helper_invocation(command: &mut Command) {
    command.arg(INTERNAL_CLEANUP_HELPER_ARG);
}

#[cfg(test)]
fn configure_cleanup_helper_invocation(command: &mut Command) {
    configure_test_hidden_invocation(command, TEST_CLEANUP_HELPER_MODE);
    if let Some(marker_root) = env::var_os(TEST_MARKER_ROOT_ENV) {
        command.env(TEST_MARKER_ROOT_ENV, marker_root);
    }
}

#[cfg(not(test))]
fn configure_target_launcher_invocation(command: &mut Command) {
    command.arg(INTERNAL_TARGET_LAUNCHER_ARG);
}

#[cfg(test)]
fn configure_target_launcher_invocation(command: &mut Command) {
    configure_test_hidden_invocation(command, TEST_TARGET_LAUNCHER_MODE);
    if let Some(marker_root) = env::var_os(TEST_MARKER_ROOT_ENV) {
        command.env(TEST_MARKER_ROOT_ENV, marker_root);
    }
}

#[cfg(test)]
fn configure_test_hidden_invocation(command: &mut Command, mode: &'static str) {
    command
        .args(["--exact", TEST_PROCESS_SUPERVISOR_HELPER, "--nocapture"])
        .env(TEST_PROCESS_SUPERVISOR_ENV, mode);
}

/// Runs the hidden mode only for the exact private argv shape and exits the process on a match.
///
/// Normal daemon invocations return immediately. Internal failures are deliberately silent.
pub(crate) fn dispatch_if_requested() {
    let mut arguments = env::args_os();
    let _program = arguments.next();
    let Some(mode) = arguments.next() else {
        return;
    };
    if arguments.next().is_some() {
        return;
    }
    if mode == OsStr::new(INTERNAL_PROCESS_SUPERVISOR_ARG) {
        let outcome = run_supervisor().unwrap_or(SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE));
        finish_supervisor(outcome)
    }
    if mode == OsStr::new(INTERNAL_CLEANUP_HELPER_ARG) {
        run_cleanup_helper().unwrap_or_else(|_| {
            // SAFETY: hidden helper failure must not unwind into daemon initialization.
            unsafe { libc::_exit(INTERNAL_FAILURE_EXIT_CODE) }
        })
    }
    if mode == OsStr::new(INTERNAL_TARGET_LAUNCHER_ARG) {
        run_target_launcher().unwrap_or_else(|_| {
            // SAFETY: hidden launcher failure must not unwind into daemon initialization.
            unsafe { libc::_exit(INTERNAL_FAILURE_EXIT_CODE) }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupervisorExit {
    Code(i32),
    Signal(i32),
}

struct SupervisorRuntime {
    cleanup_helper: Option<CleanupHelper>,
    target_outcome: Option<SupervisorExit>,
    cleanup_acknowledged: bool,
    #[cfg(test)]
    fail_next_cleanup: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LauncherExecOutcome {
    Started,
    Failed { stage: u8, errno: i32 },
}

fn run_supervisor() -> io::Result<SupervisorExit> {
    let mut control = control_stream_from_fd3()?;
    let supervisor_pid = current_pid_t()?;
    verify_session_anchor(supervisor_pid, "supervisor")?;
    #[cfg(test)]
    write_test_identity_marker("supervisor.identity", supervisor_pid)?;
    let cleanup_helper = CleanupHelper::spawn(supervisor_pid)?;
    write_frame(
        &control,
        MessageType::Ready,
        &u32::try_from(supervisor_pid)
            .map_err(|_| invalid_data("supervisor pid exceeds protocol range"))?
            .to_be_bytes(),
        deadline(),
    )?;

    let spec_frame = read_frame_expected(&control, &[MessageType::Spec], deadline())?;
    let spec = decode_launch_spec(spec_frame.payload.as_slice())?;
    write_frame(&control, MessageType::SpecOk, &[], deadline())?;
    let command_frame =
        read_frame_expected(&control, &[MessageType::Start, MessageType::Terminate], deadline())?;
    let mut runtime = SupervisorRuntime {
        cleanup_helper: Some(cleanup_helper),
        target_outcome: None,
        cleanup_acknowledged: false,
        #[cfg(test)]
        fail_next_cleanup: env::var_os(TEST_FAIL_NEXT_CLEANUP_ENV).is_some(),
    };
    if command_frame.kind == MessageType::Terminate {
        runtime.cancel_unstarted_helper()?;
        write_frame(&control, MessageType::CleanupComplete, &[], deadline())?;
        return Ok(SupervisorExit::Code(0));
    }

    let target_pid = match runtime.cleanup_helper_mut()?.start_target(&spec, CONTROL_TIMEOUT) {
        Ok(target_pid) => target_pid,
        Err(failure) => {
            let payload = encode_exec_failure(failure.stage, failure.errno);
            match runtime.perform_cleanup() {
                Ok(()) => {
                    write_frame(&control, MessageType::ExecFailed, &payload, deadline())?;
                    return Ok(SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE));
                }
                Err(_) => {
                    let control_open =
                        write_frame(&control, MessageType::CleanupFailed, &[], deadline()).is_ok();
                    return cleanup_retry_loop(
                        &mut control,
                        runtime,
                        SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE),
                        control_open,
                        true,
                    );
                }
            }
        }
    };
    if write_frame(&control, MessageType::Started, &target_pid.to_be_bytes(), deadline()).is_err() {
        let _ = control.shutdown(Shutdown::Both);
        return cleanup_retry_loop(
            &mut control,
            runtime,
            SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE),
            false,
            false,
        );
    }
    if control.set_nonblocking(true).is_err() {
        let _ = control.shutdown(Shutdown::Both);
        return cleanup_retry_loop(
            &mut control,
            runtime,
            SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE),
            false,
            false,
        );
    }

    monitor_target(&mut control, runtime, Duration::from_millis(spec.lifetime_ms))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HelperTargetState {
    Unspawned,
    ArmedRunning { target_pid: libc::pid_t },
    ArmedExitObserved { target_pid: libc::pid_t },
    AuthorityConsumed { target_pid: libc::pid_t },
    AwaitingGroupInactivity { target_pgid: libc::pid_t, raw_wait_status: i32 },
    Settled { raw_wait_status: i32 },
}

struct HelperRuntime {
    control: UnixStream,
    target: Option<Child>,
    state: HelperTargetState,
}

fn run_cleanup_helper() -> io::Result<()> {
    let control = control_stream_from_fd(CONTROL_FD)?;
    restore_waitable_sigchld()?;
    #[cfg(test)]
    write_test_identity_marker("cleanup-helper.identity", current_pid_t()?)?;
    write_all_deadline(&control, &[HELPER_READY], deadline())?;

    let mut command = [0_u8; 1];
    read_exact_deadline(&control, &mut command, deadline())?;
    if command[0] == HELPER_CANCEL_UNSTARTED {
        return Ok(());
    }
    if command[0] != HELPER_START {
        return Err(invalid_data("cleanup helper start command is invalid"));
    }
    let mut encoded_payload_len = [0_u8; 4];
    read_exact_deadline(&control, &mut encoded_payload_len, deadline())?;
    let payload_len = usize::try_from(u32::from_be_bytes(encoded_payload_len))
        .map_err(|_| invalid_data("cleanup helper launch length exceeds platform range"))?;
    if payload_len > MAX_FRAME_PAYLOAD_BYTES {
        return Err(invalid_data("cleanup helper launch plan exceeds maximum size"));
    }
    let mut payload = vec![0_u8; payload_len];
    read_exact_deadline(&control, payload.as_mut_slice(), deadline())?;
    let spec = decode_launch_spec(payload.as_slice())?;

    let mut runtime = HelperRuntime { control, target: None, state: HelperTargetState::Unspawned };
    match runtime.start_target(&spec) {
        Ok(target_pid) => {
            let mut response = [0_u8; 5];
            response[0] = HELPER_STARTED;
            response[1..].copy_from_slice(
                &u32::try_from(target_pid)
                    .map_err(|_| invalid_data("target pid exceeds helper protocol range"))?
                    .to_be_bytes(),
            );
            write_all_deadline(&runtime.control, &response, deadline())?;
        }
        Err(failure) => {
            let mut response = [0_u8; 6];
            response[0] = HELPER_EXEC_FAILED;
            response[1..].copy_from_slice(&encode_exec_failure(failure.stage, failure.errno));
            let control_open = write_all_deadline(&runtime.control, &response, deadline()).is_ok();
            return helper_cleanup_retry_loop(&mut runtime, control_open);
        }
    }

    runtime.control.set_nonblocking(true)?;
    let mut decoder = HelperCommandDecoder;
    loop {
        match runtime.observe_target_exit() {
            Ok(Some(outcome)) => {
                let response = encode_helper_cleanup_response(outcome);
                write_all_deadline(&runtime.control, &response, deadline())?;
                return Ok(());
            }
            Ok(None) => {}
            Err(_) => {
                // Retain the exact child and state so the next pass or cleanup command can retry
                // without recreating or reusing process authority.
            }
        }
        match decoder.read(&mut runtime.control)? {
            HelperCommandRead::Pending => {}
            HelperCommandRead::Cleanup => match runtime.cleanup(CLEANUP_WAIT_TIMEOUT) {
                Ok(outcome) => {
                    let response = encode_helper_cleanup_response(outcome);
                    write_all_deadline(&runtime.control, &response, deadline())?;
                    return Ok(());
                }
                Err(_) => {
                    write_all_deadline(
                        &runtime.control,
                        &[HELPER_CLEANUP_FAILED, 0, 0, 0, 0],
                        deadline(),
                    )?;
                }
            },
            HelperCommandRead::Closed => {
                return helper_cleanup_retry_loop(&mut runtime, false);
            }
        }
        thread::sleep(MONITOR_POLL_INTERVAL);
    }
}

fn helper_cleanup_retry_loop(
    runtime: &mut HelperRuntime,
    mut control_open: bool,
) -> io::Result<()> {
    loop {
        match runtime.cleanup(CLEANUP_WAIT_TIMEOUT) {
            Ok(outcome) => {
                if control_open {
                    let response = encode_helper_cleanup_response(outcome);
                    let _ = write_all_deadline(&runtime.control, &response, deadline());
                }
                return Ok(());
            }
            Err(_) => {
                if control_open {
                    control_open = write_all_deadline(
                        &runtime.control,
                        &[HELPER_CLEANUP_FAILED, 0, 0, 0, 0],
                        deadline(),
                    )
                    .is_ok();
                }
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

impl HelperRuntime {
    fn start_target(
        &mut self,
        spec: &UnixSupervisorLaunchSpec,
    ) -> Result<libc::pid_t, HelperStartFailure> {
        if self.state != HelperTargetState::Unspawned {
            return Err(HelperStartFailure {
                stage: EXEC_STAGE_LAUNCHER_SPAWN,
                errno: libc::EINVAL,
            });
        }
        let current_executable = env::current_exe().map_err(|error| HelperStartFailure {
            stage: EXEC_STAGE_LAUNCHER_SPAWN,
            errno: error.raw_os_error().unwrap_or(libc::EIO),
        })?;
        let (launcher_stream, target_stream) =
            UnixStream::pair().map_err(|error| HelperStartFailure {
                stage: EXEC_STAGE_LAUNCHER_SPAWN,
                errno: error.raw_os_error().unwrap_or(libc::EIO),
            })?;
        set_fd_cloexec(launcher_stream.as_raw_fd()).map_err(|error| HelperStartFailure {
            stage: EXEC_STAGE_LAUNCHER_SPAWN,
            errno: error.raw_os_error().unwrap_or(libc::EIO),
        })?;
        let launcher_fd = duplicate_fd_cloexec(target_stream.as_raw_fd(), MIN_DUPLICATED_FD)
            .map_err(|error| HelperStartFailure {
                stage: EXEC_STAGE_LAUNCHER_SPAWN,
                errno: error.raw_os_error().unwrap_or(libc::EIO),
            })?;
        drop(target_stream);
        let mut command = Command::new(current_executable);
        command
            .env_clear()
            .current_dir("/")
            .stdin(std::process::Stdio::inherit())
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit());
        configure_target_launcher_invocation(&mut command);
        let mut inherited_fd = Some(launcher_fd);
        // SAFETY: the closure runs after fork and before exec. It creates the target session anchor
        // and installs the pre-created private launch socket using async-signal-safe syscalls.
        unsafe {
            command.pre_exec(move || {
                restore_waitable_sigchld()?;
                if libc::setsid() < 0 {
                    return Err(io::Error::last_os_error());
                }
                install_inherited_fd(&mut inherited_fd, CONTROL_FD)
            });
        }
        let child = command.spawn().map_err(|error| HelperStartFailure {
            stage: EXEC_STAGE_LAUNCHER_SPAWN,
            errno: error.raw_os_error().unwrap_or(libc::EIO),
        })?;
        let target_pid = pid_t_from_u32(child.id()).map_err(|error| HelperStartFailure {
            stage: EXEC_STAGE_LAUNCHER_READY,
            errno: error.raw_os_error().unwrap_or(libc::ERANGE),
        })?;
        self.state = HelperTargetState::ArmedRunning { target_pid };
        let ready = read_frame_expected(&launcher_stream, &[MessageType::Ready], deadline())
            .map_err(|error| HelperStartFailure {
                stage: EXEC_STAGE_LAUNCHER_READY,
                errno: error.raw_os_error().unwrap_or(libc::EIO),
            })?;
        let reported_pid =
            decode_pid(ready.payload.as_slice()).and_then(pid_t_from_u32).map_err(|error| {
                HelperStartFailure {
                    stage: EXEC_STAGE_LAUNCHER_READY,
                    errno: error.raw_os_error().unwrap_or(libc::EPROTO),
                }
            })?;
        if reported_pid != target_pid
            || verify_session_anchor(target_pid, "target launcher").is_err()
        {
            return Err(HelperStartFailure {
                stage: EXEC_STAGE_LAUNCHER_READY,
                errno: libc::EPROTO,
            });
        }
        let payload = encode_launch_spec(spec).map_err(|error| HelperStartFailure {
            stage: EXEC_STAGE_SPEC_TRANSFER,
            errno: error.raw_os_error().unwrap_or(libc::EINVAL),
        })?;
        write_frame(&launcher_stream, MessageType::Spec, payload.as_slice(), deadline()).map_err(
            |error| HelperStartFailure {
                stage: EXEC_STAGE_SPEC_TRANSFER,
                errno: error.raw_os_error().unwrap_or(libc::EIO),
            },
        )?;
        self.target = Some(child);
        let outcome = await_launcher_exec_outcome(
            &launcher_stream,
            self.target.as_mut().expect("target child is retained before exec outcome observation"),
        )
        .map_err(|error| HelperStartFailure {
            stage: EXEC_STAGE_TARGET_EXEC,
            errno: error.raw_os_error().unwrap_or(libc::EIO),
        })?;
        match outcome {
            LauncherExecOutcome::Started => Ok(target_pid),
            LauncherExecOutcome::Failed { stage, errno } => {
                Err(HelperStartFailure { stage, errno })
            }
        }
    }

    fn observe_target_exit(&mut self) -> io::Result<Option<SupervisorExit>> {
        let target_pid = match self.state {
            HelperTargetState::ArmedRunning { target_pid } => target_pid,
            HelperTargetState::ArmedExitObserved { target_pid } => target_pid,
            HelperTargetState::AuthorityConsumed { .. }
            | HelperTargetState::AwaitingGroupInactivity { .. }
            | HelperTargetState::Settled { .. } => {
                return self.finish_after_authority_consumed().map(Some);
            }
            HelperTargetState::Unspawned => return Ok(None),
        };
        if matches!(self.state, HelperTargetState::ArmedRunning { .. })
            && !exact_child_exit_observed(target_pid)?
        {
            return Ok(None);
        }
        self.state = HelperTargetState::ArmedExitObserved { target_pid };
        self.cleanup(CLEANUP_WAIT_TIMEOUT).map(Some)
    }

    fn cleanup(&mut self, max_wait: Duration) -> io::Result<SupervisorExit> {
        let target_pid = match self.state {
            HelperTargetState::ArmedRunning { target_pid }
            | HelperTargetState::ArmedExitObserved { target_pid } => target_pid,
            HelperTargetState::AuthorityConsumed { .. }
            | HelperTargetState::AwaitingGroupInactivity { .. }
            | HelperTargetState::Settled { .. } => {
                return self.finish_after_authority_consumed();
            }
            HelperTargetState::Unspawned => {
                return Err(invalid_input("cleanup helper target has not been spawned"));
            }
        };
        // The unreaped exact leader reserves its matching PGID even after a natural exit. Consume
        // that authority exactly once before reaping because live descendants may remain.
        signal_exact_reserved_target_group(target_pid)?;
        self.state = HelperTargetState::AuthorityConsumed { target_pid };
        self.finish_after_authority_consumed_deadline(Instant::now() + max_wait)
    }

    fn finish_after_authority_consumed(&mut self) -> io::Result<SupervisorExit> {
        self.finish_after_authority_consumed_deadline(Instant::now() + CLEANUP_WAIT_TIMEOUT)
    }

    fn finish_after_authority_consumed_deadline(
        &mut self,
        operation_deadline: Instant,
    ) -> io::Result<SupervisorExit> {
        let (target_pid, raw_wait_status) = match self.state {
            HelperTargetState::AuthorityConsumed { target_pid } => {
                let raw_wait_status = reap_exact_target(target_pid, operation_deadline)?;
                self.state = HelperTargetState::AwaitingGroupInactivity {
                    target_pgid: target_pid,
                    raw_wait_status,
                };
                (target_pid, raw_wait_status)
            }
            HelperTargetState::AwaitingGroupInactivity { target_pgid, raw_wait_status } => {
                (target_pgid, raw_wait_status)
            }
            HelperTargetState::Settled { raw_wait_status } => {
                return raw_wait_status_outcome(raw_wait_status);
            }
            HelperTargetState::ArmedRunning { .. }
            | HelperTargetState::ArmedExitObserved { .. } => {
                return Err(invalid_input("nonzero signal authority has not been consumed"));
            }
            HelperTargetState::Unspawned => {
                return Err(invalid_input("cleanup helper target has not been spawned"));
            }
        };
        wait_for_process_group_inactive(target_pid, operation_deadline)?;
        self.target.take();
        self.state = HelperTargetState::Settled { raw_wait_status };
        raw_wait_status_outcome(raw_wait_status)
    }
}

fn await_launcher_exec_outcome(
    stream: &UnixStream,
    launcher: &mut Child,
) -> io::Result<LauncherExecOutcome> {
    let mut header = [0_u8; FRAME_HEADER_LEN];
    match read_exact_deadline_allow_eof(stream, &mut header, deadline())? {
        ExactReadOutcome::Eof => {
            if exact_child_exit_observed(pid_t_from_u32(launcher.id())?)? {
                return Err(io::Error::other(
                    "target launcher exited before successful exec could be established",
                ));
            }
            Ok(LauncherExecOutcome::Started)
        }
        ExactReadOutcome::Complete => {
            let (kind, payload_len) = decode_frame_header(&header)?;
            if kind != MessageType::ExecFailed || payload_len != 5 {
                return Err(invalid_data("target launcher exec response is invalid"));
            }
            let mut payload = [0_u8; 5];
            read_exact_deadline(stream, &mut payload, deadline())?;
            let (stage, errno) = decode_exec_failure(&payload)?;
            Ok(LauncherExecOutcome::Failed { stage, errno })
        }
    }
}

fn exact_child_exit_observed(target_pid: libc::pid_t) -> io::Result<bool> {
    let mut information = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
    let target_id = libc::id_t::try_from(target_pid)
        .map_err(|_| invalid_input("target pid exceeds waitid range"))?;
    loop {
        // SAFETY: target_pid identifies the helper's exact child, information is writable, and
        // WNOWAIT deliberately retains the child as the PID/PGID reservation after observation.
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                target_id,
                information.as_mut_ptr(),
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result == 0 {
            // SAFETY: waitid initializes siginfo_t on success; si_pid==0 denotes no waitable child.
            let information = unsafe { information.assume_init() };
            // SAFETY: libc exposes the platform-correct siginfo accessor.
            return Ok(unsafe { information.si_pid() } == target_pid);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(error);
    }
}

fn signal_exact_reserved_target_group(target_pid: libc::pid_t) -> io::Result<()> {
    if target_pid <= 0 {
        return Err(invalid_input("target pid must be positive"));
    }
    // SAFETY: the helper is the exact parent and has not reaped this session/group leader, so its
    // PID and matching PGID cannot be reused while this sole nonzero signal authority is exercised.
    if unsafe { libc::kill(-target_pid, libc::SIGKILL) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) && exact_child_exit_observed(target_pid)? {
        Ok(())
    } else {
        Err(error)
    }
}

fn reap_exact_target(target_pid: libc::pid_t, operation_deadline: Instant) -> io::Result<i32> {
    loop {
        let mut raw_wait_status = 0;
        // SAFETY: authority was consumed before this call; target_pid is the helper's exact child and
        // the status pointer is writable. Reaping is the irreversible point after which signalling is forbidden.
        let result = unsafe { libc::waitpid(target_pid, &mut raw_wait_status, libc::WNOHANG) };
        if result == target_pid {
            return Ok(raw_wait_status);
        }
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            return Err(error);
        }
        if Instant::now() >= operation_deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "target reap timed out"));
        }
        thread::sleep(MONITOR_POLL_INTERVAL);
    }
}

fn raw_wait_status_outcome(status: i32) -> io::Result<SupervisorExit> {
    if libc::WIFEXITED(status) {
        return Ok(SupervisorExit::Code(libc::WEXITSTATUS(status)));
    }
    if libc::WIFSIGNALED(status) {
        return Ok(SupervisorExit::Signal(libc::WTERMSIG(status)));
    }
    Err(invalid_data("target wait status is not terminal"))
}

fn encode_helper_cleanup_response(outcome: SupervisorExit) -> [u8; 5] {
    let mut response = [0_u8; 5];
    response[0] = HELPER_CLEANUP_COMPLETE;
    let encoded = match outcome {
        SupervisorExit::Code(code) => code,
        SupervisorExit::Signal(signal) => -signal,
    };
    response[1..].copy_from_slice(&encoded.to_be_bytes());
    response
}

fn decode_helper_cleanup_response(response: &[u8; 5]) -> io::Result<SupervisorExit> {
    if response[0] == HELPER_CLEANUP_FAILED {
        return Err(io::Error::other("cleanup helper reported incomplete cleanup"));
    }
    if response[0] != HELPER_CLEANUP_COMPLETE {
        return Err(invalid_data("cleanup helper response is invalid"));
    }
    let encoded = i32::from_be_bytes(
        response[1..]
            .try_into()
            .map_err(|_| invalid_data("cleanup helper outcome length is invalid"))?,
    );
    if encoded >= 0 {
        Ok(SupervisorExit::Code(encoded))
    } else {
        encoded
            .checked_neg()
            .map(SupervisorExit::Signal)
            .ok_or_else(|| invalid_data("cleanup helper signal outcome is invalid"))
    }
}

struct HelperCommandDecoder;

enum HelperCommandRead {
    Pending,
    Cleanup,
    Closed,
}

impl HelperCommandDecoder {
    fn read(&mut self, stream: &mut UnixStream) -> io::Result<HelperCommandRead> {
        let mut command = [0_u8; 1];
        match stream.read(&mut command) {
            Ok(0) => Ok(HelperCommandRead::Closed),
            Ok(1) if command[0] == HELPER_CLEANUP => Ok(HelperCommandRead::Cleanup),
            Ok(1) => Err(invalid_data("cleanup helper command is invalid")),
            Ok(_) => Err(invalid_data("cleanup helper command length is invalid")),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {
                Ok(HelperCommandRead::Pending)
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                Ok(HelperCommandRead::Pending)
            }
            Err(error) => Err(error),
        }
    }
}

#[derive(Default)]
struct HelperOutcomeDecoder {
    buffer: [u8; 5],
    offset: usize,
}

impl HelperOutcomeDecoder {
    fn read(&mut self, stream: &mut UnixStream) -> io::Result<Option<SupervisorExit>> {
        while self.offset < self.buffer.len() {
            match stream.read(&mut self.buffer[self.offset..]) {
                Ok(0) if self.offset == 0 => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "cleanup helper control closed before outcome delivery",
                    ));
                }
                Ok(0) => {
                    return Err(invalid_data(
                        "cleanup helper control closed with a partial outcome",
                    ));
                }
                Ok(read) => self.offset += read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(error) => return Err(error),
            }
        }
        decode_helper_cleanup_response(&self.buffer).map(Some)
    }
}

fn restore_waitable_sigchld() -> io::Result<()> {
    // SAFETY: zero initializes sigaction, then the handler, empty mask, and flags are set explicitly.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = libc::SIG_DFL;
    action.sa_flags = 0;
    // SAFETY: action mask is writable and SIGCHLD accepts a default disposition.
    if unsafe { libc::sigemptyset(&mut action.sa_mask) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the initialized action restores a waitable default SIGCHLD disposition.
    if unsafe { libc::sigaction(libc::SIGCHLD, &action, std::ptr::null_mut()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn install_inherited_fd(inherited: &mut Option<OwnedFd>, target_fd: RawFd) -> io::Result<()> {
    let Some(owned) = inherited.take() else {
        return Err(io::Error::from_raw_os_error(libc::EBADF));
    };
    let source_fd = owned.into_raw_fd();
    // SAFETY: source_fd is a live uniquely owned descriptor and target_fd is reserved for the
    // hidden protocol. dup2 closes any prior target and preserves the source until the explicit close.
    if unsafe { libc::dup2(source_fd, target_fd) } < 0 {
        let error = io::Error::last_os_error();
        // SAFETY: source_fd was detached from OwnedFd and remains owned here.
        unsafe { libc::close(source_fd) };
        return Err(error);
    }
    if source_fd != target_fd {
        // SAFETY: source_fd was duplicated successfully and is no longer needed.
        if unsafe { libc::close(source_fd) } != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn run_target_launcher() -> io::Result<()> {
    let stream = control_stream_from_fd3()?;
    let launcher_pid = current_pid_t()?;
    verify_session_anchor(launcher_pid, "target launcher")?;
    #[cfg(test)]
    write_test_identity_marker("target-launcher.identity", launcher_pid)?;
    write_frame(
        &stream,
        MessageType::Ready,
        &u32::try_from(launcher_pid)
            .map_err(|_| invalid_data("target launcher pid exceeds protocol range"))?
            .to_be_bytes(),
        deadline(),
    )?;
    let spec_frame = read_frame_expected(&stream, &[MessageType::Spec], deadline())?;
    let spec = decode_launch_spec(spec_frame.payload.as_slice())?;
    let mut command = target_command(&spec)?;
    let error = command.exec();
    let payload = encode_exec_failure(EXEC_STAGE_TARGET_EXEC, error.raw_os_error().unwrap_or(0));
    let _ = write_frame(&stream, MessageType::ExecFailed, &payload, deadline());
    Err(error)
}

fn target_command(spec: &UnixSupervisorLaunchSpec) -> io::Result<Command> {
    validate_launch_spec(spec)?;
    let mut command = Command::new(&spec.program);
    command.args(&spec.args).current_dir(&spec.cwd).env_clear();
    for (key, value) in &spec.environment {
        command.env(key, value);
    }
    if let Some(limits) = spec.limits {
        let cpu_seconds = limits.cpu_time_limit_ms.max(1).div_ceil(1000);
        let cpu_limit = rlim_from_u64(cpu_seconds)?;
        let memory_limit = rlim_from_u64(limits.memory_limit_bytes)?;
        // SAFETY: the closure runs after fork and before target exec and only installs precomputed
        // scalar rlimits using async-signal-safe setrlimit calls.
        unsafe {
            command.pre_exec(move || {
                let cpu = libc::rlimit { rlim_cur: cpu_limit, rlim_max: cpu_limit };
                if libc::setrlimit(libc::RLIMIT_CPU, &cpu) != 0 {
                    return Err(io::Error::last_os_error());
                }
                let memory = libc::rlimit { rlim_cur: memory_limit, rlim_max: memory_limit };
                if libc::setrlimit(libc::RLIMIT_AS, &memory) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    Ok(command)
}

fn monitor_target(
    control: &mut UnixStream,
    mut runtime: SupervisorRuntime,
    lifetime: Duration,
) -> io::Result<SupervisorExit> {
    let started_at = Instant::now();
    let mut decoder = PostStartDecoder::default();
    loop {
        let target_status = match runtime.poll_target_exit() {
            Ok(status) => status,
            Err(_) => {
                return finish_cleanup_request(
                    control,
                    runtime,
                    SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE),
                    false,
                    false,
                );
            }
        };
        if let Some(outcome) = target_status {
            return match runtime.perform_cleanup() {
                Ok(()) => {
                    let _ = write_frame(control, MessageType::CleanupComplete, &[], deadline());
                    Ok(outcome)
                }
                Err(_) => cleanup_retry_loop(control, runtime, outcome, true, false),
            };
        }
        let helper_exited = match runtime.helper_has_exited() {
            Ok(exited) => exited,
            Err(_) => {
                return finish_cleanup_request(
                    control,
                    runtime,
                    SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE),
                    false,
                    false,
                );
            }
        };
        if helper_exited {
            return match runtime.perform_cleanup() {
                Ok(()) => Ok(SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE)),
                Err(_) => cleanup_retry_loop(
                    control,
                    runtime,
                    SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE),
                    true,
                    false,
                ),
            };
        }
        if started_at.elapsed() >= lifetime {
            return finish_cleanup_request(
                control,
                runtime,
                SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE),
                false,
                false,
            );
        }
        match decoder.read(control) {
            Ok(PostStartRead::Pending) => {}
            Ok(PostStartRead::Terminate) => {
                return finish_cleanup_request(
                    control,
                    runtime,
                    SupervisorExit::Code(0),
                    true,
                    true,
                );
            }
            Ok(PostStartRead::Closed) | Err(_) => {
                let _ = control.shutdown(Shutdown::Both);
                return finish_cleanup_request(
                    control,
                    runtime,
                    SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE),
                    false,
                    false,
                );
            }
        }
        thread::sleep(MONITOR_POLL_INTERVAL);
    }
}

fn finish_cleanup_request(
    control: &mut UnixStream,
    mut runtime: SupervisorRuntime,
    fallback_outcome: SupervisorExit,
    acknowledge: bool,
    retry_requires_command: bool,
) -> io::Result<SupervisorExit> {
    match runtime.perform_cleanup() {
        Ok(()) => {
            if acknowledge {
                write_frame(control, MessageType::CleanupComplete, &[], deadline())?;
            }
            Ok(runtime.target_outcome.unwrap_or(fallback_outcome))
        }
        Err(_) => {
            let control_open = if acknowledge {
                write_frame(control, MessageType::CleanupFailed, &[], deadline()).is_ok()
            } else {
                false
            };
            cleanup_retry_loop(
                control,
                runtime,
                fallback_outcome,
                control_open,
                retry_requires_command,
            )
        }
    }
}

fn cleanup_retry_loop(
    control: &mut UnixStream,
    mut runtime: SupervisorRuntime,
    fallback_outcome: SupervisorExit,
    mut control_open: bool,
    mut retry_requires_command: bool,
) -> io::Result<SupervisorExit> {
    let _ = control.set_nonblocking(true);
    let mut decoder = PostStartDecoder::default();
    loop {
        if !control_open {
            if runtime.perform_cleanup().is_ok() {
                return Ok(runtime.target_outcome.unwrap_or(fallback_outcome));
            }
            thread::sleep(Duration::from_secs(1));
            continue;
        }
        if !retry_requires_command {
            match runtime.perform_cleanup() {
                Ok(()) => {
                    write_frame(control, MessageType::CleanupComplete, &[], deadline())?;
                    return Ok(runtime.target_outcome.unwrap_or(fallback_outcome));
                }
                Err(_) => {
                    thread::sleep(Duration::from_secs(1));
                    continue;
                }
            }
        }
        match decoder.read(control) {
            Ok(PostStartRead::Pending) => {}
            Ok(PostStartRead::Terminate) => {
                retry_requires_command = true;
                match runtime.perform_cleanup() {
                    Ok(()) => {
                        write_frame(control, MessageType::CleanupComplete, &[], deadline())?;
                        return Ok(runtime.target_outcome.unwrap_or(fallback_outcome));
                    }
                    Err(_) => {
                        if write_frame(control, MessageType::CleanupFailed, &[], deadline())
                            .is_err()
                        {
                            control_open = false;
                            let _ = control.shutdown(Shutdown::Both);
                        }
                    }
                }
            }
            Ok(PostStartRead::Closed) | Err(_) => {
                control_open = false;
                let _ = control.shutdown(Shutdown::Both);
            }
        }
        thread::sleep(MONITOR_POLL_INTERVAL);
    }
}

impl SupervisorRuntime {
    fn cleanup_helper_mut(&mut self) -> io::Result<&mut CleanupHelper> {
        self.cleanup_helper
            .as_mut()
            .ok_or_else(|| io::Error::other("cleanup helper capability is unavailable"))
    }

    fn poll_target_exit(&mut self) -> io::Result<Option<SupervisorExit>> {
        if self.target_outcome.is_some() {
            return Ok(self.target_outcome);
        }
        self.target_outcome = self.cleanup_helper_mut()?.poll_outcome()?;
        Ok(self.target_outcome)
    }

    fn perform_cleanup(&mut self) -> io::Result<()> {
        if self.cleanup_acknowledged {
            return Ok(());
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_cleanup) {
            let marker_result = write_test_marker("cleanup-fault-consumed", b"1\n");
            return Err(marker_result.err().unwrap_or_else(|| {
                io::Error::other("injected one-shot supervisor cleanup failure")
            }));
        }
        let operation_deadline = Instant::now() + CLEANUP_WAIT_TIMEOUT;
        let outcome =
            self.cleanup_helper_mut()?.request_cleanup(remaining_timeout(operation_deadline)?)?;
        self.target_outcome = self.target_outcome.or(Some(outcome));
        let mut helper = self
            .cleanup_helper
            .take()
            .ok_or_else(|| io::Error::other("cleanup helper capability is unavailable"))?;
        helper.reap(remaining_timeout(operation_deadline)?)?;
        self.cleanup_acknowledged = true;
        Ok(())
    }

    fn cancel_unstarted_helper(&mut self) -> io::Result<()> {
        let mut helper = self
            .cleanup_helper
            .take()
            .ok_or_else(|| io::Error::other("cleanup helper capability is unavailable"))?;
        helper.cancel_unstarted(CLEANUP_WAIT_TIMEOUT)?;
        self.cleanup_acknowledged = true;
        Ok(())
    }

    fn helper_has_exited(&mut self) -> io::Result<bool> {
        match self.cleanup_helper.as_mut() {
            Some(helper) => helper.has_exited(),
            None => Ok(self.cleanup_acknowledged),
        }
    }
}

fn process_group_is_active(process_group_id: libc::pid_t) -> io::Result<bool> {
    if process_group_id <= 0 {
        return Err(invalid_input("target process-group id must be positive"));
    }
    let process_group_id = u32::try_from(process_group_id)
        .map_err(|_| invalid_input("target process-group id exceeds supported range"))?;
    crate::sandbox_runner::unix_process_group_is_alive(process_group_id)
}

fn wait_for_process_group_inactive(
    process_group_id: libc::pid_t,
    operation_deadline: Instant,
) -> io::Result<()> {
    while process_group_is_active(process_group_id)? {
        if Instant::now() >= operation_deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "target process-group cleanup timed out",
            ));
        }
        thread::sleep(MONITOR_POLL_INTERVAL);
    }
    Ok(())
}

fn finish_supervisor(outcome: SupervisorExit) -> ! {
    match outcome {
        SupervisorExit::Code(code) => std::process::exit(code),
        SupervisorExit::Signal(signal) => mirror_signal(signal),
    }
}

fn mirror_signal(signal: i32) -> ! {
    // SAFETY: signal values come from wait status. Restoring the default disposition, unblocking,
    // and signalling this single-threaded supervisor reproduces the target's termination status.
    unsafe {
        libc::signal(signal, libc::SIG_DFL);
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, signal);
        libc::sigprocmask(libc::SIG_UNBLOCK, &set, std::ptr::null_mut());
        libc::kill(libc::getpid(), signal);
        libc::_exit(128_i32.saturating_add(signal));
    }
}

struct CleanupHelper {
    stream: UnixStream,
    pid: libc::pid_t,
    supervisor_pgid: libc::pid_t,
    target: CleanupHelperTarget,
    outcome_decoder: HelperOutcomeDecoder,
    pending_outcome: Option<SupervisorExit>,
    settled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HelperStartFailure {
    stage: u8,
    errno: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CleanupHelperTarget {
    Unstarted,
    StartInFlight,
    Started(libc::pid_t),
}

impl CleanupHelper {
    fn spawn(supervisor_pgid: libc::pid_t) -> io::Result<Self> {
        let current_executable = env::current_exe()?;
        let (supervisor_stream, helper_stream) = UnixStream::pair()?;
        set_fd_cloexec(supervisor_stream.as_raw_fd())?;
        let helper_control_fd = duplicate_fd_cloexec(helper_stream.as_raw_fd(), MIN_DUPLICATED_FD)?;
        drop(helper_stream);

        let mut command = Command::new(current_executable);
        command.env_clear().current_dir("/");
        configure_cleanup_helper_invocation(&mut command);
        let mut inherited_control_fd = Some(helper_control_fd);
        // SAFETY: the closure runs after fork and before exec. It only restores SIGCHLD and installs
        // the pre-created private control socket using async-signal-safe syscalls.
        unsafe {
            command.pre_exec(move || {
                restore_waitable_sigchld()?;
                install_inherited_fd(&mut inherited_control_fd, CONTROL_FD)
            });
        }
        let helper = command.spawn()?;
        let helper_pid = pid_t_from_u32(helper.id())?;
        drop(helper);

        let mut ready = [0_u8; 1];
        if let Err(error) = read_exact_deadline(&supervisor_stream, &mut ready, deadline()) {
            let _ = kill_and_reap_child(helper_pid, CLEANUP_WAIT_TIMEOUT);
            return Err(error);
        }
        if ready[0] != HELPER_READY {
            let _ = kill_and_reap_child(helper_pid, CLEANUP_WAIT_TIMEOUT);
            return Err(invalid_data("cleanup helper readiness failed"));
        }
        // SAFETY: getpgid accepts the exact positive helper pid returned by spawn.
        let helper_pgid = unsafe { libc::getpgid(helper_pid) };
        if helper_pgid != supervisor_pgid {
            let _ = kill_and_reap_child(helper_pid, CLEANUP_WAIT_TIMEOUT);
            return Err(invalid_data("cleanup helper left the supervisor ownership group"));
        }
        let helper = Self {
            stream: supervisor_stream,
            pid: helper_pid,
            supervisor_pgid,
            target: CleanupHelperTarget::Unstarted,
            outcome_decoder: HelperOutcomeDecoder::default(),
            pending_outcome: None,
            settled: false,
        };
        #[cfg(test)]
        write_test_identity_marker("cleanup-helper.identity", helper_pid)?;
        Ok(helper)
    }

    fn start_target(
        &mut self,
        spec: &UnixSupervisorLaunchSpec,
        max_wait: Duration,
    ) -> Result<u32, HelperStartFailure> {
        if self.settled || self.target != CleanupHelperTarget::Unstarted {
            return Err(HelperStartFailure {
                stage: EXEC_STAGE_LAUNCHER_SPAWN,
                errno: libc::EINVAL,
            });
        }
        let payload = encode_launch_spec(spec).map_err(|error| HelperStartFailure {
            stage: EXEC_STAGE_SPEC_TRANSFER,
            errno: error.raw_os_error().unwrap_or(libc::EINVAL),
        })?;
        let operation_deadline = Instant::now() + max_wait;
        let mut request = Vec::with_capacity(5 + payload.len());
        request.push(HELPER_START);
        request.extend_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| HelperStartFailure {
                    stage: EXEC_STAGE_SPEC_TRANSFER,
                    errno: libc::EOVERFLOW,
                })?
                .to_be_bytes(),
        );
        request.extend_from_slice(payload.as_slice());
        write_all_deadline(&self.stream, request.as_slice(), operation_deadline).map_err(
            |error| HelperStartFailure {
                stage: EXEC_STAGE_SPEC_TRANSFER,
                errno: error.raw_os_error().unwrap_or(libc::EIO),
            },
        )?;
        // A lost response is ambiguous once the complete start request reached the helper. Preserve
        // the exact helper capability so cleanup retries can settle any target it may have spawned.
        self.target = CleanupHelperTarget::StartInFlight;
        let mut response_type = [0_u8; 1];
        read_exact_deadline(&self.stream, &mut response_type, operation_deadline).map_err(
            |error| HelperStartFailure {
                stage: EXEC_STAGE_TARGET_EXEC,
                errno: error.raw_os_error().unwrap_or(libc::EIO),
            },
        )?;
        match response_type[0] {
            HELPER_STARTED => {
                let mut encoded_pid = [0_u8; 4];
                read_exact_deadline(&self.stream, &mut encoded_pid, operation_deadline).map_err(
                    |error| HelperStartFailure {
                        stage: EXEC_STAGE_LAUNCHER_READY,
                        errno: error.raw_os_error().unwrap_or(libc::EIO),
                    },
                )?;
                let target_pid = decode_pid(&encoded_pid).map_err(|error| HelperStartFailure {
                    stage: EXEC_STAGE_LAUNCHER_READY,
                    errno: error.raw_os_error().unwrap_or(libc::EPROTO),
                })?;
                let target_pid_t =
                    pid_t_from_u32(target_pid).map_err(|error| HelperStartFailure {
                        stage: EXEC_STAGE_LAUNCHER_READY,
                        errno: error.raw_os_error().unwrap_or(libc::ERANGE),
                    })?;
                if target_pid_t == self.supervisor_pgid {
                    return Err(HelperStartFailure {
                        stage: EXEC_STAGE_LAUNCHER_READY,
                        errno: libc::EPROTO,
                    });
                }
                self.target = CleanupHelperTarget::Started(target_pid_t);
                #[cfg(test)]
                write_test_marker("cleanup-helper.armed", format!("{target_pid_t}\n").as_bytes())
                    .map_err(|error| HelperStartFailure {
                    stage: EXEC_STAGE_LAUNCHER_READY,
                    errno: error.raw_os_error().unwrap_or(libc::EIO),
                })?;
                Ok(target_pid)
            }
            HELPER_EXEC_FAILED => {
                let mut payload = [0_u8; 5];
                read_exact_deadline(&self.stream, &mut payload, operation_deadline).map_err(
                    |error| HelperStartFailure {
                        stage: EXEC_STAGE_TARGET_EXEC,
                        errno: error.raw_os_error().unwrap_or(libc::EIO),
                    },
                )?;
                let (stage, errno) =
                    decode_exec_failure(&payload).map_err(|error| HelperStartFailure {
                        stage: EXEC_STAGE_TARGET_EXEC,
                        errno: error.raw_os_error().unwrap_or(libc::EPROTO),
                    })?;
                Err(HelperStartFailure { stage, errno })
            }
            _ => Err(HelperStartFailure { stage: EXEC_STAGE_TARGET_EXEC, errno: libc::EPROTO }),
        }
    }

    fn poll_outcome(&mut self) -> io::Result<Option<SupervisorExit>> {
        if let Some(outcome) = self.pending_outcome {
            return Ok(Some(outcome));
        }
        self.stream.set_nonblocking(true)?;
        let read = self.outcome_decoder.read(&mut self.stream);
        self.stream.set_nonblocking(false)?;
        let outcome = read?;
        if let Some(outcome) = outcome {
            self.pending_outcome = Some(outcome);
        }
        Ok(outcome)
    }

    fn request_cleanup(&mut self, max_wait: Duration) -> io::Result<SupervisorExit> {
        if self.settled {
            return self.pending_outcome.ok_or_else(|| {
                io::Error::new(io::ErrorKind::BrokenPipe, "cleanup helper has already exited")
            });
        }
        if let Some(outcome) = self.pending_outcome {
            return Ok(outcome);
        }
        if self.target == CleanupHelperTarget::Unstarted {
            return Err(invalid_input("cleanup helper has no target"));
        }
        let operation_deadline = Instant::now() + max_wait;
        if let Err(write_error) =
            write_all_deadline(&self.stream, &[HELPER_CLEANUP], operation_deadline)
        {
            // Autonomous cleanup may have completed and closed the peer while its outcome remains
            // readable. Consume that exact result before treating command delivery as a failure.
            return self.read_outcome_deadline(operation_deadline).map_err(|read_error| {
                io::Error::other(format!(
                    "cleanup helper command delivery failed: {write_error}; cleanup outcome was unavailable: {read_error}"
                ))
            });
        }
        self.read_outcome_deadline(operation_deadline)
    }

    fn read_outcome_deadline(&mut self, operation_deadline: Instant) -> io::Result<SupervisorExit> {
        self.stream.set_nonblocking(true)?;
        loop {
            match self.outcome_decoder.read(&mut self.stream) {
                Ok(Some(outcome)) => {
                    self.stream.set_nonblocking(false)?;
                    self.pending_outcome = Some(outcome);
                    return Ok(outcome);
                }
                Ok(None) => {
                    if Instant::now() >= operation_deadline {
                        self.stream.set_nonblocking(false)?;
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "cleanup helper outcome timed out",
                        ));
                    }
                    thread::sleep(MONITOR_POLL_INTERVAL);
                }
                Err(error) => {
                    self.stream.set_nonblocking(false)?;
                    return Err(error);
                }
            }
        }
    }

    fn reap(&mut self, max_wait: Duration) -> io::Result<()> {
        if self.settled {
            return Ok(());
        }
        waitpid_deadline(self.pid, Instant::now() + max_wait)?;
        self.settled = true;
        #[cfg(test)]
        write_test_marker("cleanup-helper.reaped", format!("{}\n", self.pid).as_bytes())?;
        Ok(())
    }

    fn cancel_unstarted(&mut self, max_wait: Duration) -> io::Result<()> {
        if self.settled {
            return Ok(());
        }
        if self.target != CleanupHelperTarget::Unstarted {
            return Err(invalid_input("cleanup helper target start is already in progress"));
        }
        let operation_deadline = Instant::now() + max_wait;
        write_all_deadline(&self.stream, &[HELPER_CANCEL_UNSTARTED], operation_deadline)?;
        waitpid_deadline(self.pid, operation_deadline)?;
        self.settled = true;
        #[cfg(test)]
        write_test_marker("cleanup-helper.reaped", format!("{}\n", self.pid).as_bytes())?;
        Ok(())
    }

    fn has_exited(&mut self) -> io::Result<bool> {
        if self.settled {
            return Ok(true);
        }
        let mut status = 0;
        // SAFETY: pid is the exact helper child returned by spawn and status is writable.
        let result = unsafe { libc::waitpid(self.pid, &mut status, libc::WNOHANG) };
        if result == 0 {
            return Ok(false);
        }
        if result == self.pid {
            self.settled = true;
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EINTR) {
            Ok(false)
        } else if error.raw_os_error() == Some(libc::ECHILD) {
            self.settled = true;
            Ok(true)
        } else {
            Err(error)
        }
    }
}

impl Drop for CleanupHelper {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let _ = kill_and_reap_child(self.pid, CLEANUP_WAIT_TIMEOUT);
        self.settled = true;
    }
}

fn waitpid_deadline(pid: libc::pid_t, operation_deadline: Instant) -> io::Result<()> {
    loop {
        let mut status = 0;
        // SAFETY: pid is the exact child returned by fork and status points to writable storage.
        let result = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
        if result == pid {
            return Ok(());
        }
        if result < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            if error.raw_os_error() == Some(libc::ECHILD) {
                return Ok(());
            }
            return Err(error);
        }
        if Instant::now() >= operation_deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "cleanup helper reap timed out"));
        }
        thread::sleep(MONITOR_POLL_INTERVAL);
    }
}

fn kill_and_reap_child(pid: libc::pid_t, max_wait: Duration) -> io::Result<()> {
    // SAFETY: pid is the exact helper child returned by fork; ESRCH is an idempotent outcome.
    if unsafe { libc::kill(pid, libc::SIGKILL) } != 0 {
        let error = io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error);
        }
    }
    waitpid_deadline(pid, Instant::now() + max_wait)
}

fn verify_session_anchor(pid: libc::pid_t, subject: &'static str) -> io::Result<()> {
    if pid <= 0 {
        return Err(invalid_data("process anchor pid is invalid"));
    }
    // SAFETY: getpgid and getsid accept a positive process id and have no pointer arguments.
    let process_group = unsafe { libc::getpgid(pid) };
    if process_group < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pid is positive and kernel-owned identity lookup has no pointer arguments.
    let session = unsafe { libc::getsid(pid) };
    if session < 0 {
        return Err(io::Error::last_os_error());
    }
    if process_group == pid && session == pid {
        Ok(())
    } else {
        Err(invalid_data(match subject {
            "supervisor" => "supervisor is not its session and process-group anchor",
            _ => "target launcher is not its session and process-group anchor",
        }))
    }
}

fn control_stream_from_fd3() -> io::Result<UnixStream> {
    control_stream_from_fd(CONTROL_FD)
}

fn control_stream_from_fd(fd: RawFd) -> io::Result<UnixStream> {
    validate_unix_stream_fd(fd)?;
    set_fd_cloexec(fd)?;
    // SAFETY: exact hidden modes receive unique ownership of each inherited private descriptor.
    Ok(unsafe { UnixStream::from_raw_fd(fd) })
}

fn validate_unix_stream_fd(fd: RawFd) -> io::Result<()> {
    let mut socket_type: libc::c_int = 0;
    let mut type_len = libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>())
        .map_err(|_| invalid_data("socket type length exceeds platform range"))?;
    // SAFETY: pointers refer to initialized writable storage of the supplied length.
    if unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_TYPE,
            (&mut socket_type as *mut libc::c_int).cast(),
            &mut type_len,
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if socket_type != libc::SOCK_STREAM {
        return Err(invalid_data("supervisor control fd is not a stream socket"));
    }
    // SAFETY: zero is a valid initial representation for sockaddr_un output storage.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    let mut address_len = libc::socklen_t::try_from(std::mem::size_of::<libc::sockaddr_un>())
        .map_err(|_| invalid_data("Unix socket address length exceeds platform range"))?;
    // SAFETY: address and length are valid writable getsockname outputs.
    if unsafe {
        libc::getsockname(fd, (&mut address as *mut libc::sockaddr_un).cast(), &mut address_len)
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    if libc::c_int::from(address.sun_family) != libc::AF_UNIX {
        return Err(invalid_data("supervisor control fd is not an AF_UNIX socket"));
    }
    Ok(())
}

#[derive(Default)]
struct PostStartDecoder {
    buffer: Vec<u8>,
}

enum PostStartRead {
    Pending,
    Terminate,
    Closed,
}

impl PostStartDecoder {
    fn read(&mut self, stream: &mut UnixStream) -> io::Result<PostStartRead> {
        let mut chunk = [0_u8; 4096];
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => {
                    if self.buffer.is_empty() {
                        return Ok(PostStartRead::Closed);
                    }
                    return Err(invalid_data("supervisor control closed with a partial frame"));
                }
                Ok(read) => {
                    let next_len =
                        self.buffer.len().checked_add(read).ok_or_else(|| {
                            invalid_data("supervisor control buffer length overflow")
                        })?;
                    if next_len > FRAME_HEADER_LEN + MAX_FRAME_PAYLOAD_BYTES {
                        return Err(invalid_data("supervisor control frame exceeds maximum size"));
                    }
                    self.buffer.extend_from_slice(&chunk[..read]);
                    if let Some(frame) = decode_buffered_frame(self.buffer.as_slice())? {
                        if frame.0 != self.buffer.len() {
                            return Err(invalid_data(
                                "supervisor control contains trailing frames",
                            ));
                        }
                        if frame.1.kind != MessageType::Terminate || !frame.1.payload.is_empty() {
                            return Err(invalid_data("supervisor post-start command is invalid"));
                        }
                        return Ok(PostStartRead::Terminate);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(PostStartRead::Pending);
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

fn encode_launch_spec(spec: &UnixSupervisorLaunchSpec) -> io::Result<Vec<u8>> {
    validate_launch_spec(spec)?;
    let mut payload = Vec::new();
    push_os_string(&mut payload, &spec.program)?;
    push_count(&mut payload, spec.args.len())?;
    for argument in &spec.args {
        push_os_string(&mut payload, argument)?;
    }
    push_os_string(&mut payload, spec.cwd.as_os_str())?;
    push_count(&mut payload, spec.environment.len())?;
    for (key, value) in &spec.environment {
        push_os_string(&mut payload, key)?;
        push_os_string(&mut payload, value)?;
    }
    match spec.limits {
        Some(limits) => {
            payload.push(1);
            payload.extend_from_slice(&limits.cpu_time_limit_ms.to_be_bytes());
            payload.extend_from_slice(&limits.memory_limit_bytes.to_be_bytes());
        }
        None => payload.push(0),
    }
    payload.extend_from_slice(&spec.lifetime_ms.to_be_bytes());
    if payload.len() > MAX_FRAME_PAYLOAD_BYTES {
        return Err(invalid_input("supervisor launch plan exceeds maximum frame size"));
    }
    Ok(payload)
}

fn decode_launch_spec(payload: &[u8]) -> io::Result<UnixSupervisorLaunchSpec> {
    let mut cursor = PayloadCursor::new(payload);
    let program = cursor.read_os_string()?;
    let argument_count = cursor.read_count(MAX_ARGUMENTS, "argument count exceeds limit")?;
    let mut args = Vec::with_capacity(argument_count);
    for _ in 0..argument_count {
        args.push(cursor.read_os_string()?);
    }
    let cwd = PathBuf::from(cursor.read_os_string()?);
    let environment_count =
        cursor.read_count(MAX_ENVIRONMENT_ENTRIES, "environment count exceeds limit")?;
    let mut environment = Vec::with_capacity(environment_count);
    for _ in 0..environment_count {
        environment.push((cursor.read_os_string()?, cursor.read_os_string()?));
    }
    let limits = match cursor.read_u8()? {
        0 => None,
        1 => Some(UnixSupervisorLimits {
            cpu_time_limit_ms: cursor.read_u64()?,
            memory_limit_bytes: cursor.read_u64()?,
        }),
        _ => return Err(invalid_data("supervisor limit presence tag is invalid")),
    };
    let lifetime_ms = cursor.read_u64()?;
    cursor.finish()?;
    let spec = UnixSupervisorLaunchSpec { program, args, cwd, environment, limits, lifetime_ms };
    validate_launch_spec(&spec)?;
    Ok(spec)
}

fn validate_launch_spec(spec: &UnixSupervisorLaunchSpec) -> io::Result<()> {
    validate_string(spec.program.as_os_str(), false, "program")?;
    if !std::path::Path::new(&spec.program).is_absolute() {
        return Err(invalid_input("supervisor program must be absolute"));
    }
    if spec.args.len() > MAX_ARGUMENTS {
        return Err(invalid_input("supervisor argument count exceeds limit"));
    }
    for argument in &spec.args {
        validate_string(argument, true, "argument")?;
    }
    validate_string(spec.cwd.as_os_str(), false, "working directory")?;
    if !spec.cwd.is_absolute() {
        return Err(invalid_input("supervisor working directory must be absolute"));
    }
    if spec.environment.len() > MAX_ENVIRONMENT_ENTRIES {
        return Err(invalid_input("supervisor environment count exceeds limit"));
    }
    let mut keys = HashSet::with_capacity(spec.environment.len());
    for (key, value) in &spec.environment {
        validate_string(key, false, "environment key")?;
        validate_string(value, true, "environment value")?;
        if key.as_bytes().contains(&b'=') {
            return Err(invalid_input("supervisor environment key contains '='"));
        }
        if !keys.insert(key.as_bytes().to_vec()) {
            return Err(invalid_input("supervisor environment contains duplicate keys"));
        }
    }
    if spec.lifetime_ms == 0 {
        return Err(invalid_input("supervisor lifetime must be positive"));
    }
    if let Some(limits) = spec.limits {
        if limits.cpu_time_limit_ms == 0 || limits.memory_limit_bytes == 0 {
            return Err(invalid_input("supervisor resource limits must be positive"));
        }
        let _ = rlim_from_u64(limits.cpu_time_limit_ms.max(1).div_ceil(1000))?;
        let _ = rlim_from_u64(limits.memory_limit_bytes)?;
    }
    Ok(())
}

fn validate_string(value: &OsStr, allow_empty: bool, field: &str) -> io::Result<()> {
    let bytes = value.as_bytes();
    if (!allow_empty && bytes.is_empty()) || bytes.len() > MAX_STRING_BYTES {
        return Err(invalid_input(match field {
            "program" => "supervisor program length is invalid",
            "working directory" => "supervisor working directory length is invalid",
            "environment key" => "supervisor environment key length is invalid",
            "environment value" => "supervisor environment value length is invalid",
            _ => "supervisor argument length is invalid",
        }));
    }
    if bytes.contains(&0) {
        return Err(invalid_input("supervisor launch string contains NUL"));
    }
    Ok(())
}

fn push_os_string(payload: &mut Vec<u8>, value: &OsStr) -> io::Result<()> {
    let bytes = value.as_bytes();
    let len = u32::try_from(bytes.len())
        .map_err(|_| invalid_input("supervisor launch string length exceeds protocol range"))?;
    payload.extend_from_slice(&len.to_be_bytes());
    payload.extend_from_slice(bytes);
    Ok(())
}

fn push_count(payload: &mut Vec<u8>, count: usize) -> io::Result<()> {
    let count = u32::try_from(count)
        .map_err(|_| invalid_input("supervisor launch count exceeds protocol range"))?;
    payload.extend_from_slice(&count.to_be_bytes());
    Ok(())
}

struct PayloadCursor<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PayloadCursor<'a> {
    const fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> io::Result<u8> {
        let value = *self
            .payload
            .get(self.offset)
            .ok_or_else(|| invalid_data("supervisor payload is truncated"))?;
        self.offset += 1;
        Ok(value)
    }

    fn read_u32(&mut self) -> io::Result<u32> {
        let bytes = self.read_exact_array::<4>()?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(&mut self) -> io::Result<u64> {
        let bytes = self.read_exact_array::<8>()?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn read_count(&mut self, maximum: usize, message: &'static str) -> io::Result<usize> {
        let count = usize::try_from(self.read_u32()?)
            .map_err(|_| invalid_data("supervisor count exceeds platform range"))?;
        if count > maximum {
            return Err(invalid_data(message));
        }
        Ok(count)
    }

    fn read_os_string(&mut self) -> io::Result<OsString> {
        let len = usize::try_from(self.read_u32()?)
            .map_err(|_| invalid_data("supervisor string length exceeds platform range"))?;
        if len > MAX_STRING_BYTES {
            return Err(invalid_data("supervisor string exceeds maximum length"));
        }
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(|| invalid_data("supervisor payload length overflow"))?;
        let bytes = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| invalid_data("supervisor payload is truncated"))?;
        self.offset = end;
        Ok(OsString::from_vec(bytes.to_vec()))
    }

    fn read_exact_array<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| invalid_data("supervisor payload length overflow"))?;
        let bytes = self
            .payload
            .get(self.offset..end)
            .ok_or_else(|| invalid_data("supervisor payload is truncated"))?;
        self.offset = end;
        bytes.try_into().map_err(|_| invalid_data("supervisor payload field length is invalid"))
    }

    fn finish(self) -> io::Result<()> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(invalid_data("supervisor payload contains trailing bytes"))
        }
    }
}

fn write_frame(
    stream: &UnixStream,
    kind: MessageType,
    payload: &[u8],
    operation_deadline: Instant,
) -> io::Result<()> {
    validate_outgoing_frame(kind, payload)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| invalid_input("supervisor frame length exceeds protocol range"))?;
    let mut header = [0_u8; FRAME_HEADER_LEN];
    header[0] = PROTOCOL_VERSION;
    header[1] = kind as u8;
    header[4..8].copy_from_slice(&payload_len.to_be_bytes());
    write_all_deadline(stream, &header, operation_deadline)?;
    write_all_deadline(stream, payload, operation_deadline)
}

fn read_frame_expected(
    stream: &UnixStream,
    expected: &[MessageType],
    operation_deadline: Instant,
) -> io::Result<Frame> {
    let mut header = [0_u8; FRAME_HEADER_LEN];
    read_exact_deadline(stream, &mut header, operation_deadline)?;
    let (kind, payload_len) = decode_frame_header(&header)?;
    if !expected.contains(&kind) {
        return Err(invalid_data("supervisor protocol message is invalid for current state"));
    }
    let mut payload = vec![0_u8; payload_len];
    read_exact_deadline(stream, payload.as_mut_slice(), operation_deadline)?;
    Ok(Frame { kind, payload })
}

fn decode_buffered_frame(buffer: &[u8]) -> io::Result<Option<(usize, Frame)>> {
    if buffer.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let header: &[u8; FRAME_HEADER_LEN] = buffer[..FRAME_HEADER_LEN]
        .try_into()
        .map_err(|_| invalid_data("supervisor frame header length is invalid"))?;
    let (kind, payload_len) = decode_frame_header(header)?;
    let total = FRAME_HEADER_LEN
        .checked_add(payload_len)
        .ok_or_else(|| invalid_data("supervisor frame length overflow"))?;
    if buffer.len() < total {
        return Ok(None);
    }
    Ok(Some((total, Frame { kind, payload: buffer[FRAME_HEADER_LEN..total].to_vec() })))
}

fn decode_frame_header(header: &[u8; FRAME_HEADER_LEN]) -> io::Result<(MessageType, usize)> {
    if header[0] != PROTOCOL_VERSION {
        return Err(invalid_data("supervisor protocol version is unsupported"));
    }
    if header[2] != 0 || header[3] != 0 {
        return Err(invalid_data("supervisor protocol reserved bits are nonzero"));
    }
    let kind = MessageType::parse(header[1])?;
    let payload_len = usize::try_from(u32::from_be_bytes(
        header[4..8]
            .try_into()
            .map_err(|_| invalid_data("supervisor frame length field is invalid"))?,
    ))
    .map_err(|_| invalid_data("supervisor frame length exceeds platform range"))?;
    if payload_len > MAX_FRAME_PAYLOAD_BYTES {
        return Err(invalid_data("supervisor frame exceeds maximum payload size"));
    }
    if kind.fixed_payload_len().is_some_and(|fixed| fixed != payload_len) {
        return Err(invalid_data("supervisor frame payload length is noncanonical"));
    }
    Ok((kind, payload_len))
}

fn validate_outgoing_frame(kind: MessageType, payload: &[u8]) -> io::Result<()> {
    if payload.len() > MAX_FRAME_PAYLOAD_BYTES {
        return Err(invalid_input("supervisor frame exceeds maximum payload size"));
    }
    if kind.fixed_payload_len().is_some_and(|fixed| fixed != payload.len()) {
        return Err(invalid_input("supervisor frame payload length is noncanonical"));
    }
    Ok(())
}

enum ExactReadOutcome {
    Complete,
    Eof,
}

fn read_exact_deadline_allow_eof(
    stream: &UnixStream,
    buffer: &mut [u8],
    operation_deadline: Instant,
) -> io::Result<ExactReadOutcome> {
    let mut offset = 0;
    while offset < buffer.len() {
        stream.set_read_timeout(Some(remaining_timeout(operation_deadline)?))?;
        let mut reader = stream;
        match reader.read(&mut buffer[offset..]) {
            Ok(0) if offset == 0 => return Ok(ExactReadOutcome::Eof),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "supervisor control closed with a partial frame",
                ));
            }
            Ok(read) => offset += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) =>
            {
                if Instant::now() >= operation_deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "supervisor control read timed out",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(ExactReadOutcome::Complete)
}

fn read_exact_deadline(
    stream: &UnixStream,
    buffer: &mut [u8],
    operation_deadline: Instant,
) -> io::Result<()> {
    let mut offset = 0;
    while offset < buffer.len() {
        stream.set_read_timeout(Some(remaining_timeout(operation_deadline)?))?;
        let mut reader = stream;
        match reader.read(&mut buffer[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "supervisor control closed",
                ));
            }
            Ok(read) => offset += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) =>
            {
                if Instant::now() >= operation_deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "supervisor control read timed out",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn write_all_deadline(
    stream: &UnixStream,
    buffer: &[u8],
    operation_deadline: Instant,
) -> io::Result<()> {
    let mut offset = 0;
    while offset < buffer.len() {
        stream.set_write_timeout(Some(remaining_timeout(operation_deadline)?))?;
        let mut writer = stream;
        match writer.write(&buffer[offset..]) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "supervisor control write returned zero",
                ));
            }
            Ok(written) => offset += written,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error)
                if matches!(error.kind(), io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut) =>
            {
                if Instant::now() >= operation_deadline {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "supervisor control write timed out",
                    ));
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn remaining_timeout(operation_deadline: Instant) -> io::Result<Duration> {
    operation_deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::TimedOut, "supervisor control deadline expired")
        })
}

fn deadline() -> Instant {
    Instant::now() + CONTROL_TIMEOUT
}

fn duplicate_fd_cloexec(fd: RawFd, minimum: libc::c_int) -> io::Result<OwnedFd> {
    // SAFETY: fcntl duplicates a valid open descriptor; negative returns are handled.
    let duplicated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, minimum) };
    if duplicated < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: duplicated is a fresh descriptor uniquely owned by this return value.
    Ok(unsafe { OwnedFd::from_raw_fd(duplicated) })
}

fn set_fd_cloexec(fd: RawFd) -> io::Result<()> {
    // SAFETY: fcntl descriptor flag operations have no pointer arguments.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: flags come from F_GETFD and adding FD_CLOEXEC is valid.
    if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn current_pid_t() -> io::Result<libc::pid_t> {
    // SAFETY: getpid has no arguments and always returns the calling process id.
    let pid = unsafe { libc::getpid() };
    if pid <= 0 {
        Err(invalid_data("supervisor pid is invalid"))
    } else {
        Ok(pid)
    }
}

fn pid_t_from_u32(pid: u32) -> io::Result<libc::pid_t> {
    libc::pid_t::try_from(pid).map_err(|_| invalid_input("pid exceeds platform range"))
}

#[cfg(test)]
fn test_marker_root() -> Option<PathBuf> {
    env::var_os(TEST_MARKER_ROOT_ENV).map(PathBuf::from)
}

#[cfg(test)]
fn write_test_marker(name: &str, contents: &[u8]) -> io::Result<()> {
    let Some(root) = test_marker_root() else {
        return Ok(());
    };
    let final_path = root.join(name);
    let temporary_path = root.join(format!(".{name}.{}.tmp", current_pid_t()?));
    std::fs::write(&temporary_path, contents)?;
    std::fs::rename(temporary_path, final_path)
}

#[cfg(test)]
fn write_test_identity_marker(name: &str, pid: libc::pid_t) -> io::Result<()> {
    // SAFETY: both identity lookups take one positive PID and no pointer arguments.
    let process_group = unsafe { libc::getpgid(pid) };
    if process_group < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: getsid takes one positive PID and no pointer arguments.
    let session = unsafe { libc::getsid(pid) };
    if session < 0 {
        return Err(io::Error::last_os_error());
    }
    write_test_marker(name, format!("{pid} {process_group} {session}\n").as_bytes())
}

#[allow(clippy::useless_conversion)]
fn rlim_from_u64(value: u64) -> io::Result<libc::rlim_t> {
    libc::rlim_t::try_from(value)
        .map_err(|_| invalid_input("resource limit exceeds platform range"))
}

fn decode_pid(payload: &[u8]) -> io::Result<u32> {
    let bytes: [u8; 4] =
        payload.try_into().map_err(|_| invalid_data("supervisor pid payload length is invalid"))?;
    let pid = u32::from_be_bytes(bytes);
    if pid == 0 {
        return Err(invalid_data("supervisor pid must be positive"));
    }
    Ok(pid)
}

fn encode_exec_failure(stage: u8, errno: i32) -> [u8; 5] {
    let mut payload = [0_u8; 5];
    payload[0] = stage;
    payload[1..].copy_from_slice(&errno.to_be_bytes());
    payload
}

fn decode_exec_failure(payload: &[u8]) -> io::Result<(u8, i32)> {
    if payload.len() != 5 {
        return Err(invalid_data("supervisor exec failure payload length is invalid"));
    }
    let errno = i32::from_be_bytes(
        payload[1..]
            .try_into()
            .map_err(|_| invalid_data("supervisor exec errno length is invalid"))?,
    );
    Ok((payload[0], errno))
}

fn lock_parent_control(
    mutex: &Mutex<ParentControlInner>,
) -> io::Result<std::sync::MutexGuard<'_, ParentControlInner>> {
    mutex.lock().map_err(|_| io::Error::other("supervisor control state lock poisoned"))
}

fn require_parent_state(actual: ParentState, expected: ParentState) -> io::Result<()> {
    if actual == expected {
        Ok(())
    } else {
        Err(invalid_input("supervisor control operation is out of order"))
    }
}

fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        process::ExitStatus,
        sync::{Mutex, MutexGuard},
    };
    use tempfile::TempDir;

    static SUPERVISOR_REGRESSION_LOCK: Mutex<()> = Mutex::new(());
    const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct ProcessIdentity {
        pid: libc::pid_t,
        process_group: libc::pid_t,
        session: libc::pid_t,
    }

    struct SupervisorFixture {
        _guard: MutexGuard<'static, ()>,
        markers: TempDir,
        supervisor: Option<Child>,
        control: Option<UnixProcessSupervisorControl>,
        supervisor_identity: Option<ProcessIdentity>,
        target_identity: Option<ProcessIdentity>,
    }

    impl SupervisorFixture {
        fn spawn(fail_next_cleanup: bool) -> Self {
            let guard =
                SUPERVISOR_REGRESSION_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            let markers = tempfile::tempdir().expect("marker directory should be created");
            let current_executable = env::current_exe().expect("test executable should resolve");
            let (mut command, control) = UnixProcessSupervisorControl::prepare(current_executable)
                .expect("supervisor command should prepare");
            command.env(TEST_MARKER_ROOT_ENV, markers.path());
            if fail_next_cleanup {
                command.env(TEST_FAIL_NEXT_CLEANUP_ENV, "1");
            }
            let supervisor = command.spawn().expect("supervisor should spawn");
            drop(command);
            let supervisor_pid = supervisor.id();
            let mut fixture = Self {
                _guard: guard,
                markers,
                supervisor: Some(supervisor),
                control: Some(control),
                supervisor_identity: None,
                target_identity: None,
            };
            fixture
                .control
                .as_ref()
                .expect("control should be present")
                .await_ready(supervisor_pid)
                .expect("supervisor should become ready");
            let supervisor_identity = read_identity_marker(fixture.marker("supervisor.identity"));
            assert_anchor(supervisor_identity);
            assert_eq!(supervisor_identity.pid, pid_t_from_u32(supervisor_pid).unwrap());
            fixture.supervisor_identity = Some(supervisor_identity);
            fixture
        }

        fn marker(&self, name: &str) -> PathBuf {
            self.markers.path().join(name)
        }

        fn set_spec(&self, target_mode: &str) {
            let current_executable = env::current_exe().expect("test executable should resolve");
            let spec = UnixSupervisorLaunchSpec {
                program: current_executable.into_os_string(),
                args: vec![
                    OsString::from("--exact"),
                    OsString::from(TEST_PROCESS_SUPERVISOR_HELPER),
                    OsString::from("--nocapture"),
                ],
                cwd: self.markers.path().to_path_buf(),
                environment: vec![
                    (OsString::from(TEST_PROCESS_SUPERVISOR_ENV), OsString::from(target_mode)),
                    (
                        OsString::from(TEST_MARKER_ROOT_ENV),
                        self.markers.path().as_os_str().to_os_string(),
                    ),
                ],
                limits: None,
                lifetime_ms: 60_000,
            };
            self.control
                .as_ref()
                .expect("control should be present")
                .set_launch_spec(spec)
                .expect("launch spec should be accepted");
        }

        fn start_target(&mut self, target_mode: &str) -> ProcessIdentity {
            self.set_spec(target_mode);
            let target_pid = self
                .control
                .as_ref()
                .expect("control should be present")
                .start_target()
                .expect("target should start");
            let target_pid = pid_t_from_u32(target_pid).expect("target pid should fit pid_t");
            self.target_identity = Some(ProcessIdentity {
                pid: target_pid,
                process_group: target_pid,
                session: target_pid,
            });
            let identity = read_identity_marker(self.marker("target.identity"));
            assert_anchor(identity);
            assert_eq!(identity.pid, target_pid);
            self.target_identity = Some(identity);
            identity
        }

        fn terminate(&self) -> io::Result<()> {
            self.control.as_ref().expect("control should be present").terminate()
        }

        fn drop_control(&mut self) {
            drop(self.control.take());
        }

        fn wait_supervisor(&mut self) -> ExitStatus {
            wait_child_exit(
                self.supervisor.as_mut().expect("supervisor should be present"),
                TEST_WAIT_TIMEOUT,
            )
            .expect("supervisor should exit")
        }
    }

    impl Drop for SupervisorFixture {
        fn drop(&mut self) {
            drop(self.control.take());
            let deadline = Instant::now() + Duration::from_secs(2);
            if let Some(supervisor) = self.supervisor.as_mut() {
                while Instant::now() < deadline {
                    if supervisor.try_wait().ok().flatten().is_some() {
                        break;
                    }
                    thread::sleep(MONITOR_POLL_INTERVAL);
                }
                if let Some(target) = self.target_identity {
                    if identity_matches(target) {
                        let _ = signal_test_process_group(target.process_group);
                    }
                }
                if supervisor.try_wait().ok().flatten().is_none() {
                    if let Some(identity) = self.supervisor_identity {
                        if identity_matches(identity) {
                            let _ = signal_test_process_group(identity.process_group);
                        }
                    } else {
                        let _ = supervisor.kill();
                    }
                    let _ = wait_child_exit(supervisor, Duration::from_secs(2));
                }
            }
        }
    }

    #[test]
    fn hidden_process_supervisor_helper() {
        let Some(mode) = env::var_os(TEST_PROCESS_SUPERVISOR_ENV) else {
            return;
        };
        if mode == OsStr::new(TEST_PROCESS_SUPERVISOR_MODE) {
            let outcome =
                run_supervisor().unwrap_or(SupervisorExit::Code(INTERNAL_FAILURE_EXIT_CODE));
            finish_supervisor(outcome)
        }
        if mode == OsStr::new(TEST_CLEANUP_HELPER_MODE) {
            run_cleanup_helper().unwrap_or_else(|_| {
                // SAFETY: hidden helper test failure must not unwind through the harness.
                unsafe { libc::_exit(INTERNAL_FAILURE_EXIT_CODE) }
            })
        }
        if mode == OsStr::new(TEST_TARGET_LAUNCHER_MODE) {
            run_target_launcher().unwrap_or_else(|_| {
                // SAFETY: hidden launcher test failure must not unwind through the harness.
                unsafe { libc::_exit(INTERNAL_FAILURE_EXIT_CODE) }
            })
        }
        if mode == OsStr::new(TEST_TARGET_MODE_BLOCK) {
            run_blocking_target(false)
                .unwrap_or_else(|_| std::process::exit(INTERNAL_FAILURE_EXIT_CODE));
        }
        if mode == OsStr::new(TEST_TARGET_MODE_SPAWN_DESCENDANT) {
            run_blocking_target(true)
                .unwrap_or_else(|_| std::process::exit(INTERNAL_FAILURE_EXIT_CODE));
        }
        if mode == OsStr::new(TEST_TARGET_MODE_SPAWN_DESCENDANT_AND_EXIT) {
            run_target_with_live_descendant()
                .unwrap_or_else(|_| std::process::exit(INTERNAL_FAILURE_EXIT_CODE));
            std::process::exit(TEST_TARGET_NATURAL_EXIT_CODE);
        }
        if mode == OsStr::new(TEST_TARGET_MODE_DESCENDANT) {
            write_test_identity_marker("descendant.identity", current_pid_t().unwrap())
                .unwrap_or_else(|_| std::process::exit(INTERNAL_FAILURE_EXIT_CODE));
            block_forever();
        }
        std::process::exit(INTERNAL_FAILURE_EXIT_CODE)
    }

    #[test]
    fn unix_process_supervisor_regression_separates_supervisor_and_target_sessions_and_groups() {
        let mut fixture = SupervisorFixture::spawn(false);
        let target = fixture.start_target(TEST_TARGET_MODE_BLOCK);
        let supervisor = fixture.supervisor_identity.unwrap();
        assert_anchor(supervisor);
        assert_anchor(target);
        assert_ne!(supervisor, target);
        fixture.terminate().expect("cleanup should be acknowledged");
        assert!(!process_group_is_active(target.process_group).unwrap());
        assert!(fixture.wait_supervisor().success());
    }

    #[test]
    fn unix_process_supervisor_regression_pre_start_terminate_acknowledges_without_target_exec() {
        let mut fixture = SupervisorFixture::spawn(false);
        fixture.set_spec(TEST_TARGET_MODE_BLOCK);
        fixture.terminate().expect("pre-start cleanup should be acknowledged");
        assert!(!fixture.marker("target-launcher.identity").exists());
        assert!(!fixture.marker("target.identity").exists());
        assert!(fixture.wait_supervisor().success());
    }

    #[test]
    fn unix_process_supervisor_regression_helper_is_supervisor_owned_armed_for_target_and_reaped_before_acknowledgement(
    ) {
        let mut fixture = SupervisorFixture::spawn(false);
        let helper = read_identity_marker(fixture.marker("cleanup-helper.identity"));
        let supervisor = fixture.supervisor_identity.unwrap();
        assert_eq!(helper.process_group, supervisor.process_group);
        assert_eq!(helper.session, supervisor.session);
        let target = fixture.start_target(TEST_TARGET_MODE_BLOCK);
        assert_eq!(read_pid_marker(fixture.marker("cleanup-helper.armed")), target.process_group);
        fixture.terminate().expect("cleanup should be acknowledged");
        assert_eq!(read_pid_marker(fixture.marker("cleanup-helper.reaped")), helper.pid);
        assert!(fixture.wait_supervisor().success());
    }

    #[test]
    fn unix_process_supervisor_regression_terminate_kills_target_group_descendants() {
        let mut fixture = SupervisorFixture::spawn(false);
        let target = fixture.start_target(TEST_TARGET_MODE_SPAWN_DESCENDANT);
        let descendant = read_identity_marker(fixture.marker("descendant.identity"));
        assert_eq!(descendant.process_group, target.process_group);
        assert_eq!(descendant.session, target.session);
        fixture.terminate().expect("cleanup should be acknowledged");
        assert!(!process_group_is_active(target.process_group).unwrap());
        assert!(fixture.wait_supervisor().success());
    }

    #[test]
    fn unix_process_supervisor_regression_natural_leader_exit_cleans_live_descendant_and_preserves_exit_status(
    ) {
        let mut fixture = SupervisorFixture::spawn(false);
        let target = fixture.start_target(TEST_TARGET_MODE_SPAWN_DESCENDANT_AND_EXIT);
        let descendant = read_identity_marker(fixture.marker("descendant.identity"));
        assert_eq!(descendant.process_group, target.process_group);
        assert_eq!(descendant.session, target.session);

        let status = fixture.wait_supervisor();

        assert_eq!(status.code(), Some(TEST_TARGET_NATURAL_EXIT_CODE));
        assert!(!process_group_is_active(target.process_group).unwrap());
        assert!(fixture.marker("cleanup-helper.reaped").exists());
    }

    #[test]
    fn unix_process_supervisor_regression_post_start_control_eof_fails_closed_and_cleans_target() {
        let mut fixture = SupervisorFixture::spawn(false);
        let target = fixture.start_target(TEST_TARGET_MODE_BLOCK);
        fixture.drop_control();
        let status = fixture.wait_supervisor();
        assert_eq!(status.code(), Some(INTERNAL_FAILURE_EXIT_CODE));
        assert!(!process_group_is_active(target.process_group).unwrap());
        assert!(fixture.marker("cleanup-helper.reaped").exists());
    }

    #[test]
    fn unix_process_supervisor_regression_first_cleanup_failure_remains_retryable_and_second_terminate_completes(
    ) {
        let mut fixture = SupervisorFixture::spawn(true);
        let target = fixture.start_target(TEST_TARGET_MODE_BLOCK);
        let first_error =
            fixture.terminate().expect_err("first cleanup should be injected to fail");
        assert!(first_error.to_string().contains("incomplete process cleanup"));
        assert!(fixture.marker("cleanup-fault-consumed").exists());
        assert!(identity_matches(target));
        fixture.terminate().expect("second cleanup should be acknowledged");
        assert!(!process_group_is_active(target.process_group).unwrap());
        assert!(fixture.marker("cleanup-helper.reaped").exists());
        assert!(fixture.wait_supervisor().success());
    }

    #[test]
    fn unix_process_supervisor_regression_natural_exit_retry_success_is_acknowledged() {
        let mut fixture = SupervisorFixture::spawn(true);
        fixture.start_target(TEST_TARGET_MODE_BLOCK);
        let target = fixture.target_identity.expect("target identity should be recorded");
        signal_test_process_group(target.process_group).expect("target group should terminate");
        wait_for_marker(fixture.marker("cleanup-fault-consumed"), TEST_WAIT_TIMEOUT)
            .expect("natural-exit cleanup retry should consume the injected failure");

        fixture
            .terminate()
            .expect("queued cleanup acknowledgement should survive the retry-success exit");

        assert!(!process_group_is_active(target.process_group).unwrap());
        assert!(fixture.marker("cleanup-helper.reaped").exists());
        assert_eq!(fixture.wait_supervisor().signal(), Some(libc::SIGKILL));
    }

    fn run_blocking_target(spawn_descendant: bool) -> io::Result<()> {
        if spawn_descendant {
            run_target_with_live_descendant()?;
        } else {
            write_test_identity_marker("target.identity", current_pid_t()?)?;
        }
        block_forever();
    }

    fn run_target_with_live_descendant() -> io::Result<()> {
        write_test_identity_marker("target.identity", current_pid_t()?)?;
        let marker_root = test_marker_root().ok_or_else(|| invalid_input("marker root missing"))?;
        let current_executable = env::current_exe()?;
        Command::new(current_executable)
            .args(["--exact", TEST_PROCESS_SUPERVISOR_HELPER, "--nocapture"])
            .env_clear()
            .env(TEST_PROCESS_SUPERVISOR_ENV, TEST_TARGET_MODE_DESCENDANT)
            .env(TEST_MARKER_ROOT_ENV, marker_root.as_os_str())
            .spawn()?;
        wait_for_marker(marker_root.join("descendant.identity"), TEST_WAIT_TIMEOUT)
    }

    fn block_forever() -> ! {
        loop {
            thread::sleep(Duration::from_secs(60));
        }
    }

    fn read_identity_marker(path: PathBuf) -> ProcessIdentity {
        wait_for_marker(path.clone(), TEST_WAIT_TIMEOUT).expect("identity marker should appear");
        let contents = fs::read_to_string(path).expect("identity marker should be readable");
        let values = contents
            .split_whitespace()
            .map(|value| value.parse::<libc::pid_t>().expect("identity value should parse"))
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 3);
        ProcessIdentity { pid: values[0], process_group: values[1], session: values[2] }
    }

    fn read_pid_marker(path: PathBuf) -> libc::pid_t {
        wait_for_marker(path.clone(), TEST_WAIT_TIMEOUT).expect("pid marker should appear");
        fs::read_to_string(path)
            .expect("pid marker should be readable")
            .trim()
            .parse()
            .expect("pid marker should parse")
    }

    fn wait_for_marker(path: PathBuf, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        while !path.is_file() {
            if Instant::now() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "test marker timed out"));
            }
            thread::sleep(MONITOR_POLL_INTERVAL);
        }
        Ok(())
    }

    fn wait_child_exit(child: &mut Child, timeout: Duration) -> io::Result<ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = child.try_wait()? {
                return Ok(status);
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(io::ErrorKind::TimedOut, "test child exit timed out"));
            }
            thread::sleep(MONITOR_POLL_INTERVAL);
        }
    }

    fn assert_anchor(identity: ProcessIdentity) {
        assert_eq!(identity.pid, identity.process_group);
        assert_eq!(identity.pid, identity.session);
    }

    fn signal_test_process_group(process_group: libc::pid_t) -> io::Result<()> {
        if process_group <= 0 {
            return Err(invalid_input("test process-group id must be positive"));
        }
        // SAFETY: tests first verify the full PID/PGID/SID identity and use this only for fixture
        // teardown or to simulate a natural external termination; production cleanup never calls it.
        if unsafe { libc::kill(-process_group, libc::SIGKILL) } == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Ok(())
        } else {
            Err(error)
        }
    }

    fn identity_matches(identity: ProcessIdentity) -> bool {
        if identity.pid <= 0 {
            return false;
        }
        // SAFETY: identity contains a previously verified positive PID.
        let process_group = unsafe { libc::getpgid(identity.pid) };
        if process_group < 0 {
            return false;
        }
        // SAFETY: identity contains a previously verified positive PID.
        let session = unsafe { libc::getsid(identity.pid) };
        identity.pid == identity.process_group
            && identity.pid == identity.session
            && process_group == identity.process_group
            && session == identity.session
    }

    #[test]
    fn launch_spec_round_trips_non_utf8_bytes() {
        let spec = UnixSupervisorLaunchSpec {
            program: OsString::from_vec(vec![b'/', b'b', b'i', b'n', b'/', b'x', 0xFF]),
            args: vec![OsString::from_vec(vec![b'a', 0xFE])],
            cwd: PathBuf::from("/tmp"),
            environment: vec![(OsString::from("KEY"), OsString::from_vec(vec![0xFD]))],
            limits: Some(UnixSupervisorLimits {
                cpu_time_limit_ms: 1_001,
                memory_limit_bytes: 1024,
            }),
            lifetime_ms: 5_000,
        };

        let encoded = encode_launch_spec(&spec).expect("launch spec should encode");
        let decoded = decode_launch_spec(encoded.as_slice()).expect("launch spec should decode");
        assert_eq!(decoded, spec);
    }

    #[test]
    fn frame_header_rejects_oversize_before_payload_allocation() {
        let mut header = [0_u8; FRAME_HEADER_LEN];
        header[0] = PROTOCOL_VERSION;
        header[1] = MessageType::Spec as u8;
        header[4..].copy_from_slice(
            &u32::try_from(MAX_FRAME_PAYLOAD_BYTES + 1)
                .expect("test size should fit u32")
                .to_be_bytes(),
        );
        assert!(decode_frame_header(&header).is_err());
    }

    #[test]
    fn launch_spec_rejects_relative_program() {
        let spec = UnixSupervisorLaunchSpec {
            program: OsString::from("bin/true"),
            args: Vec::new(),
            cwd: PathBuf::from("/"),
            environment: Vec::new(),
            limits: None,
            lifetime_ms: 1,
        };
        assert!(encode_launch_spec(&spec).is_err());
    }

    #[test]
    fn launch_spec_rejects_duplicate_environment_keys() {
        let spec = UnixSupervisorLaunchSpec {
            program: OsString::from("/bin/true"),
            args: Vec::new(),
            cwd: PathBuf::from("/"),
            environment: vec![
                (OsString::from("KEY"), OsString::from("one")),
                (OsString::from("KEY"), OsString::from("two")),
            ],
            limits: None,
            lifetime_ms: 1,
        };
        assert!(encode_launch_spec(&spec).is_err());
    }

    #[test]
    fn decode_rejects_trailing_launch_bytes() {
        let spec = UnixSupervisorLaunchSpec {
            program: OsString::from("/bin/true"),
            args: Vec::new(),
            cwd: PathBuf::from("/"),
            environment: Vec::new(),
            limits: None,
            lifetime_ms: 1,
        };
        let mut encoded = encode_launch_spec(&spec).expect("launch spec should encode");
        encoded.push(0);
        assert!(decode_launch_spec(encoded.as_slice()).is_err());
    }
}
