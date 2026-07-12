use super::*;

pub(super) struct QaDaemonEnvironment<'a> {
    pub(super) allowed_tools: &'a str,
    pub(super) state_root: &'a Path,
    pub(super) identity_root: &'a Path,
    pub(super) config_path: &'a Path,
    pub(super) vault_dir: &'a Path,
    pub(super) provider: &'a QaDaemonProviderEnvironment,
    pub(super) execution_key_digest: &'a str,
    pub(super) provider_binding_sha256: &'a str,
    pub(super) admin_token: &'a str,
    pub(super) principal: &'a str,
    pub(super) fault_launch: Option<&'a QaPreparedFaultLaunch>,
}

impl OwnedDaemonProcess {
    pub(super) fn terminate_tree(&mut self, timeout: Duration) -> bool {
        if self.cleanup_verified {
            return true;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return false;
        };
        let tree_termination_requested = match self.tree.as_ref() {
            Some(tree) => tree.terminate(deadline).is_ok(),
            None => match self.child.try_wait() {
                Ok(Some(_)) => true,
                Ok(None) => self.child.kill().is_ok(),
                Err(_) => false,
            },
        };
        let child_reaped = wait_for_child_exit_until(&mut self.child, deadline);
        let tree_inactive = match self.tree.as_ref() {
            Some(tree) => tree.wait_until_inactive(deadline).unwrap_or(false),
            None => !self.descendants_possible_without_tree,
        };
        self.cleanup_verified = tree_termination_requested && child_reaped && tree_inactive;
        self.cleanup_verified
    }

    pub(super) fn cleanup_descendants_after_observed_exit(&mut self, timeout: Duration) -> bool {
        if self.cleanup_verified {
            return true;
        }
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return false;
        };
        self.cleanup_verified = self.tree.as_ref().is_some_and(|tree| {
            tree.terminate(deadline).is_ok() && tree.wait_until_inactive(deadline).unwrap_or(false)
        });
        self.cleanup_verified
    }
}

impl Drop for OwnedDaemonProcess {
    fn drop(&mut self) {
        if !self.cleanup_verified {
            let _ = self.terminate_tree(DAEMON_TERMINATION_TIMEOUT);
        }
    }
}

#[cfg(all(test, unix))]
pub(super) fn wait_for_child_exit(child: &mut Child, timeout: Duration) -> bool {
    let Some(deadline) = Instant::now().checked_add(timeout) else {
        return false;
    };
    wait_for_child_exit_until(child, deadline)
}

fn wait_for_child_exit_until(child: &mut Child, deadline: Instant) -> bool {
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return true,
            Ok(None) => {}
            Err(_) => return false,
        }
        let now = Instant::now();
        if now >= deadline {
            return false;
        }
        thread::sleep(SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
    }
}

#[cfg(unix)]
pub(super) fn configure_daemon_process_tree(
    command: &mut Command,
) -> Result<DaemonProcessTreePreparation> {
    use std::os::unix::process::CommandExt;

    let launch_guard = lock_unpoisoned(&UNIX_PROCESS_TREE_LAUNCH_LOCK);
    // A distinct session prevents descendants from joining a pre-launch process group. It also
    // forces the fork/exec path so Apple posix_spawn cannot close the liveness descriptor.
    // SAFETY: setsid(2) is async-signal-safe and the closure performs no allocation or locking.
    unsafe {
        command.pre_exec(|| {
            if unix_setsid() == -1 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let (descendant_liveness_read, descendant_liveness_write) =
        unix_descendant_liveness_pipe().context("qa.runner.daemon_liveness_pipe_failed")?;
    let baseline_deadline = Instant::now()
        .checked_add(DAEMON_TERMINATION_TIMEOUT)
        .ok_or_else(|| anyhow::anyhow!("qa.runner.daemon_process_baseline_deadline_invalid"))?;
    let preexisting_processes = unix_process_baseline(baseline_deadline)
        .context("qa.runner.daemon_process_baseline_failed")?;
    let containment_marker = format!("{}-{}", Ulid::new(), Ulid::new());
    command.env(QA_PROCESS_TREE_MARKER_ENV, containment_marker.as_str());
    Ok(DaemonProcessTreePreparation {
        descendant_liveness_read,
        descendant_liveness_write,
        containment_marker,
        preexisting_processes,
        launch_guard,
    })
}

#[cfg(windows)]
pub(super) fn configure_daemon_process_tree(
    command: &mut Command,
) -> Result<DaemonProcessTreePreparation> {
    use std::os::windows::process::CommandExt;

    command.creation_flags(WINDOWS_CREATE_SUSPENDED);
    Ok(DaemonProcessTreePreparation {})
}

#[cfg(not(any(unix, windows)))]
pub(super) fn configure_daemon_process_tree(
    _command: &mut Command,
) -> Result<DaemonProcessTreePreparation> {
    Ok(DaemonProcessTreePreparation {})
}

#[cfg(unix)]
pub(super) fn attach_daemon_process_tree(
    child: Child,
    preparation: DaemonProcessTreePreparation,
) -> std::result::Result<OwnedDaemonProcess, Box<AttachDaemonProcessFailure>> {
    let DaemonProcessTreePreparation {
        descendant_liveness_read,
        descendant_liveness_write,
        containment_marker,
        preexisting_processes,
        launch_guard,
    } = preparation;
    drop(descendant_liveness_write);
    let process_group_id = match i32::try_from(child.id()) {
        Ok(process_group_id) => process_group_id,
        Err(error) => {
            return Err(Box::new(AttachDaemonProcessFailure {
                error: anyhow::Error::new(error)
                    .context("qa.runner.daemon_process_group_id_invalid"),
                process: OwnedDaemonProcess {
                    child,
                    tree: None,
                    descendants_possible_without_tree: true,
                    cleanup_verified: false,
                },
            }));
        }
    };
    let root_identity = match unix_process_identity(process_group_id) {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            return Err(Box::new(AttachDaemonProcessFailure {
                error: anyhow::anyhow!("qa.runner.daemon_process_identity_unavailable"),
                process: OwnedDaemonProcess {
                    child,
                    tree: None,
                    descendants_possible_without_tree: true,
                    cleanup_verified: false,
                },
            }));
        }
        Err(error) => {
            return Err(Box::new(AttachDaemonProcessFailure {
                error: anyhow::Error::new(error)
                    .context("qa.runner.daemon_process_identity_unavailable"),
                process: OwnedDaemonProcess {
                    child,
                    tree: None,
                    descendants_possible_without_tree: true,
                    cleanup_verified: false,
                },
            }));
        }
    };
    unix_register_process_tree_identity(containment_marker.as_str(), root_identity);
    let process = OwnedDaemonProcess {
        child,
        tree: Some(DaemonProcessTree {
            root_identity,
            process_group_id,
            tracked_descendants: Mutex::new(Default::default()),
            descendant_discovery_complete: Mutex::new(false),
            descendant_liveness_read: Mutex::new(descendant_liveness_read),
            containment_marker,
            preexisting_processes,
        }),
        descendants_possible_without_tree: true,
        cleanup_verified: false,
    };
    drop(launch_guard);
    Ok(process)
}

#[cfg(windows)]
pub(super) fn attach_daemon_process_tree(
    child: Child,
    _preparation: DaemonProcessTreePreparation,
) -> std::result::Result<OwnedDaemonProcess, Box<AttachDaemonProcessFailure>> {
    attach_windows_daemon_process_tree_with(child, WindowsJobHandle::new)
}

#[cfg(windows)]
pub(super) fn attach_windows_daemon_process_tree_with<NewJob>(
    child: Child,
    new_job: NewJob,
) -> std::result::Result<OwnedDaemonProcess, Box<AttachDaemonProcessFailure>>
where
    NewJob: FnOnce() -> io::Result<WindowsJobHandle>,
{
    use std::os::windows::io::AsRawHandle;

    let job = match new_job() {
        Ok(job) => job,
        Err(error) => {
            return Err(Box::new(AttachDaemonProcessFailure {
                error: anyhow::Error::new(error).context("qa.runner.daemon_job_create_failed"),
                process: OwnedDaemonProcess {
                    child,
                    tree: None,
                    descendants_possible_without_tree: false,
                    cleanup_verified: false,
                },
            }));
        }
    };
    // SAFETY: both handles are live and owned for the duration of this call.
    let assigned =
        unsafe { windows_assign_process_to_job_object(job.handle.get(), child.as_raw_handle()) };
    if assigned == 0 {
        let error = io::Error::last_os_error();
        return Err(Box::new(AttachDaemonProcessFailure {
            error: anyhow::Error::new(error).context("qa.runner.daemon_job_assign_failed"),
            process: OwnedDaemonProcess {
                child,
                tree: None,
                descendants_possible_without_tree: false,
                cleanup_verified: false,
            },
        }));
    }
    if let Err(error) = resume_suspended_windows_process(child.id()) {
        return Err(Box::new(AttachDaemonProcessFailure {
            error: anyhow::Error::new(error).context("qa.runner.daemon_resume_failed"),
            process: OwnedDaemonProcess {
                child,
                tree: Some(DaemonProcessTree { job }),
                descendants_possible_without_tree: true,
                cleanup_verified: false,
            },
        }));
    }
    Ok(OwnedDaemonProcess {
        child,
        tree: Some(DaemonProcessTree { job }),
        descendants_possible_without_tree: true,
        cleanup_verified: false,
    })
}

#[cfg(not(any(unix, windows)))]
pub(super) fn attach_daemon_process_tree(
    child: Child,
    _preparation: DaemonProcessTreePreparation,
) -> std::result::Result<OwnedDaemonProcess, Box<AttachDaemonProcessFailure>> {
    Err(Box::new(AttachDaemonProcessFailure {
        error: anyhow::anyhow!("qa.runner.daemon_process_tree_unsupported"),
        process: OwnedDaemonProcess {
            child,
            tree: None,
            descendants_possible_without_tree: true,
            cleanup_verified: false,
        },
    }))
}

#[cfg(unix)]
impl DaemonProcessTree {
    pub(super) fn terminate(&self, deadline: Instant) -> io::Result<()> {
        let discovery = self.freeze_recursive_descendants(deadline);
        let group_termination = unix_signal_process_group_if_anchored(
            &self.root_identity,
            self.process_group_id,
            UNIX_SIGKILL,
        );
        let root_termination = unix_signal_process_identity(&self.root_identity, UNIX_SIGKILL);
        let descendants = lock_unpoisoned(&self.tracked_descendants).clone();
        let mut descendant_termination_failed = false;
        for identity in descendants.values() {
            descendant_termination_failed |=
                unix_signal_process_identity(identity, UNIX_SIGKILL).is_err();
        }
        discovery?;
        group_termination?;
        root_termination?;
        if descendant_termination_failed {
            return Err(io::Error::other("recursive descendant termination failed"));
        }
        Ok(())
    }

    pub(super) fn wait_until_inactive(&self, deadline: Instant) -> io::Result<bool> {
        if !*lock_unpoisoned(&self.descendant_discovery_complete) {
            return Ok(false);
        }
        loop {
            let tracked_descendants = lock_unpoisoned(&self.tracked_descendants).clone();
            let mut descendants_active = false;
            for identity in tracked_descendants.values() {
                descendants_active |= unix_process_identity_is_active(identity)?;
            }
            let root_active = unix_process_identity_is_active(&self.root_identity)?;
            let process_table = match unix_process_table(deadline) {
                Ok(process_table) => process_table,
                Err(error) if error.kind() == io::ErrorKind::TimedOut => return Ok(false),
                Err(error) => return Err(error),
            };
            let marker_active = match unix_marker_processes(
                process_table.as_slice(),
                &self.root_identity,
                &self.preexisting_processes,
                &tracked_descendants,
                self.containment_marker.as_str(),
                deadline,
            ) {
                Ok(processes) => {
                    processes.into_iter().any(|identity| identity != self.root_identity)
                }
                Err(error) if error.kind() == io::ErrorKind::TimedOut => return Ok(false),
                Err(error) => return Err(error),
            };
            let anchored_group_active =
                root_active && unix_process_group_is_active(self.process_group_id)?;
            if !root_active
                && !anchored_group_active
                && !descendants_active
                && !marker_active
                && unix_descendant_liveness_closed(&self.descendant_liveness_read)?
            {
                return Ok(true);
            }
            let now = Instant::now();
            if now >= deadline {
                return Ok(false);
            }
            thread::sleep(SHUTDOWN_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
        }
    }

    pub(super) fn freeze_recursive_descendants(&self, deadline: Instant) -> io::Result<()> {
        const MAX_DISCOVERY_PASSES: usize = 8;

        // Freezing known members before the next process-table pass closes the fork race. A child
        // that escaped the PGID through setsid is stopped individually once its PPID edge appears.
        *lock_unpoisoned(&self.descendant_discovery_complete) = false;
        let root_active = unix_process_identity_is_active(&self.root_identity)?;
        let group_stopped = unix_signal_process_group_if_anchored(
            &self.root_identity,
            self.process_group_id,
            UNIX_SIGSTOP,
        )?;
        if root_active && !group_stopped {
            return Err(io::Error::other("daemon process group could not be frozen"));
        }
        for pass in 0..MAX_DISCOVERY_PASSES {
            ensure_unix_cleanup_before_deadline(deadline)?;
            let process_table = unix_process_table(deadline)?;
            let tracked_before = lock_unpoisoned(&self.tracked_descendants).clone();
            let roots = unix_identity_matching_roots(
                process_table.as_slice(),
                &self.root_identity,
                &tracked_before,
            );
            let discovered = unix_recursive_descendants(process_table.as_slice(), &roots);
            let previous = tracked_before.clone();
            let mut known_descendants = tracked_before;
            for snapshot in process_table
                .iter()
                .filter(|snapshot| discovered.contains(&snapshot.identity.process_id))
            {
                if snapshot.identity != self.root_identity {
                    known_descendants.insert(snapshot.identity.process_id, snapshot.identity);
                }
            }
            // Persist exact identities before either fallible operation. terminate() can then kill
            // them even if signaling or a protected process makes the marker scan fail.
            {
                let mut tracked = lock_unpoisoned(&self.tracked_descendants);
                for identity in known_descendants.values() {
                    tracked.insert(identity.process_id, *identity);
                }
            }
            unix_register_process_tree_identities(
                self.containment_marker.as_str(),
                &known_descendants,
            );
            // Stop identity-bound descendants before the environment scan so they cannot fork
            // across the scan. Their exact identities are checked directly and need no marker read.
            for identity in known_descendants.values() {
                unix_signal_process_identity(identity, UNIX_SIGSTOP)?;
            }
            let marker_processes = unix_marker_processes(
                process_table.as_slice(),
                &self.root_identity,
                &self.preexisting_processes,
                &known_descendants,
                self.containment_marker.as_str(),
                deadline,
            )?;
            let mut tracked = lock_unpoisoned(&self.tracked_descendants);
            for identity in marker_processes {
                if identity != self.root_identity {
                    tracked.insert(identity.process_id, identity);
                }
            }
            unix_register_process_tree_identities(self.containment_marker.as_str(), &tracked);
            for identity in tracked.values() {
                unix_signal_process_identity(identity, UNIX_SIGSTOP)?;
            }
            if *tracked == previous && pass > 0 {
                *lock_unpoisoned(&self.descendant_discovery_complete) = true;
                return Ok(());
            }
        }
        Err(io::Error::other("recursive descendant discovery did not converge"))
    }
}

#[cfg(unix)]
impl Drop for DaemonProcessTree {
    fn drop(&mut self) {
        unix_unregister_process_tree(self.containment_marker.as_str());
    }
}

#[cfg(unix)]
pub(super) const UNIX_SIGKILL: i32 = 9;
#[cfg(any(target_os = "linux", target_os = "android"))]
const UNIX_SIGSTOP: i32 = 19;
#[cfg(any(target_os = "macos", target_os = "ios"))]
const UNIX_SIGSTOP: i32 = 17;
#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios"))
))]
const UNIX_SIGSTOP: i32 = 19;
#[cfg(unix)]
const UNIX_ESRCH: i32 = 3;
#[cfg(target_os = "macos")]
const UNIX_ENOENT: i32 = 2;
#[cfg(unix)]
const MAX_UNIX_PROCESS_COUNT: usize = 65_536;
#[cfg(any(target_os = "linux", target_os = "android"))]
const MAX_LINUX_PROC_STAT_BYTES: usize = 4 * 1024;
#[cfg(unix)]
const MAX_UNIX_PROCESS_ENV_BYTES: usize = 1024 * 1024;
#[cfg(unix)]
const MAX_UNIX_PROCESS_ENV_TOTAL_BYTES: usize = 128 * 1024 * 1024;
#[cfg(unix)]
const UNIX_F_GETFL: i32 = 3;
#[cfg(unix)]
const UNIX_F_SETFL: i32 = 4;
#[cfg(any(target_os = "linux", target_os = "android"))]
const UNIX_O_NONBLOCK: i32 = 0x0000_0800;
#[cfg(any(target_os = "macos", target_os = "ios"))]
const UNIX_O_NONBLOCK: i32 = 0x0000_0004;
#[cfg(all(
    unix,
    not(any(target_os = "linux", target_os = "android", target_os = "macos", target_os = "ios"))
))]
const UNIX_O_NONBLOCK: i32 = 0x0000_0800;

#[cfg(unix)]
unsafe extern "C" {
    #[link_name = "kill"]
    fn unix_kill(process_id: i32, signal: i32) -> i32;
    #[link_name = "pipe"]
    fn unix_pipe(file_descriptors: *mut i32) -> i32;
    #[link_name = "fcntl"]
    fn unix_fcntl(file_descriptor: i32, command: i32, ...) -> i32;
    #[link_name = "getuid"]
    fn unix_getuid() -> u32;
    #[link_name = "setsid"]
    pub(super) fn unix_setsid() -> i32;
    #[cfg(test)]
    #[link_name = "close"]
    pub(super) fn unix_close(file_descriptor: i32) -> i32;
}

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
struct MacProcessShortBsdInfo {
    process_id: u32,
    parent_id: u32,
    process_group_id: u32,
    _status: u32,
    _command: [u8; 16],
    _flags: u32,
    owner_id: u32,
    _group_owner_id: u32,
    _real_owner_id: u32,
    _real_group_owner_id: u32,
    _saved_owner_id: u32,
    _saved_group_owner_id: u32,
    _reserved: u32,
}

#[cfg(target_os = "macos")]
#[repr(C)]
struct MacProcessBsdInfo {
    _flags: u32,
    _status: u32,
    _exit_status: u32,
    process_id: u32,
    parent_id: u32,
    owner_id: u32,
    _group_owner_id: u32,
    _real_owner_id: u32,
    _real_group_owner_id: u32,
    _saved_owner_id: u32,
    _saved_group_owner_id: u32,
    _reserved: u32,
    _command: [u8; 16],
    _name: [u8; 32],
    _open_file_count: u32,
    process_group_id: u32,
    _job_control_count: u32,
    _controlling_device: u32,
    _foreground_group_id: u32,
    _nice: i32,
    _start_seconds: u64,
    _start_microseconds: u64,
}

#[cfg(target_os = "macos")]
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
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
#[repr(C)]
struct MacProcessBsdInfoWithUniqueId {
    process: MacProcessBsdInfo,
    identity: MacProcessUniqueInfo,
}

#[cfg(target_os = "macos")]
const _: () = assert!(std::mem::size_of::<MacProcessShortBsdInfo>() == 64);
#[cfg(target_os = "macos")]
const _: () = assert!(std::mem::size_of::<MacProcessBsdInfo>() == 136);
#[cfg(target_os = "macos")]
const _: () = assert!(std::mem::size_of::<MacProcessUniqueInfo>() == 56);
#[cfg(target_os = "macos")]
const _: () = assert!(std::mem::size_of::<MacProcessBsdInfoWithUniqueId>() == 192);

#[cfg(target_os = "macos")]
#[link(name = "proc")]
unsafe extern "C" {
    fn proc_listallpids(buffer: *mut std::ffi::c_void, buffer_size: i32) -> i32;
    fn proc_pidinfo(
        process_id: i32,
        flavor: i32,
        argument: u64,
        buffer: *mut std::ffi::c_void,
        buffer_size: i32,
    ) -> i32;
}

#[cfg(target_os = "macos")]
unsafe extern "C" {
    fn sysctl(
        name: *mut i32,
        name_length: u32,
        old_value: *mut std::ffi::c_void,
        old_length: *mut usize,
        new_value: *mut std::ffi::c_void,
        new_length: usize,
    ) -> i32;
}

#[cfg(unix)]
fn unix_descendant_liveness_pipe() -> io::Result<(fs::File, fs::File)> {
    use std::os::fd::{AsRawFd, FromRawFd};

    let mut descriptors = [-1_i32; 2];
    // SAFETY: `descriptors` provides exactly two writable file-descriptor slots.
    if unsafe { unix_pipe(descriptors.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: pipe(2) returned two new owned descriptors, transferred exactly once to `File`.
    let read = unsafe { fs::File::from_raw_fd(descriptors[0]) };
    // SAFETY: pipe(2) returned two new owned descriptors, transferred exactly once to `File`.
    let write = unsafe { fs::File::from_raw_fd(descriptors[1]) };
    // SAFETY: F_GETFL reads flags from the live descriptor and takes no variadic argument.
    let flags = unsafe { unix_fcntl(read.as_raw_fd(), UNIX_F_GETFL) };
    if flags == -1 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: F_SETFL consumes one integer flag argument for the live descriptor.
    if unsafe { unix_fcntl(read.as_raw_fd(), UNIX_F_SETFL, flags | UNIX_O_NONBLOCK) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok((read, write))
}

#[cfg(unix)]
fn unix_signal_process_group(process_group_id: i32, signal: i32) -> io::Result<bool> {
    let target = process_group_id
        .checked_neg()
        .ok_or_else(|| io::Error::other("process-group id cannot be negated"))?;
    // SAFETY: kill(2) accepts any signed pid and signal; all error returns are handled.
    let result = unsafe { unix_kill(target, signal) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(UNIX_ESRCH) {
        return Ok(false);
    }
    Err(error)
}

#[cfg(unix)]
fn unix_signal_process(process_id: i32, signal: i32) -> io::Result<bool> {
    // SAFETY: kill(2) accepts the positive process id and signal; all errors are handled.
    let result = unsafe { unix_kill(process_id, signal) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(UNIX_ESRCH) {
        return Ok(false);
    }
    Err(error)
}

#[cfg(unix)]
fn unix_signal_process_identity(identity: &UnixProcessIdentity, signal: i32) -> io::Result<bool> {
    unix_signal_process_identity_with(identity, signal, unix_process_identity, unix_signal_process)
}

#[cfg(unix)]
pub(super) fn unix_signal_process_identity_with<Lookup, Signal>(
    identity: &UnixProcessIdentity,
    signal: i32,
    lookup: Lookup,
    send_signal: Signal,
) -> io::Result<bool>
where
    Lookup: FnOnce(i32) -> io::Result<Option<UnixProcessIdentity>>,
    Signal: FnOnce(i32, i32) -> io::Result<bool>,
{
    match lookup(identity.process_id)? {
        Some(current) if current == *identity => send_signal(identity.process_id, signal),
        Some(_) | None => Ok(false),
    }
}

#[cfg(unix)]
fn unix_signal_process_group_if_anchored(
    root_identity: &UnixProcessIdentity,
    process_group_id: i32,
    signal: i32,
) -> io::Result<bool> {
    match unix_process_snapshot(root_identity.process_id)? {
        Some(current)
            if current.identity == *root_identity
                && current.process_group_id == process_group_id =>
        {
            unix_signal_process_group(process_group_id, signal)
        }
        Some(_) | None => Ok(false),
    }
}

#[cfg(unix)]
fn unix_process_identity_is_active(identity: &UnixProcessIdentity) -> io::Result<bool> {
    Ok(unix_process_identity(identity.process_id)?.is_some_and(|current| current == *identity))
}

#[cfg(unix)]
pub(super) fn unix_descendant_liveness_closed(liveness_read: &Mutex<fs::File>) -> io::Result<bool> {
    let mut liveness_read = lock_unpoisoned(liveness_read);
    let mut byte = [0_u8; 1];
    match liveness_read.read(&mut byte) {
        Ok(0) => Ok(true),
        Ok(_) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => Ok(false),
        Err(error) => Err(error),
    }
}

#[cfg(unix)]
fn ensure_unix_cleanup_before_deadline(deadline: Instant) -> io::Result<()> {
    if Instant::now() >= deadline {
        Err(io::Error::new(io::ErrorKind::TimedOut, "process-tree cleanup deadline elapsed"))
    } else {
        Ok(())
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) fn unix_process_table(deadline: Instant) -> io::Result<Vec<UnixProcessSnapshot>> {
    ensure_unix_cleanup_before_deadline(deadline)?;
    let mut snapshots = Vec::new();
    for entry in fs::read_dir("/proc")? {
        ensure_unix_cleanup_before_deadline(deadline)?;
        let entry = entry?;
        let Some(process_id) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
            .filter(|process_id| *process_id > 0)
        else {
            continue;
        };
        if snapshots.len() >= MAX_UNIX_PROCESS_COUNT {
            return Err(io::Error::other("bounded process-table capacity exceeded"));
        }
        if let Some(snapshot) = linux_process_snapshot(process_id)? {
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn unix_process_identity(process_id: i32) -> io::Result<Option<UnixProcessIdentity>> {
    Ok(unix_process_snapshot(process_id)?.map(|snapshot| snapshot.identity))
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn unix_process_snapshot(process_id: i32) -> io::Result<Option<UnixProcessSnapshot>> {
    linux_process_snapshot(process_id)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn linux_process_snapshot(process_id: i32) -> io::Result<Option<UnixProcessSnapshot>> {
    use std::os::unix::fs::MetadataExt;

    let stat_path = PathBuf::from(format!("/proc/{process_id}/stat"));
    let stat = match read_bounded_process_file(stat_path.as_path(), MAX_LINUX_PROC_STAT_BYTES) {
        Ok(stat) => stat,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    let process_path = PathBuf::from(format!("/proc/{process_id}"));
    let owner_id = match fs::metadata(process_path.as_path()) {
        Ok(metadata) => metadata.uid(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    parse_linux_process_stat(process_id, stat.as_slice(), owner_id).map(Some)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn read_bounded_process_file(path: &Path, max_bytes: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(u64::try_from(max_bytes).unwrap_or(u64::MAX).saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(io::Error::other("bounded process file exceeded capacity"));
    }
    Ok(bytes)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) fn parse_linux_process_stat(
    expected_process_id: i32,
    stat: &[u8],
    owner_id: u32,
) -> io::Result<UnixProcessSnapshot> {
    let text = std::str::from_utf8(stat)
        .map_err(|_| io::Error::other("Linux process stat was not UTF-8"))?;
    let command_start =
        text.find('(').ok_or_else(|| io::Error::other("Linux process stat command missing"))?;
    let command_end = text
        .rfind(") ")
        .ok_or_else(|| io::Error::other("Linux process stat command terminator missing"))?;
    let process_id = text[..command_start]
        .trim()
        .parse::<i32>()
        .map_err(|_| io::Error::other("Linux process stat pid invalid"))?;
    if process_id != expected_process_id {
        return Err(io::Error::other("Linux process stat pid changed"));
    }
    let fields = text[command_end.saturating_add(2)..].split_whitespace().collect::<Vec<_>>();
    if fields.len() <= 19 {
        return Err(io::Error::other("Linux process stat fields missing"));
    }
    let parent_id = fields[1]
        .parse::<i32>()
        .map_err(|_| io::Error::other("Linux process stat ppid invalid"))?;
    let process_group_id = fields[2]
        .parse::<i32>()
        .map_err(|_| io::Error::other("Linux process stat pgrp invalid"))?;
    let start_token_low = fields[19]
        .parse::<u64>()
        .map_err(|_| io::Error::other("Linux process stat start token invalid"))?;
    Ok(UnixProcessSnapshot {
        identity: UnixProcessIdentity { process_id, start_token_high: 0, start_token_low },
        parent_id,
        process_group_id,
        owner_id,
    })
}

#[cfg(target_os = "macos")]
pub(super) fn unix_process_table(deadline: Instant) -> io::Result<Vec<UnixProcessSnapshot>> {
    let process_ids = mac_process_ids(deadline)?;
    // SAFETY: getuid(2) has no arguments and cannot violate memory safety.
    let current_owner_id = unsafe { unix_getuid() };
    let mut snapshots = Vec::with_capacity(process_ids.len());
    for process_id in process_ids {
        ensure_unix_cleanup_before_deadline(deadline)?;
        if let Some(snapshot) = mac_process_table_snapshot(process_id, current_owner_id)? {
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

#[cfg(target_os = "macos")]
fn mac_process_ids(deadline: Instant) -> io::Result<Vec<i32>> {
    ensure_unix_cleanup_before_deadline(deadline)?;
    let mut process_ids = vec![0_i32; MAX_UNIX_PROCESS_COUNT];
    let buffer_size = i32::try_from(process_ids.len().saturating_mul(std::mem::size_of::<i32>()))
        .map_err(|_| io::Error::other("process table buffer is too large"))?;
    // SAFETY: `process_ids` is a writable buffer of exactly `buffer_size` bytes.
    let count = unsafe { proc_listallpids(process_ids.as_mut_ptr().cast(), buffer_size) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    let count =
        usize::try_from(count).map_err(|_| io::Error::other("process table count is invalid"))?;
    if count >= process_ids.len() {
        return Err(io::Error::other("bounded process-table capacity exceeded"));
    }
    process_ids.truncate(count);
    process_ids.retain(|process_id| *process_id > 0);
    Ok(process_ids)
}

#[cfg(target_os = "macos")]
fn unix_process_identity(process_id: i32) -> io::Result<Option<UnixProcessIdentity>> {
    Ok(mac_process_unique_info(process_id)?
        .map(|information| mac_process_identity(process_id, information)))
}

#[cfg(target_os = "macos")]
fn unix_process_snapshot(process_id: i32) -> io::Result<Option<UnixProcessSnapshot>> {
    mac_process_snapshot(process_id)
}

#[cfg(target_os = "macos")]
fn mac_process_snapshot(process_id: i32) -> io::Result<Option<UnixProcessSnapshot>> {
    match mac_process_bsd_with_unique_id(process_id) {
        Ok(Some(information)) => mac_process_snapshot_from_metadata(
            process_id,
            information.process.process_id,
            information.process.parent_id,
            information.process.process_group_id,
            information.process.owner_id,
            information.identity,
        ),
        Ok(None) => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            let Some(information) = mac_process_short_bsd_info(process_id)? else {
                return Ok(None);
            };
            mac_process_snapshot_unprivileged(process_id, information)
        }
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "macos")]
fn mac_process_table_snapshot(
    process_id: i32,
    current_owner_id: u32,
) -> io::Result<Option<UnixProcessSnapshot>> {
    let Some(prefilter) = mac_process_short_bsd_info(process_id)? else {
        return Ok(None);
    };
    if prefilter.owner_id != current_owner_id {
        return Ok(None);
    }
    match mac_process_bsd_with_unique_id(process_id) {
        Ok(Some(information)) if information.process.owner_id == current_owner_id => {
            mac_process_snapshot_from_metadata(
                process_id,
                information.process.process_id,
                information.process.parent_id,
                information.process.process_group_id,
                information.process.owner_id,
                information.identity,
            )
        }
        Ok(Some(_)) | Ok(None) => Ok(None),
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            let snapshot = mac_process_snapshot_unprivileged(process_id, prefilter)?;
            Ok(snapshot.filter(|snapshot| snapshot.owner_id == current_owner_id))
        }
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "macos")]
fn mac_process_snapshot_unprivileged(
    process_id: i32,
    information_before: MacProcessShortBsdInfo,
) -> io::Result<Option<UnixProcessSnapshot>> {
    let Some(identity) = mac_process_unique_info(process_id)? else {
        return Ok(None);
    };
    let Some(information_after) = mac_process_short_bsd_info(process_id)? else {
        return Ok(None);
    };
    // Stable metadata around the unique-ID read prevents a recycled PID from mixing identities.
    if information_before != information_after {
        return Ok(None);
    }
    mac_process_snapshot_from_metadata(
        process_id,
        information_after.process_id,
        information_after.parent_id,
        information_after.process_group_id,
        information_after.owner_id,
        identity,
    )
}

#[cfg(target_os = "macos")]
fn mac_process_snapshot_from_metadata(
    process_id: i32,
    reported_process_id: u32,
    parent_id: u32,
    process_group_id: u32,
    owner_id: u32,
    identity: MacProcessUniqueInfo,
) -> io::Result<Option<UnixProcessSnapshot>> {
    if reported_process_id != u32::try_from(process_id).unwrap_or(u32::MAX) {
        return Ok(None);
    }
    Ok(Some(UnixProcessSnapshot {
        identity: mac_process_identity(process_id, identity),
        parent_id: i32::try_from(parent_id)
            .map_err(|_| io::Error::other("macOS parent pid is invalid"))?,
        process_group_id: i32::try_from(process_group_id)
            .map_err(|_| io::Error::other("macOS process group is invalid"))?,
        owner_id,
    }))
}

#[cfg(target_os = "macos")]
fn mac_process_identity(process_id: i32, information: MacProcessUniqueInfo) -> UnixProcessIdentity {
    UnixProcessIdentity {
        process_id,
        start_token_high: information.unique_id,
        start_token_low: u64::from(u32::from_ne_bytes(information.id_version.to_ne_bytes())),
    }
}

#[cfg(target_os = "macos")]
fn mac_process_bsd_with_unique_id(
    process_id: i32,
) -> io::Result<Option<MacProcessBsdInfoWithUniqueId>> {
    const PROC_PIDT_BSDINFOWITHUNIQID: i32 = 18;

    let mut information = std::mem::MaybeUninit::<MacProcessBsdInfoWithUniqueId>::zeroed();
    let buffer_size = i32::try_from(std::mem::size_of::<MacProcessBsdInfoWithUniqueId>())
        .map_err(|_| io::Error::other("macOS combined process information buffer is too large"))?;
    // SAFETY: the buffer matches the kernel's fixed PROC_PIDT_BSDINFOWITHUNIQID ABI.
    let read = unsafe {
        proc_pidinfo(
            process_id,
            PROC_PIDT_BSDINFOWITHUNIQID,
            0,
            information.as_mut_ptr().cast(),
            buffer_size,
        )
    };
    // SAFETY: the combined C layout contains only integer and byte fields.
    unsafe {
        mac_process_info_result(
            read,
            buffer_size,
            information,
            "macOS combined process information",
        )
    }
}

#[cfg(target_os = "macos")]
fn mac_process_short_bsd_info(process_id: i32) -> io::Result<Option<MacProcessShortBsdInfo>> {
    const PROC_PIDT_SHORTBSDINFO: i32 = 13;

    let mut information = std::mem::MaybeUninit::<MacProcessShortBsdInfo>::zeroed();
    let buffer_size = i32::try_from(std::mem::size_of::<MacProcessShortBsdInfo>())
        .map_err(|_| io::Error::other("macOS short process information buffer is too large"))?;
    // SAFETY: the buffer matches the kernel's fixed PROC_PIDT_SHORTBSDINFO ABI.
    let read = unsafe {
        proc_pidinfo(
            process_id,
            PROC_PIDT_SHORTBSDINFO,
            0,
            information.as_mut_ptr().cast(),
            buffer_size,
        )
    };
    // SAFETY: MacProcessShortBsdInfo is a fixed C layout containing only integer and byte fields.
    unsafe {
        mac_process_info_result(read, buffer_size, information, "macOS short process information")
    }
}

#[cfg(target_os = "macos")]
fn mac_process_unique_info(process_id: i32) -> io::Result<Option<MacProcessUniqueInfo>> {
    const PROC_PIDUNIQIDENTIFIERINFO: i32 = 17;

    let mut information = std::mem::MaybeUninit::<MacProcessUniqueInfo>::zeroed();
    let buffer_size = i32::try_from(std::mem::size_of::<MacProcessUniqueInfo>())
        .map_err(|_| io::Error::other("macOS unique process information buffer is too large"))?;
    // SAFETY: the buffer matches the kernel's fixed PROC_PIDUNIQIDENTIFIERINFO ABI.
    let read = unsafe {
        proc_pidinfo(
            process_id,
            PROC_PIDUNIQIDENTIFIERINFO,
            0,
            information.as_mut_ptr().cast(),
            buffer_size,
        )
    };
    // SAFETY: MacProcessUniqueInfo is a fixed C layout containing only integer and byte fields.
    unsafe {
        mac_process_info_result(read, buffer_size, information, "macOS unique process information")
    }
}

/// Converts a complete `proc_pidinfo` write into its plain C-layout value.
///
/// # Safety
/// `T` must match the requested flavor's fixed ABI and permit every returned byte pattern.
#[cfg(target_os = "macos")]
unsafe fn mac_process_info_result<T>(
    read: i32,
    expected_size: i32,
    information: std::mem::MaybeUninit<T>,
    description: &str,
) -> io::Result<Option<T>> {
    if read == 0 {
        let error = io::Error::last_os_error();
        return if matches!(error.raw_os_error(), Some(UNIX_ESRCH) | Some(UNIX_ENOENT)) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    if read != expected_size {
        return Err(io::Error::other(format!("{description} was truncated")));
    }
    // SAFETY: proc_pidinfo reported a complete buffer of the requested structure size.
    Ok(Some(unsafe { information.assume_init() }))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android", target_os = "macos"))))]
pub(super) fn unix_process_table(_deadline: Instant) -> io::Result<Vec<UnixProcessSnapshot>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "identity-bound process enumeration is unsupported",
    ))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android", target_os = "macos"))))]
fn unix_process_identity(_process_id: i32) -> io::Result<Option<UnixProcessIdentity>> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "identity-bound process lookup is unsupported"))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android", target_os = "macos"))))]
fn unix_process_snapshot(_process_id: i32) -> io::Result<Option<UnixProcessSnapshot>> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "identity-bound process lookup is unsupported"))
}

#[cfg(unix)]
fn unix_marker_processes(
    process_table: &[UnixProcessSnapshot],
    root_identity: &UnixProcessIdentity,
    preexisting_processes: &BTreeMap<i32, UnixProcessIdentity>,
    known_descendants: &BTreeMap<i32, UnixProcessIdentity>,
    marker: &str,
    deadline: Instant,
) -> io::Result<Vec<UnixProcessIdentity>> {
    // A scan must not overlap the spawn-to-registration window of another marker tree.
    let _launch_guard = lock_unpoisoned(&UNIX_PROCESS_TREE_LAUNCH_LOCK);
    let mut classified_processes = known_descendants.clone();
    classified_processes.extend(unix_other_tree_processes(process_table, marker));
    // The root starts a new session, so its descendants cannot enter a group that still contains
    // an exact pre-launch member. This proof avoids reading unrelated protected environments.
    let preexisting_process_groups =
        unix_preexisting_process_groups(process_table, preexisting_processes);
    // The marker is non-secret, but scanning a process environment can encounter credentials.
    // Buffers are bounded, never formatted, and zeroed immediately after the exact match check.
    let assignment = format!("{QA_PROCESS_TREE_MARKER_ENV}={marker}").into_bytes();
    // SAFETY: getuid(2) has no arguments and cannot violate memory safety.
    let current_owner_id = unsafe { unix_getuid() };
    let mut total_bytes = 0_usize;
    let mut marked = Vec::new();
    // Narrow the scan to processes whose ownership is not already proven another way. Exact
    // pre-launch identities cannot carry the marker, while the root and known descendants have
    // identity-bound liveness checks that do not expose unrelated process environments.
    for snapshot in process_table.iter().filter(|snapshot| {
        unix_process_requires_marker_scan(
            snapshot,
            root_identity,
            preexisting_processes,
            &preexisting_process_groups,
            &classified_processes,
            current_owner_id,
        )
    }) {
        ensure_unix_cleanup_before_deadline(deadline)?;
        let (has_marker, bytes_read) = unix_process_has_marker(
            snapshot.identity.process_id,
            assignment.as_slice(),
            deadline,
        )
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "process marker scan failed for pid {} ppid {} pgrp {} start {}:{} (root {}:{}): {error}",
                    snapshot.identity.process_id,
                    snapshot.parent_id,
                    snapshot.process_group_id,
                    snapshot.identity.start_token_high,
                    snapshot.identity.start_token_low,
                    root_identity.start_token_high,
                    root_identity.start_token_low,
                ),
            )
        })?;
        total_bytes = total_bytes
            .checked_add(bytes_read)
            .ok_or_else(|| io::Error::other("process environment scan overflow"))?;
        if total_bytes > MAX_UNIX_PROCESS_ENV_TOTAL_BYTES {
            return Err(io::Error::other("bounded process environment scan exceeded capacity"));
        }
        if has_marker
            && unix_process_identity(snapshot.identity.process_id)? == Some(snapshot.identity)
        {
            marked.push(snapshot.identity);
        }
    }
    Ok(marked)
}

#[cfg(unix)]
fn unix_process_tree_registry() -> &'static Mutex<UnixProcessTreeRegistry> {
    UNIX_PROCESS_TREE_REGISTRY.get_or_init(|| Mutex::new(BTreeMap::new()))
}

#[cfg(unix)]
fn unix_register_process_tree_identity(marker: &str, identity: UnixProcessIdentity) {
    lock_unpoisoned(unix_process_tree_registry())
        .entry(marker.to_owned())
        .or_default()
        .insert(identity.process_id, identity);
}

#[cfg(unix)]
fn unix_register_process_tree_identities(
    marker: &str,
    identities: &BTreeMap<i32, UnixProcessIdentity>,
) {
    let mut registry = lock_unpoisoned(unix_process_tree_registry());
    let registered = registry.entry(marker.to_owned()).or_default();
    for identity in identities.values() {
        registered.insert(identity.process_id, *identity);
    }
}

#[cfg(unix)]
fn unix_unregister_process_tree(marker: &str) {
    lock_unpoisoned(unix_process_tree_registry()).remove(marker);
}

#[cfg(unix)]
fn unix_other_tree_processes(
    process_table: &[UnixProcessSnapshot],
    current_marker: &str,
) -> BTreeMap<i32, UnixProcessIdentity> {
    let registry = lock_unpoisoned(unix_process_tree_registry());
    unix_other_tree_processes_with_registry(process_table, current_marker, &registry)
}

/// Returns exact identities owned by another active marker tree or its live descendants.
#[cfg(unix)]
pub(super) fn unix_other_tree_processes_with_registry(
    process_table: &[UnixProcessSnapshot],
    current_marker: &str,
    registry: &UnixProcessTreeRegistry,
) -> BTreeMap<i32, UnixProcessIdentity> {
    let snapshots_by_id = process_table
        .iter()
        .map(|snapshot| (snapshot.identity.process_id, snapshot))
        .collect::<BTreeMap<_, _>>();
    let mut roots = std::collections::BTreeSet::new();
    for identities in registry
        .iter()
        .filter_map(|(marker, identities)| (marker != current_marker).then_some(identities))
    {
        for identity in identities.values() {
            if snapshots_by_id
                .get(&identity.process_id)
                .is_some_and(|snapshot| snapshot.identity == *identity)
            {
                roots.insert(identity.process_id);
            }
        }
    }
    let mut owned_process_ids = unix_recursive_descendants(process_table, &roots);
    owned_process_ids.extend(roots);
    process_table
        .iter()
        .filter(|snapshot| owned_process_ids.contains(&snapshot.identity.process_id))
        .map(|snapshot| (snapshot.identity.process_id, snapshot.identity))
        .collect()
}

/// Returns groups that still contain an exact process observed before the isolated session launch.
#[cfg(unix)]
pub(super) fn unix_preexisting_process_groups(
    process_table: &[UnixProcessSnapshot],
    preexisting_processes: &BTreeMap<i32, UnixProcessIdentity>,
) -> std::collections::BTreeSet<i32> {
    process_table
        .iter()
        .filter_map(|snapshot| {
            (preexisting_processes.get(&snapshot.identity.process_id) == Some(&snapshot.identity))
                .then_some(snapshot.process_group_id)
        })
        .collect()
}

#[cfg(unix)]
pub(super) fn unix_process_requires_marker_scan(
    candidate: &UnixProcessSnapshot,
    root: &UnixProcessIdentity,
    preexisting_processes: &BTreeMap<i32, UnixProcessIdentity>,
    preexisting_process_groups: &std::collections::BTreeSet<i32>,
    known_descendants: &BTreeMap<i32, UnixProcessIdentity>,
    current_owner_id: u32,
) -> bool {
    if candidate.owner_id != current_owner_id
        || candidate.identity == *root
        || preexisting_processes.get(&candidate.identity.process_id) == Some(&candidate.identity)
        || preexisting_process_groups.contains(&candidate.process_group_id)
        || known_descendants.get(&candidate.identity.process_id) == Some(&candidate.identity)
    {
        return false;
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        // Linux start ticks are monotonic, so an older process cannot have inherited the marker.
        (candidate.identity.start_token_high, candidate.identity.start_token_low)
            >= (root.start_token_high, root.start_token_low)
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        // macOS process identity tokens are opaque rather than ordered. Every unclassified
        // same-owner candidate must therefore remain eligible for the containment-marker scan.
        let _ = root;
        true
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
fn unix_process_baseline(deadline: Instant) -> io::Result<BTreeMap<i32, UnixProcessIdentity>> {
    // SAFETY: getuid(2) has no arguments and cannot violate memory safety.
    let current_owner_id = unsafe { unix_getuid() };
    Ok(unix_process_table(deadline)?
        .into_iter()
        .filter(|snapshot| snapshot.owner_id == current_owner_id)
        .map(|snapshot| (snapshot.identity.process_id, snapshot.identity))
        .collect())
}

#[cfg(target_os = "macos")]
fn unix_process_baseline(deadline: Instant) -> io::Result<BTreeMap<i32, UnixProcessIdentity>> {
    let process_ids = mac_process_ids(deadline)?;
    // SAFETY: getuid(2) has no arguments and cannot violate memory safety.
    let current_owner_id = unsafe { unix_getuid() };
    mac_process_baseline_with(process_ids.as_slice(), current_owner_id, |process_id| {
        ensure_unix_cleanup_before_deadline(deadline)?;
        mac_process_table_snapshot(process_id, current_owner_id)
    })
}

/// Collects exact pre-launch identities while leaving protected macOS processes unclassified.
///
/// # Errors
/// Returns a lookup error unless it represents a process that macOS does not permit inspecting.
#[cfg(target_os = "macos")]
pub(super) fn mac_process_baseline_with<Lookup>(
    process_ids: &[i32],
    current_owner_id: u32,
    mut lookup: Lookup,
) -> io::Result<BTreeMap<i32, UnixProcessIdentity>>
where
    Lookup: FnMut(i32) -> io::Result<Option<UnixProcessSnapshot>>,
{
    let mut baseline = BTreeMap::new();
    for &process_id in process_ids {
        let snapshot = match lookup(process_id) {
            Ok(snapshot) => snapshot,
            // Baseline entries only exempt exact identities from marker reads. Leaving a protected
            // process unclassified keeps strict cleanup enumeration fail-closed if it persists.
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => continue,
            Err(error) => return Err(error),
        };
        let Some(snapshot) = snapshot else {
            continue;
        };
        if snapshot.owner_id == current_owner_id {
            baseline.insert(snapshot.identity.process_id, snapshot.identity);
        }
    }
    Ok(baseline)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn unix_process_has_marker(
    process_id: i32,
    assignment: &[u8],
    deadline: Instant,
) -> io::Result<(bool, usize)> {
    ensure_unix_cleanup_before_deadline(deadline)?;
    let path = PathBuf::from(format!("/proc/{process_id}/environ"));
    let mut bytes = match read_bounded_process_file(path.as_path(), MAX_UNIX_PROCESS_ENV_BYTES) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((false, 0)),
        Err(error) => return Err(error),
    };
    let bytes_read = bytes.len();
    let has_marker = bytes.split(|byte| *byte == 0).any(|entry| entry == assignment);
    bytes.fill(0);
    Ok((has_marker, bytes_read))
}

#[cfg(target_os = "macos")]
fn unix_process_has_marker(
    process_id: i32,
    assignment: &[u8],
    deadline: Instant,
) -> io::Result<(bool, usize)> {
    const CTL_KERN: i32 = 1;
    const KERN_PROCARGS2: i32 = 49;

    ensure_unix_cleanup_before_deadline(deadline)?;
    let mut name = [CTL_KERN, KERN_PROCARGS2, process_id];
    let mut bytes = vec![0_u8; MAX_UNIX_PROCESS_ENV_BYTES];
    let mut length = bytes.len();
    // SAFETY: `bytes` is writable for `length` bytes and the MIB identifies one process image.
    if unsafe {
        sysctl(
            name.as_mut_ptr(),
            u32::try_from(name.len()).unwrap_or(u32::MAX),
            bytes.as_mut_ptr().cast(),
            &mut length,
            std::ptr::null_mut(),
            0,
        )
    } != 0
    {
        bytes.fill(0);
        let error = io::Error::last_os_error();
        return if matches!(error.raw_os_error(), Some(UNIX_ESRCH) | Some(UNIX_ENOENT)) {
            Ok((false, 0))
        } else {
            Err(error)
        };
    }
    if length > bytes.len() {
        bytes.fill(0);
        return Err(io::Error::other("process environment grew during bounded scan"));
    }
    let has_marker = bytes[..length].split(|byte| *byte == 0).any(|entry| entry == assignment);
    bytes.fill(0);
    Ok((has_marker, length))
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android", target_os = "macos"))))]
fn unix_process_has_marker(
    _process_id: i32,
    _assignment: &[u8],
    _deadline: Instant,
) -> io::Result<(bool, usize)> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "process environment containment scan is unsupported",
    ))
}

#[cfg(unix)]
pub(super) fn unix_recursive_descendants(
    process_table: &[UnixProcessSnapshot],
    roots: &std::collections::BTreeSet<i32>,
) -> std::collections::BTreeSet<i32> {
    let mut family = roots.clone();
    loop {
        let previous_count = family.len();
        for snapshot in process_table {
            if family.contains(&snapshot.parent_id) {
                family.insert(snapshot.identity.process_id);
            }
        }
        if family.len() == previous_count {
            break;
        }
    }
    for root in roots {
        family.remove(root);
    }
    family
}

#[cfg(unix)]
pub(super) fn unix_identity_matching_roots(
    process_table: &[UnixProcessSnapshot],
    root_identity: &UnixProcessIdentity,
    tracked: &BTreeMap<i32, UnixProcessIdentity>,
) -> std::collections::BTreeSet<i32> {
    process_table
        .iter()
        .filter_map(|snapshot| {
            let identity = snapshot.identity;
            (identity == *root_identity
                || tracked.get(&identity.process_id).is_some_and(|expected| *expected == identity))
            .then_some(identity.process_id)
        })
        .collect()
}

#[cfg(unix)]
fn unix_process_group_is_active(process_group_id: i32) -> io::Result<bool> {
    unix_signal_process_group(process_group_id, 0)
}

#[cfg(windows)]
impl DaemonProcessTree {
    pub(super) fn terminate(&self, _deadline: Instant) -> io::Result<()> {
        self.job.terminate()
    }

    pub(super) fn wait_until_inactive(&self, deadline: Instant) -> io::Result<bool> {
        self.job.wait_until_inactive(deadline.saturating_duration_since(Instant::now()))
    }
}

#[cfg(not(any(unix, windows)))]
impl DaemonProcessTree {
    pub(super) fn terminate(&self, _deadline: Instant) -> io::Result<()> {
        Err(io::Error::new(io::ErrorKind::Unsupported, "process-tree cleanup unsupported"))
    }

    pub(super) fn wait_until_inactive(&self, _deadline: Instant) -> io::Result<bool> {
        Ok(false)
    }
}
