use super::*;

pub(super) struct QaDaemonEnvironment<'a> {
    pub(super) allowed_tools: &'a str,
    pub(super) state_root: &'a Path,
    pub(super) identity_root: &'a Path,
    pub(super) config_path: &'a Path,
    pub(super) vault_dir: &'a Path,
    pub(super) provider: &'a QaDaemonProviderEnvironment,
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

    command.process_group(0);
    // SAFETY: the no-op closure performs no allocation, locking, or other non-async-signal-safe
    // work. Registering it forces the fork/exec path so Apple posix_spawn cannot apply a
    // close-by-default policy to the liveness descriptor.
    unsafe {
        command.pre_exec(|| Ok(()));
    }
    let (descendant_liveness_read, descendant_liveness_write) =
        unix_descendant_liveness_pipe().context("qa.runner.daemon_liveness_pipe_failed")?;
    let containment_marker = format!("{}-{}", Ulid::new(), Ulid::new());
    command.env(QA_PROCESS_TREE_MARKER_ENV, containment_marker.as_str());
    Ok(DaemonProcessTreePreparation {
        descendant_liveness_read,
        descendant_liveness_write,
        containment_marker,
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
) -> std::result::Result<OwnedDaemonProcess, AttachDaemonProcessFailure> {
    drop(preparation.descendant_liveness_write);
    let process_group_id = match i32::try_from(child.id()) {
        Ok(process_group_id) => process_group_id,
        Err(error) => {
            return Err(AttachDaemonProcessFailure {
                error: anyhow::Error::new(error)
                    .context("qa.runner.daemon_process_group_id_invalid"),
                process: OwnedDaemonProcess {
                    child,
                    tree: None,
                    descendants_possible_without_tree: true,
                    cleanup_verified: false,
                },
            });
        }
    };
    let root_identity = match unix_process_identity(process_group_id) {
        Ok(Some(identity)) => identity,
        Ok(None) => {
            return Err(AttachDaemonProcessFailure {
                error: anyhow::anyhow!("qa.runner.daemon_process_identity_unavailable"),
                process: OwnedDaemonProcess {
                    child,
                    tree: None,
                    descendants_possible_without_tree: true,
                    cleanup_verified: false,
                },
            });
        }
        Err(error) => {
            return Err(AttachDaemonProcessFailure {
                error: anyhow::Error::new(error)
                    .context("qa.runner.daemon_process_identity_unavailable"),
                process: OwnedDaemonProcess {
                    child,
                    tree: None,
                    descendants_possible_without_tree: true,
                    cleanup_verified: false,
                },
            });
        }
    };
    Ok(OwnedDaemonProcess {
        child,
        tree: Some(DaemonProcessTree {
            root_identity,
            process_group_id,
            tracked_descendants: Mutex::new(Default::default()),
            descendant_discovery_complete: Mutex::new(false),
            descendant_liveness_read: Mutex::new(preparation.descendant_liveness_read),
            containment_marker: preparation.containment_marker,
        }),
        descendants_possible_without_tree: true,
        cleanup_verified: false,
    })
}

#[cfg(windows)]
pub(super) fn attach_daemon_process_tree(
    child: Child,
    _preparation: DaemonProcessTreePreparation,
) -> std::result::Result<OwnedDaemonProcess, AttachDaemonProcessFailure> {
    attach_windows_daemon_process_tree_with(child, WindowsJobHandle::new)
}

#[cfg(windows)]
pub(super) fn attach_windows_daemon_process_tree_with<NewJob>(
    child: Child,
    new_job: NewJob,
) -> std::result::Result<OwnedDaemonProcess, AttachDaemonProcessFailure>
where
    NewJob: FnOnce() -> io::Result<WindowsJobHandle>,
{
    use std::os::windows::io::AsRawHandle;

    let job = match new_job() {
        Ok(job) => job,
        Err(error) => {
            return Err(AttachDaemonProcessFailure {
                error: anyhow::Error::new(error).context("qa.runner.daemon_job_create_failed"),
                process: OwnedDaemonProcess {
                    child,
                    tree: None,
                    descendants_possible_without_tree: false,
                    cleanup_verified: false,
                },
            });
        }
    };
    // SAFETY: both handles are live and owned for the duration of this call.
    let assigned =
        unsafe { windows_assign_process_to_job_object(job.handle.get(), child.as_raw_handle()) };
    if assigned == 0 {
        let error = io::Error::last_os_error();
        return Err(AttachDaemonProcessFailure {
            error: anyhow::Error::new(error).context("qa.runner.daemon_job_assign_failed"),
            process: OwnedDaemonProcess {
                child,
                tree: None,
                descendants_possible_without_tree: false,
                cleanup_verified: false,
            },
        });
    }
    if let Err(error) = resume_suspended_windows_process(child.id()) {
        return Err(AttachDaemonProcessFailure {
            error: anyhow::Error::new(error).context("qa.runner.daemon_resume_failed"),
            process: OwnedDaemonProcess {
                child,
                tree: Some(DaemonProcessTree { job }),
                descendants_possible_without_tree: true,
                cleanup_verified: false,
            },
        });
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
) -> std::result::Result<OwnedDaemonProcess, AttachDaemonProcessFailure> {
    Err(AttachDaemonProcessFailure {
        error: anyhow::anyhow!("qa.runner.daemon_process_tree_unsupported"),
        process: OwnedDaemonProcess {
            child,
            tree: None,
            descendants_possible_without_tree: true,
            cleanup_verified: false,
        },
    })
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
            let mut descendants_active = false;
            for identity in lock_unpoisoned(&self.tracked_descendants).values() {
                descendants_active |= unix_process_identity_is_active(identity)?;
            }
            let root_active = unix_process_identity_is_active(&self.root_identity)?;
            let process_table = unix_process_table(deadline)?;
            let marker_active = unix_marker_processes(
                process_table.as_slice(),
                self.containment_marker.as_str(),
                deadline,
            )?
            .into_iter()
            .any(|identity| identity != self.root_identity);
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
            let marker_processes = unix_marker_processes(
                process_table.as_slice(),
                self.containment_marker.as_str(),
                deadline,
            )?;
            let roots = unix_identity_matching_roots(
                process_table.as_slice(),
                &self.root_identity,
                &lock_unpoisoned(&self.tracked_descendants),
            );
            let discovered = unix_recursive_descendants(process_table.as_slice(), &roots);
            let mut tracked = lock_unpoisoned(&self.tracked_descendants);
            let previous = tracked.clone();
            for snapshot in process_table
                .iter()
                .filter(|snapshot| discovered.contains(&snapshot.identity.process_id))
            {
                if snapshot.identity != self.root_identity {
                    tracked.insert(snapshot.identity.process_id, snapshot.identity);
                }
            }
            for identity in marker_processes {
                if identity != self.root_identity {
                    tracked.insert(identity.process_id, identity);
                }
            }
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
    #[cfg(test)]
    #[link_name = "setsid"]
    fn unix_setsid() -> i32;
    #[cfg(test)]
    #[link_name = "closefrom"]
    fn unix_closefrom(first_file_descriptor: i32);
}

#[cfg(target_os = "macos")]
#[repr(C)]
pub(super) struct MacProcessBsdInfo {
    _flags: u32,
    _status: u32,
    _xstatus: u32,
    pub(super) process_id: u32,
    pub(super) parent_id: u32,
    pub(super) owner_id: u32,
    _group_owner_id: u32,
    _real_owner_id: u32,
    _real_group_owner_id: u32,
    _saved_owner_id: u32,
    _saved_group_owner_id: u32,
    _reserved: u32,
    _command: [u8; 16],
    _name: [u8; 32],
    _open_file_count: u32,
    pub(super) process_group_id: u32,
    _job_control_count: u32,
    _controlling_device: u32,
    _foreground_group_id: u32,
    _nice: i32,
    start_seconds: u64,
    start_microseconds: u64,
}

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
    let mut snapshots = Vec::with_capacity(count);
    for process_id in process_ids {
        ensure_unix_cleanup_before_deadline(deadline)?;
        if process_id > 0 {
            if let Some(snapshot) = mac_process_snapshot(process_id)? {
                snapshots.push(snapshot);
            }
        }
    }
    Ok(snapshots)
}

#[cfg(target_os = "macos")]
fn unix_process_identity(process_id: i32) -> io::Result<Option<UnixProcessIdentity>> {
    Ok(unix_process_snapshot(process_id)?.map(|snapshot| snapshot.identity))
}

#[cfg(target_os = "macos")]
fn unix_process_snapshot(process_id: i32) -> io::Result<Option<UnixProcessSnapshot>> {
    mac_process_snapshot(process_id)
}

#[cfg(target_os = "macos")]
fn mac_process_snapshot(process_id: i32) -> io::Result<Option<UnixProcessSnapshot>> {
    const PROC_PIDTBSDINFO: i32 = 3;

    let mut information = std::mem::MaybeUninit::<MacProcessBsdInfo>::zeroed();
    let buffer_size = i32::try_from(std::mem::size_of::<MacProcessBsdInfo>())
        .map_err(|_| io::Error::other("macOS process information buffer is too large"))?;
    // SAFETY: `information` is a correctly sized writable buffer for PROC_PIDTBSDINFO.
    let read = unsafe {
        proc_pidinfo(process_id, PROC_PIDTBSDINFO, 0, information.as_mut_ptr().cast(), buffer_size)
    };
    if read == 0 {
        let error = io::Error::last_os_error();
        return if matches!(error.raw_os_error(), Some(UNIX_ESRCH) | Some(UNIX_ENOENT)) {
            Ok(None)
        } else {
            Err(error)
        };
    }
    if read != buffer_size {
        return Err(io::Error::other("macOS process information was truncated"));
    }
    // SAFETY: proc_pidinfo reported a complete buffer of the requested structure size.
    let information = unsafe { information.assume_init() };
    if information.process_id != u32::try_from(process_id).unwrap_or(u32::MAX) {
        return Ok(None);
    }
    Ok(Some(UnixProcessSnapshot {
        identity: UnixProcessIdentity {
            process_id,
            start_token_high: information.start_seconds,
            start_token_low: information.start_microseconds,
        },
        parent_id: i32::try_from(information.parent_id)
            .map_err(|_| io::Error::other("macOS parent pid is invalid"))?,
        process_group_id: i32::try_from(information.process_group_id)
            .map_err(|_| io::Error::other("macOS process group is invalid"))?,
        owner_id: information.owner_id,
    }))
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
    marker: &str,
    deadline: Instant,
) -> io::Result<Vec<UnixProcessIdentity>> {
    // The marker is non-secret, but scanning a process environment can encounter credentials.
    // Buffers are bounded, never formatted, and zeroed immediately after the exact match check.
    let assignment = format!("{QA_PROCESS_TREE_MARKER_ENV}={marker}").into_bytes();
    // SAFETY: getuid(2) has no arguments and cannot violate memory safety.
    let current_owner_id = unsafe { unix_getuid() };
    let mut total_bytes = 0_usize;
    let mut marked = Vec::new();
    for snapshot in process_table.iter().filter(|snapshot| snapshot.owner_id == current_owner_id) {
        ensure_unix_cleanup_before_deadline(deadline)?;
        let (has_marker, bytes_read) =
            unix_process_has_marker(snapshot.identity.process_id, assignment.as_slice(), deadline)?;
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
