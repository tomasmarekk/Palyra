use std::{
    fs,
    path::{Path, PathBuf},
    thread,
    time::{Duration, SystemTime},
};

#[cfg(windows)]
use std::{
    io::Read,
    os::windows::process::CommandExt,
    process::{Command, Stdio},
};

use ulid::Ulid;

use crate::{
    current_unix_ms,
    filesystem::{
        canonicalize_existing_dir, ensure_owner_only_file, ensure_path_within_root,
        validate_no_parent_components,
    },
    VaultError, VaultScope,
};

pub(crate) const METADATA_FILE: &str = "metadata.json";
pub(crate) const METADATA_LOCK_FILE: &str = "metadata.lock";
pub(crate) const METADATA_VERSION: u32 = 1;
const METADATA_LOCK_TIMEOUT: Duration = Duration::from_secs(3);
const METADATA_LOCK_RETRY: Duration = Duration::from_millis(20);
const METADATA_LOCK_STALE_AGE: Duration = Duration::from_secs(30);
#[cfg(windows)]
const WINDOWS_TASKLIST_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(windows)]
const WINDOWS_TASKLIST_POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct MetadataFile {
    pub(crate) version: u32,
    pub(crate) entries: Vec<MetadataEntry>,
}

impl Default for MetadataFile {
    fn default() -> Self {
        Self { version: METADATA_VERSION, entries: Vec::new() }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct MetadataEntry {
    pub(crate) scope: VaultScope,
    pub(crate) key: String,
    pub(crate) object_id: String,
    pub(crate) created_at_unix_ms: i64,
    pub(crate) updated_at_unix_ms: i64,
    pub(crate) value_bytes: usize,
}

pub(crate) struct MetadataLockGuard {
    path: PathBuf,
}

impl Drop for MetadataLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MetadataLockMarker {
    pid: u32,
}

pub(crate) fn metadata_path(root: &Path) -> PathBuf {
    root.join(METADATA_FILE)
}

pub(crate) fn ensure_metadata_exists(root: &Path) -> Result<(), VaultError> {
    let _lock = acquire_metadata_lock(root)?;
    let metadata_path = metadata_path(root);
    ensure_path_within_root(root, metadata_path.as_path(), "vault metadata path")?;
    if metadata_path.exists() {
        return Ok(());
    }
    write_metadata(root, &MetadataFile::default())
}

pub(crate) fn acquire_metadata_lock(root: &Path) -> Result<MetadataLockGuard, VaultError> {
    let lock_parent = canonicalize_existing_dir(root, "vault root directory")?;
    let lock_path = lock_parent.join(METADATA_LOCK_FILE);
    ensure_path_within_root(root, lock_path.as_path(), "vault metadata lock path")?;
    let started = SystemTime::now();
    loop {
        match fs::OpenOptions::new().create_new(true).write(true).open(&lock_path) {
            Ok(mut file) => {
                let marker = format!("pid={} ts_ms={}\n", std::process::id(), current_unix_ms()?);
                initialize_metadata_lock_marker(&lock_path, &mut file, marker.as_str())?;
                ensure_owner_only_file(&lock_path)?;
                return Ok(MetadataLockGuard { path: lock_path });
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if maybe_reclaim_stale_lock(lock_path.as_path())? {
                    continue;
                }
                if started.elapsed().unwrap_or(METADATA_LOCK_TIMEOUT) >= METADATA_LOCK_TIMEOUT {
                    return Err(VaultError::Io(format!(
                        "timed out waiting for metadata lock {}",
                        lock_path.display()
                    )));
                }
                thread::sleep(METADATA_LOCK_RETRY);
            }
            Err(error) => {
                return Err(VaultError::Io(format!(
                    "failed to acquire metadata lock {}: {error}",
                    lock_path.display()
                )));
            }
        }
    }
}

pub(crate) fn read_metadata(root: &Path) -> Result<MetadataFile, VaultError> {
    let root = canonicalize_existing_dir(root, "vault root directory")?;
    let path = root.join(METADATA_FILE);
    ensure_path_within_root(root.as_path(), path.as_path(), "vault metadata path")?;
    if !path.exists() {
        return Ok(MetadataFile::default());
    }
    let bytes = fs::read(&path).map_err(|error| {
        VaultError::Io(format!("failed to read metadata file {}: {error}", path.display()))
    })?;
    let parsed: MetadataFile = serde_json::from_slice(bytes.as_slice()).map_err(|error| {
        VaultError::Io(format!("failed to parse metadata file {}: {error}", path.display()))
    })?;
    if parsed.version != METADATA_VERSION {
        return Err(VaultError::Io(format!(
            "unsupported metadata version {} in {}",
            parsed.version,
            path.display()
        )));
    }
    Ok(parsed)
}

pub(crate) fn write_metadata(root: &Path, metadata: &MetadataFile) -> Result<(), VaultError> {
    let root = canonicalize_existing_dir(root, "vault root directory")?;
    let path = root.join(METADATA_FILE);
    let tmp_path = path.with_extension(format!("tmp.{}", Ulid::new()));
    ensure_path_within_root(root.as_path(), path.as_path(), "vault metadata path")?;
    ensure_path_within_root(root.as_path(), tmp_path.as_path(), "vault metadata temporary path")?;
    let payload = serde_json::to_vec_pretty(metadata).map_err(|error| {
        VaultError::Io(format!("failed to serialize metadata file {}: {error}", path.display()))
    })?;
    fs::write(&tmp_path, payload).map_err(|error| {
        VaultError::Io(format!(
            "failed to write metadata temporary file {}: {error}",
            tmp_path.display()
        ))
    })?;
    ensure_owner_only_file(&tmp_path)?;
    fs::rename(&tmp_path, &path).map_err(|error| {
        VaultError::Io(format!("failed to finalize metadata file {}: {error}", path.display()))
    })?;
    ensure_owner_only_file(&path)?;
    Ok(())
}

fn initialize_metadata_lock_marker(
    lock_path: &Path,
    file: &mut fs::File,
    marker: &str,
) -> Result<(), VaultError> {
    initialize_metadata_lock_marker_with_writer(lock_path, marker, |payload| {
        use std::io::Write as _;
        file.write_all(payload)?;
        file.sync_all()
    })
}

pub(crate) fn initialize_metadata_lock_marker_with_writer(
    lock_path: &Path,
    marker: &str,
    write_marker: impl FnOnce(&[u8]) -> std::io::Result<()>,
) -> Result<(), VaultError> {
    if let Err(error) = write_marker(marker.as_bytes()) {
        let _ = fs::remove_file(lock_path);
        return Err(VaultError::Io(format!(
            "failed to initialize metadata lock marker {}: {error}",
            lock_path.display()
        )));
    }
    Ok(())
}

fn maybe_reclaim_stale_lock(lock_path: &Path) -> Result<bool, VaultError> {
    maybe_reclaim_stale_lock_with_policy(lock_path, SystemTime::now(), METADATA_LOCK_STALE_AGE)
}

pub(crate) fn maybe_reclaim_stale_lock_with_policy(
    lock_path: &Path,
    now: SystemTime,
    stale_age: Duration,
) -> Result<bool, VaultError> {
    validate_no_parent_components(lock_path, "vault metadata lock path")?;
    if lock_path.file_name().and_then(|value| value.to_str()) != Some(METADATA_LOCK_FILE) {
        return Err(VaultError::Io(format!(
            "vault metadata lock path must end with '{METADATA_LOCK_FILE}'"
        )));
    }
    let lock_parent = lock_path.parent().ok_or_else(|| {
        VaultError::Io("vault metadata lock path must include a parent".to_owned())
    })?;
    let canonical_lock_parent =
        canonicalize_existing_dir(lock_parent, "vault metadata lock parent directory")?;
    let resolved_lock_path = canonical_lock_parent.join(METADATA_LOCK_FILE);
    let metadata = match fs::metadata(&resolved_lock_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(true);
        }
        Err(error) => {
            return Err(VaultError::Io(format!(
                "failed to inspect metadata lock {}: {error}",
                resolved_lock_path.display()
            )));
        }
    };
    let modified = metadata.modified().map_err(|error| {
        VaultError::Io(format!(
            "failed to inspect metadata lock timestamp {}: {error}",
            resolved_lock_path.display()
        ))
    })?;
    if now.duration_since(modified).unwrap_or(Duration::ZERO) < stale_age {
        return Ok(false);
    }
    if let Ok(raw_marker) = fs::read_to_string(&resolved_lock_path) {
        if let Some(marker) = parse_metadata_lock_marker(raw_marker.as_str()) {
            if metadata_lock_owner_is_alive(marker.pid) {
                return Ok(false);
            }
        }
    }
    match fs::remove_file(&resolved_lock_path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(VaultError::Io(format!(
            "failed to reclaim stale metadata lock {}: {error}",
            resolved_lock_path.display()
        ))),
    }
}

fn parse_metadata_lock_marker(raw: &str) -> Option<MetadataLockMarker> {
    let mut pid = None;
    let mut ts_ms_seen = false;
    for part in raw.split_whitespace() {
        if let Some(value) = part.strip_prefix("pid=") {
            pid = value.parse::<u32>().ok();
            continue;
        }
        if let Some(value) = part.strip_prefix("ts_ms=") {
            ts_ms_seen = value.parse::<u64>().is_ok();
        }
    }
    if !ts_ms_seen {
        return None;
    }
    Some(MetadataLockMarker { pid: pid? })
}

#[cfg(unix)]
fn metadata_lock_owner_is_alive(pid: u32) -> bool {
    let Ok(pid_i32) = i32::try_from(pid) else {
        return true;
    };
    let result = unsafe { libc::kill(pid_i32, 0) };
    if result == 0 {
        return true;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(code) if code == libc::ESRCH => false,
        Some(code) if code == libc::EPERM => true,
        _ => true,
    }
}

#[cfg(windows)]
fn metadata_lock_owner_is_alive(pid: u32) -> bool {
    let output = run_tasklist_for_pid(pid, WINDOWS_TASKLIST_TIMEOUT);
    let Ok(output) = output else {
        return false;
    };
    if !output.status.success() {
        return false;
    }
    let pid_marker = format!(",\"{pid}\"");
    String::from_utf8_lossy(&output.stdout).lines().any(|line| line.contains(&pid_marker))
}

#[cfg(windows)]
fn run_tasklist_for_pid(pid: u32, timeout: Duration) -> std::io::Result<std::process::Output> {
    let mut command = Command::new("tasklist");
    command.creation_flags(CREATE_NO_WINDOW);
    let mut child = command
        .arg("/FI")
        .arg(format!("PID eq {pid}"))
        .args(["/FO", "CSV", "/NH"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    wait_for_child_output_with_timeout(&mut child, timeout)
}

#[cfg(windows)]
fn wait_for_child_output_with_timeout(
    child: &mut std::process::Child,
    timeout: Duration,
) -> std::io::Result<std::process::Output> {
    let started_at = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            return collect_child_output(child, status);
        }
        if started_at.elapsed() >= timeout {
            let _ = child.kill();
            let _ = child.wait();
            return Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("subprocess exceeded timeout of {}ms", timeout.as_millis()),
            ));
        }
        thread::sleep(WINDOWS_TASKLIST_POLL_INTERVAL);
    }
}

#[cfg(windows)]
fn collect_child_output(
    child: &mut std::process::Child,
    status: std::process::ExitStatus,
) -> std::io::Result<std::process::Output> {
    let mut stdout = Vec::new();
    if let Some(stdout_pipe) = child.stdout.as_mut() {
        stdout_pipe.read_to_end(&mut stdout)?;
    }
    let mut stderr = Vec::new();
    if let Some(stderr_pipe) = child.stderr.as_mut() {
        stderr_pipe.read_to_end(&mut stderr)?;
    }
    Ok(std::process::Output { status, stdout, stderr })
}

#[cfg(all(not(unix), not(windows)))]
fn metadata_lock_owner_is_alive(_pid: u32) -> bool {
    true
}
