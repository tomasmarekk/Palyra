//! TOML persistence for the registry and runtime-state documents.
//!
//! Writes are serialized across processes with a `.lock` sentinel file and applied via
//! write-temp-then-rename so a crash never leaves a half-written registry. Documents
//! contain vault references and bookkeeping only — never secret values.

use std::{
    env, fs,
    path::{Component, Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{
    constants::{
        ENV_REGISTRY_PATH, ENV_STATE_ROOT, REGISTRY_FILE, REGISTRY_LOCK_MAX_ATTEMPTS,
        REGISTRY_LOCK_RETRY_DELAY_MS, REGISTRY_LOCK_STALE_AFTER_SECS, REGISTRY_VERSION,
        RUNTIME_STATE_FILE, RUNTIME_STATE_VERSION,
    },
    error::AuthProfileError,
    models::{AuthProfileOrderRecord, AuthProfileRecord, AuthProfileRuntimeRecord},
};

/// On-disk shape of `auth_profiles.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RegistryDocument {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) profiles: Vec<AuthProfileRecord>,
}

impl Default for RegistryDocument {
    fn default() -> Self {
        Self { version: REGISTRY_VERSION, profiles: Vec::new() }
    }
}

/// On-disk shape of `auth_profile_runtime_state.toml`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct RuntimeStateDocument {
    pub(crate) version: u32,
    #[serde(default)]
    pub(crate) records: Vec<AuthProfileRuntimeRecord>,
    #[serde(default)]
    pub(crate) profile_orders: Vec<AuthProfileOrderRecord>,
}

impl Default for RuntimeStateDocument {
    fn default() -> Self {
        Self { version: RUNTIME_STATE_VERSION, records: Vec::new(), profile_orders: Vec::new() }
    }
}

/// Resolves the state root from `PALYRA_STATE_ROOT`, defaulting next to the identity store.
pub(crate) fn resolve_state_root(identity_store_root: &Path) -> Result<PathBuf, AuthProfileError> {
    if let Ok(raw) = env::var(ENV_STATE_ROOT) {
        let state_root = normalize_configured_path(raw.as_str(), ENV_STATE_ROOT)?;
        return Ok(if state_root.is_absolute() {
            state_root
        } else {
            identity_store_root.join(state_root)
        });
    }
    // Default places auth state beside, not inside, the identity store directory.
    Ok(identity_store_root
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| identity_store_root.to_path_buf()))
}

/// Resolves the registry file path, honoring the `PALYRA_AUTH_PROFILES_PATH` override.
pub(crate) fn resolve_registry_path(state_root: &Path) -> Result<PathBuf, AuthProfileError> {
    if let Ok(raw) = env::var(ENV_REGISTRY_PATH) {
        let configured = normalize_configured_path(raw.as_str(), ENV_REGISTRY_PATH)?;
        return Ok(if configured.is_absolute() { configured } else { state_root.join(configured) });
    }
    Ok(state_root.join(REGISTRY_FILE))
}

pub(crate) fn resolve_runtime_state_path(state_root: &Path) -> PathBuf {
    state_root.join(RUNTIME_STATE_FILE)
}

/// Validates an env-configured path, rejecting `..` so a relative override cannot
/// silently escape the root it is joined onto.
fn normalize_configured_path(raw: &str, field: &'static str) -> Result<PathBuf, AuthProfileError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(AuthProfileError::InvalidPath {
            field,
            message: "path cannot be empty".to_owned(),
        });
    }
    let path = PathBuf::from(trimmed);
    for component in path.components() {
        if matches!(component, Component::ParentDir) {
            return Err(AuthProfileError::InvalidPath {
                field,
                message: "path cannot contain '..' segments".to_owned(),
            });
        }
    }
    Ok(path)
}

/// Serializes and atomically persists the registry document under the cross-process lock.
pub(crate) fn persist_registry(
    path: &Path,
    document: &RegistryDocument,
) -> Result<(), AuthProfileError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| AuthProfileError::WriteRegistry {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let _lock = acquire_registry_lock(path)
        .map_err(|source| AuthProfileError::WriteRegistry { path: path.to_path_buf(), source })?;
    let serialized = toml::to_string_pretty(document)?;
    write_registry_atomically(path, serialized.as_str())
        .map_err(|source| AuthProfileError::WriteRegistry { path: path.to_path_buf(), source })?;
    Ok(())
}

/// Serializes and atomically persists runtime state under the cross-process lock.
pub(crate) fn persist_runtime_state(
    path: &Path,
    document: &RuntimeStateDocument,
) -> Result<(), AuthProfileError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| AuthProfileError::WriteRegistry {
            path: parent.to_path_buf(),
            source,
        })?;
    }
    let _lock = acquire_registry_lock(path)
        .map_err(|source| AuthProfileError::WriteRegistry { path: path.to_path_buf(), source })?;
    let serialized = toml::to_string_pretty(document)?;
    write_registry_atomically(path, serialized.as_str())
        .map_err(|source| AuthProfileError::WriteRegistry { path: path.to_path_buf(), source })?;
    Ok(())
}

/// Guard owning the `.lock` sentinel file; releasing removes the file.
struct RegistryLock {
    lock_path: PathBuf,
}

impl Drop for RegistryLock {
    fn drop(&mut self) {
        // Best effort: a leaked lock file is reclaimed by the staleness check.
        let _ = fs::remove_file(&self.lock_path);
    }
}

/// Acquires the cross-process write lock for `path` by atomically creating a sentinel.
///
/// Stale sentinels (older than `REGISTRY_LOCK_STALE_AFTER_SECS`) are reclaimed so a
/// crashed process cannot block writers forever — a deliberate availability-over-strictness
/// tradeoff: a writer stalled past the stale window can race a reclaiming writer, and the
/// atomic rename below keeps the file intact (last writer wins) in that case.
fn acquire_registry_lock(path: &Path) -> Result<RegistryLock, std::io::Error> {
    let lock_path = registry_lock_path(path);
    let stale_after = Duration::from_secs(REGISTRY_LOCK_STALE_AFTER_SECS);
    for attempt in 0..=REGISTRY_LOCK_MAX_ATTEMPTS {
        // `create_new` fails when the file exists, making creation the atomic mutex.
        match fs::OpenOptions::new().create_new(true).write(true).open(&lock_path) {
            Ok(_) => return Ok(RegistryLock { lock_path }),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                if reclaim_stale_registry_lock(lock_path.as_path(), stale_after) {
                    continue;
                }
                if attempt == REGISTRY_LOCK_MAX_ATTEMPTS {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!(
                            "timed out waiting for auth profile registry lock at {}",
                            lock_path.display()
                        ),
                    ));
                }
                std::thread::sleep(Duration::from_millis(REGISTRY_LOCK_RETRY_DELAY_MS));
            }
            Err(error) => return Err(error),
        }
    }
    Err(std::io::Error::other("auth profile registry lock acquisition exhausted retry budget"))
}

fn registry_lock_path(path: &Path) -> PathBuf {
    let mut lock_name = path.as_os_str().to_os_string();
    lock_name.push(".lock");
    PathBuf::from(lock_name)
}

fn reclaim_stale_registry_lock(lock_path: &Path, stale_after: Duration) -> bool {
    let metadata = match fs::metadata(lock_path) {
        Ok(value) => value,
        Err(_) => return false,
    };
    let modified = match metadata.modified() {
        Ok(value) => value,
        Err(_) => return false,
    };
    let is_stale = SystemTime::now().duration_since(modified).unwrap_or_default() >= stale_after;
    if !is_stale {
        return false;
    }
    fs::remove_file(lock_path).is_ok()
}

/// Replaces `path` with `payload` via write-temp-then-rename so readers never observe a
/// partially written file.
fn write_registry_atomically(path: &Path, payload: &str) -> Result<(), std::io::Error> {
    let timestamp_ns = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    let mut temporary_name = path.as_os_str().to_os_string();
    temporary_name.push(format!(".tmp.{}.{}", std::process::id(), timestamp_ns));
    let temporary_path = PathBuf::from(temporary_name);

    fs::write(&temporary_path, payload)?;
    if let Err(rename_error) = fs::rename(&temporary_path, path) {
        if !path.exists() || !path.is_file() {
            let _ = fs::remove_file(&temporary_path);
            return Err(rename_error);
        }
        // Windows can refuse rename-over-existing (e.g. the target is open without
        // FILE_SHARE_DELETE), so swap the old file aside, install the new one, and
        // roll back the original if installation fails.
        let mut rollback_name = path.as_os_str().to_os_string();
        rollback_name.push(format!(".swap.{}.{}", std::process::id(), timestamp_ns));
        let rollback_path = PathBuf::from(rollback_name);
        fs::rename(path, &rollback_path)?;
        if let Err(install_error) = fs::rename(&temporary_path, path) {
            let _ = fs::rename(&rollback_path, path);
            let _ = fs::remove_file(&temporary_path);
            return Err(install_error);
        }
        let _ = fs::remove_file(&rollback_path);
    }
    Ok(())
}

/// Returns the current wall-clock time as unix milliseconds.
///
/// # Errors
/// Returns [`AuthProfileError::InvalidSystemTime`] when the clock reads before the epoch.
pub(crate) fn unix_ms_now() -> Result<i64, AuthProfileError> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    // Truncating u128 -> i64 cast is unreachable in practice: i64 milliseconds cover
    // roughly 292 million years past the epoch.
    Ok(elapsed.as_millis() as i64)
}
