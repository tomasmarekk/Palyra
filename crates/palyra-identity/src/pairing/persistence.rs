use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Mutex, MutexGuard},
    thread,
    time::{Duration, Instant, SystemTime},
};

use serde::de::DeserializeOwned;

use crate::{
    ca::{CertificateAuthority, StoredCertificateAuthority},
    error::{IdentityError, IdentityResult},
    store::{FilesystemSecretStore, SecretStore},
    unix_ms,
};

use super::{
    models::{
        LegacyPersistedIdentityStateBundle, PersistedIdentityState, PersistedIdentityStateBundle,
    },
    IdentityManager,
};

pub(super) const IDENTITY_STATE_BUNDLE_KEY: &str = "identity/state.v1.json";
pub(super) const GATEWAY_CA_STATE_KEY: &str = "identity/ca/state.json";
const PAIRED_DEVICES_STATE_KEY: &str = "identity/pairing/paired_devices.json";
const REVOKED_DEVICES_STATE_KEY: &str = "identity/pairing/revoked_devices.json";
const REVOKED_CERTIFICATES_STATE_KEY: &str = "identity/pairing/revoked_certificates.json";
pub(super) const MAX_ACTIVE_PAIRING_SESSIONS: usize = 10_000;
pub(super) const DEFAULT_PAIRING_START_RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
pub(super) const DEFAULT_PAIRING_MAX_STARTS_PER_WINDOW: usize = 1_024;
pub(super) const IDENTITY_STATE_LOCK_FILENAME: &str = ".identity-state.lock";
pub(super) const IDENTITY_STATE_LOCK_TIMEOUT: Duration = Duration::from_secs(3);
const IDENTITY_STATE_LOCK_RETRY: Duration = Duration::from_millis(20);
const IDENTITY_STATE_STALE_LOCK_AGE: Duration = Duration::from_secs(30);

static IDENTITY_STATE_PROCESS_LOCK: Mutex<()> = Mutex::new(());

struct FilesystemStateLockGuard {
    path: PathBuf,
}

impl Drop for FilesystemStateLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(super) struct StateMutationGuard {
    _process: MutexGuard<'static, ()>,
    _filesystem: Option<FilesystemStateLockGuard>,
}

impl IdentityManager {
    pub(super) fn mutate_persisted_state<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> IdentityResult<T>,
    ) -> IdentityResult<T> {
        let _guard = self.acquire_state_mutation_guard()?;
        self.reload_persisted_state()?;
        let result = operation(self)?;
        self.persist_identity_state_bundle()?;
        Ok(result)
    }

    pub(super) fn acquire_state_mutation_guard(&self) -> IdentityResult<StateMutationGuard> {
        let process = IDENTITY_STATE_PROCESS_LOCK.lock().map_err(|_| {
            IdentityError::Internal("identity state process lock poisoned".to_owned())
        })?;
        let filesystem = self.acquire_filesystem_state_lock()?;
        Ok(StateMutationGuard { _process: process, _filesystem: filesystem })
    }

    fn acquire_filesystem_state_lock(&self) -> IdentityResult<Option<FilesystemStateLockGuard>> {
        let Some(store) = self.store.as_any().downcast_ref::<FilesystemSecretStore>() else {
            return Ok(None);
        };
        let lock_path = store.root_path().join(IDENTITY_STATE_LOCK_FILENAME);
        let start = Instant::now();
        loop {
            match fs::OpenOptions::new().create_new(true).write(true).open(&lock_path) {
                Ok(mut file) => {
                    let marker = format!(
                        "pid={} ts_ms={}\n",
                        std::process::id(),
                        unix_ms(SystemTime::now())?
                    );
                    initialize_filesystem_state_lock_marker(
                        &lock_path,
                        &mut file,
                        marker.as_str(),
                    )?;
                    return Ok(Some(FilesystemStateLockGuard { path: lock_path }));
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if try_reclaim_stale_filesystem_lock(
                        &lock_path,
                        SystemTime::now(),
                        IDENTITY_STATE_STALE_LOCK_AGE,
                    )? {
                        continue;
                    }
                    if start.elapsed() >= IDENTITY_STATE_LOCK_TIMEOUT {
                        return Err(IdentityError::Internal(format!(
                            "timed out waiting for identity state lock at {} (lock stealing disabled to prevent state corruption; remove stale lock file if no process owns it)",
                            lock_path.display()
                        )));
                    }
                    thread::sleep(IDENTITY_STATE_LOCK_RETRY);
                }
                Err(error) => return Err(IdentityError::Internal(error.to_string())),
            }
        }
    }

    pub(super) fn reload_persisted_state(&mut self) -> IdentityResult<()> {
        let state = if let Some(state) = load_identity_state_bundle(self.store.as_ref())? {
            state
        } else {
            let (state, _) = load_identity_state(self.store.as_ref())?;
            state
        };
        self.apply_persisted_state(state)
    }

    fn apply_persisted_state(&mut self, state: PersistedIdentityState) -> IdentityResult<()> {
        self.ca = CertificateAuthority::from_stored(&state.ca)?;
        self.paired_devices = state.paired_devices;
        self.revoked_devices = state.revoked_devices;
        self.revoked_certificate_fingerprints = state.revoked_certificate_fingerprints;
        self.state_generation = state.generation;
        Ok(())
    }

    pub(super) fn persist_identity_state_bundle(&mut self) -> IdentityResult<()> {
        let next_generation = self.state_generation.saturating_add(1);
        write_sealed_json(self.store.as_ref(), GATEWAY_CA_STATE_KEY, &self.ca.to_stored())?;
        let state = PersistedIdentityStateBundle {
            generation: next_generation,
            paired_devices: self.paired_devices.clone(),
            revoked_devices: self.revoked_devices.clone(),
            revoked_certificate_fingerprints: self.revoked_certificate_fingerprints.clone(),
        };
        write_sealed_json(self.store.as_ref(), IDENTITY_STATE_BUNDLE_KEY, &state)?;
        self.state_generation = next_generation;
        Ok(())
    }
}

fn initialize_filesystem_state_lock_marker(
    lock_path: &Path,
    file: &mut fs::File,
    marker: &str,
) -> IdentityResult<()> {
    initialize_filesystem_state_lock_marker_with_writer(lock_path, marker, |payload| {
        file.write_all(payload)?;
        file.sync_all()
    })
}

pub(super) fn initialize_filesystem_state_lock_marker_with_writer(
    lock_path: &Path,
    marker: &str,
    write_marker: impl FnOnce(&[u8]) -> std::io::Result<()>,
) -> IdentityResult<()> {
    if let Err(error) = write_marker(marker.as_bytes()) {
        let _ = fs::remove_file(lock_path);
        return Err(IdentityError::Internal(format!(
            "failed to initialize identity state lock marker {}: {error}",
            lock_path.display()
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FilesystemLockMarker {
    pid: u32,
    ts_ms: u64,
}

pub(super) fn try_reclaim_stale_filesystem_lock(
    lock_path: &Path,
    now: SystemTime,
    stale_age: Duration,
) -> IdentityResult<bool> {
    let marker_raw = match fs::read_to_string(lock_path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(_) => return Ok(false),
    };
    let Some(marker) = parse_filesystem_lock_marker(&marker_raw) else {
        if !lock_file_age_is_stale(lock_path, now, stale_age)? {
            return Ok(false);
        }
        return remove_filesystem_lock_file(lock_path);
    };
    if !lock_marker_is_stale(marker, now, stale_age)? {
        return Ok(false);
    }
    if process_is_alive(marker.pid) {
        return Ok(false);
    }
    remove_filesystem_lock_file(lock_path)
}

fn parse_filesystem_lock_marker(raw: &str) -> Option<FilesystemLockMarker> {
    let mut pid = None;
    let mut ts_ms = None;
    for part in raw.split_whitespace() {
        if let Some(value) = part.strip_prefix("pid=") {
            pid = value.parse::<u32>().ok();
            continue;
        }
        if let Some(value) = part.strip_prefix("ts_ms=") {
            ts_ms = value.parse::<u64>().ok();
        }
    }
    Some(FilesystemLockMarker { pid: pid?, ts_ms: ts_ms? })
}

fn lock_marker_is_stale(
    marker: FilesystemLockMarker,
    now: SystemTime,
    stale_age: Duration,
) -> IdentityResult<bool> {
    let now_ms = unix_ms(now)?;
    let stale_age_ms = u64::try_from(stale_age.as_millis()).map_err(|_| {
        IdentityError::Internal("identity state stale lock age overflow".to_owned())
    })?;
    Ok(now_ms.saturating_sub(marker.ts_ms) >= stale_age_ms)
}

fn lock_file_age_is_stale(
    lock_path: &Path,
    now: SystemTime,
    stale_age: Duration,
) -> IdentityResult<bool> {
    let metadata = match fs::metadata(lock_path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(_) => return Ok(false),
    };
    let modified = match metadata.modified() {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    Ok(now.duration_since(modified).unwrap_or_default() >= stale_age)
}

fn remove_filesystem_lock_file(lock_path: &Path) -> IdentityResult<bool> {
    match fs::remove_file(lock_path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(_) => Ok(false),
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    let Ok(pid_i32) = i32::try_from(pid) else {
        return true;
    };
    // SAFETY: calling `kill(pid, 0)` only probes process existence/permission and does not send
    // a signal. Inputs are validated above.
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
fn process_is_alive(pid: u32) -> bool {
    palyra_common::windows_security::process_is_alive(pid).unwrap_or(false)
}

#[cfg(all(not(unix), not(windows)))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

fn load_or_init_gateway_ca(store: &dyn SecretStore) -> IdentityResult<CertificateAuthority> {
    match store.read_secret(GATEWAY_CA_STATE_KEY) {
        Ok(raw) => {
            let state: StoredCertificateAuthority = serde_json::from_slice(&raw)
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            CertificateAuthority::from_stored(&state)
        }
        Err(IdentityError::SecretNotFound) => {
            let ca = CertificateAuthority::new("Palyra Gateway CA")?;
            write_sealed_json(store, GATEWAY_CA_STATE_KEY, &ca.to_stored())?;
            Ok(ca)
        }
        Err(error) => Err(error),
    }
}

pub(super) fn load_identity_state(
    store: &dyn SecretStore,
) -> IdentityResult<(PersistedIdentityState, bool)> {
    if let Some(bundle) = load_identity_state_bundle(store)? {
        return Ok((bundle, true));
    }

    let ca = load_or_init_gateway_ca(store)?;
    let paired_devices = read_json_or_default(store, PAIRED_DEVICES_STATE_KEY)?;
    let revoked_devices = read_json_or_default(store, REVOKED_DEVICES_STATE_KEY)?;
    let revoked_certificate_fingerprints =
        read_json_or_default(store, REVOKED_CERTIFICATES_STATE_KEY)?;

    Ok((
        PersistedIdentityState {
            generation: 0,
            ca: ca.to_stored(),
            paired_devices,
            revoked_devices,
            revoked_certificate_fingerprints,
        },
        false,
    ))
}

fn load_identity_state_bundle(
    store: &dyn SecretStore,
) -> IdentityResult<Option<PersistedIdentityState>> {
    match store.read_secret(IDENTITY_STATE_BUNDLE_KEY) {
        Ok(raw) => {
            if let Ok(legacy) = serde_json::from_slice::<LegacyPersistedIdentityStateBundle>(&raw) {
                write_sealed_json(store, GATEWAY_CA_STATE_KEY, &legacy.ca)?;
                return Ok(Some(PersistedIdentityState {
                    generation: legacy.generation,
                    ca: legacy.ca,
                    paired_devices: legacy.paired_devices,
                    revoked_devices: legacy.revoked_devices,
                    revoked_certificate_fingerprints: legacy.revoked_certificate_fingerprints,
                }));
            }

            let state: PersistedIdentityStateBundle = serde_json::from_slice(&raw)
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            let ca = load_or_init_gateway_ca(store)?.to_stored();
            Ok(Some(PersistedIdentityState {
                generation: state.generation,
                ca,
                paired_devices: state.paired_devices,
                revoked_devices: state.revoked_devices,
                revoked_certificate_fingerprints: state.revoked_certificate_fingerprints,
            }))
        }
        Err(IdentityError::SecretNotFound) => Ok(None),
        Err(error) => Err(error),
    }
}

fn read_json_or_default<T>(store: &dyn SecretStore, key: &str) -> IdentityResult<T>
where
    T: DeserializeOwned + Default,
{
    match store.read_secret(key) {
        Ok(raw) => {
            serde_json::from_slice(&raw).map_err(|error| IdentityError::Internal(error.to_string()))
        }
        Err(IdentityError::SecretNotFound) => Ok(T::default()),
        Err(error) => Err(error),
    }
}

fn write_sealed_json<T>(store: &dyn SecretStore, key: &str, value: &T) -> IdentityResult<()>
where
    T: serde::Serialize,
{
    let encoded =
        serde_json::to_vec(value).map_err(|error| IdentityError::Internal(error.to_string()))?;
    store.write_sealed_value(key, &encoded)
}
