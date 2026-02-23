mod backend;
mod envelope;
mod scope;

#[cfg(windows)]
use std::process::{Command, Stdio};
use std::{
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Context;
use hkdf::Hkdf;
use palyra_common::default_identity_store_root;
use palyra_identity::{FilesystemSecretStore, IdentityManager, SecretStore};
use sha2::{Digest, Sha256};
use ulid::Ulid;

use crate::backend::{select_backend, BlobBackend};
pub use crate::backend::{BackendKind, BackendPreference};
use crate::envelope::{open, seal, EnvelopePayload};
pub use crate::scope::VaultScope;

const METADATA_FILE: &str = "metadata.json";
const METADATA_LOCK_FILE: &str = "metadata.lock";
const METADATA_VERSION: u32 = 1;
const METADATA_LOCK_TIMEOUT: Duration = Duration::from_secs(3);
const METADATA_LOCK_RETRY: Duration = Duration::from_millis(20);
const METADATA_LOCK_STALE_AGE: Duration = Duration::from_secs(30);
#[cfg(windows)]
const WINDOWS_TASKLIST_TIMEOUT: Duration = Duration::from_secs(2);
#[cfg(windows)]
const WINDOWS_TASKLIST_POLL_INTERVAL: Duration = Duration::from_millis(10);

const KEY_DERIVATION_SALT: &[u8] = b"palyra.vault.kek.v1";
const KEY_DERIVATION_INFO: &[u8] = b"envelope:kek";
const AAD_PREFIX: &str = "palyra.vault.v1";
const IDENTITY_STATE_BUNDLE_KEY: &str = "identity/state.v1.json";
const LEGACY_CA_STATE_KEY: &str = "identity/ca/state.json";

const MAX_SECRET_KEY_BYTES: usize = 128;
pub const MAX_SCOPE_SEGMENT_BYTES: usize = 256;
const DEFAULT_MAX_SECRET_BYTES: usize = 64 * 1024;
#[cfg(windows)]
const WINDOWS_SYSTEM_SID: &str = "S-1-5-18";

#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    #[error("secret not found")]
    NotFound,
    #[error("invalid scope: {0}")]
    InvalidScope(String),
    #[error("invalid secret key: {0}")]
    InvalidKey(String),
    #[error("invalid object id: {0}")]
    InvalidObjectId(String),
    #[error("secret value exceeds max bytes ({actual} > {max})")]
    ValueTooLarge { actual: usize, max: usize },
    #[error("vault backend unavailable: {0}")]
    BackendUnavailable(String),
    #[error("vault crypto failure: {0}")]
    Crypto(String),
    #[error("vault I/O failure: {0}")]
    Io(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretMetadata {
    pub scope: VaultScope,
    pub key: String,
    pub created_at_unix_ms: i64,
    pub updated_at_unix_ms: i64,
    pub value_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultRef {
    pub scope: VaultScope,
    pub key: String,
}

#[derive(Debug, Clone)]
pub struct VaultConfig {
    pub root: Option<PathBuf>,
    pub identity_store_root: Option<PathBuf>,
    pub backend_preference: BackendPreference,
    pub max_secret_bytes: usize,
}

impl Default for VaultConfig {
    fn default() -> Self {
        Self {
            root: None,
            identity_store_root: None,
            backend_preference: BackendPreference::Auto,
            max_secret_bytes: DEFAULT_MAX_SECRET_BYTES,
        }
    }
}

pub struct Vault {
    root: PathBuf,
    backend: Box<dyn BlobBackend>,
    max_secret_bytes: usize,
    kek: [u8; 32],
}

impl Vault {
    pub fn open_default() -> Result<Self, VaultError> {
        Self::open_with_config(VaultConfig::default())
    }

    pub fn open_with_config(config: VaultConfig) -> Result<Self, VaultError> {
        let identity_store_root = if let Some(path) = config.identity_store_root {
            path
        } else {
            default_identity_store_root()
                .context("failed to resolve default identity store root")
                .map_err(|error| VaultError::Io(error.to_string()))?
        };
        let root = if let Some(path) = config.root {
            path
        } else if let Ok(path) = std::env::var("PALYRA_VAULT_DIR") {
            PathBuf::from(path)
        } else {
            default_vault_root(identity_store_root.as_path())
        };
        if config.max_secret_bytes == 0 {
            return Err(VaultError::InvalidKey(
                "max secret bytes must be greater than zero".to_owned(),
            ));
        }

        ensure_owner_only_dir(&root)?;
        let backend = select_backend(&root, config.backend_preference)?;
        let kek = derive_device_kek(identity_store_root.as_path())?;
        let vault = Self { root, backend, max_secret_bytes: config.max_secret_bytes, kek };
        vault.ensure_metadata_exists()?;
        Ok(vault)
    }

    #[must_use]
    pub fn backend_kind(&self) -> BackendKind {
        self.backend.kind()
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn put_secret(
        &self,
        scope: &VaultScope,
        key: &str,
        value: &[u8],
    ) -> Result<SecretMetadata, VaultError> {
        validate_secret_key(key)?;
        if value.len() > self.max_secret_bytes {
            return Err(VaultError::ValueTooLarge {
                actual: value.len(),
                max: self.max_secret_bytes,
            });
        }
        let aad = build_aad(scope, key);
        let envelope = seal(value, &self.kek, aad.as_slice())?;
        let payload = serde_json::to_vec(&envelope).map_err(|error| {
            VaultError::Io(format!("failed to serialize envelope payload: {error}"))
        })?;
        let object_id = object_id_for(scope, key);
        let now = current_unix_ms()?;

        let _lock = self.acquire_metadata_lock()?;
        let mut index = self.read_metadata()?;
        let existing_entry_index =
            index.entries.iter().position(|entry| entry.scope == *scope && entry.key == key);
        let previous_blob = if existing_entry_index.is_some() {
            Some(self.backend.get_blob(object_id.as_str())?)
        } else {
            None
        };
        self.backend.put_blob(object_id.as_str(), payload.as_slice())?;

        let entry = if let Some(existing_index) = existing_entry_index {
            let existing = &mut index.entries[existing_index];
            existing.updated_at_unix_ms = now;
            existing.value_bytes = value.len();
            existing.object_id = object_id.clone();
            existing.clone()
        } else {
            let created = MetadataEntry {
                scope: scope.clone(),
                key: key.to_owned(),
                object_id: object_id.clone(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
                value_bytes: value.len(),
            };
            index.entries.push(created.clone());
            created
        };
        if let Err(write_error) = self.write_metadata(&index) {
            let rollback_result = if let Some(previous_blob) = previous_blob.as_ref() {
                self.backend.put_blob(object_id.as_str(), previous_blob.as_slice())
            } else {
                self.backend.delete_blob(object_id.as_str())
            };
            if let Err(rollback_error) = rollback_result {
                return Err(VaultError::Io(format!(
                    "failed to persist metadata after blob write and failed to rollback blob: write_error={write_error}; rollback_error={rollback_error}"
                )));
            }
            return Err(VaultError::Io(format!(
                "failed to persist metadata after blob write: {write_error}"
            )));
        }
        Ok(entry.into())
    }

    pub fn get_secret(&self, scope: &VaultScope, key: &str) -> Result<Vec<u8>, VaultError> {
        validate_secret_key(key)?;
        let _lock = self.acquire_metadata_lock()?;
        let index = self.read_metadata()?;
        let entry = index
            .entries
            .iter()
            .find(|entry| entry.scope == *scope && entry.key == key)
            .cloned()
            .ok_or(VaultError::NotFound)?;
        let payload = self.backend.get_blob(entry.object_id.as_str())?;
        let envelope: EnvelopePayload =
            serde_json::from_slice(payload.as_slice()).map_err(|error| {
                VaultError::Crypto(format!("failed to parse envelope payload: {error}"))
            })?;
        let aad = build_aad(scope, key);
        open(&envelope, &self.kek, aad.as_slice())
    }

    pub fn delete_secret(&self, scope: &VaultScope, key: &str) -> Result<bool, VaultError> {
        validate_secret_key(key)?;
        let _lock = self.acquire_metadata_lock()?;
        let mut index = self.read_metadata()?;
        let index_before_delete = index.clone();
        let mut deleted = false;
        let mut removed_object_id = None;
        index.entries.retain(|entry| {
            if entry.scope == *scope && entry.key == key {
                deleted = true;
                removed_object_id = Some(entry.object_id.clone());
                false
            } else {
                true
            }
        });
        if let Some(object_id) = removed_object_id {
            self.write_metadata(&index)?;
            if let Err(error) = self.backend.delete_blob(object_id.as_str()) {
                self.write_metadata(&index_before_delete).map_err(|rollback_error| {
                    VaultError::Io(format!(
                        "failed to delete secret blob and rollback metadata: delete_error={error}; rollback_error={rollback_error}"
                    ))
                })?;
                return Err(error);
            }
        }
        Ok(deleted)
    }

    pub fn list_secrets(&self, scope: &VaultScope) -> Result<Vec<SecretMetadata>, VaultError> {
        let _lock = self.acquire_metadata_lock()?;
        let index = self.read_metadata()?;
        let mut results = index
            .entries
            .iter()
            .filter(|entry| entry.scope == *scope)
            .cloned()
            .map(SecretMetadata::from)
            .collect::<Vec<_>>();
        results.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(results)
    }

    fn ensure_metadata_exists(&self) -> Result<(), VaultError> {
        let _lock = self.acquire_metadata_lock()?;
        if self.metadata_path().exists() {
            return Ok(());
        }
        self.write_metadata(&MetadataFile::default())
    }

    fn metadata_path(&self) -> PathBuf {
        self.root.join(METADATA_FILE)
    }

    fn metadata_lock_path(&self) -> PathBuf {
        self.root.join(METADATA_LOCK_FILE)
    }

    fn acquire_metadata_lock(&self) -> Result<MetadataLockGuard, VaultError> {
        let lock_path = self.metadata_lock_path();
        let started = SystemTime::now();
        loop {
            match fs::OpenOptions::new().create_new(true).write(true).open(&lock_path) {
                Ok(file) => {
                    let marker =
                        format!("pid={} ts_ms={}\n", std::process::id(), current_unix_ms()?);
                    drop(file);
                    fs::write(&lock_path, marker.as_bytes()).map_err(|error| {
                        VaultError::Io(format!(
                            "failed to initialize metadata lock marker {}: {error}",
                            lock_path.display()
                        ))
                    })?;
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

    fn read_metadata(&self) -> Result<MetadataFile, VaultError> {
        let path = self.metadata_path();
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

    fn write_metadata(&self, metadata: &MetadataFile) -> Result<(), VaultError> {
        let path = self.metadata_path();
        let tmp_path = path.with_extension(format!("tmp.{}", Ulid::new()));
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
}

impl VaultRef {
    pub fn parse(raw: &str) -> Result<Self, VaultError> {
        let normalized = raw.trim();
        let (scope_raw, key_raw) = normalized.split_once('/').ok_or_else(|| {
            VaultError::InvalidKey(
                "vault ref must have shape '<scope>/<key>' (for example 'global/openai_api_key')"
                    .to_owned(),
            )
        })?;
        let scope = scope_raw.parse::<VaultScope>()?;
        validate_secret_key(key_raw)?;
        Ok(Self { scope, key: key_raw.to_owned() })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MetadataFile {
    version: u32,
    entries: Vec<MetadataEntry>,
}

impl Default for MetadataFile {
    fn default() -> Self {
        Self { version: METADATA_VERSION, entries: Vec::new() }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MetadataEntry {
    scope: VaultScope,
    key: String,
    object_id: String,
    created_at_unix_ms: i64,
    updated_at_unix_ms: i64,
    value_bytes: usize,
}

impl From<MetadataEntry> for SecretMetadata {
    fn from(value: MetadataEntry) -> Self {
        Self {
            scope: value.scope,
            key: value.key,
            created_at_unix_ms: value.created_at_unix_ms,
            updated_at_unix_ms: value.updated_at_unix_ms,
            value_bytes: value.value_bytes,
        }
    }
}

struct MetadataLockGuard {
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

fn maybe_reclaim_stale_lock(lock_path: &Path) -> Result<bool, VaultError> {
    maybe_reclaim_stale_lock_with_policy(lock_path, SystemTime::now(), METADATA_LOCK_STALE_AGE)
}

fn maybe_reclaim_stale_lock_with_policy(
    lock_path: &Path,
    now: SystemTime,
    stale_age: Duration,
) -> Result<bool, VaultError> {
    let metadata = fs::metadata(lock_path).map_err(|error| {
        VaultError::Io(format!("failed to inspect metadata lock {}: {error}", lock_path.display()))
    })?;
    let modified = metadata.modified().map_err(|error| {
        VaultError::Io(format!(
            "failed to inspect metadata lock timestamp {}: {error}",
            lock_path.display()
        ))
    })?;
    if now.duration_since(modified).unwrap_or(Duration::ZERO) < stale_age {
        return Ok(false);
    }
    if let Ok(raw_marker) = fs::read_to_string(lock_path) {
        if let Some(marker) = parse_metadata_lock_marker(raw_marker.as_str()) {
            if metadata_lock_owner_is_alive(marker.pid) {
                return Ok(false);
            }
        }
    }
    fs::remove_file(lock_path).map_err(|error| {
        VaultError::Io(format!(
            "failed to reclaim stale metadata lock {}: {error}",
            lock_path.display()
        ))
    })?;
    Ok(true)
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
    // SAFETY: `kill(pid, 0)` only probes process existence/permission.
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
    let mut child = Command::new("tasklist")
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

fn derive_device_kek(identity_store_root: &Path) -> Result<[u8; 32], VaultError> {
    let store = FilesystemSecretStore::new(identity_store_root).map_err(|error| {
        VaultError::Io(format!(
            "failed to initialize identity store for key derivation at {}: {error}",
            identity_store_root.display()
        ))
    })?;
    let store: Arc<dyn SecretStore> = Arc::new(store);
    let _manager = IdentityManager::with_store(store.clone()).map_err(|error| {
        VaultError::Io(format!("failed to initialize identity manager for key derivation: {error}"))
    })?;
    let seed_material = store
        .read_secret(IDENTITY_STATE_BUNDLE_KEY)
        .or_else(|_| store.read_secret(LEGACY_CA_STATE_KEY))
        .map_err(|error| {
            VaultError::Crypto(format!(
                "failed to read identity key material for vault KEK derivation: {error}"
            ))
        })?;
    let seed = extract_kek_seed_material(seed_material.as_slice())?;
    derive_kek_from_seed_material(seed.as_slice())
}

fn extract_kek_seed_material(raw_state: &[u8]) -> Result<Vec<u8>, VaultError> {
    let parsed: serde_json::Value = serde_json::from_slice(raw_state).map_err(|error| {
        VaultError::Crypto(format!(
            "failed to parse identity state for vault KEK derivation: {error}"
        ))
    })?;
    if let Some(private_key) =
        parsed.pointer("/ca/private_key_pem").and_then(serde_json::Value::as_str)
    {
        if private_key.trim().is_empty() {
            return Err(VaultError::Crypto(
                "identity state contains empty ca.private_key_pem".to_owned(),
            ));
        }
        return Ok(private_key.as_bytes().to_vec());
    }
    if let Some(private_key) = parsed.get("private_key_pem").and_then(serde_json::Value::as_str) {
        if private_key.trim().is_empty() {
            return Err(VaultError::Crypto(
                "identity CA state contains empty private_key_pem".to_owned(),
            ));
        }
        return Ok(private_key.as_bytes().to_vec());
    }
    Err(VaultError::Crypto(
        "identity state is missing ca.private_key_pem key material for vault KEK derivation"
            .to_owned(),
    ))
}

fn derive_kek_from_seed_material(seed_material: &[u8]) -> Result<[u8; 32], VaultError> {
    let seed_hash = Sha256::digest(seed_material);
    let hkdf = Hkdf::<Sha256>::new(Some(KEY_DERIVATION_SALT), seed_hash.as_slice());
    let mut output = [0_u8; 32];
    hkdf.expand(KEY_DERIVATION_INFO, &mut output)
        .map_err(|_| VaultError::Crypto("failed to derive vault KEK".to_owned()))?;
    Ok(output)
}

fn default_vault_root(identity_store_root: &Path) -> PathBuf {
    if identity_store_root.file_name().is_some_and(|name| name == "identity") {
        if let Some(parent) = identity_store_root.parent() {
            return parent.join("vault");
        }
    }
    identity_store_root.join("vault")
}

fn build_aad(scope: &VaultScope, key: &str) -> Vec<u8> {
    format!("{AAD_PREFIX}|{}|{key}", scope.as_storage_str()).into_bytes()
}

fn validate_secret_key(raw: &str) -> Result<(), VaultError> {
    let key = raw.trim();
    if key.is_empty() {
        return Err(VaultError::InvalidKey("secret key cannot be empty".to_owned()));
    }
    if key.len() > MAX_SECRET_KEY_BYTES {
        return Err(VaultError::InvalidKey(format!(
            "secret key exceeds max bytes ({} > {MAX_SECRET_KEY_BYTES})",
            key.len()
        )));
    }
    if !key
        .chars()
        .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(VaultError::InvalidKey(
            "secret key can only contain lowercase letters, digits, '.', '_' or '-'".to_owned(),
        ));
    }
    Ok(())
}

fn object_id_for(scope: &VaultScope, key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.vault.object.v1");
    hasher.update(scope.as_storage_str().as_bytes());
    hasher.update([0_u8]);
    hasher.update(key.as_bytes());
    format!("obj_{}", hex::encode(hasher.finalize()))
}

fn current_unix_ms() -> Result<i64, VaultError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| VaultError::Io(format!("system clock before unix epoch: {error}")))?;
    Ok(duration.as_millis() as i64)
}

pub struct SensitiveBytes(Vec<u8>);

impl SensitiveBytes {
    pub fn new(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for SensitiveBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Drop for SensitiveBytes {
    fn drop(&mut self) {
        self.0.fill(0);
    }
}

pub fn ensure_owner_only_dir(path: &Path) -> Result<(), VaultError> {
    fs::create_dir_all(path).map_err(|error| {
        VaultError::Io(format!("failed to create directory {}: {error}", path.display()))
    })?;
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(path, owner_sid.as_str(), true)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|error| {
            VaultError::Io(format!(
                "failed to enforce owner-only directory permissions on {}: {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

pub fn ensure_owner_only_file(path: &Path) -> Result<(), VaultError> {
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(path, owner_sid.as_str(), false)?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).map_err(|error| {
            VaultError::Io(format!(
                "failed to enforce owner-only file permissions on {}: {error}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

#[cfg(windows)]
fn current_user_sid() -> Result<String, VaultError> {
    let output =
        Command::new("whoami").args(["/user", "/fo", "csv", "/nh"]).output().map_err(|error| {
            VaultError::Io(format!("failed to execute whoami for vault ACL: {error}"))
        })?;
    if !output.status.success() {
        return Err(VaultError::Io(format!(
            "whoami returned non-success status {} while resolving vault ACL user SID: stdout={} stderr={}",
            output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        )));
    }
    parse_whoami_sid_csv(String::from_utf8_lossy(&output.stdout).trim()).ok_or_else(|| {
        VaultError::Io("failed to parse current user SID from whoami output".to_owned())
    })
}

#[cfg(windows)]
fn parse_whoami_sid_csv(raw: &str) -> Option<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    for ch in raw.chars() {
        match ch {
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => {
                fields.push(current.trim().to_owned());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    fields.push(current.trim().to_owned());
    if fields.len() < 2 {
        return None;
    }
    let sid = fields[1].trim().trim_matches('"').to_owned();
    if sid.starts_with("S-1-") {
        Some(sid)
    } else {
        None
    }
}

#[cfg(windows)]
fn harden_windows_path_permissions(
    path: &Path,
    owner_sid: &str,
    is_directory: bool,
) -> Result<(), VaultError> {
    let grant_mask = if is_directory { "(OI)(CI)F" } else { "F" };
    let owner_grant = format!("*{owner_sid}:{grant_mask}");
    let system_grant = format!("*{WINDOWS_SYSTEM_SID}:{grant_mask}");
    let output = Command::new("icacls")
        .arg(path)
        .args(["/inheritance:r", "/grant:r"])
        .arg(owner_grant)
        .args(["/grant:r"])
        .arg(system_grant)
        .output()
        .map_err(|error| {
            VaultError::Io(format!("failed to execute icacls for {}: {error}", path.display()))
        })?;
    if !output.status.success() {
        return Err(VaultError::Io(format!(
            "icacls returned non-success status {} for {}: stdout={} stderr={}",
            output.status.code().map_or_else(|| "unknown".to_owned(), |code| code.to_string()),
            path.display(),
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim(),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        backend::{BackendKind, BlobBackend},
        derive_kek_from_seed_material, ensure_owner_only_dir, extract_kek_seed_material,
        maybe_reclaim_stale_lock_with_policy, BackendPreference, Vault, VaultConfig, VaultError,
        VaultRef, VaultScope,
    };
    use anyhow::Result;
    use std::{
        collections::HashMap,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tempfile::tempdir;

    #[derive(Default)]
    struct FailingDeleteBackend {
        objects: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl BlobBackend for FailingDeleteBackend {
        fn kind(&self) -> BackendKind {
            BackendKind::EncryptedFile
        }

        fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
            let mut objects = self
                .objects
                .lock()
                .map_err(|_| VaultError::Io("failing-delete backend mutex poisoned".to_owned()))?;
            objects.insert(object_id.to_owned(), payload.to_vec());
            Ok(())
        }

        fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
            let objects = self
                .objects
                .lock()
                .map_err(|_| VaultError::Io("failing-delete backend mutex poisoned".to_owned()))?;
            objects.get(object_id).cloned().ok_or(VaultError::NotFound)
        }

        fn delete_blob(&self, _object_id: &str) -> Result<(), VaultError> {
            Err(VaultError::Io("injected backend delete failure".to_owned()))
        }
    }

    struct MetadataWriteFailureBackend {
        root: PathBuf,
        objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    }

    impl BlobBackend for MetadataWriteFailureBackend {
        fn kind(&self) -> BackendKind {
            BackendKind::EncryptedFile
        }

        fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
            let mut objects = self.objects.lock().map_err(|_| {
                VaultError::Io("metadata-failure backend mutex poisoned".to_owned())
            })?;
            objects.insert(object_id.to_owned(), payload.to_vec());
            drop(objects);

            // Force metadata persistence failure after blob write by replacing metadata file path
            // with a directory before `write_metadata` attempts its atomic rename.
            let metadata_path = self.root.join(super::METADATA_FILE);
            if metadata_path.exists() {
                if metadata_path.is_dir() {
                    std::fs::remove_dir_all(&metadata_path).map_err(|error| {
                        VaultError::Io(format!(
                            "failed to reset metadata sabotage directory {}: {error}",
                            metadata_path.display()
                        ))
                    })?;
                } else {
                    std::fs::remove_file(&metadata_path).map_err(|error| {
                        VaultError::Io(format!(
                            "failed to reset metadata sabotage file {}: {error}",
                            metadata_path.display()
                        ))
                    })?;
                }
            }
            std::fs::create_dir(&metadata_path).map_err(|error| {
                VaultError::Io(format!(
                    "failed to create metadata sabotage directory {}: {error}",
                    metadata_path.display()
                ))
            })?;
            Ok(())
        }

        fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
            let objects = self.objects.lock().map_err(|_| {
                VaultError::Io("metadata-failure backend mutex poisoned".to_owned())
            })?;
            objects.get(object_id).cloned().ok_or(VaultError::NotFound)
        }

        fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
            let mut objects = self.objects.lock().map_err(|_| {
                VaultError::Io("metadata-failure backend mutex poisoned".to_owned())
            })?;
            objects.remove(object_id);
            Ok(())
        }
    }

    #[test]
    fn vault_put_get_list_delete_roundtrip() -> Result<()> {
        let temp = tempdir()?;
        let identity_root = temp.path().join("identity");
        let vault_root = temp.path().join("vault");
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(vault_root),
            identity_store_root: Some(identity_root),
            backend_preference: BackendPreference::EncryptedFile,
            max_secret_bytes: 1024,
        })?;
        let scope = VaultScope::Global;
        let key = "openai_api_key";
        let value = b"sk-test-secret";
        vault.put_secret(&scope, key, value)?;

        let listed = vault.list_secrets(&scope)?;
        assert_eq!(listed.len(), 1, "exactly one secret should be listed");
        assert_eq!(listed[0].key, key);

        let loaded = vault.get_secret(&scope, key)?;
        assert_eq!(loaded, value);

        let deleted = vault.delete_secret(&scope, key)?;
        assert!(deleted, "delete should report existing secret removal");
        assert!(matches!(vault.get_secret(&scope, key), Err(super::VaultError::NotFound)));
        Ok(())
    }

    #[test]
    fn vault_decryption_fails_with_different_identity_root() -> Result<()> {
        let temp = tempdir()?;
        let shared_vault_root = temp.path().join("vault");
        let first = Vault::open_with_config(VaultConfig {
            root: Some(shared_vault_root.clone()),
            identity_store_root: Some(temp.path().join("identity-a")),
            backend_preference: BackendPreference::EncryptedFile,
            max_secret_bytes: 1024,
        })?;
        first.put_secret(&VaultScope::Global, "token", b"alpha")?;

        let second = Vault::open_with_config(VaultConfig {
            root: Some(shared_vault_root),
            identity_store_root: Some(temp.path().join("identity-b")),
            backend_preference: BackendPreference::EncryptedFile,
            max_secret_bytes: 1024,
        })?;
        let error = second
            .get_secret(&VaultScope::Global, "token")
            .expect_err("different identity root must not decrypt stored secret");
        assert!(
            matches!(error, super::VaultError::Crypto(_)),
            "unexpected error for wrong key material: {error}"
        );
        Ok(())
    }

    #[test]
    fn vault_delete_restores_metadata_when_backend_delete_fails() -> Result<()> {
        let temp = tempdir()?;
        let vault_root = temp.path().join("vault");
        ensure_owner_only_dir(&vault_root)?;
        let vault = Vault {
            root: vault_root,
            backend: Box::new(FailingDeleteBackend::default()),
            max_secret_bytes: 1024,
            kek: derive_kek_from_seed_material(b"palyra.vault.tests.delete_rollback")?,
        };
        vault.ensure_metadata_exists()?;
        let scope = VaultScope::Principal { principal_id: "user:ops".to_owned() };
        vault.put_secret(&scope, "api_key", b"secret-value")?;

        let error = vault
            .delete_secret(&scope, "api_key")
            .expect_err("delete should fail when backend returns an I/O error");
        assert!(
            matches!(error, VaultError::Io(message) if message.contains("injected backend delete failure")),
            "delete error should preserve backend failure context"
        );
        let loaded = vault.get_secret(&scope, "api_key")?;
        assert_eq!(loaded, b"secret-value");
        Ok(())
    }

    #[test]
    fn vault_put_secret_rolls_back_blob_when_metadata_write_fails() -> Result<()> {
        let temp = tempdir()?;
        let vault_root = temp.path().join("vault");
        ensure_owner_only_dir(&vault_root)?;
        let objects = Arc::new(Mutex::new(HashMap::new()));
        let vault = Vault {
            root: vault_root.clone(),
            backend: Box::new(MetadataWriteFailureBackend {
                root: vault_root,
                objects: Arc::clone(&objects),
            }),
            max_secret_bytes: 1024,
            kek: derive_kek_from_seed_material(b"palyra.vault.tests.put_rollback")?,
        };
        let scope =
            VaultScope::Channel { channel_name: "cli".to_owned(), account_id: "acct-1".to_owned() };
        let error = vault
            .put_secret(&scope, "api_key", b"secret-value")
            .expect_err("put should fail when metadata persistence is sabotaged");
        assert!(
            matches!(error, VaultError::Io(message) if message.contains("failed to persist metadata after blob write")),
            "put error should preserve metadata failure context"
        );
        let object_count = objects
            .lock()
            .map_err(|_| VaultError::Io("metadata-failure backend mutex poisoned".to_owned()))?
            .len();
        assert_eq!(object_count, 0, "blob rollback should remove orphaned payload");
        Ok(())
    }

    #[test]
    fn vault_put_secret_restores_previous_blob_when_metadata_write_fails() -> Result<()> {
        let temp = tempdir()?;
        let vault_root = temp.path().join("vault");
        ensure_owner_only_dir(&vault_root)?;
        let objects = Arc::new(Mutex::new(HashMap::new()));
        let vault = Vault {
            root: vault_root.clone(),
            backend: Box::new(MetadataWriteFailureBackend {
                root: vault_root,
                objects: Arc::clone(&objects),
            }),
            max_secret_bytes: 1024,
            kek: derive_kek_from_seed_material(b"palyra.vault.tests.update_rollback")?,
        };
        vault.ensure_metadata_exists()?;

        let scope = VaultScope::Principal { principal_id: "user:ops".to_owned() };
        let key = "api_key";
        let original_value = b"original-secret";
        let object_id = super::object_id_for(&scope, key);
        let original_payload = serde_json::to_vec(&super::seal(
            original_value,
            &vault.kek,
            super::build_aad(&scope, key).as_slice(),
        )?)
        .map_err(|error| {
            VaultError::Io(format!("failed to serialize original envelope payload: {error}"))
        })?;
        let expected_restored_payload = original_payload.clone();
        objects
            .lock()
            .map_err(|_| VaultError::Io("metadata-failure backend mutex poisoned".to_owned()))?
            .insert(object_id.clone(), original_payload);
        let now = super::current_unix_ms()?;
        vault.write_metadata(&super::MetadataFile {
            version: super::METADATA_VERSION,
            entries: vec![super::MetadataEntry {
                scope: scope.clone(),
                key: key.to_owned(),
                object_id: object_id.clone(),
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
                value_bytes: original_value.len(),
            }],
        })?;

        let error = vault
            .put_secret(&scope, key, b"updated-secret")
            .expect_err("update should fail when metadata persistence is sabotaged");
        assert!(
            matches!(error, VaultError::Io(message) if message.contains("failed to persist metadata after blob write")),
            "put error should preserve metadata failure context"
        );
        let restored = objects
            .lock()
            .map_err(|_| VaultError::Io("metadata-failure backend mutex poisoned".to_owned()))?
            .get(&object_id)
            .cloned()
            .ok_or(VaultError::NotFound)?;
        assert_eq!(
            restored, expected_restored_payload,
            "update rollback should restore previous encrypted blob payload"
        );
        Ok(())
    }

    #[test]
    fn metadata_lock_reclaim_keeps_live_owner_when_stale_age_elapsed() -> Result<()> {
        let temp = tempdir()?;
        let lock_path = temp.path().join(super::METADATA_LOCK_FILE);
        std::fs::write(&lock_path, format!("pid={} ts_ms=0\n", std::process::id()))?;

        let reclaimed = maybe_reclaim_stale_lock_with_policy(
            &lock_path,
            std::time::SystemTime::now(),
            Duration::ZERO,
        )?;
        assert!(!reclaimed, "live owner lock must not be reclaimed");
        assert!(lock_path.exists(), "live owner lock file should remain");
        Ok(())
    }

    #[test]
    fn metadata_lock_reclaim_removes_dead_owner_when_stale_age_elapsed() -> Result<()> {
        let temp = tempdir()?;
        let lock_path = temp.path().join(super::METADATA_LOCK_FILE);
        std::fs::write(&lock_path, format!("pid={} ts_ms=0\n", i32::MAX))?;

        let reclaimed = maybe_reclaim_stale_lock_with_policy(
            &lock_path,
            std::time::SystemTime::now(),
            Duration::ZERO,
        )?;
        assert!(reclaimed, "dead owner lock should be reclaimed");
        assert!(!lock_path.exists(), "reclaimed lock file should be removed");
        Ok(())
    }

    #[cfg(windows)]
    #[test]
    fn parse_whoami_sid_csv_extracts_sid_field() {
        let parsed = super::parse_whoami_sid_csv(
            r#""desktop\operator","S-1-5-21-123456789-111111111-222222222-1001""#,
        );
        assert_eq!(
            parsed.as_deref(),
            Some("S-1-5-21-123456789-111111111-222222222-1001"),
            "whoami CSV parser should extract SID from second column"
        );
    }

    #[cfg(windows)]
    #[test]
    fn windows_owner_only_helpers_apply_acl_to_dir_and_file() -> Result<()> {
        let temp = tempdir()?;
        let dir = temp.path().join("vault-acl");
        ensure_owner_only_dir(&dir)?;
        let file = dir.join("metadata.lock");
        std::fs::write(&file, b"marker")?;
        super::ensure_owner_only_file(&file)?;
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn encrypted_file_backend_enforces_owner_only_permissions() -> Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir()?;
        let identity_root = temp.path().join("identity");
        let vault_root = temp.path().join("vault");
        let vault = Vault::open_with_config(VaultConfig {
            root: Some(vault_root.clone()),
            identity_store_root: Some(identity_root),
            backend_preference: BackendPreference::EncryptedFile,
            max_secret_bytes: 1024,
        })?;
        vault.put_secret(&VaultScope::Global, "secret", b"value")?;

        let root_mode = std::fs::metadata(&vault_root)?.permissions().mode() & 0o777;
        let metadata_mode =
            std::fs::metadata(vault_root.join("metadata.json"))?.permissions().mode() & 0o777;
        let objects_mode =
            std::fs::metadata(vault_root.join("objects"))?.permissions().mode() & 0o777;
        assert_eq!(root_mode, 0o700);
        assert_eq!(metadata_mode, 0o600);
        assert_eq!(objects_mode, 0o700);
        Ok(())
    }

    #[test]
    fn vault_ref_parser_supports_expected_shape() {
        let parsed =
            VaultRef::parse("global/openai_api_key").expect("valid vault ref should parse");
        assert_eq!(parsed.scope, VaultScope::Global);
        assert_eq!(parsed.key, "openai_api_key");
    }

    #[test]
    fn kek_derivation_uses_stable_private_key_material() -> Result<()> {
        let state_before = br#"{
            "generation": 1,
            "ca": {
                "certificate_pem": "cert-a",
                "private_key_pem": "private-key-material",
                "sequence": 10
            }
        }"#;
        let state_after = br#"{
            "generation": 4,
            "ca": {
                "certificate_pem": "cert-b",
                "private_key_pem": "private-key-material",
                "sequence": 99
            }
        }"#;
        let seed_before = extract_kek_seed_material(state_before)?;
        let seed_after = extract_kek_seed_material(state_after)?;
        let kek_before = derive_kek_from_seed_material(seed_before.as_slice())?;
        let kek_after = derive_kek_from_seed_material(seed_after.as_slice())?;
        assert_eq!(
            kek_before, kek_after,
            "KEK derivation must remain stable across state metadata changes"
        );
        Ok(())
    }
}
