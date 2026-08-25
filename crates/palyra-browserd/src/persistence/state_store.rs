//! Encrypted on-disk store for persisted browser session state and the profile registry.
//!
//! Blobs are sealed with ChaCha20-Poly1305 (layout: magic, nonce, ciphertext+tag) and written
//! atomically (create-new tmp file, fsync, rename) into an owner-only directory; symlinks are
//! rejected at every filesystem touchpoint.

use crate::*;

#[cfg(windows)]
use palyra_common::windows_security;

/// On-disk (encrypted) browser session snapshot.
///
/// Serde field names are the persisted format — do not rename. `state_revision` defaults to 0
/// so snapshots written before revision tracking still deserialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistedSessionSnapshot {
    pub(crate) v: u32,
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) tabs: Vec<BrowserTabRecord>,
    pub(crate) tab_order: Vec<String>,
    pub(crate) active_tab_id: String,
    pub(crate) permissions: SessionPermissionsInternal,
    pub(crate) cookie_jar: HashMap<String, HashMap<String, String>>,
    pub(crate) storage_entries: HashMap<String, HashMap<String, String>>,
    #[serde(default)]
    pub(crate) state_revision: u64,
    pub(crate) saved_at_unix_ms: u64,
}

/// Canonical tab form for snapshot hashing: sorted `typed_inputs`, console log excluded.
///
/// Excluding the console log keeps diagnostic noise from invalidating state hashes.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct BrowserTabRecordForHash {
    pub(crate) tab_id: String,
    pub(crate) last_title: String,
    pub(crate) last_url: Option<String>,
    pub(crate) last_page_body: String,
    pub(crate) scroll_x: i64,
    pub(crate) scroll_y: i64,
    pub(crate) typed_inputs: BTreeMap<String, String>,
    pub(crate) network_log: VecDeque<NetworkLogEntryInternal>,
}

/// Hash payload matching the pre-`state_revision` snapshot layout.
///
/// Kept so profiles written by older builds still pass restore validation (see
/// `validate_restored_snapshot_against_profile`).
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PersistedSessionSnapshotLegacyForHash {
    pub(crate) v: u32,
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) tabs: Vec<BrowserTabRecord>,
    pub(crate) tab_order: Vec<String>,
    pub(crate) active_tab_id: String,
    pub(crate) permissions: SessionPermissionsInternal,
    pub(crate) cookie_jar: HashMap<String, HashMap<String, String>>,
    pub(crate) storage_entries: HashMap<String, HashMap<String, String>>,
    pub(crate) saved_at_unix_ms: u64,
}

/// A decrypted snapshot plus the SHA-256 of its plaintext for cheap integrity comparison.
#[derive(Debug, Clone)]
pub(crate) struct LoadedPersistedSessionSnapshot {
    pub(crate) snapshot: PersistedSessionSnapshot,
    pub(crate) raw_hash_sha256: String,
}

#[derive(Debug, Clone, Copy)]
enum SnapshotDecodeFailure {
    Decrypt,
    Deserialize,
}

impl SnapshotDecodeFailure {
    const fn audit_label(self) -> &'static str {
        match self {
            Self::Decrypt => "decrypt_failed",
            Self::Deserialize => "deserialize_failed",
        }
    }
}

/// Canonical (sorted-map) snapshot form so hashes are stable across `HashMap` iteration orders.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct PersistedSessionSnapshotForHash {
    pub(crate) v: u32,
    pub(crate) principal: String,
    pub(crate) channel: Option<String>,
    pub(crate) tabs: Vec<BrowserTabRecordForHash>,
    pub(crate) tab_order: Vec<String>,
    pub(crate) active_tab_id: String,
    pub(crate) permissions: SessionPermissionsInternal,
    pub(crate) cookie_jar: BTreeMap<String, BTreeMap<String, String>>,
    pub(crate) storage_entries: BTreeMap<String, BTreeMap<String, String>>,
    pub(crate) state_revision: u64,
    pub(crate) saved_at_unix_ms: u64,
}

/// Handle to the encrypted browserd state directory (session snapshots and profile registry).
#[derive(Debug, Clone)]
pub(crate) struct PersistedStateStore {
    pub(crate) root_dir: PathBuf,
    pub(crate) key: [u8; STATE_KEY_LEN],
}

/// Builds the persisted state store from environment configuration.
///
/// Returns `Ok(None)` — persistence disabled — when the state key env var (`STATE_KEY_ENV`) is
/// unset or empty.
///
/// # Errors
/// Fails when the key does not decode, the configured state dir is invalid, or store
/// initialization (dir creation/hardening) fails.
pub(crate) fn build_state_store_from_env() -> Result<Option<PersistedStateStore>> {
    let key_raw = match std::env::var(STATE_KEY_ENV) {
        Ok(value) => value.trim().to_owned(),
        Err(_) => return Ok(None),
    };
    if key_raw.is_empty() {
        return Ok(None);
    }
    let key = decode_state_key(key_raw.as_str())?;
    let configured_state_dir = std::env::var(STATE_DIR_ENV)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .map(|value| normalize_configured_state_path(value.as_str(), STATE_DIR_ENV))
        .transpose()?;
    let state_dir = match configured_state_dir {
        Some(path) => path,
        None => default_browserd_state_dir()?,
    };
    Ok(Some(PersistedStateStore::new(state_dir, key)?))
}

/// Validates an operator-configured state path.
///
/// Parent (`..`) segments are rejected so a configured path can never escape upward out of the
/// directory the operator pointed at.
///
/// # Errors
/// Fails when the path is empty or contains a parent segment.
pub(crate) fn normalize_configured_state_path(raw: &str, field: &'static str) -> Result<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        anyhow::bail!("{field} cannot be empty");
    }
    let path = PathBuf::from(trimmed);
    for component in path.components() {
        if matches!(component, std::path::Component::ParentDir) {
            anyhow::bail!("{field} cannot contain '..' path segments");
        }
    }
    Ok(path)
}

/// Resolves the default browserd state directory from the process environment.
///
/// # Errors
/// Fails when no suitable base directory variable is set; see
/// [`default_browserd_state_dir_from_env`].
pub(crate) fn default_browserd_state_dir() -> Result<PathBuf> {
    default_browserd_state_dir_from_env(
        std::env::var_os(STATE_ROOT_ENV),
        std::env::var_os("APPDATA"),
        std::env::var_os("LOCALAPPDATA"),
        std::env::var_os("XDG_STATE_HOME"),
        std::env::var_os("HOME"),
    )
}

/// Resolves the state directory from explicit environment values (injectable for tests).
///
/// Resolution order: the Palyra state root, then the platform convention — `APPDATA`/
/// `LOCALAPPDATA` on Windows, `~/Library/Application Support` on macOS, `XDG_STATE_HOME` or
/// `~/.local/state` elsewhere.
///
/// # Errors
/// Fails when none of the applicable variables is set or the state root is invalid.
pub(crate) fn default_browserd_state_dir_from_env(
    state_root: Option<OsString>,
    appdata: Option<OsString>,
    local_appdata: Option<OsString>,
    xdg_state_home: Option<OsString>,
    home: Option<OsString>,
) -> Result<PathBuf> {
    if let Some(state_root_raw) = state_root {
        let normalized = normalize_configured_state_path(
            state_root_raw.to_string_lossy().as_ref(),
            STATE_ROOT_ENV,
        )?;
        return Ok(normalized.join("browserd"));
    }
    #[cfg(windows)]
    {
        let _ = xdg_state_home;
        let _ = home;
        if let Some(appdata) = appdata {
            return Ok(PathBuf::from(appdata).join("Palyra").join("browserd"));
        }
        if let Some(local_appdata) = local_appdata {
            return Ok(PathBuf::from(local_appdata).join("Palyra").join("browserd"));
        }
        anyhow::bail!(
            "failed to resolve browserd state dir: APPDATA/LOCALAPPDATA are unset and {STATE_ROOT_ENV} is not configured"
        );
    }
    #[cfg(target_os = "macos")]
    {
        let _ = appdata;
        let _ = local_appdata;
        let _ = xdg_state_home;
        let home = home.ok_or_else(|| {
            anyhow::anyhow!(
                "failed to resolve browserd state dir: HOME is unset and {STATE_ROOT_ENV} is not configured"
            )
        })?;
        return Ok(PathBuf::from(home)
            .join("Library")
            .join("Application Support")
            .join("Palyra")
            .join("browserd"));
    }
    #[cfg(all(not(windows), not(target_os = "macos")))]
    {
        let _ = appdata;
        let _ = local_appdata;
        if let Some(xdg_state_home) = xdg_state_home {
            return Ok(PathBuf::from(xdg_state_home).join("palyra").join("browserd"));
        }
        let home = home.ok_or_else(|| {
            anyhow::anyhow!(
                "failed to resolve browserd state dir: XDG_STATE_HOME/HOME are unset and {STATE_ROOT_ENV} is not configured"
            )
        })?;
        Ok(PathBuf::from(home).join(".local").join("state").join("palyra").join("browserd"))
    }
}

/// Creates `path` (if needed) and restricts it to the owning user.
///
/// Mode 0700 on unix; owner-only ACLs on Windows.
///
/// # Errors
/// Fails when the directory cannot be created or permissions cannot be applied.
pub(crate) fn ensure_owner_only_dir(path: &Path) -> Result<()> {
    match fs::create_dir_all(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
        Err(error) => {
            return Err(error).with_context(|| {
                format!("failed to create browserd state dir '{}'", path.display())
            });
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).with_context(|| {
            format!(
                "failed to enforce owner-only directory permissions on browserd state dir '{}'",
                path.display()
            )
        })?;
    }
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(path, owner_sid.as_str(), true)?;
    }
    Ok(())
}

/// Restricts an existing file to the owning user (0600 on unix, owner-only ACLs on Windows).
///
/// # Errors
/// Fails when permissions cannot be applied.
pub(crate) fn ensure_owner_only_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to enforce owner-only permissions on browserd state file '{}'",
                path.display()
            )
        })?;
    }
    #[cfg(windows)]
    {
        let owner_sid = current_user_sid()?;
        harden_windows_path_permissions(path, owner_sid.as_str(), false)?;
    }
    Ok(())
}

/// Resolves the current user's SID for state-dir ACL hardening.
///
/// # Errors
/// Fails when the SID cannot be resolved from the process token.
#[cfg(windows)]
pub(crate) fn current_user_sid() -> Result<String> {
    windows_security::current_user_sid().with_context(|| "failed to resolve browserd state ACL SID")
}

/// Applies owner-only Windows ACLs to a state path.
///
/// # Errors
/// Fails when the ACLs cannot be applied.
#[cfg(windows)]
pub(crate) fn harden_windows_path_permissions(
    path: &Path,
    owner_sid: &str,
    is_directory: bool,
) -> Result<()> {
    windows_security::harden_windows_path_permissions(path, owner_sid, is_directory)
        .with_context(|| format!("failed to harden browserd state path '{}'", path.display()))
}

/// Decodes the base64 master state key from its env-var value.
///
/// # Errors
/// Fails when the value is not valid base64 or does not decode to exactly `STATE_KEY_LEN`
/// bytes.
pub(crate) fn decode_state_key(raw: &str) -> Result<[u8; STATE_KEY_LEN]> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw)
        .context("failed to decode PALYRA_BROWSERD_STATE_ENCRYPTION_KEY as base64")?;
    if decoded.len() != STATE_KEY_LEN {
        anyhow::bail!(
            "PALYRA_BROWSERD_STATE_ENCRYPTION_KEY must decode to exactly {STATE_KEY_LEN} bytes"
        );
    }
    let mut key = [0_u8; STATE_KEY_LEN];
    key.copy_from_slice(decoded.as_slice());
    Ok(key)
}

impl PersistedStateStore {
    /// Opens (creating and hardening if needed) the state directory and sweeps stale tmp files
    /// left behind by interrupted atomic writes.
    ///
    /// # Errors
    /// Fails when the directory is a symlink, cannot be created/hardened, or the tmp sweep
    /// fails.
    pub(crate) fn new(root_dir: PathBuf, key: [u8; STATE_KEY_LEN]) -> Result<Self> {
        ensure_path_is_not_symlink(root_dir.as_path(), "browserd state dir")?;
        ensure_owner_only_dir(root_dir.as_path())?;
        ensure_path_is_secure_directory(root_dir.as_path(), "browserd state dir")?;
        let store = Self { root_dir, key };
        store.cleanup_tmp_files()?;
        Ok(store)
    }

    /// Path of the encrypted snapshot file for `state_id`.
    pub(crate) fn snapshot_path(&self, state_id: &str) -> PathBuf {
        self.root_dir.join(format!("{state_id}.enc"))
    }

    /// Fresh tmp path for an atomic snapshot write; the ULID suffix keeps concurrent writers
    /// from colliding.
    pub(crate) fn tmp_snapshot_path(&self, state_id: &str) -> PathBuf {
        self.root_dir.join(format!("{state_id}.{}.{}", Ulid::generate(), STATE_TMP_EXTENSION))
    }

    /// Path of the encrypted profile registry file.
    pub(crate) fn profile_registry_path(&self) -> PathBuf {
        self.root_dir.join(PROFILE_REGISTRY_FILE_NAME)
    }

    /// Removes tmp files left behind by interrupted atomic writes; a missing dir is fine.
    ///
    /// # Errors
    /// Fails when the directory cannot be enumerated or contains a symlink entry.
    pub(crate) fn cleanup_tmp_files(&self) -> Result<()> {
        let entries = match fs::read_dir(self.root_dir.as_path()) {
            Ok(value) => value,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to enumerate browser state dir '{}' for tmp cleanup",
                        self.root_dir.display()
                    )
                })
            }
        };
        for entry in entries {
            let entry = entry.with_context(|| {
                format!("failed to read browser state entry in '{}'", self.root_dir.display())
            })?;
            let path = entry.path();
            let file_type = entry.file_type().with_context(|| {
                format!("failed to inspect browser state entry type for '{}'", path.display())
            })?;
            if file_type.is_symlink() {
                anyhow::bail!(
                    "browser state dir '{}' contains unexpected symlink entry '{}'",
                    self.root_dir.display(),
                    path.display()
                );
            }
            if path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case(STATE_TMP_EXTENSION))
            {
                let _ = fs::remove_file(path.as_path());
            }
        }
        Ok(())
    }

    /// Loads and decrypts a session snapshot; `Ok(None)` when no snapshot exists.
    ///
    /// `profile_id` selects the per-profile derived key and must match the one used to save.
    ///
    /// Authentication or format failures quarantine the unreadable blob and
    /// return `Ok(None)`, allowing a fresh session to recover. Filesystem,
    /// permission, or quarantine failures remain hard errors.
    ///
    /// # Errors
    /// Fails when the file cannot be read securely or an unreadable blob cannot
    /// be moved into the audit quarantine.
    pub(crate) fn load_snapshot(
        &self,
        state_id: &str,
        profile_id: Option<&str>,
    ) -> Result<Option<LoadedPersistedSessionSnapshot>> {
        let path = self.snapshot_path(state_id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = read_hardened_file(path.as_path(), "persisted browser state")?;
        let key = derive_state_encryption_key(&self.key, profile_id);
        let decrypted = match decrypt_state_blob(&key, bytes.as_slice()) {
            Ok(value) => value,
            Err(error) => {
                self.quarantine_unreadable_snapshot(
                    state_id,
                    path.as_path(),
                    SnapshotDecodeFailure::Decrypt,
                    &error,
                )?;
                return Ok(None);
            }
        };
        let snapshot: PersistedSessionSnapshot = match serde_json::from_slice(decrypted.as_slice())
        {
            Ok(value) => value,
            Err(error) => {
                self.quarantine_unreadable_snapshot(
                    state_id,
                    path.as_path(),
                    SnapshotDecodeFailure::Deserialize,
                    &error,
                )?;
                return Ok(None);
            }
        };
        Ok(Some(LoadedPersistedSessionSnapshot {
            snapshot,
            raw_hash_sha256: sha256_hex(decrypted.as_slice()),
        }))
    }

    fn quarantine_unreadable_snapshot(
        &self,
        state_id: &str,
        source_path: &Path,
        failure: SnapshotDecodeFailure,
        decode_error: &dyn std::fmt::Display,
    ) -> Result<()> {
        ensure_path_is_not_symlink(source_path, "persisted browser state")?;
        let quarantine_name =
            format!("{state_id}.{}.{}.invalid", Ulid::generate(), failure.audit_label());
        let quarantine_path = self.root_dir.join(quarantine_name.as_str());
        fs::rename(source_path, quarantine_path.as_path()).with_context(|| {
            format!(
                "failed to quarantine unreadable persisted browser state '{}' as '{}'",
                source_path.display(),
                quarantine_path.display()
            )
        })?;
        ensure_owner_only_file(quarantine_path.as_path())?;
        sync_directory(self.root_dir.as_path())?;
        warn!(
            state_id,
            failure = failure.audit_label(),
            quarantine_file = quarantine_name,
            error = %decode_error,
            "quarantined unreadable persisted browser state and will start with clean state"
        );
        Ok(())
    }

    /// Encrypts and atomically writes a session snapshot under `state_id`.
    ///
    /// # Errors
    /// Fails when serialization, encryption, or the hardened atomic write fails.
    pub(crate) fn save_snapshot(
        &self,
        state_id: &str,
        profile_id: Option<&str>,
        snapshot: &PersistedSessionSnapshot,
    ) -> Result<()> {
        let serialized =
            serde_json::to_vec(snapshot).context("failed to serialize persisted browser state")?;
        let key = derive_state_encryption_key(&self.key, profile_id);
        let encrypted =
            encrypt_state_blob(&key, serialized.as_slice()).context("failed to encrypt state")?;
        let target_path = self.snapshot_path(state_id);
        let tmp_path = self.tmp_snapshot_path(state_id);
        write_hardened_file_atomic(
            self.root_dir.as_path(),
            target_path.as_path(),
            tmp_path.as_path(),
            encrypted.as_slice(),
            "persisted browser state",
        )?;
        Ok(())
    }

    /// Deletes the snapshot for `state_id`; missing snapshots are not an error.
    ///
    /// # Errors
    /// Fails when the path is a symlink or removal fails.
    pub(crate) fn delete_snapshot(&self, state_id: &str) -> Result<()> {
        let path = self.snapshot_path(state_id);
        if !path.exists() {
            return Ok(());
        }
        ensure_path_is_not_symlink(path.as_path(), "persisted browser state")?;
        fs::remove_file(path.as_path()).with_context(|| {
            format!("failed to delete persisted browser state '{}'", path.display())
        })?;
        Ok(())
    }

    /// Loads, decrypts, and normalizes the profile registry; missing file yields the default.
    ///
    /// The registry is always encrypted with the master key (not a per-profile key) because it
    /// must be readable before any profile is selected.
    ///
    /// # Errors
    /// Fails when the file cannot be read, decryption fails, or the plaintext does not
    /// deserialize.
    pub(crate) fn load_profile_registry(&self) -> Result<BrowserProfileRegistryDocument> {
        let path = self.profile_registry_path();
        if !path.exists() {
            return Ok(BrowserProfileRegistryDocument::default());
        }
        let bytes = read_hardened_file(path.as_path(), "browser profile registry")?;
        let decrypted = decrypt_state_blob(&self.key, bytes.as_slice()).with_context(|| {
            format!("failed to decrypt browser profile registry '{}'", path.display())
        })?;
        let mut registry: BrowserProfileRegistryDocument =
            serde_json::from_slice(decrypted.as_slice()).with_context(|| {
                format!("failed to deserialize browser profile registry '{}'", path.display())
            })?;
        normalize_profile_registry(&mut registry);
        Ok(registry)
    }

    /// Encrypts and atomically writes the profile registry, enforcing its size cap.
    ///
    /// # Errors
    /// Fails when serialization or encryption fails, the serialized registry exceeds
    /// `MAX_PROFILE_REGISTRY_BYTES`, or the hardened atomic write fails.
    pub(crate) fn save_profile_registry(
        &self,
        registry: &BrowserProfileRegistryDocument,
    ) -> Result<()> {
        let serialized = serde_json::to_vec(registry)
            .context("failed to serialize browser profile registry document")?;
        if serialized.len() > MAX_PROFILE_REGISTRY_BYTES {
            anyhow::bail!(
                "browser profile registry exceeds max bytes ({} > {})",
                serialized.len(),
                MAX_PROFILE_REGISTRY_BYTES
            );
        }
        let encrypted = encrypt_state_blob(&self.key, serialized.as_slice())
            .context("failed to encrypt browser profile registry")?;
        let target_path = self.profile_registry_path();
        let tmp_path = self.root_dir.join(format!(
            "{}.{}.{}",
            PROFILE_REGISTRY_FILE_NAME,
            Ulid::generate(),
            STATE_TMP_EXTENSION
        ));
        write_hardened_file_atomic(
            self.root_dir.as_path(),
            target_path.as_path(),
            tmp_path.as_path(),
            encrypted.as_slice(),
            "browser profile registry",
        )?;
        Ok(())
    }
}

/// Rejects symlinks at `path`; a missing path passes.
///
/// # Errors
/// Fails when the path is a symlink or its metadata cannot be inspected.
pub(crate) fn ensure_path_is_not_symlink(path: &Path, context: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                anyhow::bail!("{context} '{}' must not be a symlink", path.display());
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| {
            format!("failed to inspect {context} path '{}' for symlink checks", path.display())
        }),
    }
}

/// Requires `path` to exist as a real (non-symlink) directory.
///
/// # Errors
/// Fails when the path is missing, a symlink, or not a directory.
pub(crate) fn ensure_path_is_secure_directory(path: &Path, context: &str) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("failed to inspect {context} '{}'", path.display()))?;
    if metadata.file_type().is_symlink() {
        anyhow::bail!("{context} '{}' must not be a symlink", path.display());
    }
    if !metadata.is_dir() {
        anyhow::bail!("{context} '{}' must be a directory", path.display());
    }
    Ok(())
}

/// Reads a state file with symlink rejection (and `O_NOFOLLOW` on unix to close the
/// check-then-open race).
///
/// # Errors
/// Fails when the path is a symlink or the open/read fails.
pub(crate) fn read_hardened_file(path: &Path, context: &str) -> Result<Vec<u8>> {
    ensure_path_is_not_symlink(path, context)?;
    #[cfg(unix)]
    {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
            .with_context(|| format!("failed to open {context} '{}' for read", path.display()))?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .with_context(|| format!("failed to read {context} '{}'", path.display()))?;
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        fs::read(path).with_context(|| format!("failed to read {context} '{}'", path.display()))
    }
}

/// Writes `payload` via create-new tmp file, fsync, rename, then directory fsync.
///
/// `create_new` plus the symlink checks ensure the write can never follow an attacker-placed
/// link, and the rename makes the visible update atomic on the same filesystem.
///
/// # Errors
/// Fails when any of the checks, the tmp write/fsync, the rename, or permission hardening
/// fails.
pub(crate) fn write_hardened_file_atomic(
    root_dir: &Path,
    target_path: &Path,
    tmp_path: &Path,
    payload: &[u8],
    context: &str,
) -> Result<()> {
    ensure_path_is_secure_directory(root_dir, "browserd state dir")?;
    ensure_path_is_not_symlink(target_path, context)?;
    ensure_path_is_not_symlink(tmp_path, "browserd temporary state file")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(tmp_path)
            .with_context(|| format!("failed to create tmp {context} '{}'", tmp_path.display()))?;
        file.write_all(payload)
            .with_context(|| format!("failed to write tmp {context} '{}'", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to fsync tmp {context} '{}'", tmp_path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let mut file =
            fs::OpenOptions::new().create_new(true).write(true).open(tmp_path).with_context(
                || format!("failed to create tmp {context} '{}'", tmp_path.display()),
            )?;
        file.write_all(payload)
            .with_context(|| format!("failed to write tmp {context} '{}'", tmp_path.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to fsync tmp {context} '{}'", tmp_path.display()))?;
    }
    ensure_owner_only_file(tmp_path)?;
    fs::rename(tmp_path, target_path).with_context(|| {
        format!(
            "failed to atomically move tmp {context} '{}' into '{}'",
            tmp_path.display(),
            target_path.display()
        )
    })?;
    ensure_owner_only_file(target_path)?;
    sync_directory(root_dir)?;
    Ok(())
}

/// Fsyncs a directory so a completed rename survives power loss; no-op on non-unix.
///
/// # Errors
/// Fails when the directory cannot be opened or synced (unix only).
pub(crate) fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let directory = fs::File::open(path)
            .with_context(|| format!("failed to open directory '{}' for fsync", path.display()))?;
        directory
            .sync_all()
            .with_context(|| format!("failed to fsync directory '{}'", path.display()))?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

/// Seals `plaintext` with ChaCha20-Poly1305 into the state blob layout (magic, nonce,
/// ciphertext+tag).
///
/// A fresh random 96-bit nonce is generated per call; `LessSafeKey` is ring's API for
/// caller-managed nonces, and uniqueness comes from that per-seal randomness.
///
/// # Errors
/// Fails when key initialization, nonce generation, or sealing fails.
pub(crate) fn encrypt_state_blob(key: &[u8; STATE_KEY_LEN], plaintext: &[u8]) -> Result<Vec<u8>> {
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key)
        .map_err(|_| anyhow::anyhow!("failed to initialize state cipher key"))?;
    let key = LessSafeKey::new(unbound_key);
    let mut nonce_bytes = [0_u8; STATE_NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce_bytes)
        .map_err(|_| anyhow::anyhow!("failed to generate state encryption nonce"))?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut in_out = plaintext.to_vec();
    key.seal_in_place_append_tag(nonce, Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to seal state payload"))?;
    let mut output = Vec::with_capacity(STATE_FILE_MAGIC.len() + STATE_NONCE_LEN + in_out.len());
    output.extend_from_slice(STATE_FILE_MAGIC);
    output.extend_from_slice(&nonce_bytes);
    output.extend_from_slice(in_out.as_slice());
    Ok(output)
}

/// Opens a state blob produced by [`encrypt_state_blob`].
///
/// # Errors
/// Fails when the blob is too short, the magic header does not match, or authenticated
/// decryption fails (wrong key or tampered data).
pub(crate) fn decrypt_state_blob(key: &[u8; STATE_KEY_LEN], encrypted: &[u8]) -> Result<Vec<u8>> {
    if encrypted.len() < STATE_FILE_MAGIC.len() + STATE_NONCE_LEN {
        anyhow::bail!("state payload is too short");
    }
    if &encrypted[..STATE_FILE_MAGIC.len()] != STATE_FILE_MAGIC {
        anyhow::bail!("state payload magic header is invalid");
    }
    let mut nonce_bytes = [0_u8; STATE_NONCE_LEN];
    nonce_bytes.copy_from_slice(
        &encrypted[STATE_FILE_MAGIC.len()..STATE_FILE_MAGIC.len() + STATE_NONCE_LEN],
    );
    let mut in_out = encrypted[STATE_FILE_MAGIC.len() + STATE_NONCE_LEN..].to_vec();
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key)
        .map_err(|_| anyhow::anyhow!("failed to initialize state cipher key"))?;
    let key = LessSafeKey::new(unbound_key);
    let plaintext = key
        .open_in_place(Nonce::assume_unique_for_key(nonce_bytes), Aad::empty(), &mut in_out)
        .map_err(|_| anyhow::anyhow!("failed to open state payload"))?;
    Ok(plaintext.to_vec())
}

/// Derives a per-profile data-encryption key from the master key; profile-less state uses the
/// master key directly.
///
/// SHA-256 over (namespace, master key, profile id) — deterministic by design so existing
/// profile blobs stay decryptable across restarts.
pub(crate) fn derive_state_encryption_key(
    master_key: &[u8; STATE_KEY_LEN],
    profile_id: Option<&str>,
) -> [u8; STATE_KEY_LEN] {
    let Some(profile_id) = profile_id else {
        return *master_key;
    };
    let mut context = DigestContext::new(&SHA256);
    context.update(STATE_PROFILE_DEK_NAMESPACE);
    context.update(master_key);
    context.update(profile_id.as_bytes());
    let digest = context.finish();
    let mut key = [0_u8; STATE_KEY_LEN];
    key.copy_from_slice(digest.as_ref());
    key
}

/// SHA-256 of `bytes` as a lowercase hex string.
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut context = DigestContext::new(&SHA256);
    context.update(bytes);
    encode_hex(context.finish().as_ref())
}

/// Encodes bytes as lowercase hex.
pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|value| format!("{value:02x}")).collect::<String>()
}
