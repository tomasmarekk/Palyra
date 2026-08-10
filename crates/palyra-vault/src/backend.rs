//! Blob storage backends for sealed envelopes: OS-native keystores (macOS Keychain, Linux
//! Secret Service, Windows DPAPI) with an encrypted-file store as the portable fallback.
//!
//! The first successful selection is pinned in a `backend.kind` marker file so a vault never
//! silently switches backends (which would strand previously stored blobs). Backends only ever
//! see envelope ciphertext produced by `envelope.rs`.

use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::{
    io::Write,
    process::{Command, Stdio},
};

#[cfg(any(target_os = "linux", target_os = "macos"))]
use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
#[cfg(windows)]
use palyra_common::windows_security::{dpapi_protect_current_user, dpapi_unprotect_current_user};
use ulid::Ulid;

use crate::{
    canonicalize_existing_dir, ensure_owner_only_dir, ensure_owner_only_file,
    ensure_path_within_root, normalize_storage_object_id, write_new_owner_only_file, VaultError,
};

const BACKEND_MARKER_FILE: &str = "backend.kind";
const OBJECTS_DIR: &str = "objects";
const OBJECTS_STORE_FILE: &str = "objects.store.json";
const MAX_OBJECTS_STORE_BYTES: u64 = 32 * 1024 * 1024;
// serde_json renders Vec<u8> as a number array; the widest element is `255,` = 4 bytes.
const JSON_U8_WORST_CASE_BYTES: u64 = 4;
#[cfg(target_os = "macos")]
const KEYCHAIN_SERVICE_NAME: &str = "palyra.vault.v1";
#[cfg(target_os = "linux")]
const SECRET_TOOL_SERVICE_ATTR: &str = "service";
#[cfg(target_os = "linux")]
const SECRET_TOOL_SERVICE_NAME: &str = "palyra.vault.v1";
#[cfg(target_os = "linux")]
const SECRET_TOOL_KEY_ATTR: &str = "key";
#[cfg(windows)]
const WINDOWS_DPAPI_OBJECTS_DIR: &str = "objects_dpapi";

/// Concrete blob backend a vault is bound to; persisted in the `backend.kind` marker file.
///
/// OS-specific variants only exist on their platform, so a vault directory moved across
/// operating systems fails closed instead of decoding foreign blobs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendKind {
    /// Portable JSON object store under the vault root, available everywhere.
    EncryptedFile,
    /// macOS `security` keychain items.
    #[cfg(target_os = "macos")]
    MacosKeychain,
    /// freedesktop Secret Service via `secret-tool`.
    #[cfg(target_os = "linux")]
    LinuxSecretService,
    /// Per-user DPAPI-protected files under the vault root.
    #[cfg(windows)]
    WindowsDpapi,
}

impl BackendKind {
    /// Returns the stable marker-file/serde identifier for this backend.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EncryptedFile => "encrypted_file",
            #[cfg(target_os = "macos")]
            Self::MacosKeychain => "macos_keychain",
            #[cfg(target_os = "linux")]
            Self::LinuxSecretService => "linux_secret_service",
            #[cfg(windows)]
            Self::WindowsDpapi => "windows_dpapi",
        }
    }

    /// Parses a marker-file value back into a backend kind.
    ///
    /// # Errors
    /// Returns [`VaultError::BackendUnavailable`] for unknown markers, including markers written
    /// by another operating system's backend.
    pub fn parse(raw: &str) -> Result<Self, VaultError> {
        let normalized = raw.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "encrypted_file" => Ok(Self::EncryptedFile),
            #[cfg(target_os = "macos")]
            "macos_keychain" => Ok(Self::MacosKeychain),
            #[cfg(target_os = "linux")]
            "linux_secret_service" => Ok(Self::LinuxSecretService),
            #[cfg(windows)]
            "windows_dpapi" => Ok(Self::WindowsDpapi),
            _ => Err(VaultError::BackendUnavailable(format!(
                "unsupported vault backend kind marker '{raw}'"
            ))),
        }
    }
}

/// Backend selection strategy used when a vault is created for the first time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendPreference {
    /// Prefer the platform keystore, falling back to the encrypted-file store.
    Auto,
    /// Always use the portable encrypted-file store.
    EncryptedFile,
}

/// Storage interface for sealed envelope blobs, keyed by normalized object id.
pub(crate) trait BlobBackend: Send + Sync {
    /// Identifies which concrete backend this is.
    fn kind(&self) -> BackendKind;
    /// Stores or overwrites the blob for `object_id`.
    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError>;
    /// Loads the blob for `object_id`, or [`VaultError::NotFound`].
    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError>;
    /// Removes the blob for `object_id`; missing blobs are not an error.
    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError>;
}

/// Selects the backend for `root`, honoring an existing marker before `preference`.
///
/// The marker takes priority so a vault keeps the backend its blobs were written to even if the
/// caller's preference (or keystore availability) changes later; the marker is written
/// atomically via temp-file + rename.
pub(crate) fn select_backend(
    root: &Path,
    preference: BackendPreference,
) -> Result<Box<dyn BlobBackend>, VaultError> {
    ensure_owner_only_dir(root)?;
    let root = canonicalize_existing_dir(root, "vault backend root directory")?;
    let marker_path = root.join(BACKEND_MARKER_FILE);
    if marker_path.exists() {
        let marker = fs::read_to_string(&marker_path).map_err(|error| {
            VaultError::Io(format!(
                "failed to read backend marker {}: {error}",
                marker_path.display()
            ))
        })?;
        let kind = BackendKind::parse(marker.trim())?;
        let backend = backend_for_kind(kind, root.as_path())?;
        return Ok(backend);
    }

    let backend = match preference {
        BackendPreference::EncryptedFile => {
            backend_for_kind(BackendKind::EncryptedFile, root.as_path())?
        }
        BackendPreference::Auto => choose_auto_backend(root.as_path())?,
    };
    let marker_tmp = marker_path.with_extension(format!("tmp.{}", Ulid::new()));
    fs::write(&marker_tmp, backend.kind().as_str().as_bytes()).map_err(|error| {
        VaultError::Io(format!("failed to write backend marker {}: {error}", marker_tmp.display()))
    })?;
    ensure_owner_only_file(&marker_tmp)?;
    fs::rename(&marker_tmp, &marker_path).map_err(|error| {
        VaultError::Io(format!(
            "failed to finalize backend marker {}: {error}",
            marker_path.display()
        ))
    })?;
    ensure_owner_only_file(&marker_path)?;
    Ok(backend)
}

/// Picks the best available backend: OS keystore first, encrypted file as the last resort.
fn choose_auto_backend(root: &Path) -> Result<Box<dyn BlobBackend>, VaultError> {
    #[cfg(target_os = "macos")]
    {
        if MacosKeychainBackend::is_available() {
            return Ok(Box::new(MacosKeychainBackend::new()));
        }
    }

    #[cfg(target_os = "linux")]
    {
        if LinuxSecretServiceBackend::is_available() {
            return Ok(Box::new(LinuxSecretServiceBackend::new()));
        }
    }

    #[cfg(windows)]
    {
        if WindowsDpapiBackend::is_available() {
            return Ok(Box::new(WindowsDpapiBackend::new(root)?));
        }
    }

    backend_for_kind(BackendKind::EncryptedFile, root)
}

/// Instantiates a specific backend kind, failing closed when it is not currently available.
fn backend_for_kind(kind: BackendKind, root: &Path) -> Result<Box<dyn BlobBackend>, VaultError> {
    match kind {
        BackendKind::EncryptedFile => Ok(Box::new(EncryptedFileBackend::new(root)?)),
        #[cfg(target_os = "macos")]
        BackendKind::MacosKeychain => {
            if !MacosKeychainBackend::is_available() {
                return Err(VaultError::BackendUnavailable(
                    "macOS keychain backend is unavailable".to_owned(),
                ));
            }
            Ok(Box::new(MacosKeychainBackend::new()))
        }
        #[cfg(target_os = "linux")]
        BackendKind::LinuxSecretService => {
            if !LinuxSecretServiceBackend::is_available() {
                return Err(VaultError::BackendUnavailable(
                    "linux secret service backend is unavailable".to_owned(),
                ));
            }
            Ok(Box::new(LinuxSecretServiceBackend::new()))
        }
        #[cfg(windows)]
        BackendKind::WindowsDpapi => {
            if !WindowsDpapiBackend::is_available() {
                return Err(VaultError::BackendUnavailable(
                    "windows DPAPI backend is unavailable".to_owned(),
                ));
            }
            Ok(Box::new(WindowsDpapiBackend::new(root)?))
        }
    }
}

/// Portable fallback backend: all blobs live in one size-capped JSON file under `objects/`.
///
/// The blobs are already envelope-encrypted, so this file adds availability, not secrecy; it is
/// still owner-only and written atomically.
#[derive(Debug, Clone)]
struct EncryptedFileBackend {
    objects_root: PathBuf,
}

impl EncryptedFileBackend {
    /// Opens the objects store, migrating any legacy one-file-per-object blobs into it.
    fn new(root: &Path) -> Result<Self, VaultError> {
        let objects_root = root.join(OBJECTS_DIR);
        ensure_owner_only_dir(&objects_root)?;
        let objects_root =
            canonicalize_existing_dir(objects_root.as_path(), "encrypted-file objects directory")?;
        let store_path = objects_root.join(OBJECTS_STORE_FILE);
        ensure_path_within_root(
            objects_root.as_path(),
            store_path.as_path(),
            "encrypted-file objects store path",
        )?;
        let legacy_store = Self::read_legacy_store(objects_root.as_path())?;
        if !store_path.exists() {
            Self::write_store_at_path(objects_root.as_path(), &legacy_store)?;
        } else if !legacy_store.is_empty() {
            // Consolidated-store entries win over legacy per-object files: the store is the
            // newer format, so a duplicate id means the legacy file is the stale copy.
            let mut merged_store = Self::read_store_at_path(objects_root.as_path())?;
            let mut changed = false;
            for (object_id, payload) in legacy_store {
                if merged_store.contains_key(&object_id) {
                    continue;
                }
                merged_store.insert(object_id, payload);
                changed = true;
            }
            if changed {
                Self::write_store_at_path(objects_root.as_path(), &merged_store)?;
            }
        }
        Ok(Self { objects_root })
    }

    fn read_store(&self) -> Result<BTreeMap<String, Vec<u8>>, VaultError> {
        Self::read_store_at_path(self.objects_root.as_path())
    }

    fn write_store(&self, store: &BTreeMap<String, Vec<u8>>) -> Result<(), VaultError> {
        Self::write_store_at_path(self.objects_root.as_path(), store)
    }

    fn read_store_at_path(store_root: &Path) -> Result<BTreeMap<String, Vec<u8>>, VaultError> {
        let store_root = canonicalize_existing_dir(store_root, "encrypted-file objects directory")?;
        let store_path = store_root.join(OBJECTS_STORE_FILE);
        ensure_path_within_root(
            store_root.as_path(),
            store_path.as_path(),
            "encrypted-file objects store path",
        )?;
        let store_size = fs::metadata(&store_path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                VaultError::NotFound
            } else {
                VaultError::Io(format!(
                    "failed to read encrypted-file objects store metadata {}: {error}",
                    store_path.display()
                ))
            }
        })?;
        if store_size.len() > MAX_OBJECTS_STORE_BYTES {
            return Err(VaultError::Io(format!(
                "encrypted-file objects store {} exceeds max size ({} > {})",
                store_path.display(),
                store_size.len(),
                MAX_OBJECTS_STORE_BYTES
            )));
        }
        let bytes = fs::read(&store_path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                VaultError::NotFound
            } else {
                VaultError::Io(format!(
                    "failed to read encrypted-file objects store {}: {error}",
                    store_path.display()
                ))
            }
        })?;
        serde_json::from_slice::<BTreeMap<String, Vec<u8>>>(bytes.as_slice()).map_err(|error| {
            VaultError::Io(format!(
                "failed to parse encrypted-file objects store {}: {error}",
                store_path.display()
            ))
        })
    }

    fn write_store_at_path(
        store_root: &Path,
        store: &BTreeMap<String, Vec<u8>>,
    ) -> Result<(), VaultError> {
        let store_root = canonicalize_existing_dir(store_root, "encrypted-file objects directory")?;
        let store_path = store_root.join(OBJECTS_STORE_FILE);
        ensure_path_within_root(
            store_root.as_path(),
            store_path.as_path(),
            "encrypted-file objects store path",
        )?;
        let payload = serde_json::to_vec(store).map_err(|error| {
            VaultError::Io(format!(
                "failed to serialize encrypted-file objects store {}: {error}",
                store_path.display()
            ))
        })?;
        if payload.len() as u64 > MAX_OBJECTS_STORE_BYTES {
            return Err(VaultError::Io(format!(
                "encrypted-file objects store {} exceeds max size after update ({} > {})",
                store_path.display(),
                payload.len(),
                MAX_OBJECTS_STORE_BYTES
            )));
        }
        let tmp_path = store_root.join(format!("{}.tmp.{}", OBJECTS_STORE_FILE, Ulid::new()));
        ensure_path_within_root(
            store_root.as_path(),
            tmp_path.as_path(),
            "encrypted-file temporary objects store path",
        )?;
        write_new_owner_only_file(&tmp_path, payload.as_slice()).map_err(|error| {
            VaultError::Io(format!(
                "failed to write encrypted-file temporary objects store {}: {error}",
                tmp_path.display()
            ))
        })?;
        fs::rename(&tmp_path, &store_path).map_err(|error| {
            VaultError::Io(format!(
                "failed to finalize encrypted-file objects store {}: {error}",
                store_path.display()
            ))
        })?;
        ensure_owner_only_file(&store_path)?;
        Ok(())
    }

    /// Collects legacy one-file-per-object blobs, enforcing the consolidated-store size budget
    /// up front so a directory of oversized files cannot balloon memory during migration.
    fn read_legacy_store(objects_root: &Path) -> Result<BTreeMap<String, Vec<u8>>, VaultError> {
        let mut legacy_store = BTreeMap::new();
        // Starts at 2 for the serialized store's surrounding `{}`.
        let mut estimated_store_bytes = 2_u64;
        for entry in fs::read_dir(objects_root).map_err(|error| {
            VaultError::Io(format!(
                "failed to enumerate encrypted-file objects directory {}: {error}",
                objects_root.display()
            ))
        })? {
            let entry = entry.map_err(|error| {
                VaultError::Io(format!(
                    "failed to inspect encrypted-file objects directory {}: {error}",
                    objects_root.display()
                ))
            })?;
            let file_type = entry.file_type().map_err(|error| {
                VaultError::Io(format!(
                    "failed to inspect encrypted-file object entry in {}: {error}",
                    objects_root.display()
                ))
            })?;
            if !file_type.is_file() || file_type.is_symlink() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name == OBJECTS_STORE_FILE || name.starts_with(&format!("{OBJECTS_STORE_FILE}.tmp."))
            {
                continue;
            }
            // Files that are not valid object ids are foreign artifacts; skip them rather than
            // failing the whole migration.
            let Ok(object_id) = normalize_storage_object_id(name) else {
                continue;
            };
            let object_path = entry.path();
            ensure_path_within_root(
                objects_root,
                object_path.as_path(),
                "encrypted-file legacy object path",
            )?;
            let metadata = fs::metadata(&object_path).map_err(|error| {
                VaultError::Io(format!(
                    "failed to inspect encrypted-file legacy object {}: {error}",
                    object_path.display()
                ))
            })?;
            estimated_store_bytes = checked_legacy_store_budget(
                estimated_store_bytes,
                object_id.as_str(),
                metadata.len(),
                object_path.as_path(),
            )?;
            let payload = read_legacy_object_limited(object_path.as_path(), metadata.len())?;
            legacy_store.insert(object_id, payload);
        }
        Ok(legacy_store)
    }
}

/// Adds one legacy object's worst-case serialized size to the running migration budget,
/// rejecting the migration before any read once it would exceed [`MAX_OBJECTS_STORE_BYTES`].
fn checked_legacy_store_budget(
    current_estimated_bytes: u64,
    object_id: &str,
    payload_len: u64,
    object_path: &Path,
) -> Result<u64, VaultError> {
    let entry_budget =
        estimate_legacy_object_json_bytes(object_id, payload_len).ok_or_else(|| {
            VaultError::Io(format!(
                "encrypted-file legacy object {} exceeds migration size accounting limits",
                object_path.display()
            ))
        })?;
    let next_estimated = current_estimated_bytes.checked_add(entry_budget).ok_or_else(|| {
        VaultError::Io(format!(
            "encrypted-file legacy object {} exceeds migration size accounting limits",
            object_path.display()
        ))
    })?;
    if next_estimated > MAX_OBJECTS_STORE_BYTES {
        return Err(VaultError::Io(format!(
            "encrypted-file legacy object {} exceeds max migration size estimate ({} > {})",
            object_path.display(),
            next_estimated,
            MAX_OBJECTS_STORE_BYTES
        )));
    }
    Ok(next_estimated)
}

/// Upper-bounds one `"<id>":[bytes...],` JSON entry: quoted key + colon, bracketed worst-case
/// number array, trailing comma. `None` signals u64 overflow (treated as over budget).
fn estimate_legacy_object_json_bytes(object_id: &str, payload_len: u64) -> Option<u64> {
    let key_budget = object_id.len() as u64 + 3;
    let value_budget = payload_len.checked_mul(JSON_U8_WORST_CASE_BYTES)?.checked_add(2)?;
    key_budget.checked_add(value_budget)?.checked_add(1)
}

/// Reads a legacy object file, failing if it grew past its inspected size mid-read (TOCTOU
/// guard: the size was budget-checked from metadata before this call).
fn read_legacy_object_limited(path: &Path, max_bytes: u64) -> Result<Vec<u8>, VaultError> {
    let file = fs::File::open(path).map_err(|error| {
        VaultError::Io(format!(
            "failed to read encrypted-file legacy object {}: {error}",
            path.display()
        ))
    })?;
    let mut reader = file.take(max_bytes.saturating_add(1));
    let mut payload = Vec::new();
    reader.read_to_end(&mut payload).map_err(|error| {
        VaultError::Io(format!(
            "failed to read encrypted-file legacy object {}: {error}",
            path.display()
        ))
    })?;
    if payload.len() as u64 > max_bytes {
        return Err(VaultError::Io(format!(
            "encrypted-file legacy object {} changed while reading and exceeded its inspected size ({} > {})",
            path.display(),
            payload.len(),
            max_bytes
        )));
    }
    Ok(payload)
}

impl BlobBackend for EncryptedFileBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::EncryptedFile
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let object_key = normalize_storage_object_id(object_id)?;
        let mut store = match self.read_store() {
            Ok(store) => store,
            Err(VaultError::NotFound) => BTreeMap::new(),
            Err(error) => return Err(error),
        };
        store.insert(object_key, payload.to_vec());
        self.write_store(&store)?;
        Ok(())
    }

    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
        let object_key = normalize_storage_object_id(object_id)?;
        let store = self.read_store()?;
        store.get(object_key.as_str()).cloned().ok_or(VaultError::NotFound)
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let object_key = normalize_storage_object_id(object_id)?;
        let mut store = match self.read_store() {
            Ok(store) => store,
            Err(VaultError::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };
        if store.remove(object_key.as_str()).is_none() {
            return Ok(());
        }
        self.write_store(&store)
    }
}

/// Stores blobs as generic passwords in the user keychain via the `security` CLI.
///
/// Payloads are base64-wrapped because keychain passwords are strings, and they are piped via
/// stdin (trailing bare `-w`) so secret bytes never appear in argv, where any local process
/// could read them from the process table.
#[cfg(target_os = "macos")]
#[derive(Debug, Default, Clone)]
struct MacosKeychainBackend;

#[cfg(target_os = "macos")]
impl MacosKeychainBackend {
    fn new() -> Self {
        Self
    }

    fn is_available() -> bool {
        Command::new("security")
            .arg("-h")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }
}

/// Builds `security add-generic-password` argv; the trailing bare `-w` makes `security` read
/// the password from stdin (pinned by `keychain_add_args_keep_password_out_of_argv`).
#[cfg(target_os = "macos")]
fn keychain_add_args<'a>(object_id: &'a str) -> [&'a str; 7] {
    ["add-generic-password", "-U", "-a", object_id, "-s", KEYCHAIN_SERVICE_NAME, "-w"]
}

#[cfg(target_os = "macos")]
impl BlobBackend for MacosKeychainBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::MacosKeychain
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let encoded = STANDARD_NO_PAD.encode(payload);
        let mut child = Command::new("security")
            .args(keychain_add_args(object_id))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute security add-generic-password: {error}"))
            })?;
        {
            let mut stdin = child.stdin.take().ok_or_else(|| {
                VaultError::Io("security add-generic-password did not expose stdin".to_owned())
            })?;
            stdin.write_all(encoded.as_bytes()).map_err(|error| {
                VaultError::Io(format!(
                    "failed to write security add-generic-password payload: {error}"
                ))
            })?;
        }
        let output = child.wait_with_output().map_err(|error| {
            VaultError::Io(format!("failed waiting for security add-generic-password: {error}"))
        })?;
        if !output.status.success() {
            return Err(VaultError::Io(format!(
                "security add-generic-password failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
        let output = Command::new("security")
            .args(["find-generic-password", "-w", "-a", object_id, "-s", KEYCHAIN_SERVICE_NAME])
            .output()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute security find-generic-password: {error}"))
            })?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
            if stderr.contains("could not be found") {
                return Err(VaultError::NotFound);
            }
            return Err(VaultError::Io(format!(
                "security find-generic-password failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let encoded = String::from_utf8(output.stdout)
            .map_err(|error| {
                VaultError::Io(format!("keychain returned non-UTF8 payload: {error}"))
            })?
            .trim()
            .to_owned();
        STANDARD_NO_PAD
            .decode(encoded.as_bytes())
            .map_err(|error| VaultError::Io(format!("failed to decode keychain payload: {error}")))
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let output = Command::new("security")
            .args(["delete-generic-password", "-a", object_id, "-s", KEYCHAIN_SERVICE_NAME])
            .output()
            .map_err(|error| {
                VaultError::Io(format!(
                    "failed to execute security delete-generic-password: {error}"
                ))
            })?;
        if output.status.success() {
            return Ok(());
        }
        let stderr = String::from_utf8_lossy(&output.stderr).to_ascii_lowercase();
        if stderr.contains("could not be found") {
            return Ok(());
        }
        Err(VaultError::Io(format!(
            "security delete-generic-password failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

#[cfg(all(test, target_os = "macos"))]
mod macos_tests {
    use super::keychain_add_args;

    #[test]
    fn keychain_add_args_keep_password_out_of_argv() {
        let args = keychain_add_args("obj_123");
        assert_eq!(args[0], "add-generic-password");
        assert_eq!(args[3], "obj_123");
        let password_flag_index =
            args.iter().position(|arg| *arg == "-w").expect("password flag should exist");
        assert_eq!(
            password_flag_index,
            args.len() - 1,
            "password must be supplied via stdin, not argv"
        );
    }
}

/// Stores blobs in the freedesktop Secret Service via `secret-tool`.
///
/// Payloads are base64-wrapped (binary-safe transport through a text CLI) and piped via stdin
/// so secret bytes never appear in argv.
#[cfg(target_os = "linux")]
#[derive(Debug, Default, Clone)]
struct LinuxSecretServiceBackend;

#[cfg(target_os = "linux")]
impl LinuxSecretServiceBackend {
    fn new() -> Self {
        Self
    }

    fn is_available() -> bool {
        Command::new("secret-tool")
            .arg("--help")
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }
}

/// Heuristically detects "item does not exist" stderr output.
///
/// `secret-tool` exits non-zero for both missing items and real failures, and its phrasing
/// varies across versions/locales, so several known phrases are matched (pinned by the tests
/// below); anything unrecognized is treated as a real error.
#[cfg(target_os = "linux")]
fn secret_tool_stderr_is_not_found(stderr: &[u8]) -> bool {
    let normalized = String::from_utf8_lossy(stderr).to_ascii_lowercase();
    normalized.contains("not found")
        || normalized.contains("no such secret")
        || normalized.contains("no such item")
        || normalized.contains("could not be found")
}

#[cfg(target_os = "linux")]
impl BlobBackend for LinuxSecretServiceBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::LinuxSecretService
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let encoded = STANDARD_NO_PAD.encode(payload);
        let mut child = Command::new("secret-tool")
            .args([
                "store",
                "--label",
                "Palyra Vault Secret",
                SECRET_TOOL_SERVICE_ATTR,
                SECRET_TOOL_SERVICE_NAME,
                SECRET_TOOL_KEY_ATTR,
                object_id,
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute secret-tool store: {error}"))
            })?;
        let stdin = child
            .stdin
            .as_mut()
            .ok_or_else(|| VaultError::Io("secret-tool store did not expose stdin".to_owned()))?;
        stdin.write_all(encoded.as_bytes()).map_err(|error| {
            VaultError::Io(format!("failed to write secret-tool store payload: {error}"))
        })?;
        let output = child.wait_with_output().map_err(|error| {
            VaultError::Io(format!("failed waiting for secret-tool store: {error}"))
        })?;
        if !output.status.success() {
            return Err(VaultError::Io(format!(
                "secret-tool store failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        Ok(())
    }

    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
        let output = Command::new("secret-tool")
            .args([
                "lookup",
                SECRET_TOOL_SERVICE_ATTR,
                SECRET_TOOL_SERVICE_NAME,
                SECRET_TOOL_KEY_ATTR,
                object_id,
            ])
            .output()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute secret-tool lookup: {error}"))
            })?;
        if !output.status.success() {
            if secret_tool_stderr_is_not_found(output.stderr.as_slice()) {
                return Err(VaultError::NotFound);
            }
            return Err(VaultError::Io(format!(
                "secret-tool lookup failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let encoded = String::from_utf8(output.stdout)
            .map_err(|error| {
                VaultError::Io(format!("secret-tool returned non-UTF8 payload: {error}"))
            })?
            .trim()
            .to_owned();
        STANDARD_NO_PAD.decode(encoded.as_bytes()).map_err(|error| {
            VaultError::Io(format!("failed to decode secret-tool payload: {error}"))
        })
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let output = Command::new("secret-tool")
            .args([
                "clear",
                SECRET_TOOL_SERVICE_ATTR,
                SECRET_TOOL_SERVICE_NAME,
                SECRET_TOOL_KEY_ATTR,
                object_id,
            ])
            .output()
            .map_err(|error| {
                VaultError::Io(format!("failed to execute secret-tool clear: {error}"))
            })?;
        if output.status.success() {
            return Ok(());
        }
        if secret_tool_stderr_is_not_found(output.stderr.as_slice()) {
            return Ok(());
        }
        Err(VaultError::Io(format!(
            "secret-tool clear failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

#[cfg(all(test, target_os = "linux"))]
mod linux_tests {
    use super::secret_tool_stderr_is_not_found;

    #[test]
    fn secret_tool_not_found_detection_matches_expected_phrases() {
        assert!(secret_tool_stderr_is_not_found(
            b"No such secret item at path /org/freedesktop/secrets"
        ));
        assert!(secret_tool_stderr_is_not_found(b"could not be found"));
        assert!(secret_tool_stderr_is_not_found(b"NOT FOUND"));
    }

    #[test]
    fn secret_tool_not_found_detection_ignores_unrelated_failures() {
        assert!(!secret_tool_stderr_is_not_found(b"Cannot autolaunch D-Bus without X11 $DISPLAY"));
    }
}

/// Stores blobs as per-user DPAPI-protected files under `objects_dpapi/`.
///
/// DPAPI binds the ciphertext to the Windows user account, so copying the files to another
/// user or machine cannot recover them.
#[cfg(windows)]
#[derive(Debug, Clone)]
struct WindowsDpapiBackend {
    objects_root: PathBuf,
}

#[cfg(windows)]
impl WindowsDpapiBackend {
    fn new(root: &Path) -> Result<Self, VaultError> {
        let objects_root = root.join(WINDOWS_DPAPI_OBJECTS_DIR);
        ensure_owner_only_dir(&objects_root)?;
        Ok(Self { objects_root })
    }

    // DPAPI ships with every supported Windows version, so availability is unconditional.
    fn is_available() -> bool {
        true
    }

    /// Maps an object id to its file path, re-validating the charset so an id can never carry
    /// path separators into the join.
    fn object_path(&self, object_id: &str) -> Result<PathBuf, VaultError> {
        if object_id.is_empty()
            || !object_id
                .chars()
                .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-'))
        {
            return Err(VaultError::InvalidObjectId(
                "object id must only contain lowercase alnum, '_' or '-'".to_owned(),
            ));
        }
        Ok(self.objects_root.join(object_id))
    }
}

#[cfg(windows)]
impl BlobBackend for WindowsDpapiBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::WindowsDpapi
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let protected = dpapi_protect(payload)?;
        let path = self.object_path(object_id)?;
        let tmp_path = path.with_extension(format!("tmp.{}", Ulid::new()));
        fs::write(&tmp_path, protected).map_err(|error| {
            VaultError::Io(format!("failed to write DPAPI object {}: {error}", tmp_path.display()))
        })?;
        ensure_owner_only_file(&tmp_path)?;
        fs::rename(&tmp_path, &path).map_err(|error| {
            VaultError::Io(format!("failed to finalize DPAPI object {}: {error}", path.display()))
        })?;
        ensure_owner_only_file(&path)?;
        Ok(())
    }

    fn get_blob(&self, object_id: &str) -> Result<Vec<u8>, VaultError> {
        let path = self.object_path(object_id)?;
        let protected = fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                VaultError::NotFound
            } else {
                VaultError::Io(format!("failed to read DPAPI object {}: {error}", path.display()))
            }
        })?;
        dpapi_unprotect(protected.as_slice())
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let path = self.object_path(object_id)?;
        if path.exists() {
            fs::remove_file(&path).map_err(|error| {
                VaultError::Io(format!("failed to delete DPAPI object {}: {error}", path.display()))
            })?;
        }
        Ok(())
    }
}

#[cfg(windows)]
fn dpapi_protect(raw: &[u8]) -> Result<Vec<u8>, VaultError> {
    dpapi_protect_current_user(raw)
        .map_err(|error| VaultError::Io(format!("failed to protect DPAPI payload: {error}")))
}

#[cfg(windows)]
fn dpapi_unprotect(protected: &[u8]) -> Result<Vec<u8>, VaultError> {
    dpapi_unprotect_current_user(protected)
        .map_err(|error| VaultError::Io(format!("failed to unprotect DPAPI payload: {error}")))
}

#[cfg(test)]
mod tests {
    use super::{
        BlobBackend, EncryptedFileBackend, JSON_U8_WORST_CASE_BYTES, MAX_OBJECTS_STORE_BYTES,
        OBJECTS_STORE_FILE,
    };
    use crate::VaultError;
    use std::{collections::BTreeMap, fs, path::Path};
    use tempfile::tempdir;

    const TEST_OBJECT_ID: &str =
        "obj_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const SECOND_TEST_OBJECT_ID: &str =
        "obj_abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd";

    #[test]
    fn encrypted_file_backend_rejects_oversized_store_reads() {
        let temp = tempdir().expect("tempdir should be created");
        let backend =
            EncryptedFileBackend::new(temp.path()).expect("backend should initialize cleanly");
        let store_path = backend.objects_root.join(OBJECTS_STORE_FILE);
        fs::write(&store_path, vec![b' '; MAX_OBJECTS_STORE_BYTES as usize + 1])
            .expect("oversized store should be written");

        let result = backend.get_blob(TEST_OBJECT_ID);
        assert!(
            matches!(result, Err(VaultError::Io(ref message)) if message.contains("exceeds max size")),
            "unexpected result: {result:?}"
        );
    }

    #[test]
    fn encrypted_file_backend_rejects_oversized_store_writes() {
        let temp = tempdir().expect("tempdir should be created");
        let backend =
            EncryptedFileBackend::new(temp.path()).expect("backend should initialize cleanly");
        let mut store = BTreeMap::new();
        store.insert(
            TEST_OBJECT_ID.to_owned(),
            vec![255_u8; (MAX_OBJECTS_STORE_BYTES / 4) as usize + 1024],
        );

        let result = backend.write_store(&store);
        assert!(
            matches!(result, Err(VaultError::Io(ref message)) if message.contains("exceeds max size after update")),
            "unexpected result: {result:?}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn encrypted_file_store_is_created_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempdir().expect("tempdir should be created");
        let backend =
            EncryptedFileBackend::new(temp.path()).expect("backend should initialize cleanly");

        backend
            .put_blob(TEST_OBJECT_ID, b"encrypted-secret")
            .expect("encrypted store write should succeed");

        let mode = fs::metadata(backend.objects_root.join(OBJECTS_STORE_FILE))
            .expect("objects store metadata should load")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[test]
    fn encrypted_file_backend_rejects_oversized_legacy_object_before_read() {
        let temp = tempdir().expect("tempdir should be created");
        let objects_root = temp.path().join("objects");
        fs::create_dir_all(&objects_root).expect("objects root should be created");
        write_sparse_legacy_object(
            objects_root.as_path(),
            TEST_OBJECT_ID,
            (MAX_OBJECTS_STORE_BYTES / JSON_U8_WORST_CASE_BYTES) + 1024,
        );

        let result = EncryptedFileBackend::new(temp.path());
        assert!(
            matches!(result, Err(VaultError::Io(ref message)) if message.contains("exceeds max migration size estimate")),
            "unexpected result: {result:?}"
        );
    }

    #[test]
    fn encrypted_file_backend_rejects_cumulative_legacy_object_budget() {
        let temp = tempdir().expect("tempdir should be created");
        let objects_root = temp.path().join("objects");
        fs::create_dir_all(&objects_root).expect("objects root should be created");
        let object_len = (MAX_OBJECTS_STORE_BYTES / (JSON_U8_WORST_CASE_BYTES * 2)) + 1024;
        write_sparse_legacy_object(objects_root.as_path(), TEST_OBJECT_ID, object_len);
        write_sparse_legacy_object(objects_root.as_path(), SECOND_TEST_OBJECT_ID, object_len);

        let result = EncryptedFileBackend::new(temp.path());
        assert!(
            matches!(result, Err(VaultError::Io(ref message)) if message.contains("exceeds max migration size estimate")),
            "unexpected result: {result:?}"
        );
    }

    #[test]
    fn encrypted_file_backend_migrates_legacy_object_files_into_store() {
        let temp = tempdir().expect("tempdir should be created");
        let objects_root = temp.path().join("objects");
        fs::create_dir_all(&objects_root).expect("objects root should be created");
        fs::write(objects_root.join(TEST_OBJECT_ID), b"legacy-secret")
            .expect("legacy object file should be written");

        let backend =
            EncryptedFileBackend::new(temp.path()).expect("backend should initialize cleanly");

        let payload = backend.get_blob(TEST_OBJECT_ID).expect("legacy object should migrate");
        assert_eq!(payload, b"legacy-secret");

        let store_path = backend.objects_root.join(OBJECTS_STORE_FILE);
        let store_bytes = fs::read(store_path).expect("objects store should be readable");
        let store = serde_json::from_slice::<BTreeMap<String, Vec<u8>>>(&store_bytes)
            .expect("migrated store should be valid json");
        assert_eq!(
            store.get(TEST_OBJECT_ID).map(Vec::as_slice),
            Some(&b"legacy-secret"[..]),
            "legacy object should be persisted into the consolidated objects store"
        );
    }

    #[test]
    fn encrypted_file_backend_merges_legacy_object_files_into_existing_store() {
        let temp = tempdir().expect("tempdir should be created");
        let objects_root = temp.path().join("objects");
        fs::create_dir_all(&objects_root).expect("objects root should be created");
        fs::write(
            objects_root.join(OBJECTS_STORE_FILE),
            serde_json::to_vec(&BTreeMap::<String, Vec<u8>>::from([(
                TEST_OBJECT_ID.to_owned(),
                b"store-secret".to_vec(),
            )]))
            .expect("seed store should serialize"),
        )
        .expect("seed store should be written");
        fs::write(objects_root.join(SECOND_TEST_OBJECT_ID), b"legacy-only-secret")
            .expect("legacy-only object file should be written");

        let backend =
            EncryptedFileBackend::new(temp.path()).expect("backend should initialize cleanly");

        let legacy_only = backend
            .get_blob(SECOND_TEST_OBJECT_ID)
            .expect("legacy-only object should be merged into the store");
        let preserved = backend
            .get_blob(TEST_OBJECT_ID)
            .expect("existing store object should remain accessible");
        assert_eq!(legacy_only, b"legacy-only-secret");
        assert_eq!(preserved, b"store-secret");
    }

    fn write_sparse_legacy_object(objects_root: &Path, object_id: &str, len: u64) {
        let file = fs::File::create(objects_root.join(object_id))
            .expect("legacy object file should be created");
        file.set_len(len).expect("legacy object file length should be set");
    }

    #[cfg(windows)]
    #[test]
    fn dpapi_roundtrip_preserves_payload_bytes() {
        let payload = b"desktop-secret-token";
        let sealed = super::dpapi_protect(payload).expect("DPAPI protect should succeed");
        let opened =
            super::dpapi_unprotect(sealed.as_slice()).expect("DPAPI unprotect should succeed");
        assert_eq!(opened, payload);
    }
}
