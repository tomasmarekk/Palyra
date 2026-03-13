#[cfg(windows)]
use palyra_common::windows_security;
#[cfg(windows)]
use std::collections::HashSet;
#[cfg(windows)]
use std::sync::OnceLock;
use std::{
    any::Any,
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use getrandom::fill as fill_random_bytes;
use ring::aead::{Aad, LessSafeKey, Nonce, UnboundKey, CHACHA20_POLY1305};

use crate::error::{IdentityError, IdentityResult};

const SECRET_STORE_ENCRYPTION_MAGIC: &[u8; 4] = b"IDS1";
const SECRET_STORE_ENCRYPTION_KEY_BYTES: usize = 32;
const SECRET_STORE_ENCRYPTION_NONCE_BYTES: usize = 12;
const SECRET_STORE_KEY_FILE: &str = ".store-key.v1";

pub trait SecretStore: Send + Sync {
    fn write_secret(&self, key: &str, value: &[u8]) -> IdentityResult<()>;
    fn write_sealed_value(&self, key: &str, value: &[u8]) -> IdentityResult<()>;
    fn read_secret(&self, key: &str) -> IdentityResult<Vec<u8>>;
    fn delete_secret(&self, key: &str) -> IdentityResult<()>;
    fn as_any(&self) -> &dyn Any;
}

#[derive(Default)]
pub struct InMemorySecretStore {
    state: Mutex<HashMap<String, Vec<u8>>>,
}

impl InMemorySecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl SecretStore for InMemorySecretStore {
    fn write_secret(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        self.insert_value(key, value)
    }

    fn write_sealed_value(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        self.insert_value(key, value)
    }

    fn read_secret(&self, key: &str) -> IdentityResult<Vec<u8>> {
        let state = self
            .state
            .lock()
            .map_err(|_| IdentityError::Internal("store lock poisoned".to_owned()))?;
        state.get(key).cloned().ok_or(IdentityError::SecretNotFound)
    }

    fn delete_secret(&self, key: &str) -> IdentityResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| IdentityError::Internal("store lock poisoned".to_owned()))?;
        state.remove(key);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl InMemorySecretStore {
    fn insert_value(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| IdentityError::Internal("store lock poisoned".to_owned()))?;
        state.insert(key.to_owned(), value.to_vec());
        Ok(())
    }
}

pub struct FilesystemSecretStore {
    root: PathBuf,
    encryption_key: [u8; SECRET_STORE_ENCRYPTION_KEY_BYTES],
    #[cfg(windows)]
    owner_sid: String,
}

impl FilesystemSecretStore {
    pub fn new(root: impl Into<PathBuf>) -> IdentityResult<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|error| IdentityError::Internal(error.to_string()))?;
        #[cfg(windows)]
        {
            let owner_sid = current_user_sid()?;
            harden_windows_path_permissions(&root, owner_sid.as_str(), true)?;
            let encryption_key =
                load_or_create_store_encryption_key(root.as_path(), Some(owner_sid.as_str()))?;
            Ok(Self { root, encryption_key, owner_sid })
        }
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            let encryption_key = load_or_create_store_encryption_key(root.as_path(), None)?;
            Ok(Self { root, encryption_key })
        }
    }

    fn key_path(&self, key: &str) -> IdentityResult<PathBuf> {
        if key.is_empty()
            || key.contains("..")
            || key.contains('\\')
            || key.contains(':')
            || key.starts_with('/')
        {
            return Err(IdentityError::InvalidSecretStoreKey);
        }
        Ok(self.root.join(hex::encode(key.as_bytes())))
    }

    #[must_use]
    pub fn root_path(&self) -> &Path {
        &self.root
    }
}

impl SecretStore for FilesystemSecretStore {
    fn write_secret(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        self.write_value(key, value)
    }

    fn write_sealed_value(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        self.write_value(key, value)
    }

    fn read_secret(&self, key: &str) -> IdentityResult<Vec<u8>> {
        let path = self.key_path(key)?;
        let bytes = fs::read(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                IdentityError::SecretNotFound
            } else {
                IdentityError::Internal(error.to_string())
            }
        })?;
        decrypt_secret_payload(&self.encryption_key, bytes.as_slice())
    }

    fn delete_secret(&self, key: &str) -> IdentityResult<()> {
        let path = self.key_path(key)?;
        if path.exists() {
            fs::remove_file(path).map_err(|error| IdentityError::Internal(error.to_string()))?;
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FilesystemSecretStore {
    fn write_value(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        let encrypted = encrypt_secret_payload(&self.encryption_key, value)?;
        #[cfg(windows)]
        {
            use std::io::Write;

            let path = self.key_path(key)?;
            let tmp_path = loop {
                let candidate = path.with_extension(format!("tmp.{}", ulid::Ulid::new()));
                if !candidate.exists() {
                    break candidate;
                }
            };

            let write_result: IdentityResult<()> = (|| {
                let mut file = fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .open(&tmp_path)
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
                harden_windows_path_permissions(&tmp_path, self.owner_sid.as_str(), false)?;
                file.write_all(encrypted.as_slice())
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
                file.sync_all().map_err(|error| IdentityError::Internal(error.to_string()))?;
                fs::rename(&tmp_path, &path)
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
                harden_windows_path_permissions(&path, self.owner_sid.as_str(), false)?;
                Ok(())
            })();

            if write_result.is_err() && tmp_path.exists() {
                let _ = fs::remove_file(&tmp_path);
            }
            write_result
        }
        #[cfg(not(windows))]
        {
            use std::{
                io::Write,
                os::unix::fs::{OpenOptionsExt, PermissionsExt},
            };

            let path = self.key_path(key)?;
            let tmp_path = loop {
                let candidate = path.with_extension(format!("tmp.{}", ulid::Ulid::new()));
                if !candidate.exists() {
                    break candidate;
                }
            };

            let write_result: IdentityResult<()> = (|| {
                let mut file = fs::OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o600)
                    .open(&tmp_path)
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
                file.set_permissions(fs::Permissions::from_mode(0o600))
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
                file.write_all(encrypted.as_slice())
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
                file.sync_all().map_err(|error| IdentityError::Internal(error.to_string()))?;
                fs::rename(&tmp_path, &path)
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
                if let Some(parent) = path.parent() {
                    fs::File::open(parent)
                        .map_err(|error| IdentityError::Internal(error.to_string()))?
                        .sync_all()
                        .map_err(|error| IdentityError::Internal(error.to_string()))?;
                }
                Ok(())
            })();

            if write_result.is_err() && tmp_path.exists() {
                let _ = fs::remove_file(&tmp_path);
            }
            write_result
        }
    }
}

fn encrypt_secret_payload(
    encryption_key: &[u8; SECRET_STORE_ENCRYPTION_KEY_BYTES],
    plaintext: &[u8],
) -> IdentityResult<Vec<u8>> {
    let mut nonce_bytes = [0_u8; SECRET_STORE_ENCRYPTION_NONCE_BYTES];
    fill_random_bytes(&mut nonce_bytes).map_err(|error| {
        IdentityError::Cryptographic(format!("failed to generate identity store nonce: {error}"))
    })?;
    let cipher = build_store_cipher(encryption_key)?;
    let nonce = Nonce::assume_unique_for_key(nonce_bytes);
    let mut ciphertext = plaintext.to_vec();
    cipher.seal_in_place_append_tag(nonce, Aad::empty(), &mut ciphertext).map_err(|_| {
        IdentityError::Cryptographic("failed to encrypt identity store payload at rest".to_owned())
    })?;
    let mut encoded = Vec::with_capacity(
        SECRET_STORE_ENCRYPTION_MAGIC.len()
            + SECRET_STORE_ENCRYPTION_NONCE_BYTES
            + ciphertext.len(),
    );
    encoded.extend_from_slice(SECRET_STORE_ENCRYPTION_MAGIC);
    encoded.extend_from_slice(&nonce_bytes);
    encoded.extend_from_slice(ciphertext.as_slice());
    Ok(encoded)
}

fn decrypt_secret_payload(
    encryption_key: &[u8; SECRET_STORE_ENCRYPTION_KEY_BYTES],
    encoded: &[u8],
) -> IdentityResult<Vec<u8>> {
    if !encoded.starts_with(SECRET_STORE_ENCRYPTION_MAGIC) {
        return Ok(encoded.to_vec());
    }
    if encoded.len() < SECRET_STORE_ENCRYPTION_MAGIC.len() + SECRET_STORE_ENCRYPTION_NONCE_BYTES {
        return Err(IdentityError::Cryptographic(
            "identity store payload header is truncated".to_owned(),
        ));
    }

    let mut nonce_bytes = [0_u8; SECRET_STORE_ENCRYPTION_NONCE_BYTES];
    nonce_bytes.copy_from_slice(
        &encoded[SECRET_STORE_ENCRYPTION_MAGIC.len()
            ..SECRET_STORE_ENCRYPTION_MAGIC.len() + SECRET_STORE_ENCRYPTION_NONCE_BYTES],
    );
    let mut ciphertext = encoded
        [SECRET_STORE_ENCRYPTION_MAGIC.len() + SECRET_STORE_ENCRYPTION_NONCE_BYTES..]
        .to_vec();
    let cipher = build_store_cipher(encryption_key)?;
    let plaintext = cipher
        .open_in_place(
            Nonce::assume_unique_for_key(nonce_bytes),
            Aad::empty(),
            ciphertext.as_mut_slice(),
        )
        .map_err(|_| {
            IdentityError::Cryptographic(
                "failed to decrypt identity store payload at rest".to_owned(),
            )
        })?;
    Ok(plaintext.to_vec())
}

fn build_store_cipher(
    encryption_key: &[u8; SECRET_STORE_ENCRYPTION_KEY_BYTES],
) -> IdentityResult<LessSafeKey> {
    let unbound = UnboundKey::new(&CHACHA20_POLY1305, encryption_key).map_err(|_| {
        IdentityError::Cryptographic("failed to initialize identity store cipher".to_owned())
    })?;
    Ok(LessSafeKey::new(unbound))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StoreEncryptionKeyWriteOutcome {
    Created,
    AlreadyExists,
}

fn load_or_create_store_encryption_key(
    root: &Path,
    #[cfg(windows)] owner_sid: Option<&str>,
    #[cfg(not(windows))] _owner_sid: Option<&str>,
) -> IdentityResult<[u8; SECRET_STORE_ENCRYPTION_KEY_BYTES]> {
    let key_path = root.join(SECRET_STORE_KEY_FILE);
    if key_path.exists() {
        return read_store_encryption_key(key_path.as_path());
    }

    let mut raw_key = [0_u8; SECRET_STORE_ENCRYPTION_KEY_BYTES];
    fill_random_bytes(&mut raw_key).map_err(|error| {
        IdentityError::Cryptographic(format!(
            "failed to generate identity store encryption key: {error}"
        ))
    })?;
    match write_store_encryption_key_if_absent(
        key_path.as_path(),
        &raw_key,
        #[cfg(windows)]
        owner_sid,
    )? {
        StoreEncryptionKeyWriteOutcome::Created => Ok(raw_key),
        StoreEncryptionKeyWriteOutcome::AlreadyExists => {
            read_store_encryption_key(key_path.as_path())
        }
    }
}

fn read_store_encryption_key(
    path: &Path,
) -> IdentityResult<[u8; SECRET_STORE_ENCRYPTION_KEY_BYTES]> {
    let stored = fs::read(path).map_err(|error| IdentityError::Internal(error.to_string()))?;
    #[cfg(windows)]
    let unwrapped =
        windows_security::dpapi_unprotect_current_user(stored.as_slice()).map_err(|error| {
            IdentityError::Cryptographic(format!(
                "failed to unprotect identity store encryption key: {error}"
            ))
        })?;
    #[cfg(not(windows))]
    let unwrapped = stored;

    let key_bytes: [u8; SECRET_STORE_ENCRYPTION_KEY_BYTES] =
        unwrapped.try_into().map_err(|_| {
            IdentityError::Cryptographic(
                "identity store encryption key has invalid length".to_owned(),
            )
        })?;
    Ok(key_bytes)
}

fn write_store_encryption_key_if_absent(
    path: &Path,
    raw_key: &[u8; SECRET_STORE_ENCRYPTION_KEY_BYTES],
    #[cfg(windows)] owner_sid: Option<&str>,
) -> IdentityResult<StoreEncryptionKeyWriteOutcome> {
    #[cfg(windows)]
    let encoded = windows_security::dpapi_protect_current_user(raw_key).map_err(|error| {
        IdentityError::Cryptographic(format!(
            "failed to protect identity store encryption key: {error}"
        ))
    })?;
    #[cfg(not(windows))]
    let encoded = raw_key.to_vec();

    let write_result: IdentityResult<StoreEncryptionKeyWriteOutcome> = (|| {
        #[cfg(windows)]
        {
            use std::io::Write;

            let mut file = fs::OpenOptions::new().create_new(true).write(true).open(path).map_err(
                |error| {
                    if error.kind() == std::io::ErrorKind::AlreadyExists {
                        IdentityError::SecretNotFound
                    } else {
                        IdentityError::Internal(error.to_string())
                    }
                },
            )?;
            harden_windows_path_permissions(
                path,
                owner_sid.ok_or_else(|| {
                    IdentityError::Internal(
                        "identity store encryption key owner SID is missing".to_owned(),
                    )
                })?,
                false,
            )?;
            file.write_all(encoded.as_slice())
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            file.sync_all().map_err(|error| IdentityError::Internal(error.to_string()))?;
            harden_windows_path_permissions(
                path,
                owner_sid.ok_or_else(|| {
                    IdentityError::Internal(
                        "identity store encryption key owner SID is missing".to_owned(),
                    )
                })?,
                false,
            )?;
            return Ok(StoreEncryptionKeyWriteOutcome::Created);
        }
        #[cfg(not(windows))]
        {
            use std::{
                io::Write,
                os::unix::fs::{OpenOptionsExt, PermissionsExt},
            };

            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(0o600)
                .open(path)
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::AlreadyExists {
                        IdentityError::SecretNotFound
                    } else {
                        IdentityError::Internal(error.to_string())
                    }
                })?;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            file.write_all(encoded.as_slice())
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            file.sync_all().map_err(|error| IdentityError::Internal(error.to_string()))?;
            if let Some(parent) = path.parent() {
                fs::File::open(parent)
                    .map_err(|error| IdentityError::Internal(error.to_string()))?
                    .sync_all()
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
            }
            return Ok(StoreEncryptionKeyWriteOutcome::Created);
        }
        #[allow(unreachable_code)]
        Ok(StoreEncryptionKeyWriteOutcome::Created)
    })();

    match write_result {
        Ok(outcome) => Ok(outcome),
        Err(IdentityError::SecretNotFound) => Ok(StoreEncryptionKeyWriteOutcome::AlreadyExists),
        Err(error) => {
            if path.exists() {
                let _ = fs::remove_file(path);
            }
            Err(error)
        }
    }
}

#[cfg(windows)]
static WINDOWS_CURRENT_USER_SID: OnceLock<Mutex<Option<String>>> = OnceLock::new();
#[cfg(windows)]
static HARDENED_WINDOWS_PATHS: OnceLock<Mutex<HashSet<(PathBuf, bool)>>> = OnceLock::new();

#[cfg(windows)]
fn current_user_sid() -> IdentityResult<String> {
    let cache = WINDOWS_CURRENT_USER_SID.get_or_init(|| Mutex::new(None));
    {
        let cached = cache
            .lock()
            .map_err(|_| IdentityError::Internal("current user SID cache poisoned".to_owned()))?;
        if let Some(sid) = cached.as_ref() {
            return Ok(sid.clone());
        }
    }
    let sid = current_user_sid_uncached()?;
    cache
        .lock()
        .map_err(|_| IdentityError::Internal("current user SID cache poisoned".to_owned()))?
        .replace(sid.clone());
    Ok(sid)
}

#[cfg(windows)]
fn current_user_sid_uncached() -> IdentityResult<String> {
    windows_security::current_user_sid().map_err(|error| {
        IdentityError::Internal(format!("failed to resolve current user SID: {error}"))
    })
}

#[cfg(all(test, windows))]
#[allow(dead_code)]
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
) -> IdentityResult<()> {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let cache = HARDENED_WINDOWS_PATHS.get_or_init(|| Mutex::new(HashSet::new()));
    {
        let cache = cache.lock().map_err(|_| {
            IdentityError::Internal("identity path hardening cache poisoned".to_owned())
        })?;
        if cache.contains(&(canonical.clone(), is_directory)) {
            return Ok(());
        }
    }
    windows_security::harden_windows_path_permissions(path, owner_sid, is_directory).map_err(
        |error| {
            IdentityError::Internal(format!(
                "failed to harden Windows permissions for {}: {error}",
                path.display()
            ))
        },
    )?;
    cache
        .lock()
        .map_err(|_| IdentityError::Internal("identity path hardening cache poisoned".to_owned()))?
        .insert((canonical, is_directory));
    Ok(())
}

pub fn default_identity_storage_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join(".palyra").join("identity")
}

#[cfg(test)]
mod tests {
    #[cfg(windows)]
    use super::{FilesystemSecretStore, SecretStore};
    #[cfg(unix)]
    use super::{FilesystemSecretStore, SecretStore};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    #[cfg(unix)]
    use std::sync::Arc;
    #[cfg(unix)]
    use std::thread;
    #[cfg(windows)]
    use tempfile::tempdir;
    #[cfg(unix)]
    use tempfile::tempdir;

    #[cfg(unix)]
    #[test]
    fn filesystem_secret_store_sets_owner_only_permissions() {
        let temp = tempdir().expect("temp dir should initialize");
        let store = FilesystemSecretStore::new(temp.path()).expect("store should initialize");
        store.write_secret("device/test.json", br#"{"ok":true}"#).expect("secret should persist");
        let file_path =
            store.key_path("device/test.json").expect("test key should map to a filesystem path");
        let file_mode = std::fs::metadata(file_path)
            .expect("file metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        let dir_mode = std::fs::metadata(temp.path())
            .expect("directory metadata should be readable")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600);
        assert_eq!(dir_mode, 0o700);
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_secret_store_prevents_key_path_collisions() {
        let temp = tempdir().expect("temp dir should initialize");
        let store = FilesystemSecretStore::new(temp.path()).expect("store should initialize");
        let first_key = "device/a/b";
        let second_key = "device__a__b";

        store.write_secret(first_key, b"first").expect("first secret should persist");
        store.write_secret(second_key, b"second").expect("second secret should persist");

        let first_value = store.read_secret(first_key).expect("first secret should be readable");
        let second_value = store.read_secret(second_key).expect("second secret should be readable");
        assert_eq!(first_value, b"first");
        assert_eq!(second_value, b"second");
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_secret_store_deletes_only_requested_key() {
        let temp = tempdir().expect("temp dir should initialize");
        let store = FilesystemSecretStore::new(temp.path()).expect("store should initialize");
        let first_key = "device/a/b";
        let second_key = "device__a__b";

        store.write_secret(first_key, b"first").expect("first secret should persist");
        store.write_secret(second_key, b"second").expect("second secret should persist");
        store.delete_secret(first_key).expect("first secret should delete");

        let second_value =
            store.read_secret(second_key).expect("second secret should remain available");
        assert_eq!(second_value, b"second");
        assert!(matches!(
            store.read_secret(first_key),
            Err(crate::error::IdentityError::SecretNotFound)
        ));
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_secret_store_overwrites_atomically_and_keeps_json_readable() {
        let temp = tempdir().expect("temp dir should initialize");
        let store = FilesystemSecretStore::new(temp.path()).expect("store should initialize");
        let key = "identity/pairing/paired_devices.json";
        let key_path = store.key_path(key).expect("key should map to path");
        let key_file_name = key_path
            .file_name()
            .expect("key path should include file name")
            .to_string_lossy()
            .into_owned();

        for sequence in 1..=5 {
            let payload = format!(r#"{{"sequence":{sequence}}}"#);
            store.write_secret(key, payload.as_bytes()).expect("secret should persist");
            let readback = store.read_secret(key).expect("secret should be readable");
            let parsed: serde_json::Value =
                serde_json::from_slice(&readback).expect("persisted secret should be valid JSON");
            assert_eq!(parsed["sequence"].as_u64(), Some(sequence));
            let stale_tmp_exists = std::fs::read_dir(temp.path())
                .expect("temporary directory should be readable")
                .filter_map(Result::ok)
                .filter_map(|entry| entry.file_name().into_string().ok())
                .any(|file_name| file_name.starts_with(format!("{key_file_name}.tmp.").as_str()));
            assert!(!stale_tmp_exists, "temporary swap files should be cleaned up");
        }
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_secret_store_encrypts_payloads_at_rest() {
        let temp = tempdir().expect("temp dir should initialize");
        let store = FilesystemSecretStore::new(temp.path()).expect("store should initialize");
        let key = "identity/pairing/paired_devices.json";
        let payload = br#"{"sequence":7}"#;

        store.write_secret(key, payload).expect("secret should persist");

        let raw_path = store.key_path(key).expect("key should map to path");
        let raw_bytes = std::fs::read(raw_path).expect("raw store bytes should be readable");
        assert_ne!(raw_bytes, payload, "secret payload should not persist in plaintext");
        assert!(
            raw_bytes.starts_with(super::SECRET_STORE_ENCRYPTION_MAGIC),
            "encrypted payload should include the expected store header"
        );
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_secret_store_reads_legacy_plaintext_payloads() {
        let temp = tempdir().expect("temp dir should initialize");
        let store = FilesystemSecretStore::new(temp.path()).expect("store should initialize");
        let key = "identity/pairing/paired_devices.json";
        let payload = br#"{"sequence":9}"#;
        let raw_path = store.key_path(key).expect("key should map to path");
        std::fs::write(&raw_path, payload).expect("legacy plaintext payload should be writable");

        let loaded = store.read_secret(key).expect("legacy plaintext payload should still load");
        assert_eq!(loaded, payload);
    }

    #[test]
    fn store_encryption_key_creation_is_first_writer_wins() {
        let temp = tempdir().expect("temp dir should initialize");
        let key_path = temp.path().join(super::SECRET_STORE_KEY_FILE);
        let first_key = [0x11_u8; super::SECRET_STORE_ENCRYPTION_KEY_BYTES];
        let second_key = [0x22_u8; super::SECRET_STORE_ENCRYPTION_KEY_BYTES];
        #[cfg(windows)]
        let owner_sid = super::current_user_sid().expect("current user SID should resolve");

        let first_write = super::write_store_encryption_key_if_absent(
            key_path.as_path(),
            &first_key,
            #[cfg(windows)]
            Some(owner_sid.as_str()),
        )
        .expect("first key write should succeed");
        assert_eq!(first_write, super::StoreEncryptionKeyWriteOutcome::Created);

        let second_write = super::write_store_encryption_key_if_absent(
            key_path.as_path(),
            &second_key,
            #[cfg(windows)]
            Some(owner_sid.as_str()),
        )
        .expect("second key write should observe existing file");
        assert_eq!(second_write, super::StoreEncryptionKeyWriteOutcome::AlreadyExists);

        let persisted =
            super::read_store_encryption_key(key_path.as_path()).expect("key should round-trip");
        assert_eq!(persisted, first_key);
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_secret_store_handles_concurrent_writes_without_tmp_collisions() {
        let temp = tempdir().expect("temp dir should initialize");
        let store =
            Arc::new(FilesystemSecretStore::new(temp.path()).expect("store should initialize"));
        let key = "identity/pairing/paired_devices.json";

        let mut handles = Vec::new();
        for worker in 0..8 {
            let store = Arc::clone(&store);
            let key = key.to_owned();
            handles.push(thread::spawn(move || {
                for sequence in 0..50 {
                    let payload = format!(r#"{{"worker":{worker},"sequence":{sequence}}}"#);
                    store
                        .write_secret(&key, payload.as_bytes())
                        .expect("concurrent secret write should succeed");
                }
            }));
        }
        for handle in handles {
            handle.join().expect("worker should join successfully");
        }

        let key_path = store.key_path(key).expect("key should map to path");
        let key_file_name = key_path
            .file_name()
            .expect("key path should include file name")
            .to_string_lossy()
            .into_owned();
        let stale_tmp_files: Vec<String> = std::fs::read_dir(temp.path())
            .expect("temporary directory should be readable")
            .filter_map(Result::ok)
            .filter_map(|entry| entry.file_name().into_string().ok())
            .filter(|file_name| file_name.starts_with(format!("{key_file_name}.tmp.").as_str()))
            .collect();
        assert!(
            stale_tmp_files.is_empty(),
            "temporary files should not remain after concurrent writes: {stale_tmp_files:?}"
        );
    }

    #[cfg(windows)]
    #[test]
    fn filesystem_secret_store_roundtrips_on_windows() {
        let temp = tempdir().expect("temp dir should initialize");
        let store = FilesystemSecretStore::new(temp.path()).expect("store should initialize");
        let key = "identity/pairing/paired_devices.json";
        let payload = br#"{"device":"ok"}"#;
        store.write_secret(key, payload).expect("secret should persist");
        let loaded = store.read_secret(key).expect("secret should be readable");
        assert_eq!(loaded, payload);
        store.delete_secret(key).expect("secret should delete");
        assert!(matches!(store.read_secret(key), Err(crate::error::IdentityError::SecretNotFound)));
    }

    #[cfg(windows)]
    #[test]
    fn filesystem_secret_store_encrypts_payloads_at_rest_on_windows() {
        let temp = tempdir().expect("temp dir should initialize");
        let store = FilesystemSecretStore::new(temp.path()).expect("store should initialize");
        let key = "identity/pairing/paired_devices.json";
        let payload = br#"{"device":"ok"}"#;

        store.write_secret(key, payload).expect("secret should persist");

        let raw_path = store.key_path(key).expect("key should map to path");
        let raw_bytes = std::fs::read(raw_path).expect("raw store bytes should be readable");
        assert_ne!(raw_bytes, payload, "secret payload should not persist in plaintext");
        assert!(
            raw_bytes.starts_with(super::SECRET_STORE_ENCRYPTION_MAGIC),
            "encrypted payload should include the expected store header"
        );
    }
}
