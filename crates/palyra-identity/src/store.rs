#[cfg(windows)]
use palyra_common::windows_security;
#[cfg(windows)]
use std::collections::HashSet;
use std::{
    any::Any,
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use crate::error::{IdentityError, IdentityResult};

pub trait SecretStore: Send + Sync {
    fn write_secret(&self, key: &str, value: &[u8]) -> IdentityResult<()>;
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| IdentityError::Internal("store lock poisoned".to_owned()))?;
        state.insert(key.to_owned(), value.to_vec());
        Ok(())
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

pub struct FilesystemSecretStore {
    root: PathBuf,
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
            Ok(Self { root, owner_sid })
        }
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            Ok(Self { root })
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
                file.write_all(value)
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
                file.write_all(value)
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

    fn read_secret(&self, key: &str) -> IdentityResult<Vec<u8>> {
        let path = self.key_path(key)?;
        fs::read(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                IdentityError::SecretNotFound
            } else {
                IdentityError::Internal(error.to_string())
            }
        })
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

#[cfg(all(test, windows))]
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
}
