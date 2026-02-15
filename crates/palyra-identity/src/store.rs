use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use crate::error::{IdentityError, IdentityResult};

pub trait SecretStore: Send + Sync {
    fn write_secret(&self, key: &str, value: &[u8]) -> IdentityResult<()>;
    fn read_secret(&self, key: &str) -> IdentityResult<Vec<u8>>;
    fn delete_secret(&self, key: &str) -> IdentityResult<()>;
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
}

pub struct FilesystemSecretStore {
    root: PathBuf,
}

impl FilesystemSecretStore {
    pub fn new(root: impl Into<PathBuf>) -> IdentityResult<Self> {
        let root = root.into();
        #[cfg(windows)]
        {
            let _ = &root;
            Err(IdentityError::Internal(
                "FilesystemSecretStore on Windows is disabled until ACL hardening is implemented"
                    .to_owned(),
            ))
        }
        #[cfg(not(windows))]
        {
            use std::os::unix::fs::PermissionsExt;

            fs::create_dir_all(&root)
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
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
}

impl SecretStore for FilesystemSecretStore {
    fn write_secret(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
        #[cfg(windows)]
        {
            let _ = key;
            let _ = value;
            Err(IdentityError::Internal(
                "FilesystemSecretStore on Windows is disabled until ACL hardening is implemented"
                    .to_owned(),
            ))
        }
        #[cfg(not(windows))]
        {
            use std::{
                io::Write,
                os::unix::fs::{OpenOptionsExt, PermissionsExt},
            };

            let path = self.key_path(key)?;
            let tmp_path = path.with_extension("tmp");
            if tmp_path.exists() {
                fs::remove_file(&tmp_path)
                    .map_err(|error| IdentityError::Internal(error.to_string()))?;
            }

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
}

pub fn default_identity_storage_path(root: impl AsRef<Path>) -> PathBuf {
    root.as_ref().join(".palyra").join("identity")
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::{FilesystemSecretStore, SecretStore};
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
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
        let tmp_path = key_path.with_extension("tmp");

        for sequence in 1..=5 {
            let payload = format!(r#"{{"sequence":{sequence}}}"#);
            store.write_secret(key, payload.as_bytes()).expect("secret should persist");
            let readback = store.read_secret(key).expect("secret should be readable");
            let parsed: serde_json::Value =
                serde_json::from_slice(&readback).expect("persisted secret should be valid JSON");
            assert_eq!(parsed["sequence"].as_u64(), Some(sequence));
            assert!(!tmp_path.exists(), "temporary swap file should be cleaned up");
        }
    }
}
