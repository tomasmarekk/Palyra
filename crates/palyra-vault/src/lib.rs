mod backend;
mod envelope;
mod scope;

use std::{
    fs,
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

const KEY_DERIVATION_SALT: &[u8] = b"palyra.vault.kek.v1";
const KEY_DERIVATION_INFO: &[u8] = b"envelope:kek";
const AAD_PREFIX: &str = "palyra.vault.v1";
const IDENTITY_STATE_BUNDLE_KEY: &str = "identity/state.v1.json";
const LEGACY_CA_STATE_KEY: &str = "identity/ca/state.json";

const MAX_SECRET_KEY_BYTES: usize = 128;
pub const MAX_SCOPE_SEGMENT_BYTES: usize = 256;
const DEFAULT_MAX_SECRET_BYTES: usize = 64 * 1024;

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
        self.backend.put_blob(object_id.as_str(), payload.as_slice())?;

        let mut found = None;
        for entry in &mut index.entries {
            if entry.scope == *scope && entry.key == key {
                entry.updated_at_unix_ms = now;
                entry.value_bytes = value.len();
                entry.object_id = object_id.clone();
                found = Some(entry.clone());
                break;
            }
        }
        let entry = if let Some(existing) = found {
            existing
        } else {
            let created = MetadataEntry {
                scope: scope.clone(),
                key: key.to_owned(),
                object_id,
                created_at_unix_ms: now,
                updated_at_unix_ms: now,
                value_bytes: value.len(),
            };
            index.entries.push(created.clone());
            created
        };
        self.write_metadata(&index)?;
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
            self.backend.delete_blob(object_id.as_str())?;
            self.write_metadata(&index)?;
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

fn maybe_reclaim_stale_lock(lock_path: &Path) -> Result<bool, VaultError> {
    let metadata = fs::metadata(lock_path).map_err(|error| {
        VaultError::Io(format!("failed to inspect metadata lock {}: {error}", lock_path.display()))
    })?;
    let modified = metadata.modified().map_err(|error| {
        VaultError::Io(format!(
            "failed to inspect metadata lock timestamp {}: {error}",
            lock_path.display()
        ))
    })?;
    if SystemTime::now().duration_since(modified).unwrap_or(Duration::ZERO)
        < METADATA_LOCK_STALE_AGE
    {
        return Ok(false);
    }
    fs::remove_file(lock_path).map_err(|error| {
        VaultError::Io(format!(
            "failed to reclaim stale metadata lock {}: {error}",
            lock_path.display()
        ))
    })?;
    Ok(true)
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
    #[cfg(not(unix))]
    let _ = path;
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

#[cfg(test)]
mod tests {
    use super::{
        derive_kek_from_seed_material, extract_kek_seed_material, BackendPreference, Vault,
        VaultConfig, VaultRef, VaultScope,
    };
    use anyhow::Result;
    use tempfile::tempdir;

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
