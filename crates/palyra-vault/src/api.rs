//! Public vault API: open a device-bound vault and put/get/list/delete scoped secrets.
//!
//! Coordinates the blob backend, envelope crypto, and the metadata index under a single
//! file-based metadata lock, with blob/metadata rollback so the two stores never diverge.

use std::path::{Path, PathBuf};

use anyhow::Context;
use palyra_common::default_identity_store_root;

use crate::{
    backend::{select_backend, BackendKind, BackendPreference, BlobBackend},
    crypto::{build_aad, derive_device_kek, validate_secret_key},
    envelope::{open, seal, EnvelopePayload},
    filesystem::{default_vault_root, ensure_owner_only_dir, normalize_vault_root_path},
    metadata::{self, MetadataEntry, MetadataFile, MetadataLockGuard},
    scope::VaultScope,
};

const DEFAULT_MAX_SECRET_BYTES: usize = 64 * 1024;

/// Failure modes surfaced by vault operations.
///
/// Display strings are user-visible and pinned by downstream fixtures; messages never contain
/// secret material.
#[derive(Debug, thiserror::Error)]
pub enum VaultError {
    /// No secret exists for the requested scope/key (or backend object).
    #[error("secret not found")]
    NotFound,
    /// A scope string failed parsing or segment validation.
    #[error("invalid scope: {0}")]
    InvalidScope(String),
    /// A secret key (or vault reference) failed validation.
    #[error("invalid secret key: {0}")]
    InvalidKey(String),
    /// A backend object id is not in canonical `obj_<hex>` form.
    #[error("invalid object id: {0}")]
    InvalidObjectId(String),
    /// A secret value exceeds the configured size limit.
    #[error("secret value exceeds max bytes ({actual} > {max})")]
    ValueTooLarge {
        /// Size of the rejected value in bytes.
        actual: usize,
        /// Configured maximum in bytes.
        max: usize,
    },
    /// The pinned or requested blob backend cannot be used on this system.
    #[error("vault backend unavailable: {0}")]
    BackendUnavailable(String),
    /// Key derivation, sealing, or envelope authentication/decryption failed.
    #[error("vault crypto failure: {0}")]
    Crypto(String),
    /// Filesystem, locking, process, or serialization I/O failed.
    #[error("vault I/O failure: {0}")]
    Io(String),
}

/// Redacted descriptor of a stored secret: identity, timestamps, and size — never the value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretMetadata {
    /// Scope the secret belongs to.
    pub scope: VaultScope,
    /// Secret key within the scope.
    pub key: String,
    /// Creation time in unix milliseconds.
    pub created_at_unix_ms: i64,
    /// Last-update time in unix milliseconds.
    pub updated_at_unix_ms: i64,
    /// Plaintext size of the stored value in bytes.
    pub value_bytes: usize,
}

/// Parsed `<scope>/<key>` reference addressing a single vault secret.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultRef {
    /// Scope the reference addresses.
    pub scope: VaultScope,
    /// Validated secret key within the scope.
    pub key: String,
}

/// Configuration for [`Vault::open_with_config`].
#[derive(Debug, Clone)]
pub struct VaultConfig {
    /// Vault root directory; `None` falls back to `PALYRA_VAULT_DIR`, then the default location
    /// next to the identity store.
    pub root: Option<PathBuf>,
    /// Identity store root used for KEK derivation; `None` uses the platform default.
    pub identity_store_root: Option<PathBuf>,
    /// Backend selection strategy (OS-native first or encrypted file only).
    pub backend_preference: BackendPreference,
    /// Maximum plaintext size accepted by [`Vault::put_secret`]; must be greater than zero.
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

/// Handle to an opened vault: a blob backend plus a metadata index, keyed by a device-bound KEK.
///
/// All mutating operations serialize on a per-vault file lock, so concurrent processes sharing
/// one vault root cannot corrupt the metadata index.
pub struct Vault {
    pub(crate) root: PathBuf,
    pub(crate) backend: Box<dyn BlobBackend>,
    pub(crate) max_secret_bytes: usize,
    // AIDEV-NOTE: the KEK is held in plain memory for the vault's lifetime and is not zeroized
    // on drop; wrapping it (e.g. zeroize/secrecy) needs a dependency decision. See the related
    // note on SensitiveBytes in crypto.rs.
    pub(crate) kek: [u8; 32],
}

impl Vault {
    /// Opens the vault using [`VaultConfig::default`].
    ///
    /// # Errors
    /// Same failure modes as [`Vault::open_with_config`].
    pub fn open_default() -> Result<Self, VaultError> {
        Self::open_with_config(VaultConfig::default())
    }

    /// Opens (creating if needed) the vault described by `config`.
    ///
    /// Resolves the root directory (explicit config, then `PALYRA_VAULT_DIR`, then the default
    /// next to the identity store), enforces owner-only permissions on it, selects or re-attaches
    /// the blob backend, and derives the device KEK from the identity store.
    ///
    /// # Errors
    /// Returns [`VaultError::Io`] for path/permission/identity-store failures,
    /// [`VaultError::BackendUnavailable`] when a previously pinned backend cannot be used,
    /// [`VaultError::Crypto`] when KEK derivation fails, and [`VaultError::InvalidKey`] when
    /// `max_secret_bytes` is zero.
    pub fn open_with_config(config: VaultConfig) -> Result<Self, VaultError> {
        let identity_store_root = if let Some(path) = config.identity_store_root {
            path
        } else {
            default_identity_store_root()
                .context("failed to resolve default identity store root")
                .map_err(|error| VaultError::Io(error.to_string()))?
        };
        let root_raw = if let Some(path) = config.root {
            path
        } else if let Ok(path) = std::env::var("PALYRA_VAULT_DIR") {
            PathBuf::from(path)
        } else {
            default_vault_root(identity_store_root.as_path())
        };
        let root = normalize_vault_root_path(root_raw)?;
        if config.max_secret_bytes == 0 {
            return Err(VaultError::InvalidKey(
                "max secret bytes must be greater than zero".to_owned(),
            ));
        }

        ensure_owner_only_dir(&root)?;
        let root = crate::canonicalize_existing_dir(root.as_path(), "vault root directory")?;
        let backend = select_backend(&root, config.backend_preference)?;
        let kek = derive_device_kek(identity_store_root.as_path())?;
        let vault = Self { root, backend, max_secret_bytes: config.max_secret_bytes, kek };
        vault.ensure_metadata_exists()?;
        Ok(vault)
    }

    /// Returns the kind of blob backend this vault is bound to.
    #[must_use]
    pub fn backend_kind(&self) -> BackendKind {
        self.backend.kind()
    }

    /// Returns the canonicalized vault root directory.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Stores or overwrites a secret value under `scope`/`key` and returns its metadata.
    ///
    /// The value is sealed with a fresh data-encryption key before it reaches the backend; the
    /// backend only ever sees ciphertext (the OS keystore backends add their own layer on top).
    ///
    /// # Errors
    /// Returns [`VaultError::InvalidKey`] for malformed keys, [`VaultError::ValueTooLarge`] when
    /// the value exceeds the configured limit, and [`VaultError::Crypto`]/[`VaultError::Io`] for
    /// sealing, backend, or metadata persistence failures (metadata failures roll the blob back).
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
        let aad = crate::build_aad(scope, key);
        let envelope = seal(value, &self.kek, aad.as_slice())?;
        let payload = serde_json::to_vec(&envelope).map_err(|error| {
            VaultError::Io(format!("failed to serialize envelope payload: {error}"))
        })?;
        let object_id = crate::object_id_for(scope, key);
        let now = crate::current_unix_ms()?;

        let _lock = self.acquire_metadata_lock()?;
        let mut index = self.read_metadata()?;
        let existing_entry_index =
            index.entries.iter().position(|entry| entry.scope == *scope && entry.key == key);
        // Captured before the overwrite so a failed metadata write below can restore the
        // previous ciphertext instead of leaving blob and index out of sync.
        // AIDEV-NOTE: if the index lists an entry whose blob is gone, this surfaces NotFound and
        // the overwrite is rejected; recovering (treating NotFound as "no previous blob") would
        // be a behavior change.
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

    /// Decrypts and returns the plaintext secret stored under `scope`/`key`.
    ///
    /// # Errors
    /// Returns [`VaultError::NotFound`] when no entry matches, [`VaultError::InvalidKey`] for
    /// malformed keys, and [`VaultError::Crypto`]/[`VaultError::Io`] when the envelope cannot be
    /// read or authenticated (including AAD scope/key mismatch and wrong device KEK).
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

    /// Deletes the secret under `scope`/`key`; returns `false` when no entry existed.
    ///
    /// Metadata is updated before the blob is removed; if the backend delete fails, the metadata
    /// entry is restored so the secret stays addressable.
    ///
    /// # Errors
    /// Returns [`VaultError::InvalidKey`] for malformed keys and [`VaultError::Io`] for metadata
    /// or backend failures.
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

    /// Lists metadata for the secrets in `scope`, sorted by key, without decrypting any values.
    ///
    /// # Errors
    /// Returns [`VaultError::Io`] when the metadata index cannot be locked, read, or parsed.
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

    /// Lists metadata for every local vault secret without reading or decrypting secret values.
    ///
    /// The returned records include scope, key, timestamps, and stored byte counts so callers can
    /// build redacted inventory views without exposing secret material. Results are sorted by
    /// scope, then key.
    ///
    /// # Errors
    /// Returns [`VaultError::Io`] when the metadata index cannot be locked, read, or parsed.
    pub fn list_all_secrets(&self) -> Result<Vec<SecretMetadata>, VaultError> {
        let _lock = self.acquire_metadata_lock()?;
        let index = self.read_metadata()?;
        let mut results =
            index.entries.iter().cloned().map(SecretMetadata::from).collect::<Vec<_>>();
        results.sort_by(|left, right| {
            left.scope.to_string().cmp(&right.scope.to_string()).then(left.key.cmp(&right.key))
        });
        Ok(results)
    }

    pub(crate) fn ensure_metadata_exists(&self) -> Result<(), VaultError> {
        metadata::ensure_metadata_exists(self.root.as_path())
    }

    pub(crate) fn acquire_metadata_lock(&self) -> Result<MetadataLockGuard, VaultError> {
        metadata::acquire_metadata_lock(self.root.as_path())
    }

    pub(crate) fn read_metadata(&self) -> Result<MetadataFile, VaultError> {
        metadata::read_metadata(self.root.as_path())
    }

    pub(crate) fn write_metadata(&self, metadata: &MetadataFile) -> Result<(), VaultError> {
        metadata::write_metadata(self.root.as_path(), metadata)
    }
}

impl VaultRef {
    /// Parses a `<scope>/<key>` reference (for example `global/openai_api_key`).
    ///
    /// Only the first `/` separates scope from key; the key itself may not contain `/`.
    ///
    /// # Errors
    /// Returns [`VaultError::InvalidKey`] when the separator is missing or the key is malformed,
    /// and [`VaultError::InvalidScope`] when the scope segment does not parse.
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
