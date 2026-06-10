//! Crate-level vault tests: put/get/list/delete roundtrips, blob/metadata rollback on injected
//! backend failures, metadata-lock staleness reclaim, owner-only permission enforcement, and
//! KEK derivation stability across identity-state rewrites.

use crate::{
    backend::{BackendKind, BackendPreference, BlobBackend},
    crypto::{derive_device_kek, derive_kek_from_seed_material, extract_kek_seed_material},
    envelope::seal,
    filesystem::ensure_owner_only_dir,
    metadata::{
        initialize_metadata_lock_marker_with_writer, maybe_reclaim_stale_lock_with_policy,
        MetadataEntry, MetadataFile, METADATA_FILE, METADATA_LOCK_FILE, METADATA_VERSION,
    },
    Vault, VaultConfig, VaultError, VaultRef, VaultScope,
};
use anyhow::Result;
use palyra_identity::{CertificateAuthority, FilesystemSecretStore, SecretStore};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};
use tempfile::tempdir;

/// In-memory backend whose `delete_blob` always fails, to exercise delete rollback.
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

/// In-memory backend whose `put_blob` sabotages the vault's next metadata write by replacing
/// `metadata.json` with a directory, to exercise put rollback paths.
struct MetadataWriteFailureBackend {
    root: PathBuf,
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

impl BlobBackend for MetadataWriteFailureBackend {
    fn kind(&self) -> BackendKind {
        BackendKind::EncryptedFile
    }

    fn put_blob(&self, object_id: &str, payload: &[u8]) -> Result<(), VaultError> {
        let mut objects = self
            .objects
            .lock()
            .map_err(|_| VaultError::Io("metadata-failure backend mutex poisoned".to_owned()))?;
        objects.insert(object_id.to_owned(), payload.to_vec());
        drop(objects);

        let canonical_root =
            crate::canonicalize_existing_dir(self.root.as_path(), "metadata sabotage root")?;
        let metadata_path = canonical_root.join(METADATA_FILE);
        crate::ensure_path_within_root(
            canonical_root.as_path(),
            metadata_path.as_path(),
            "metadata sabotage path",
        )?;
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
        let objects = self
            .objects
            .lock()
            .map_err(|_| VaultError::Io("metadata-failure backend mutex poisoned".to_owned()))?;
        objects.get(object_id).cloned().ok_or(VaultError::NotFound)
    }

    fn delete_blob(&self, object_id: &str) -> Result<(), VaultError> {
        let mut objects = self
            .objects
            .lock()
            .map_err(|_| VaultError::Io("metadata-failure backend mutex poisoned".to_owned()))?;
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
    assert!(matches!(vault.get_secret(&scope, key), Err(VaultError::NotFound)));
    Ok(())
}

#[test]
fn vault_accepts_env_style_uppercase_secret_keys() -> Result<()> {
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
    let key = "PALYRA_E2E_API_KEY";
    let value = b"sk-test-secret";

    vault.put_secret(&scope, key, value)?;
    assert_eq!(vault.get_secret(&scope, key)?, value);

    let listed = vault.list_secrets(&scope)?;
    assert_eq!(listed.len(), 1, "uppercase env-style key should be listed exactly once");
    assert_eq!(listed[0].key, key);
    assert_eq!(
        VaultRef::parse("global/PALYRA_E2E_API_KEY")?.key,
        key,
        "vault references should preserve env-style key casing"
    );
    Ok(())
}

#[test]
fn vault_list_all_secrets_returns_all_scopes_sorted() -> Result<()> {
    let temp = tempdir()?;
    let identity_root = temp.path().join("identity");
    let vault_root = temp.path().join("vault");
    let vault = Vault::open_with_config(VaultConfig {
        root: Some(vault_root),
        identity_store_root: Some(identity_root),
        backend_preference: BackendPreference::EncryptedFile,
        max_secret_bytes: 1024,
    })?;
    vault.put_secret(
        &VaultScope::Principal { principal_id: "user:ops".to_owned() },
        "ops_token",
        b"ops-secret",
    )?;
    vault.put_secret(&VaultScope::Global, "api_key", b"global-secret")?;

    let listed = vault.list_all_secrets()?;
    let references =
        listed.iter().map(|entry| format!("{}/{}", entry.scope, entry.key)).collect::<Vec<_>>();

    assert_eq!(references, vec!["global/api_key", "principal:user:ops/ops_token"]);
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
        matches!(error, VaultError::Crypto(_)),
        "unexpected error for wrong key material: {error}"
    );
    Ok(())
}

#[test]
fn vault_delete_restores_metadata_when_backend_delete_fails() -> Result<()> {
    let temp = tempdir()?;
    let vault_root_raw = temp.path().join("vault");
    ensure_owner_only_dir(&vault_root_raw)?;
    let vault_root = crate::canonicalize_existing_dir(vault_root_raw.as_path(), "vault test root")?;
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
    let vault_root_raw = temp.path().join("vault");
    ensure_owner_only_dir(&vault_root_raw)?;
    let vault_root = crate::canonicalize_existing_dir(vault_root_raw.as_path(), "vault test root")?;
    let objects = Arc::new(Mutex::new(HashMap::new()));
    let vault = Vault {
        root: vault_root.clone(),
        backend: Box::new(MetadataWriteFailureBackend {
            root: vault_root.clone(),
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
    let vault_root_raw = temp.path().join("vault");
    ensure_owner_only_dir(&vault_root_raw)?;
    let vault_root = crate::canonicalize_existing_dir(vault_root_raw.as_path(), "vault test root")?;
    let objects = Arc::new(Mutex::new(HashMap::new()));
    let vault = Vault {
        root: vault_root.clone(),
        backend: Box::new(MetadataWriteFailureBackend {
            root: vault_root.clone(),
            objects: Arc::clone(&objects),
        }),
        max_secret_bytes: 1024,
        kek: derive_kek_from_seed_material(b"palyra.vault.tests.update_rollback")?,
    };
    vault.ensure_metadata_exists()?;

    let scope = VaultScope::Principal { principal_id: "user:ops".to_owned() };
    let key = "api_key";
    let original_value = b"original-secret";
    let object_id = crate::object_id_for(&scope, key);
    let original_payload = serde_json::to_vec(&seal(
        original_value,
        &vault.kek,
        crate::build_aad(&scope, key).as_slice(),
    )?)
    .map_err(|error| {
        VaultError::Io(format!("failed to serialize original envelope payload: {error}"))
    })?;
    let expected_restored_payload = original_payload.clone();
    objects
        .lock()
        .map_err(|_| VaultError::Io("metadata-failure backend mutex poisoned".to_owned()))?
        .insert(object_id.clone(), original_payload);
    let now = crate::current_unix_ms()?;
    vault.write_metadata(&MetadataFile {
        version: METADATA_VERSION,
        entries: vec![MetadataEntry {
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
    let lock_path = temp.path().join(METADATA_LOCK_FILE);
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
    let lock_path = temp.path().join(METADATA_LOCK_FILE);
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

#[test]
fn metadata_lock_reclaim_treats_missing_lock_as_released() -> Result<()> {
    let temp = tempdir()?;
    let lock_path = temp.path().join(METADATA_LOCK_FILE);
    let reclaimed = maybe_reclaim_stale_lock_with_policy(
        &lock_path,
        std::time::SystemTime::now(),
        Duration::ZERO,
    )?;
    assert!(reclaimed, "missing lock should be treated as already released for retry loops");
    Ok(())
}

#[test]
fn metadata_lock_marker_init_failure_removes_lock_file() -> Result<()> {
    let temp = tempdir()?;
    let lock_path = temp.path().join(METADATA_LOCK_FILE);
    std::fs::write(&lock_path, b"seed-lock")?;

    let error =
        initialize_metadata_lock_marker_with_writer(&lock_path, "pid=1 ts_ms=0\n", |_payload| {
            Err(std::io::Error::other("injected marker write failure"))
        })
        .expect_err("marker initialization should fail when writer returns error");
    assert!(
        matches!(error, VaultError::Io(message) if message.contains("failed to initialize metadata lock marker")),
        "marker initialization error should preserve lock-path context"
    );
    assert!(!lock_path.exists(), "lock file should be removed when marker initialization fails");
    Ok(())
}

#[test]
fn metadata_lock_reacquire_succeeds_after_marker_init_failure() -> Result<()> {
    let temp = tempdir()?;
    let identity_root = temp.path().join("identity");
    let vault_root = temp.path().join("vault");
    let lock_path = vault_root.join(METADATA_LOCK_FILE);
    let vault = Vault::open_with_config(VaultConfig {
        root: Some(vault_root),
        identity_store_root: Some(identity_root),
        backend_preference: BackendPreference::EncryptedFile,
        max_secret_bytes: 1024,
    })?;
    std::fs::write(&lock_path, b"seed-lock")?;

    let error =
        initialize_metadata_lock_marker_with_writer(&lock_path, "pid=1 ts_ms=0\n", |_payload| {
            Err(std::io::Error::other("injected marker sync failure"))
        })
        .expect_err("marker initialization should fail for injected writer failure");
    assert!(
        matches!(error, VaultError::Io(message) if message.contains("failed to initialize metadata lock marker")),
        "marker initialization failure should be surfaced as vault I/O error"
    );
    assert!(
        !lock_path.exists(),
        "lock artifact must be removed so next acquisition can proceed immediately"
    );

    let guard = vault.acquire_metadata_lock()?;
    assert!(
        lock_path.exists(),
        "successful lock acquisition should materialize lock file while guard is held"
    );
    drop(guard);
    assert!(!lock_path.exists(), "dropping guard should release metadata lock file");
    Ok(())
}

#[cfg(windows)]
#[test]
fn parse_whoami_sid_csv_extracts_sid_field() {
    let parsed = crate::filesystem::parse_whoami_sid_csv(
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
    crate::ensure_owner_only_file(&file)?;
    Ok(())
}

#[test]
fn owner_only_dir_rejects_parent_traversal_components() {
    let temp = tempdir().expect("temporary test directory should be created");
    let unsafe_path = temp.path().join("vault").join("..").join("escape");

    let error = ensure_owner_only_dir(unsafe_path.as_path())
        .expect_err("owner-only directory path must reject parent traversal");

    assert!(error.to_string().contains("parent directory traversal"), "unexpected error: {error}");
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
    let objects_mode = std::fs::metadata(vault_root.join("objects"))?.permissions().mode() & 0o777;
    assert_eq!(root_mode, 0o700);
    assert_eq!(metadata_mode, 0o600);
    assert_eq!(objects_mode, 0o700);
    Ok(())
}

#[test]
fn vault_ref_parser_supports_expected_shape() {
    let parsed = VaultRef::parse("global/openai_api_key").expect("valid vault ref should parse");
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

#[test]
fn derive_device_kek_falls_back_to_standalone_ca_state() -> Result<()> {
    let temp = tempdir()?;
    let store = FilesystemSecretStore::new(temp.path())?;
    let bundle_without_private_key = br#"{
        "generation": 4,
        "paired_devices": {},
        "revoked_devices": {},
        "revoked_certificate_fingerprints": []
    }"#;
    store.write_secret("identity/state.v1.json", bundle_without_private_key)?;

    let ca = CertificateAuthority::new("Vault KEK Test CA")?.to_stored();
    let ca_state = serde_json::to_vec(&ca)?;
    store.write_sealed_value("identity/ca/state.json", ca_state.as_slice())?;

    let derived = derive_device_kek(temp.path())?;
    let expected = derive_kek_from_seed_material(ca.private_key_pem.as_bytes())?;
    assert_eq!(
        derived, expected,
        "vault KEK derivation must fall back to standalone CA state when the public bundle omits key material"
    );
    Ok(())
}
