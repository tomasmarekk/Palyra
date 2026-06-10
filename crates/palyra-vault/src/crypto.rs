//! Key derivation, secret-key validation, and crypto-adjacent helpers.
//!
//! Derives the device-bound key-encryption key (KEK) from the `palyra-identity` CA private key,
//! so vault ciphertext is only recoverable on a machine holding the same identity store. Also
//! owns AAD construction, deterministic object-id hashing, and [`SensitiveBytes`].

use std::{
    path::Path,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use hkdf::Hkdf;
use palyra_identity::{FilesystemSecretStore, IdentityManager, SecretStore};
use sha2::{Digest, Sha256};

use crate::{VaultError, VaultScope};

const KEY_DERIVATION_SALT: &[u8] = b"palyra.vault.kek.v1";
const KEY_DERIVATION_INFO: &[u8] = b"envelope:kek";
const AAD_PREFIX: &str = "palyra.vault.v1";
const IDENTITY_STATE_BUNDLE_KEY: &str = "identity/state.v1.json";
const LEGACY_CA_STATE_KEY: &str = "identity/ca/state.json";
const MAX_SECRET_KEY_BYTES: usize = 128;

/// Derives the device KEK from the identity CA private key stored under `identity_store_root`.
pub(crate) fn derive_device_kek(identity_store_root: &Path) -> Result<[u8; 32], VaultError> {
    let store = FilesystemSecretStore::new(identity_store_root).map_err(|error| {
        VaultError::Io(format!(
            "failed to initialize identity store for key derivation at {}: {error}",
            identity_store_root.display()
        ))
    })?;
    let store: Arc<dyn SecretStore> = Arc::new(store);
    // Constructing the manager bootstraps identity state (CA key material) on first run; it is
    // only needed for that side effect, the seed is read straight from the store below.
    let _manager = IdentityManager::with_store(store.clone()).map_err(|error| {
        VaultError::Io(format!("failed to initialize identity manager for key derivation: {error}"))
    })?;
    let seed = load_kek_seed_material(store.as_ref())?;
    derive_kek_from_seed_material(seed.as_slice())
}

/// Loads the raw seed material, preferring the bundled identity state over the legacy CA state.
///
/// Existing vaults were keyed off whichever state file was present at creation time, so the
/// fallback order must stay bundle-first to keep KEK derivation stable across upgrades.
fn load_kek_seed_material(store: &dyn SecretStore) -> Result<Vec<u8>, VaultError> {
    if let Ok(bundle_state) = store.read_secret(IDENTITY_STATE_BUNDLE_KEY) {
        if let Ok(seed) = extract_kek_seed_material(bundle_state.as_slice()) {
            return Ok(seed);
        }
    }

    let ca_state = store.read_secret(LEGACY_CA_STATE_KEY).map_err(|error| {
        VaultError::Crypto(format!(
            "failed to read identity key material for vault KEK derivation: {error}"
        ))
    })?;
    extract_kek_seed_material(ca_state.as_slice())
}

/// Extracts the CA private-key PEM (the KEK seed) from a serialized identity state document.
///
/// Only the private key is used — not certificates, sequence numbers, or other rotating
/// metadata — so the derived KEK survives identity-state rewrites that keep the same key.
pub(crate) fn extract_kek_seed_material(raw_state: &[u8]) -> Result<Vec<u8>, VaultError> {
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

/// Derives the 32-byte KEK from seed material via SHA-256 + HKDF-SHA256.
///
/// The salt/info constants are part of the on-disk format: changing them re-keys every existing
/// vault. The pre-hash normalizes arbitrary-length PEM input before HKDF extraction.
pub(crate) fn derive_kek_from_seed_material(seed_material: &[u8]) -> Result<[u8; 32], VaultError> {
    let seed_hash = Sha256::digest(seed_material);
    let hkdf = Hkdf::<Sha256>::new(Some(KEY_DERIVATION_SALT), seed_hash.as_slice());
    let mut output = [0_u8; 32];
    hkdf.expand(KEY_DERIVATION_INFO, &mut output)
        .map_err(|_| VaultError::Crypto("failed to derive vault KEK".to_owned()))?;
    Ok(output)
}

/// Builds the AEAD associated data binding an envelope to its scope and key.
///
/// `|` may appear inside scope segments, but the key charset excludes it, so the
/// (scope, key) -> AAD mapping stays injective and ciphertext cannot be re-bound to another
/// entry without failing authentication.
pub(crate) fn build_aad(scope: &VaultScope, key: &str) -> Vec<u8> {
    format!("{AAD_PREFIX}|{}|{key}", scope.as_storage_str()).into_bytes()
}

/// Validates a secret key: non-empty, at most 128 bytes, ASCII alphanumerics plus `.`, `_`, `-`.
pub(crate) fn validate_secret_key(raw: &str) -> Result<(), VaultError> {
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
        .all(|ch| ch.is_ascii_alphabetic() || ch.is_ascii_digit() || matches!(ch, '.' | '_' | '-'))
    {
        return Err(VaultError::InvalidKey(
            "secret key can only contain ASCII letters, digits, '.', '_' or '-'".to_owned(),
        ));
    }
    Ok(())
}

/// Derives the deterministic backend object id (`obj_<sha256 hex>`) for a scope/key pair.
///
/// Hashing keeps scope and key names out of OS keystore item names; the NUL separator prevents
/// (scope, key) boundary ambiguity in the digest input.
pub(crate) fn object_id_for(scope: &VaultScope, key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.vault.object.v1");
    hasher.update(scope.as_storage_str().as_bytes());
    hasher.update([0_u8]);
    hasher.update(key.as_bytes());
    format!("obj_{}", hex::encode(hasher.finalize()))
}

/// Normalizes an object id to canonical `obj_<64 lowercase hex>` form, rejecting anything else.
///
/// Backends use the result as a file or keystore item name, so this is also the guard that keeps
/// untrusted ids from smuggling path separators into storage paths.
pub(crate) fn normalize_storage_object_id(raw: &str) -> Result<String, VaultError> {
    let normalized = raw.trim();
    let digest = normalized.strip_prefix("obj_").ok_or_else(|| {
        VaultError::InvalidObjectId(
            "object id must have the shape 'obj_<64 lowercase hex characters>'".to_owned(),
        )
    })?;
    if digest.len() != 64 || !digest.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return Err(VaultError::InvalidObjectId(
            "object id must have the shape 'obj_<64 lowercase hex characters>'".to_owned(),
        ));
    }
    let decoded = hex::decode(digest).map_err(|error| {
        VaultError::InvalidObjectId(format!(
            "object id must contain valid hex digest bytes: {error}"
        ))
    })?;
    if decoded.len() != 32 {
        return Err(VaultError::InvalidObjectId(
            "object id must include a 32-byte digest".to_owned(),
        ));
    }
    Ok(format!("obj_{}", hex::encode(decoded)))
}

/// Returns the current wall-clock time as unix milliseconds.
pub(crate) fn current_unix_ms() -> Result<i64, VaultError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| VaultError::Io(format!("system clock before unix epoch: {error}")))?;
    // Truncating u128 -> i64 is safe here: unix milliseconds stay below i64::MAX for ~292
    // million years.
    Ok(duration.as_millis() as i64)
}

/// Secret byte buffer that is zeroed when dropped.
///
/// Deliberately implements neither `Debug` nor `Display` so the contents cannot leak through
/// formatting; expose them only via [`AsRef`] and avoid copying them out unless required.
pub struct SensitiveBytes(pub(crate) Vec<u8>);

impl SensitiveBytes {
    /// Wraps an already-materialized secret value, taking ownership of the buffer.
    #[must_use]
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
        // AIDEV-NOTE: best-effort wipe only — a plain fill can be elided by the optimizer and
        // does not cover earlier reallocations or copies made before wrapping. A guaranteed wipe
        // needs the `zeroize` crate (volatile writes + compiler fences), which is a dependency
        // decision out of scope here.
        self.0.fill(0);
    }
}
