//! Envelope encryption: each secret is sealed with a fresh random data-encryption key (DEK),
//! and the DEK is sealed with the device KEK, both via ChaCha20-Poly1305 with random nonces.
//!
//! The KEK therefore only ever encrypts 32-byte random DEKs, and the scope/key-binding AAD is
//! authenticated by both layers. Field names and the version/algorithm literals are the on-disk
//! format — changing them orphans existing vault payloads.

use anyhow::Context;
use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use getrandom::fill as fill_random_bytes;
use ring::aead::{Aad, LessSafeKey, Nonce, Tag, UnboundKey, CHACHA20_POLY1305};
use zeroize::Zeroizing;

use crate::{SensitiveBytes, VaultError};

const NONCE_BYTES: usize = 12;
const MAC_BYTES: usize = 16;
const DEK_BYTES: usize = 32;
type SealedBlob = ([u8; NONCE_BYTES], Vec<u8>, [u8; MAC_BYTES]);

/// Serialized envelope: base64 nonce/ciphertext/MAC for both the secret and the wrapped DEK.
///
/// Contains only ciphertext and public parameters — safe to hand to any blob backend. Field
/// names are serde-pinned on-disk format; do not rename.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EnvelopePayload {
    /// Envelope format version; only version 1 exists.
    pub version: u32,
    /// AEAD algorithm identifier; only `chacha20_poly1305` exists.
    pub algorithm: String,
    /// AAD the envelope was sealed with, stored for mismatch diagnostics before decryption.
    pub aad_b64: String,
    /// Nonce for the secret layer.
    pub secret_nonce_b64: String,
    /// Secret value encrypted under the DEK.
    pub secret_ciphertext_b64: String,
    /// Poly1305 tag for the secret layer.
    pub secret_mac_b64: String,
    /// Nonce for the DEK-wrapping layer.
    pub dek_nonce_b64: String,
    /// DEK encrypted under the device KEK.
    pub dek_ciphertext_b64: String,
    /// Poly1305 tag for the DEK-wrapping layer.
    pub dek_mac_b64: String,
}

/// Seals `value` into a two-layer envelope bound to `aad` under the device KEK.
///
/// # Errors
/// Returns [`VaultError::Crypto`] when OS randomness is unavailable or AEAD sealing fails.
pub fn seal(
    value: &[u8],
    kek: &[u8; DEK_BYTES],
    aad: &[u8],
) -> Result<EnvelopePayload, VaultError> {
    let dek = Zeroizing::new(secure_random_array("data encryption key")?);
    let (secret_nonce, secret_ciphertext, secret_mac) = seal_with_key(&dek, value, aad)?;
    let (dek_nonce, dek_ciphertext, dek_mac) = seal_with_key(kek, dek.as_slice(), aad)?;

    Ok(EnvelopePayload {
        version: 1,
        algorithm: "chacha20_poly1305".to_owned(),
        aad_b64: STANDARD_NO_PAD.encode(aad),
        secret_nonce_b64: STANDARD_NO_PAD.encode(secret_nonce),
        secret_ciphertext_b64: STANDARD_NO_PAD.encode(secret_ciphertext),
        secret_mac_b64: STANDARD_NO_PAD.encode(secret_mac),
        dek_nonce_b64: STANDARD_NO_PAD.encode(dek_nonce),
        dek_ciphertext_b64: STANDARD_NO_PAD.encode(dek_ciphertext),
        dek_mac_b64: STANDARD_NO_PAD.encode(dek_mac),
    })
}

/// Opens an envelope sealed by [`seal`], authenticating both layers against `expected_aad`.
///
/// # Errors
/// Returns [`VaultError::Crypto`] for unsupported version/algorithm, AAD mismatch, malformed
/// base64 fields, or authentication/decryption failure on either layer (e.g. a KEK derived from
/// a different identity).
pub fn open(
    payload: &EnvelopePayload,
    kek: &[u8; DEK_BYTES],
    expected_aad: &[u8],
) -> Result<Vec<u8>, VaultError> {
    if payload.version != 1 {
        return Err(VaultError::Crypto("unsupported envelope version".to_owned()));
    }
    if payload.algorithm != "chacha20_poly1305" {
        return Err(VaultError::Crypto("unsupported envelope algorithm".to_owned()));
    }
    // Early non-constant-time check for a clearer error; the AAD is public data, and the AEAD
    // layers below still authenticate `expected_aad` cryptographically.
    let aad = decode(payload.aad_b64.as_str(), "aad")?;
    if aad != expected_aad {
        return Err(VaultError::Crypto("envelope aad mismatch".to_owned()));
    }
    let dek_nonce = decode_fixed::<NONCE_BYTES>(payload.dek_nonce_b64.as_str(), "dek nonce")?;
    let dek_ciphertext = decode(payload.dek_ciphertext_b64.as_str(), "dek ciphertext")?;
    let dek_mac = decode_fixed::<MAC_BYTES>(payload.dek_mac_b64.as_str(), "dek mac")?;
    let dek_sensitive = SensitiveBytes::new(open_with_key(
        kek,
        &dek_nonce,
        dek_ciphertext,
        &dek_mac,
        aad.as_slice(),
    )?);
    if dek_sensitive.as_ref().len() != DEK_BYTES {
        return Err(VaultError::Crypto("derived dek length mismatch".to_owned()));
    }
    let mut dek = Zeroizing::new([0_u8; DEK_BYTES]);
    dek.copy_from_slice(dek_sensitive.as_ref());

    let secret_nonce =
        decode_fixed::<NONCE_BYTES>(payload.secret_nonce_b64.as_str(), "secret nonce")?;
    let secret_ciphertext = decode(payload.secret_ciphertext_b64.as_str(), "secret ciphertext")?;
    let secret_mac = decode_fixed::<MAC_BYTES>(payload.secret_mac_b64.as_str(), "secret mac")?;
    open_with_key(&dek, &secret_nonce, secret_ciphertext, &secret_mac, aad.as_slice())
}

/// Encrypts `plaintext` under `key_bytes` with a freshly drawn random nonce.
fn seal_with_key(
    key_bytes: &[u8; DEK_BYTES],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<SealedBlob, VaultError> {
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key_bytes)
        .map_err(|_| VaultError::Crypto("failed to initialize AEAD key".to_owned()))?;
    // `LessSafeKey` leaves nonce management to us: the DEK layer encrypts exactly once per key,
    // and the long-lived KEK only wraps DEKs at human-driven write rates, so random 96-bit
    // nonces keep collision probability negligible for this workload.
    let key = LessSafeKey::new(unbound_key);
    let nonce = secure_random_array("envelope nonce")?;
    let nonce_value = Nonce::assume_unique_for_key(nonce);
    let mut in_out = plaintext.to_vec();
    let tag = key
        .seal_in_place_separate_tag(nonce_value, Aad::from(aad), &mut in_out)
        .map_err(|_| VaultError::Crypto("failed to encrypt envelope payload".to_owned()))?;
    let mut mac = [0_u8; MAC_BYTES];
    mac.copy_from_slice(tag.as_ref());
    Ok((nonce, in_out, mac))
}

/// Authenticates and decrypts `ciphertext` in place, returning the same buffer as plaintext.
fn open_with_key(
    key_bytes: &[u8; DEK_BYTES],
    nonce: &[u8; NONCE_BYTES],
    mut ciphertext: Vec<u8>,
    mac: &[u8; MAC_BYTES],
    aad: &[u8],
) -> Result<Vec<u8>, VaultError> {
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key_bytes)
        .map_err(|_| VaultError::Crypto("failed to initialize AEAD key".to_owned()))?;
    let key = LessSafeKey::new(unbound_key);
    let nonce_value = Nonce::assume_unique_for_key(*nonce);
    let tag = Tag::try_from(mac.as_slice())
        .map_err(|_| VaultError::Crypto("envelope mac is malformed".to_owned()))?;
    let plaintext = key
        .open_in_place_separate_tag(nonce_value, Aad::from(aad), tag, &mut ciphertext, 0..)
        .map_err(|_| VaultError::Crypto("failed to decrypt envelope payload".to_owned()))?;
    let plaintext_len = plaintext.len();
    ciphertext.truncate(plaintext_len);
    Ok(ciphertext)
}

/// Decodes an unpadded-base64 envelope field, labelling failures for diagnostics.
fn decode(raw: &str, label: &str) -> Result<Vec<u8>, VaultError> {
    STANDARD_NO_PAD
        .decode(raw.as_bytes())
        .with_context(|| format!("failed to decode {label}"))
        .map_err(|error| VaultError::Crypto(error.to_string()))
}

/// Decodes an envelope field that must be exactly `N` bytes (nonces, MACs).
fn decode_fixed<const N: usize>(raw: &str, label: &str) -> Result<[u8; N], VaultError> {
    let decoded = decode(raw, label)?;
    let slice = decoded.as_slice();
    slice.try_into().map_err(|_| VaultError::Crypto(format!("{label} length mismatch")))
}

/// Fills an `N`-byte array from the OS CSPRNG (used for DEKs and nonces).
fn secure_random_array<const N: usize>(label: &str) -> Result<[u8; N], VaultError> {
    let mut bytes = [0_u8; N];
    fill_random_bytes(&mut bytes).map_err(|error| {
        VaultError::Crypto(format!("failed to read OS randomness for {label}: {error}"))
    })?;
    Ok(bytes)
}
