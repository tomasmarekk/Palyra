use anyhow::Context;
use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use rand::random;
use ring::aead::{Aad, LessSafeKey, Nonce, Tag, UnboundKey, CHACHA20_POLY1305};

use crate::{SensitiveBytes, VaultError};

const NONCE_BYTES: usize = 12;
const MAC_BYTES: usize = 16;
const DEK_BYTES: usize = 32;
type SealedBlob = ([u8; NONCE_BYTES], Vec<u8>, [u8; MAC_BYTES]);

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EnvelopePayload {
    pub version: u32,
    pub algorithm: String,
    pub aad_b64: String,
    pub secret_nonce_b64: String,
    pub secret_ciphertext_b64: String,
    pub secret_mac_b64: String,
    pub dek_nonce_b64: String,
    pub dek_ciphertext_b64: String,
    pub dek_mac_b64: String,
}

pub fn seal(
    value: &[u8],
    kek: &[u8; DEK_BYTES],
    aad: &[u8],
) -> Result<EnvelopePayload, VaultError> {
    let mut dek = random::<[u8; DEK_BYTES]>();
    let (secret_nonce, secret_ciphertext, secret_mac) = seal_with_key(&dek, value, aad)?;
    let (dek_nonce, dek_ciphertext, dek_mac) = seal_with_key(kek, &dek, aad)?;
    dek.fill(0);

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
    let mut dek = [0_u8; DEK_BYTES];
    dek.copy_from_slice(dek_sensitive.as_ref());

    let secret_nonce =
        decode_fixed::<NONCE_BYTES>(payload.secret_nonce_b64.as_str(), "secret nonce")?;
    let secret_ciphertext = decode(payload.secret_ciphertext_b64.as_str(), "secret ciphertext")?;
    let secret_mac = decode_fixed::<MAC_BYTES>(payload.secret_mac_b64.as_str(), "secret mac")?;
    let plaintext =
        open_with_key(&dek, &secret_nonce, secret_ciphertext, &secret_mac, aad.as_slice())?;
    dek.fill(0);
    Ok(plaintext)
}

fn seal_with_key(
    key_bytes: &[u8; DEK_BYTES],
    plaintext: &[u8],
    aad: &[u8],
) -> Result<SealedBlob, VaultError> {
    let unbound_key = UnboundKey::new(&CHACHA20_POLY1305, key_bytes)
        .map_err(|_| VaultError::Crypto("failed to initialize AEAD key".to_owned()))?;
    let key = LessSafeKey::new(unbound_key);
    let nonce = random::<[u8; NONCE_BYTES]>();
    let nonce_value = Nonce::assume_unique_for_key(nonce);
    let mut in_out = plaintext.to_vec();
    let tag = key
        .seal_in_place_separate_tag(nonce_value, Aad::from(aad), &mut in_out)
        .map_err(|_| VaultError::Crypto("failed to encrypt envelope payload".to_owned()))?;
    let mut mac = [0_u8; MAC_BYTES];
    mac.copy_from_slice(tag.as_ref());
    Ok((nonce, in_out, mac))
}

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
    Ok(plaintext.to_vec())
}

fn decode(raw: &str, label: &str) -> Result<Vec<u8>, VaultError> {
    STANDARD_NO_PAD
        .decode(raw.as_bytes())
        .with_context(|| format!("failed to decode {label}"))
        .map_err(|error| VaultError::Crypto(error.to_string()))
}

fn decode_fixed<const N: usize>(raw: &str, label: &str) -> Result<[u8; N], VaultError> {
    let decoded = decode(raw, label)?;
    let slice = decoded.as_slice();
    slice.try_into().map_err(|_| VaultError::Crypto(format!("{label} length mismatch")))
}
