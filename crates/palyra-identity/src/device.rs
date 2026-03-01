use std::fmt;

use anyhow::Context;
use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine as _};
use ed25519_dalek::{SigningKey, VerifyingKey};
use palyra_common::validate_canonical_id;
use rand::random;
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

use crate::{
    error::{IdentityError, IdentityResult},
    store::SecretStore,
};

#[derive(Clone)]
pub struct DeviceIdentity {
    pub device_id: String,
    signing_key: SigningKey,
    x25519_secret: StaticSecret,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct StoredDeviceIdentity {
    device_id: String,
    signing_key_b64: String,
    x25519_secret_b64: String,
}

impl DeviceIdentity {
    pub fn generate(device_id: &str) -> IdentityResult<Self> {
        validate_canonical_id(device_id)
            .map_err(|error| IdentityError::InvalidCanonicalDeviceId(error.to_string()))?;

        let signing_key = SigningKey::from_bytes(&random::<[u8; 32]>());
        let x25519_bytes = random::<[u8; 32]>();
        let x25519_secret = StaticSecret::from(x25519_bytes);

        Ok(Self { device_id: device_id.to_owned(), signing_key, x25519_secret })
    }

    #[must_use]
    pub fn signing_key(&self) -> &SigningKey {
        &self.signing_key
    }

    #[must_use]
    pub fn signing_public_key(&self) -> [u8; 32] {
        self.signing_key.verifying_key().to_bytes()
    }

    #[must_use]
    pub fn verifying_key(&self) -> VerifyingKey {
        self.signing_key.verifying_key()
    }

    #[must_use]
    pub fn x25519_secret(&self) -> &StaticSecret {
        &self.x25519_secret
    }

    #[must_use]
    pub fn x25519_public_key(&self) -> [u8; 32] {
        X25519PublicKey::from(&self.x25519_secret).to_bytes()
    }

    #[must_use]
    pub fn fingerprint(&self) -> String {
        let hash = Sha256::digest(self.signing_public_key());
        hex::encode(hash)
    }

    pub fn store(&self, store: &dyn SecretStore) -> IdentityResult<()> {
        let payload = StoredDeviceIdentity {
            device_id: self.device_id.clone(),
            signing_key_b64: STANDARD_NO_PAD.encode(self.signing_key.to_bytes()),
            x25519_secret_b64: STANDARD_NO_PAD.encode(self.x25519_secret.to_bytes()),
        };
        let key = format!("device/{}/identity.json", self.device_id);
        let data = serde_json::to_vec(&payload)
            .map_err(|error| IdentityError::Internal(error.to_string()))?;
        store.write_secret(&key, &data)
    }

    pub fn load(store: &dyn SecretStore, device_id: &str) -> IdentityResult<Self> {
        validate_canonical_id(device_id)
            .map_err(|error| IdentityError::InvalidCanonicalDeviceId(error.to_string()))?;

        let key = format!("device/{device_id}/identity.json");
        let raw = store.read_secret(&key)?;
        let stored: StoredDeviceIdentity = serde_json::from_slice(&raw)
            .map_err(|error| IdentityError::Internal(error.to_string()))?;
        if stored.device_id != device_id {
            return Err(IdentityError::Internal("stored device identity mismatch".to_owned()));
        }

        let signing_bytes = decode_fixed_32(&stored.signing_key_b64)?;
        let x25519_bytes = decode_fixed_32(&stored.x25519_secret_b64)?;
        let signing_key = SigningKey::from_bytes(&signing_bytes);
        let x25519_secret = StaticSecret::from(x25519_bytes);
        Ok(Self { device_id: stored.device_id, signing_key, x25519_secret })
    }
}

impl fmt::Debug for DeviceIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DeviceIdentity")
            .field("device_id", &self.device_id)
            .field("signing_public_key_hex", &hex::encode(self.signing_public_key()))
            .field("x25519_public_key_hex", &hex::encode(self.x25519_public_key()))
            .finish()
    }
}

fn decode_fixed_32(value: &str) -> IdentityResult<[u8; 32]> {
    let bytes = STANDARD_NO_PAD
        .decode(value.as_bytes())
        .context("base64 decode failed")
        .map_err(|error| IdentityError::Internal(error.to_string()))?;
    bytes
        .try_into()
        .map_err(|_| IdentityError::Internal("decoded secret length mismatch".to_owned()))
}

#[cfg(test)]
mod tests {
    use super::DeviceIdentity;
    use crate::store::InMemorySecretStore;

    #[test]
    fn device_identity_store_roundtrip() {
        let store = InMemorySecretStore::new();
        let device_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
        let identity = DeviceIdentity::generate(device_id).expect("identity should generate");
        identity.store(&store).expect("identity should store");
        let loaded = DeviceIdentity::load(&store, device_id).expect("identity should load");
        assert_eq!(identity.device_id, loaded.device_id);
        assert_eq!(identity.fingerprint(), loaded.fingerprint());
    }
}
