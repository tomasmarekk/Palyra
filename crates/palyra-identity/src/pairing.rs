use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};

use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use hkdf::Hkdf;
use palyra_common::validate_canonical_id;
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

use crate::{
    ca::{CertificateAuthority, IssuedCertificate},
    device::DeviceIdentity,
    error::{IdentityError, IdentityResult},
    store::{InMemorySecretStore, SecretStore},
    unix_ms, DEFAULT_CERT_VALIDITY, DEFAULT_PAIRING_WINDOW, DEFAULT_ROTATION_THRESHOLD,
    PAIRING_PROTOCOL_VERSION,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PairingClientKind {
    Cli,
    Desktop,
    Node,
}

impl PairingClientKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Desktop => "desktop",
            Self::Node => "node",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PairingMethod {
    Pin { code: String },
    Qr { token: String },
}

impl PairingMethod {
    pub(crate) fn proof(&self) -> &str {
        match self {
            Self::Pin { code } => code,
            Self::Qr { token } => token,
        }
    }

    #[must_use]
    pub fn display_label(&self) -> String {
        match self {
            Self::Pin { code } => format!("pin:{}", "*".repeat(code.len())),
            Self::Qr { token } => format!("qr:{}...", token.chars().take(6).collect::<String>()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PairingSession {
    pub session_id: String,
    pub protocol_version: u32,
    pub client_kind: PairingClientKind,
    pub method: PairingMethod,
    pub gateway_ephemeral_public: [u8; 32],
    pub challenge: [u8; 32],
    pub expires_at_unix_ms: u64,
}

#[derive(Clone)]
struct ActivePairingSession {
    public: PairingSession,
    gateway_ephemeral_secret: StaticSecret,
}

#[derive(Debug, Clone)]
pub struct DevicePairingHello {
    pub session_id: String,
    pub protocol_version: u32,
    pub device_id: String,
    pub client_kind: PairingClientKind,
    pub proof: String,
    pub device_signing_public: [u8; 32],
    pub device_x25519_public: [u8; 32],
    pub challenge_signature: [u8; 64],
    pub transcript_mac: [u8; 32],
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PairedDevice {
    pub device_id: String,
    pub client_kind: PairingClientKind,
    pub identity_fingerprint: String,
    pub signing_public_key_hex: String,
    pub transcript_hash_hex: String,
    pub current_certificate: IssuedCertificate,
}

#[derive(Debug, Clone)]
pub struct PairingResult {
    pub device: PairedDevice,
    pub gateway_ca_certificate_pem: String,
}

#[derive(Debug, Clone)]
pub struct RevokedDevice {
    pub device_id: String,
    pub reason: String,
    pub revoked_at_unix_ms: u64,
}

pub struct IdentityManager {
    store: Arc<dyn SecretStore>,
    pairing_window: Duration,
    certificate_validity: Duration,
    rotation_threshold: Duration,
    active_sessions: HashMap<String, ActivePairingSession>,
    paired_devices: HashMap<String, PairedDevice>,
    revoked_devices: HashMap<String, RevokedDevice>,
    ca: CertificateAuthority,
}

impl IdentityManager {
    pub fn with_store(store: Arc<dyn SecretStore>) -> IdentityResult<Self> {
        Ok(Self {
            store,
            pairing_window: DEFAULT_PAIRING_WINDOW,
            certificate_validity: DEFAULT_CERT_VALIDITY,
            rotation_threshold: DEFAULT_ROTATION_THRESHOLD,
            active_sessions: HashMap::new(),
            paired_devices: HashMap::new(),
            revoked_devices: HashMap::new(),
            ca: CertificateAuthority::new("Palyra Gateway CA")?,
        })
    }

    pub fn with_memory_store() -> IdentityResult<Self> {
        Self::with_store(Arc::new(InMemorySecretStore::new()))
    }

    pub fn set_pairing_window(&mut self, value: Duration) {
        self.pairing_window = value;
    }

    pub fn set_certificate_validity(&mut self, value: Duration) {
        self.certificate_validity = value;
    }

    pub fn set_rotation_threshold(&mut self, value: Duration) {
        self.rotation_threshold = value;
    }

    #[must_use]
    pub fn gateway_ca_certificate_pem(&self) -> String {
        self.ca.certificate_pem.clone()
    }

    pub fn issue_gateway_server_certificate(
        &mut self,
        common_name: &str,
    ) -> IdentityResult<IssuedCertificate> {
        self.ca.issue_server_certificate(common_name, self.certificate_validity)
    }

    pub fn start_pairing(
        &mut self,
        client_kind: PairingClientKind,
        method: PairingMethod,
        now: SystemTime,
    ) -> IdentityResult<PairingSession> {
        validate_pairing_method(&method)?;

        let session_id = ulid::Ulid::new().to_string();
        let mut gateway_secret_bytes = [0_u8; 32];
        OsRng.fill_bytes(&mut gateway_secret_bytes);
        let gateway_ephemeral_secret = StaticSecret::from(gateway_secret_bytes);
        let gateway_ephemeral_public = X25519PublicKey::from(&gateway_ephemeral_secret).to_bytes();

        let mut challenge = [0_u8; 32];
        OsRng.fill_bytes(&mut challenge);

        let expires_at = now + self.pairing_window;
        let session = PairingSession {
            session_id: session_id.clone(),
            protocol_version: PAIRING_PROTOCOL_VERSION,
            client_kind,
            method,
            gateway_ephemeral_public,
            challenge,
            expires_at_unix_ms: unix_ms(expires_at)?,
        };

        self.active_sessions.insert(
            session_id,
            ActivePairingSession { public: session.clone(), gateway_ephemeral_secret },
        );
        Ok(session)
    }

    pub fn build_device_hello(
        &self,
        session: &PairingSession,
        device: &DeviceIdentity,
        proof: &str,
    ) -> IdentityResult<DevicePairingHello> {
        validate_canonical_id(&device.device_id)
            .map_err(|error| IdentityError::InvalidCanonicalDeviceId(error.to_string()))?;

        let gateway_public = X25519PublicKey::from(session.gateway_ephemeral_public);
        let shared_secret = device.x25519_secret().diffie_hellman(&gateway_public);
        let transcript_context = transcript_context(
            &session.session_id,
            session.protocol_version,
            device.device_id.as_str(),
            session.client_kind,
        );
        let transcript_mac = derive_transcript_mac(
            shared_secret.as_bytes(),
            &session.challenge,
            &transcript_context,
        )?;

        let signature_payload = pairing_signature_payload(
            session.protocol_version,
            &session.session_id,
            &session.challenge,
            &session.gateway_ephemeral_public,
            &device.device_id,
            session.client_kind,
            proof,
        );
        let signature = device.signing_key().sign(&signature_payload);

        Ok(DevicePairingHello {
            session_id: session.session_id.clone(),
            protocol_version: session.protocol_version,
            device_id: device.device_id.clone(),
            client_kind: session.client_kind,
            proof: proof.to_owned(),
            device_signing_public: device.signing_public_key(),
            device_x25519_public: device.x25519_public_key(),
            challenge_signature: signature.to_bytes(),
            transcript_mac,
        })
    }

    pub fn complete_pairing(
        &mut self,
        hello: DevicePairingHello,
        now: SystemTime,
    ) -> IdentityResult<PairingResult> {
        validate_canonical_id(&hello.device_id)
            .map_err(|error| IdentityError::InvalidCanonicalDeviceId(error.to_string()))?;
        if self.revoked_devices.contains_key(&hello.device_id) {
            return Err(IdentityError::DeviceRevoked);
        }

        let active = self
            .active_sessions
            .get(&hello.session_id)
            .ok_or(IdentityError::PairingSessionNotFound)?
            .clone();
        if unix_ms(now)? > active.public.expires_at_unix_ms {
            self.active_sessions.remove(&hello.session_id);
            return Err(IdentityError::PairingSessionExpired);
        }
        if hello.protocol_version != active.public.protocol_version {
            return Err(IdentityError::PairingVersionMismatch {
                expected: active.public.protocol_version,
                got: hello.protocol_version,
            });
        }
        if hello.client_kind != active.public.client_kind {
            return Err(IdentityError::PairingClientKindMismatch);
        }
        if hello.proof != active.public.method.proof() {
            return Err(IdentityError::InvalidPairingProof);
        }

        let verifying_key = VerifyingKey::from_bytes(&hello.device_signing_public)
            .map_err(|_| IdentityError::SignatureVerificationFailed)?;
        let signature_payload = pairing_signature_payload(
            hello.protocol_version,
            &hello.session_id,
            &active.public.challenge,
            &active.public.gateway_ephemeral_public,
            &hello.device_id,
            hello.client_kind,
            &hello.proof,
        );
        let signature = Signature::from_bytes(&hello.challenge_signature);
        verifying_key
            .verify(&signature_payload, &signature)
            .map_err(|_| IdentityError::SignatureVerificationFailed)?;

        let device_public = X25519PublicKey::from(hello.device_x25519_public);
        let shared_secret = active.gateway_ephemeral_secret.diffie_hellman(&device_public);
        let transcript_context = transcript_context(
            &hello.session_id,
            hello.protocol_version,
            &hello.device_id,
            hello.client_kind,
        );
        let expected_mac = derive_transcript_mac(
            shared_secret.as_bytes(),
            &active.public.challenge,
            &transcript_context,
        )?;
        if expected_mac != hello.transcript_mac {
            return Err(IdentityError::TranscriptVerificationFailed);
        }

        self.active_sessions.remove(&hello.session_id);
        let transcript_hash_hex = hex::encode(Sha256::digest(expected_mac));
        let identity_fingerprint = hex::encode(Sha256::digest(hello.device_signing_public));
        let certificate = self.ca.issue_client_certificate(
            hello.device_id.as_str(),
            identity_fingerprint.as_str(),
            self.certificate_validity,
        )?;

        let paired = PairedDevice {
            device_id: hello.device_id.clone(),
            client_kind: hello.client_kind,
            identity_fingerprint,
            signing_public_key_hex: hex::encode(hello.device_signing_public),
            transcript_hash_hex,
            current_certificate: certificate,
        };
        self.persist_paired_device(&paired)?;
        self.paired_devices.insert(hello.device_id.clone(), paired.clone());

        Ok(PairingResult {
            device: paired,
            gateway_ca_certificate_pem: self.ca.certificate_pem.clone(),
        })
    }

    pub fn force_rotate_device_certificate(
        &mut self,
        device_id: &str,
    ) -> IdentityResult<IssuedCertificate> {
        if self.revoked_devices.contains_key(device_id) {
            return Err(IdentityError::DeviceRevoked);
        }
        let paired =
            self.paired_devices.get(device_id).cloned().ok_or(IdentityError::DeviceNotPaired)?;

        let rotated = self.ca.issue_client_certificate(
            device_id,
            paired.identity_fingerprint.as_str(),
            self.certificate_validity,
        )?;
        let mut updated = paired;
        updated.current_certificate = rotated.clone();
        self.persist_paired_device(&updated)?;
        self.paired_devices.insert(device_id.to_owned(), updated);
        Ok(rotated)
    }

    pub fn rotate_device_certificate_if_due(
        &mut self,
        device_id: &str,
        now: SystemTime,
    ) -> IdentityResult<IssuedCertificate> {
        if self.revoked_devices.contains_key(device_id) {
            return Err(IdentityError::DeviceRevoked);
        }
        let paired =
            self.paired_devices.get(device_id).cloned().ok_or(IdentityError::DeviceNotPaired)?;
        if should_rotate_certificate(&paired.current_certificate, now, self.rotation_threshold)? {
            return self.force_rotate_device_certificate(device_id);
        }
        Ok(paired.current_certificate)
    }

    pub fn revoke_device(
        &mut self,
        device_id: &str,
        reason: &str,
        now: SystemTime,
    ) -> IdentityResult<()> {
        let revoked = RevokedDevice {
            device_id: device_id.to_owned(),
            reason: reason.to_owned(),
            revoked_at_unix_ms: unix_ms(now)?,
        };
        self.revoked_devices.insert(device_id.to_owned(), revoked);
        self.paired_devices.remove(device_id);
        let key = format!("paired/{device_id}/record.json");
        self.store.delete_secret(&key)
    }

    #[must_use]
    pub fn paired_device(&self, device_id: &str) -> Option<&PairedDevice> {
        self.paired_devices.get(device_id)
    }

    #[must_use]
    pub fn revoked_devices(&self) -> HashSet<String> {
        self.revoked_devices.keys().cloned().collect()
    }

    fn persist_paired_device(&self, paired: &PairedDevice) -> IdentityResult<()> {
        let key = format!("paired/{}/record.json", paired.device_id);
        let encoded = serde_json::to_vec(paired)
            .map_err(|error| IdentityError::Internal(error.to_string()))?;
        self.store.write_secret(&key, &encoded)
    }
}

pub fn should_rotate_certificate(
    certificate: &IssuedCertificate,
    now: SystemTime,
    threshold: Duration,
) -> IdentityResult<bool> {
    let now_ms = unix_ms(now)?;
    let threshold_ms: u64 = threshold
        .as_millis()
        .try_into()
        .map_err(|_| IdentityError::Internal("rotation threshold overflow".to_owned()))?;
    Ok(certificate.expires_at_unix_ms <= now_ms.saturating_add(threshold_ms))
}

fn validate_pairing_method(method: &PairingMethod) -> IdentityResult<()> {
    match method {
        PairingMethod::Pin { code } => {
            let valid = code.len() == 6 && code.chars().all(|ch| ch.is_ascii_digit());
            if !valid {
                return Err(IdentityError::InvalidPairingProof);
            }
        }
        PairingMethod::Qr { token } => {
            if token.len() < 16 || token.len() > 128 {
                return Err(IdentityError::InvalidPairingProof);
            }
        }
    }
    Ok(())
}

fn pairing_signature_payload(
    protocol_version: u32,
    session_id: &str,
    challenge: &[u8; 32],
    gateway_ephemeral_public: &[u8; 32],
    device_id: &str,
    client_kind: PairingClientKind,
    proof: &str,
) -> Vec<u8> {
    let mut payload = Vec::with_capacity(256);
    payload.extend_from_slice(b"palyra-pairing-v1");
    payload.extend_from_slice(&protocol_version.to_le_bytes());
    payload.extend_from_slice(session_id.as_bytes());
    payload.extend_from_slice(challenge);
    payload.extend_from_slice(gateway_ephemeral_public);
    payload.extend_from_slice(device_id.as_bytes());
    payload.extend_from_slice(client_kind.as_str().as_bytes());
    payload.extend_from_slice(proof.as_bytes());
    payload
}

fn transcript_context(
    session_id: &str,
    protocol_version: u32,
    device_id: &str,
    client_kind: PairingClientKind,
) -> Vec<u8> {
    let mut context = Vec::with_capacity(128);
    context.extend_from_slice(b"palyra-mtls-transcript-v1");
    context.extend_from_slice(session_id.as_bytes());
    context.extend_from_slice(&protocol_version.to_le_bytes());
    context.extend_from_slice(device_id.as_bytes());
    context.extend_from_slice(client_kind.as_str().as_bytes());
    context
}

fn derive_transcript_mac(
    shared_secret: &[u8; 32],
    challenge: &[u8; 32],
    transcript_context: &[u8],
) -> IdentityResult<[u8; 32]> {
    let hkdf = Hkdf::<Sha256>::new(Some(challenge), shared_secret);
    let mut output = [0_u8; 32];
    hkdf.expand(transcript_context, &mut output)
        .map_err(|_| IdentityError::Cryptographic("hkdf expansion failed".to_owned()))?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::DeviceIdentity;
    use proptest::prelude::*;

    fn sample_device_id() -> &'static str {
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    }

    #[test]
    fn pairing_rejects_downgrade_attempt() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
        let session = manager
            .start_pairing(
                PairingClientKind::Node,
                PairingMethod::Pin { code: "123456".to_owned() },
                SystemTime::now(),
            )
            .expect("pairing session should start");
        let mut hello =
            manager.build_device_hello(&session, &device, "123456").expect("hello should build");
        hello.protocol_version = 0;

        let result = manager.complete_pairing(hello, SystemTime::now());
        assert!(matches!(
            result,
            Err(IdentityError::PairingVersionMismatch { expected: 1, got: 0 })
        ));
    }

    proptest! {
        #[test]
        fn transcript_mac_is_symmetric(
            gateway_secret_bytes in any::<[u8; 32]>(),
            device_secret_bytes in any::<[u8; 32]>(),
            challenge in any::<[u8; 32]>(),
        ) {
            let gateway_secret = StaticSecret::from(gateway_secret_bytes);
            let device_secret = StaticSecret::from(device_secret_bytes);
            let gateway_public = X25519PublicKey::from(&gateway_secret);
            let device_public = X25519PublicKey::from(&device_secret);

            let gateway_shared = gateway_secret.diffie_hellman(&device_public);
            let device_shared = device_secret.diffie_hellman(&gateway_public);
            let context = b"prop-test-context";
            let gateway_mac = derive_transcript_mac(gateway_shared.as_bytes(), &challenge, context).expect("derive should work");
            let device_mac = derive_transcript_mac(device_shared.as_bytes(), &challenge, context).expect("derive should work");
            prop_assert_eq!(gateway_mac, device_mac);
        }
    }
}
