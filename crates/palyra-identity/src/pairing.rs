use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, SystemTime},
};

use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use hkdf::Hkdf;
use palyra_common::validate_canonical_id;
use rand::{rngs::OsRng, RngCore};
use rustls::pki_types::{pem::PemObject, CertificateDer};
use serde::de::DeserializeOwned;
use sha2::{Digest, Sha256};
use x25519_dalek::{PublicKey as X25519PublicKey, StaticSecret};

use crate::{
    ca::{CertificateAuthority, IssuedCertificate, StoredCertificateAuthority},
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
    #[serde(default)]
    pub certificate_fingerprints: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PairingResult {
    pub device: PairedDevice,
    pub gateway_ca_certificate_pem: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RevokedDevice {
    pub device_id: String,
    pub reason: String,
    pub revoked_at_unix_ms: u64,
}

const GATEWAY_CA_STATE_KEY: &str = "identity/ca/state.json";
const PAIRED_DEVICES_STATE_KEY: &str = "identity/pairing/paired_devices.json";
const REVOKED_DEVICES_STATE_KEY: &str = "identity/pairing/revoked_devices.json";
const REVOKED_CERTIFICATES_STATE_KEY: &str = "identity/pairing/revoked_certificates.json";
const MAX_ACTIVE_PAIRING_SESSIONS: usize = 10_000;

pub struct IdentityManager {
    store: Arc<dyn SecretStore>,
    pairing_window: Duration,
    certificate_validity: Duration,
    rotation_threshold: Duration,
    active_sessions: HashMap<String, ActivePairingSession>,
    paired_devices: HashMap<String, PairedDevice>,
    revoked_devices: HashMap<String, RevokedDevice>,
    revoked_certificate_fingerprints: HashSet<String>,
    ca: CertificateAuthority,
}

impl IdentityManager {
    pub fn with_store(store: Arc<dyn SecretStore>) -> IdentityResult<Self> {
        let ca = load_or_init_gateway_ca(store.as_ref())?;
        let paired_devices = load_paired_devices(store.as_ref())?;
        let revoked_devices = load_revoked_devices(store.as_ref())?;
        let revoked_certificate_fingerprints =
            load_revoked_certificate_fingerprints(store.as_ref())?;

        Ok(Self {
            store,
            pairing_window: DEFAULT_PAIRING_WINDOW,
            certificate_validity: DEFAULT_CERT_VALIDITY,
            rotation_threshold: DEFAULT_ROTATION_THRESHOLD,
            active_sessions: HashMap::new(),
            paired_devices,
            revoked_devices,
            revoked_certificate_fingerprints,
            ca,
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
        let issued = self.ca.issue_server_certificate(common_name, self.certificate_validity)?;
        self.persist_gateway_ca_state()?;
        Ok(issued)
    }

    fn prune_expired_sessions(
        &mut self,
        now: SystemTime,
        retain_session_id: Option<&str>,
    ) -> IdentityResult<()> {
        let now_ms = unix_ms(now)?;
        self.active_sessions.retain(|session_id, session| {
            session.public.expires_at_unix_ms > now_ms
                || retain_session_id.is_some_and(|retain| retain == session_id)
        });
        Ok(())
    }

    pub fn start_pairing(
        &mut self,
        client_kind: PairingClientKind,
        method: PairingMethod,
        now: SystemTime,
    ) -> IdentityResult<PairingSession> {
        self.prune_expired_sessions(now, None)?;
        if self.active_sessions.len() >= MAX_ACTIVE_PAIRING_SESSIONS {
            return Err(IdentityError::PairingSessionCapacityExceeded {
                limit: MAX_ACTIVE_PAIRING_SESSIONS,
            });
        }

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
        self.prune_expired_sessions(now, Some(hello.session_id.as_str()))?;
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
        if !constant_time_eq(hello.proof.as_bytes(), active.public.method.proof().as_bytes()) {
            self.active_sessions.remove(&hello.session_id);
            return Err(IdentityError::InvalidPairingProof);
        }

        let verifying_key =
            VerifyingKey::from_bytes(&hello.device_signing_public).map_err(|_| {
                self.active_sessions.remove(&hello.session_id);
                IdentityError::SignatureVerificationFailed
            })?;
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
        verifying_key.verify(&signature_payload, &signature).map_err(|_| {
            self.active_sessions.remove(&hello.session_id);
            IdentityError::SignatureVerificationFailed
        })?;

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
            self.active_sessions.remove(&hello.session_id);
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
        self.persist_gateway_ca_state()?;
        let certificate_fingerprint = certificate_fingerprint_hex(&certificate.certificate_pem)?;

        let paired = PairedDevice {
            device_id: hello.device_id.clone(),
            client_kind: hello.client_kind,
            identity_fingerprint,
            signing_public_key_hex: hex::encode(hello.device_signing_public),
            transcript_hash_hex,
            current_certificate: certificate.clone(),
            certificate_fingerprints: vec![certificate_fingerprint],
        };
        if let Some(previous) = self.paired_devices.get(&hello.device_id).cloned() {
            self.revoke_superseded_certificates(&previous)?;
            self.persist_revoked_certificate_fingerprints()?;
        }
        self.paired_devices.insert(hello.device_id.clone(), paired.clone());
        self.persist_paired_devices()?;

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
        let previous_fingerprint =
            certificate_fingerprint_hex(&paired.current_certificate.certificate_pem)?;

        let rotated = self.ca.issue_client_certificate(
            device_id,
            paired.identity_fingerprint.as_str(),
            self.certificate_validity,
        )?;
        self.persist_gateway_ca_state()?;
        let rotated_fingerprint = certificate_fingerprint_hex(&rotated.certificate_pem)?;
        let previous_fingerprints = paired.certificate_fingerprints.clone();
        let mut updated = paired;
        updated.current_certificate = rotated.clone();
        if !updated.certificate_fingerprints.contains(&rotated_fingerprint) {
            updated.certificate_fingerprints.push(rotated_fingerprint);
        }
        self.revoked_certificate_fingerprints.insert(previous_fingerprint);
        for fingerprint in previous_fingerprints {
            self.revoked_certificate_fingerprints.insert(fingerprint);
        }
        self.paired_devices.insert(device_id.to_owned(), updated);
        self.persist_revoked_certificate_fingerprints()?;
        self.persist_paired_devices()?;
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
        if let Some(paired) = self.paired_devices.remove(device_id) {
            self.revoke_superseded_certificates(&paired)?;
        }
        let revoked = RevokedDevice {
            device_id: device_id.to_owned(),
            reason: reason.to_owned(),
            revoked_at_unix_ms: unix_ms(now)?,
        };
        self.revoked_devices.insert(device_id.to_owned(), revoked);
        self.persist_paired_devices()?;
        self.persist_revoked_devices()?;
        self.persist_revoked_certificate_fingerprints()?;
        Ok(())
    }

    #[must_use]
    pub fn paired_device(&self, device_id: &str) -> Option<&PairedDevice> {
        self.paired_devices.get(device_id)
    }

    #[must_use]
    pub fn revoked_devices(&self) -> HashSet<String> {
        self.revoked_devices.keys().cloned().collect()
    }

    #[must_use]
    pub fn revoked_certificate_fingerprints(&self) -> HashSet<String> {
        self.revoked_certificate_fingerprints.clone()
    }

    fn persist_gateway_ca_state(&self) -> IdentityResult<()> {
        write_json(self.store.as_ref(), GATEWAY_CA_STATE_KEY, &self.ca.to_stored())
    }

    fn persist_paired_devices(&self) -> IdentityResult<()> {
        write_json(self.store.as_ref(), PAIRED_DEVICES_STATE_KEY, &self.paired_devices)
    }

    fn persist_revoked_devices(&self) -> IdentityResult<()> {
        write_json(self.store.as_ref(), REVOKED_DEVICES_STATE_KEY, &self.revoked_devices)
    }

    fn persist_revoked_certificate_fingerprints(&self) -> IdentityResult<()> {
        write_json(
            self.store.as_ref(),
            REVOKED_CERTIFICATES_STATE_KEY,
            &self.revoked_certificate_fingerprints,
        )
    }

    fn revoke_superseded_certificates(&mut self, paired: &PairedDevice) -> IdentityResult<()> {
        for fingerprint in &paired.certificate_fingerprints {
            self.revoked_certificate_fingerprints.insert(fingerprint.clone());
        }
        self.revoked_certificate_fingerprints
            .insert(certificate_fingerprint_hex(&paired.current_certificate.certificate_pem)?);
        Ok(())
    }
}

fn load_or_init_gateway_ca(store: &dyn SecretStore) -> IdentityResult<CertificateAuthority> {
    match store.read_secret(GATEWAY_CA_STATE_KEY) {
        Ok(raw) => {
            let state: StoredCertificateAuthority = serde_json::from_slice(&raw)
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            CertificateAuthority::from_stored(&state)
        }
        Err(IdentityError::SecretNotFound) => {
            let ca = CertificateAuthority::new("Palyra Gateway CA")?;
            write_json(store, GATEWAY_CA_STATE_KEY, &ca.to_stored())?;
            Ok(ca)
        }
        Err(error) => Err(error),
    }
}

fn load_paired_devices(store: &dyn SecretStore) -> IdentityResult<HashMap<String, PairedDevice>> {
    read_json_or_default(store, PAIRED_DEVICES_STATE_KEY)
}

fn load_revoked_devices(store: &dyn SecretStore) -> IdentityResult<HashMap<String, RevokedDevice>> {
    read_json_or_default(store, REVOKED_DEVICES_STATE_KEY)
}

fn load_revoked_certificate_fingerprints(
    store: &dyn SecretStore,
) -> IdentityResult<HashSet<String>> {
    read_json_or_default(store, REVOKED_CERTIFICATES_STATE_KEY)
}

fn read_json_or_default<T>(store: &dyn SecretStore, key: &str) -> IdentityResult<T>
where
    T: DeserializeOwned + Default,
{
    match store.read_secret(key) {
        Ok(raw) => {
            serde_json::from_slice(&raw).map_err(|error| IdentityError::Internal(error.to_string()))
        }
        Err(IdentityError::SecretNotFound) => Ok(T::default()),
        Err(error) => Err(error),
    }
}

fn write_json<T>(store: &dyn SecretStore, key: &str, value: &T) -> IdentityResult<()>
where
    T: serde::Serialize,
{
    let encoded =
        serde_json::to_vec(value).map_err(|error| IdentityError::Internal(error.to_string()))?;
    store.write_secret(key, &encoded)
}

fn certificate_fingerprint_hex(certificate_pem: &str) -> IdentityResult<String> {
    let der = CertificateDer::from_pem_slice(certificate_pem.as_bytes())
        .map_err(|_| IdentityError::CertificateParsingFailed)?;
    Ok(hex::encode(Sha256::digest(der.as_ref())))
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

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut diff = left.len() ^ right.len();
    let max_len = left.len().max(right.len());

    for index in 0..max_len {
        let left_byte = left.get(index).copied().unwrap_or(0);
        let right_byte = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left_byte ^ right_byte);
    }

    diff == 0
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::device::DeviceIdentity;
    use proptest::prelude::*;

    fn sample_device_id() -> &'static str {
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    }

    fn start_pin_pairing(
        manager: &mut IdentityManager,
        proof: &str,
    ) -> (DeviceIdentity, PairingSession, DevicePairingHello) {
        let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
        let session = manager
            .start_pairing(
                PairingClientKind::Node,
                PairingMethod::Pin { code: "123456".to_owned() },
                SystemTime::now(),
            )
            .expect("pairing session should start");
        let hello =
            manager.build_device_hello(&session, &device, proof).expect("hello should build");
        (device, session, hello)
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

    #[test]
    fn failed_proof_invalidates_pairing_session() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let (_, session, wrong_hello) = start_pin_pairing(&mut manager, "000000");

        let first = manager.complete_pairing(wrong_hello, SystemTime::now());
        assert!(matches!(first, Err(IdentityError::InvalidPairingProof)));

        let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
        let retry = manager
            .build_device_hello(&session, &device, "123456")
            .expect("retry hello should build");
        let second = manager.complete_pairing(retry, SystemTime::now());
        assert!(matches!(second, Err(IdentityError::PairingSessionNotFound)));
    }

    #[test]
    fn failed_signature_invalidates_pairing_session() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let (_, session, mut wrong_hello) = start_pin_pairing(&mut manager, "123456");
        wrong_hello.challenge_signature[0] ^= 0x01;

        let first = manager.complete_pairing(wrong_hello, SystemTime::now());
        assert!(matches!(first, Err(IdentityError::SignatureVerificationFailed)));

        let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
        let retry = manager
            .build_device_hello(&session, &device, "123456")
            .expect("retry hello should build");
        let second = manager.complete_pairing(retry, SystemTime::now());
        assert!(matches!(second, Err(IdentityError::PairingSessionNotFound)));
    }

    #[test]
    fn failed_transcript_mac_invalidates_pairing_session() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let (_, session, mut wrong_hello) = start_pin_pairing(&mut manager, "123456");
        wrong_hello.transcript_mac[0] ^= 0x01;

        let first = manager.complete_pairing(wrong_hello, SystemTime::now());
        assert!(matches!(first, Err(IdentityError::TranscriptVerificationFailed)));

        let device = DeviceIdentity::generate(sample_device_id()).expect("device should generate");
        let retry = manager
            .build_device_hello(&session, &device, "123456")
            .expect("retry hello should build");
        let second = manager.complete_pairing(retry, SystemTime::now());
        assert!(matches!(second, Err(IdentityError::PairingSessionNotFound)));
    }

    #[test]
    fn constant_time_eq_returns_true_for_equal_inputs() {
        assert!(constant_time_eq(b"123456", b"123456"));
    }

    #[test]
    fn constant_time_eq_returns_false_for_different_same_length_inputs() {
        assert!(!constant_time_eq(b"123456", b"123457"));
    }

    #[test]
    fn constant_time_eq_returns_false_for_different_length_inputs() {
        assert!(!constant_time_eq(b"123456", b"1234567"));
    }

    #[test]
    fn start_pairing_prunes_expired_sessions() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        manager.set_pairing_window(Duration::from_millis(1));
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        let expired = manager
            .start_pairing(
                PairingClientKind::Node,
                PairingMethod::Pin { code: "123456".to_owned() },
                now,
            )
            .expect("expired candidate should be created");

        let fresh = manager
            .start_pairing(
                PairingClientKind::Node,
                PairingMethod::Pin { code: "123456".to_owned() },
                now + Duration::from_secs(1),
            )
            .expect("fresh session should be created");

        assert!(!manager.active_sessions.contains_key(expired.session_id.as_str()));
        assert!(manager.active_sessions.contains_key(fresh.session_id.as_str()));
        assert_eq!(manager.active_sessions.len(), 1);
    }

    #[test]
    fn start_pairing_enforces_active_session_capacity() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        let seed = manager
            .start_pairing(
                PairingClientKind::Node,
                PairingMethod::Pin { code: "123456".to_owned() },
                now,
            )
            .expect("seed session should be created");
        let seed_active = manager
            .active_sessions
            .get(seed.session_id.as_str())
            .cloned()
            .expect("seed session should be in active set");

        manager.active_sessions.clear();
        for index in 0..MAX_ACTIVE_PAIRING_SESSIONS {
            manager.active_sessions.insert(format!("session-{index}"), seed_active.clone());
        }

        let result = manager.start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            now,
        );

        assert!(matches!(
            result,
            Err(IdentityError::PairingSessionCapacityExceeded {
                limit: MAX_ACTIVE_PAIRING_SESSIONS
            })
        ));
    }

    #[test]
    fn start_pairing_succeeds_when_under_capacity() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000);

        for offset in 0..32 {
            manager
                .start_pairing(
                    PairingClientKind::Node,
                    PairingMethod::Pin { code: "123456".to_owned() },
                    now + Duration::from_secs(offset),
                )
                .expect("session should be created while under cap");
        }
    }

    #[test]
    fn identity_state_is_loaded_from_secret_store() {
        let store = Arc::new(InMemorySecretStore::new());
        let mut first =
            IdentityManager::with_store(store.clone()).expect("manager should initialize");
        let ca_before = first.gateway_ca_certificate_pem();
        let (_, _, hello) = start_pin_pairing(&mut first, "123456");
        let paired =
            first.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");

        drop(first);
        let second = IdentityManager::with_store(store).expect("manager should reload from store");

        assert_eq!(second.gateway_ca_certificate_pem(), ca_before);
        let restored =
            second.paired_device(sample_device_id()).expect("paired device should be rehydrated");
        assert_eq!(
            restored.current_certificate.sequence,
            paired.device.current_certificate.sequence
        );
        assert!(
            restored.current_certificate.private_key_pem.is_empty(),
            "private key must not be persisted in paired device records"
        );
        assert_eq!(restored.certificate_fingerprints.len(), 1);
    }

    #[test]
    fn revocation_state_is_loaded_from_secret_store() {
        let store = Arc::new(InMemorySecretStore::new());
        let mut first =
            IdentityManager::with_store(store.clone()).expect("manager should initialize");
        let (_, _, hello) = start_pin_pairing(&mut first, "123456");
        first.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
        first
            .revoke_device(sample_device_id(), "test", SystemTime::now())
            .expect("revocation should succeed");

        let revoked_fingerprints = first.revoked_certificate_fingerprints();
        assert!(!revoked_fingerprints.is_empty(), "revoked cert index should contain certificate");

        drop(first);
        let second = IdentityManager::with_store(store).expect("manager should reload from store");
        assert!(second.revoked_devices().contains(sample_device_id()));
        assert_eq!(second.revoked_certificate_fingerprints(), revoked_fingerprints);
    }

    #[test]
    fn rotate_device_certificate_revokes_previous_fingerprint() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let (_, _, hello) = start_pin_pairing(&mut manager, "123456");
        let paired =
            manager.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
        let previous_fingerprint =
            certificate_fingerprint_hex(&paired.device.current_certificate.certificate_pem)
                .expect("previous certificate fingerprint should parse");

        let rotated = manager
            .force_rotate_device_certificate(sample_device_id())
            .expect("certificate rotation should succeed");
        let rotated_fingerprint = certificate_fingerprint_hex(&rotated.certificate_pem)
            .expect("rotated certificate fingerprint should parse");

        let revoked = manager.revoked_certificate_fingerprints();
        assert!(
            revoked.contains(&previous_fingerprint),
            "previous certificate fingerprint must be revoked"
        );
        assert!(
            !revoked.contains(&rotated_fingerprint),
            "active rotated certificate fingerprint must remain valid"
        );
    }

    #[test]
    fn repairing_same_device_revokes_superseded_certificate() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let (_, _, first_hello) = start_pin_pairing(&mut manager, "123456");
        let first_pairing = manager
            .complete_pairing(first_hello, SystemTime::now())
            .expect("first pairing should complete");
        let first_fingerprint =
            certificate_fingerprint_hex(&first_pairing.device.current_certificate.certificate_pem)
                .expect("first certificate fingerprint should parse");

        let replacement_device = DeviceIdentity::generate(sample_device_id())
            .expect("replacement device should generate");
        let replacement_session = manager
            .start_pairing(
                PairingClientKind::Node,
                PairingMethod::Pin { code: "123456".to_owned() },
                SystemTime::now(),
            )
            .expect("replacement pairing session should start");
        let replacement_hello = manager
            .build_device_hello(&replacement_session, &replacement_device, "123456")
            .expect("replacement hello should build");
        let second_pairing = manager
            .complete_pairing(replacement_hello, SystemTime::now())
            .expect("replacement pairing should complete");
        let second_fingerprint =
            certificate_fingerprint_hex(&second_pairing.device.current_certificate.certificate_pem)
                .expect("second certificate fingerprint should parse");

        let revoked = manager.revoked_certificate_fingerprints();
        assert!(
            revoked.contains(&first_fingerprint),
            "superseded certificate fingerprint must be revoked after re-pair"
        );
        assert!(
            !revoked.contains(&second_fingerprint),
            "newly paired certificate fingerprint must remain valid"
        );
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
