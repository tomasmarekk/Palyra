use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
    thread,
    time::{Duration, Instant, SystemTime},
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
    store::{FilesystemSecretStore, InMemorySecretStore, SecretStore},
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

#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

impl std::fmt::Debug for PairingMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pin { code } => f.debug_struct("Pin").field("code_len", &code.len()).finish(),
            Self::Qr { token } => f.debug_struct("Qr").field("token_len", &token.len()).finish(),
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

#[derive(Clone)]
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

impl std::fmt::Debug for DevicePairingHello {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DevicePairingHello")
            .field("session_id", &self.session_id)
            .field("protocol_version", &self.protocol_version)
            .field("device_id", &self.device_id)
            .field("client_kind", &self.client_kind)
            .field("proof_len", &self.proof.len())
            .field("device_signing_public", &self.device_signing_public)
            .field("device_x25519_public", &self.device_x25519_public)
            .field("challenge_signature", &"<redacted>")
            .field("transcript_mac", &"<redacted>")
            .finish()
    }
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedIdentityState {
    #[serde(default)]
    generation: u64,
    ca: StoredCertificateAuthority,
    paired_devices: HashMap<String, PairedDevice>,
    revoked_devices: HashMap<String, RevokedDevice>,
    revoked_certificate_fingerprints: HashSet<String>,
}

const IDENTITY_STATE_BUNDLE_KEY: &str = "identity/state.v1.json";
const GATEWAY_CA_STATE_KEY: &str = "identity/ca/state.json";
const PAIRED_DEVICES_STATE_KEY: &str = "identity/pairing/paired_devices.json";
const REVOKED_DEVICES_STATE_KEY: &str = "identity/pairing/revoked_devices.json";
const REVOKED_CERTIFICATES_STATE_KEY: &str = "identity/pairing/revoked_certificates.json";
const MAX_ACTIVE_PAIRING_SESSIONS: usize = 10_000;
const IDENTITY_STATE_LOCK_FILENAME: &str = ".identity-state.lock";
const IDENTITY_STATE_LOCK_TIMEOUT: Duration = Duration::from_secs(3);
const IDENTITY_STATE_LOCK_RETRY: Duration = Duration::from_millis(20);
const IDENTITY_STATE_STALE_LOCK_AGE: Duration = Duration::from_secs(30);

static IDENTITY_STATE_PROCESS_LOCK: Mutex<()> = Mutex::new(());

struct FilesystemStateLockGuard {
    path: PathBuf,
}

impl Drop for FilesystemStateLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

struct StateMutationGuard {
    _process: MutexGuard<'static, ()>,
    _filesystem: Option<FilesystemStateLockGuard>,
}

pub struct IdentityManager {
    store: Arc<dyn SecretStore>,
    pairing_window: Duration,
    certificate_validity: Duration,
    rotation_threshold: Duration,
    active_sessions: HashMap<String, ActivePairingSession>,
    paired_devices: HashMap<String, PairedDevice>,
    revoked_devices: HashMap<String, RevokedDevice>,
    revoked_certificate_fingerprints: HashSet<String>,
    state_generation: u64,
    ca: CertificateAuthority,
}

impl IdentityManager {
    pub fn with_store(store: Arc<dyn SecretStore>) -> IdentityResult<Self> {
        let (state, loaded_from_bundle) = load_identity_state(store.as_ref())?;
        let ca = CertificateAuthority::from_stored(&state.ca)?;

        let mut manager = Self {
            store,
            pairing_window: DEFAULT_PAIRING_WINDOW,
            certificate_validity: DEFAULT_CERT_VALIDITY,
            rotation_threshold: DEFAULT_ROTATION_THRESHOLD,
            active_sessions: HashMap::new(),
            paired_devices: state.paired_devices,
            revoked_devices: state.revoked_devices,
            revoked_certificate_fingerprints: state.revoked_certificate_fingerprints,
            state_generation: state.generation,
            ca,
        };
        if !loaded_from_bundle {
            manager.persist_identity_state_bundle()?;
        }

        Ok(manager)
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

    fn mutate_persisted_state<T>(
        &mut self,
        operation: impl FnOnce(&mut Self) -> IdentityResult<T>,
    ) -> IdentityResult<T> {
        let _guard = self.acquire_state_mutation_guard()?;
        self.reload_persisted_state()?;
        let result = operation(self)?;
        self.persist_identity_state_bundle()?;
        Ok(result)
    }

    fn acquire_state_mutation_guard(&self) -> IdentityResult<StateMutationGuard> {
        let process = IDENTITY_STATE_PROCESS_LOCK.lock().map_err(|_| {
            IdentityError::Internal("identity state process lock poisoned".to_owned())
        })?;
        let filesystem = self.acquire_filesystem_state_lock()?;
        Ok(StateMutationGuard { _process: process, _filesystem: filesystem })
    }

    fn acquire_filesystem_state_lock(&self) -> IdentityResult<Option<FilesystemStateLockGuard>> {
        let Some(store) = self.store.as_any().downcast_ref::<FilesystemSecretStore>() else {
            return Ok(None);
        };
        let lock_path = store.root_path().join(IDENTITY_STATE_LOCK_FILENAME);
        let start = Instant::now();
        loop {
            match fs::OpenOptions::new().create_new(true).write(true).open(&lock_path) {
                Ok(mut file) => {
                    let marker = format!(
                        "pid={} ts_ms={}\n",
                        std::process::id(),
                        unix_ms(SystemTime::now())?
                    );
                    file.write_all(marker.as_bytes())
                        .map_err(|error| IdentityError::Internal(error.to_string()))?;
                    file.sync_all().map_err(|error| IdentityError::Internal(error.to_string()))?;
                    return Ok(Some(FilesystemStateLockGuard { path: lock_path }));
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    if try_reclaim_stale_filesystem_lock(
                        &lock_path,
                        SystemTime::now(),
                        IDENTITY_STATE_STALE_LOCK_AGE,
                    )? {
                        continue;
                    }
                    if start.elapsed() >= IDENTITY_STATE_LOCK_TIMEOUT {
                        return Err(IdentityError::Internal(format!(
                            "timed out waiting for identity state lock at {} (lock stealing disabled to prevent state corruption; remove stale lock file if no process owns it)",
                            lock_path.display()
                        )));
                    }
                    thread::sleep(IDENTITY_STATE_LOCK_RETRY);
                }
                Err(error) => return Err(IdentityError::Internal(error.to_string())),
            }
        }
    }

    fn reload_persisted_state(&mut self) -> IdentityResult<()> {
        let state = if let Some(state) = load_identity_state_bundle(self.store.as_ref())? {
            state
        } else {
            let (state, _) = load_identity_state(self.store.as_ref())?;
            state
        };
        self.apply_persisted_state(state)
    }

    fn apply_persisted_state(&mut self, state: PersistedIdentityState) -> IdentityResult<()> {
        self.ca = CertificateAuthority::from_stored(&state.ca)?;
        self.paired_devices = state.paired_devices;
        self.revoked_devices = state.revoked_devices;
        self.revoked_certificate_fingerprints = state.revoked_certificate_fingerprints;
        self.state_generation = state.generation;
        Ok(())
    }
    pub fn issue_gateway_server_certificate(
        &mut self,
        common_name: &str,
    ) -> IdentityResult<IssuedCertificate> {
        self.mutate_persisted_state(|manager| {
            manager.ca.issue_server_certificate(common_name, manager.certificate_validity)
        })
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
        self.mutate_persisted_state(|manager| manager.complete_pairing_inner(hello, now))
    }

    fn complete_pairing_inner(
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
        if !constant_time_eq(expected_mac.as_slice(), hello.transcript_mac.as_slice()) {
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
        }
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
        self.mutate_persisted_state(|manager| {
            manager.force_rotate_device_certificate_inner(device_id)
        })
    }

    fn force_rotate_device_certificate_inner(
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
        Ok(rotated)
    }

    pub fn rotate_device_certificate_if_due(
        &mut self,
        device_id: &str,
        now: SystemTime,
    ) -> IdentityResult<IssuedCertificate> {
        let _guard = self.acquire_state_mutation_guard()?;
        self.reload_persisted_state()?;
        if self.revoked_devices.contains_key(device_id) {
            return Err(IdentityError::DeviceRevoked);
        }
        let paired =
            self.paired_devices.get(device_id).cloned().ok_or(IdentityError::DeviceNotPaired)?;
        if paired.current_certificate.private_key_pem.is_empty() {
            let rotated = self.force_rotate_device_certificate_inner(device_id)?;
            self.persist_identity_state_bundle()?;
            return Ok(rotated);
        }
        if should_rotate_certificate(&paired.current_certificate, now, self.rotation_threshold)? {
            let rotated = self.force_rotate_device_certificate_inner(device_id)?;
            self.persist_identity_state_bundle()?;
            return Ok(rotated);
        }
        Ok(paired.current_certificate)
    }

    pub fn revoke_device(
        &mut self,
        device_id: &str,
        reason: &str,
        now: SystemTime,
    ) -> IdentityResult<()> {
        self.mutate_persisted_state(|manager| {
            if let Some(paired) = manager.paired_devices.remove(device_id) {
                manager.revoke_superseded_certificates(&paired)?;
            }
            let revoked = RevokedDevice {
                device_id: device_id.to_owned(),
                reason: reason.to_owned(),
                revoked_at_unix_ms: unix_ms(now)?,
            };
            manager.revoked_devices.insert(device_id.to_owned(), revoked);
            Ok(())
        })
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

    fn persist_identity_state_bundle(&mut self) -> IdentityResult<()> {
        let next_generation = self.state_generation.saturating_add(1);
        let state = PersistedIdentityState {
            generation: next_generation,
            ca: self.ca.to_stored(),
            paired_devices: self.paired_devices.clone(),
            revoked_devices: self.revoked_devices.clone(),
            revoked_certificate_fingerprints: self.revoked_certificate_fingerprints.clone(),
        };
        write_json(self.store.as_ref(), IDENTITY_STATE_BUNDLE_KEY, &state)?;
        self.state_generation = next_generation;
        Ok(())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FilesystemLockMarker {
    pid: u32,
    ts_ms: u64,
}

fn try_reclaim_stale_filesystem_lock(
    lock_path: &Path,
    now: SystemTime,
    stale_age: Duration,
) -> IdentityResult<bool> {
    let marker_raw = match fs::read_to_string(lock_path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(_) => return Ok(false),
    };
    let Some(marker) = parse_filesystem_lock_marker(&marker_raw) else {
        if !lock_file_age_is_stale(lock_path, now, stale_age)? {
            return Ok(false);
        }
        return remove_filesystem_lock_file(lock_path);
    };
    if !lock_marker_is_stale(marker, now, stale_age)? {
        return Ok(false);
    }
    if process_is_alive(marker.pid) {
        return Ok(false);
    }
    remove_filesystem_lock_file(lock_path)
}

fn parse_filesystem_lock_marker(raw: &str) -> Option<FilesystemLockMarker> {
    let mut pid = None;
    let mut ts_ms = None;
    for part in raw.split_whitespace() {
        if let Some(value) = part.strip_prefix("pid=") {
            pid = value.parse::<u32>().ok();
            continue;
        }
        if let Some(value) = part.strip_prefix("ts_ms=") {
            ts_ms = value.parse::<u64>().ok();
        }
    }
    Some(FilesystemLockMarker { pid: pid?, ts_ms: ts_ms? })
}

fn lock_marker_is_stale(
    marker: FilesystemLockMarker,
    now: SystemTime,
    stale_age: Duration,
) -> IdentityResult<bool> {
    let now_ms = unix_ms(now)?;
    let stale_age_ms = u64::try_from(stale_age.as_millis()).map_err(|_| {
        IdentityError::Internal("identity state stale lock age overflow".to_owned())
    })?;
    Ok(now_ms.saturating_sub(marker.ts_ms) >= stale_age_ms)
}

fn lock_file_age_is_stale(
    lock_path: &Path,
    now: SystemTime,
    stale_age: Duration,
) -> IdentityResult<bool> {
    let metadata = match fs::metadata(lock_path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(_) => return Ok(false),
    };
    let modified = match metadata.modified() {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };
    Ok(now.duration_since(modified).unwrap_or_default() >= stale_age)
}

fn remove_filesystem_lock_file(lock_path: &Path) -> IdentityResult<bool> {
    match fs::remove_file(lock_path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(_) => Ok(false),
    }
}

#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    let Ok(pid_i32) = i32::try_from(pid) else {
        return true;
    };
    // SAFETY: calling `kill(pid, 0)` only probes process existence/permission and does not send
    // a signal. Inputs are validated above.
    let result = unsafe { libc::kill(pid_i32, 0) };
    if result == 0 {
        return true;
    }
    match std::io::Error::last_os_error().raw_os_error() {
        Some(code) if code == libc::ESRCH => false,
        Some(code) if code == libc::EPERM => true,
        _ => true,
    }
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    true
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

fn load_identity_state(store: &dyn SecretStore) -> IdentityResult<(PersistedIdentityState, bool)> {
    if let Some(bundle) = load_identity_state_bundle(store)? {
        return Ok((bundle, true));
    }

    let ca = load_or_init_gateway_ca(store)?;
    let paired_devices = read_json_or_default(store, PAIRED_DEVICES_STATE_KEY)?;
    let revoked_devices = read_json_or_default(store, REVOKED_DEVICES_STATE_KEY)?;
    let revoked_certificate_fingerprints =
        read_json_or_default(store, REVOKED_CERTIFICATES_STATE_KEY)?;

    Ok((
        PersistedIdentityState {
            generation: 0,
            ca: ca.to_stored(),
            paired_devices,
            revoked_devices,
            revoked_certificate_fingerprints,
        },
        false,
    ))
}

fn load_identity_state_bundle(
    store: &dyn SecretStore,
) -> IdentityResult<Option<PersistedIdentityState>> {
    match store.read_secret(IDENTITY_STATE_BUNDLE_KEY) {
        Ok(raw) => {
            let state: PersistedIdentityState = serde_json::from_slice(&raw)
                .map_err(|error| IdentityError::Internal(error.to_string()))?;
            Ok(Some(state))
        }
        Err(IdentityError::SecretNotFound) => Ok(None),
        Err(error) => Err(error),
    }
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
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::SystemTime,
    };
    #[cfg(not(windows))]
    use std::{fs, time::Instant};

    use super::*;
    use crate::{device::DeviceIdentity, store::SecretStore};
    use proptest::prelude::*;
    #[cfg(not(windows))]
    use tempfile::TempDir;

    struct ToggleFailSecretStore {
        state: Mutex<HashMap<String, Vec<u8>>>,
        fail_writes: Mutex<bool>,
    }

    impl ToggleFailSecretStore {
        fn new() -> Self {
            Self { state: Mutex::new(HashMap::new()), fail_writes: Mutex::new(false) }
        }

        fn set_fail_writes(&self, value: bool) {
            let mut guard =
                self.fail_writes.lock().expect("fail_writes lock should not be poisoned");
            *guard = value;
        }
    }

    impl SecretStore for ToggleFailSecretStore {
        fn write_secret(&self, key: &str, value: &[u8]) -> IdentityResult<()> {
            let fail_writes = self.fail_writes.lock().map_err(|_| {
                IdentityError::Internal("store fail_writes lock poisoned".to_owned())
            })?;
            if *fail_writes {
                return Err(IdentityError::Internal("injected write failure".to_owned()));
            }
            drop(fail_writes);

            let mut state = self
                .state
                .lock()
                .map_err(|_| IdentityError::Internal("store state lock poisoned".to_owned()))?;
            state.insert(key.to_owned(), value.to_vec());
            Ok(())
        }

        fn read_secret(&self, key: &str) -> IdentityResult<Vec<u8>> {
            let state = self
                .state
                .lock()
                .map_err(|_| IdentityError::Internal("store state lock poisoned".to_owned()))?;
            state.get(key).cloned().ok_or(IdentityError::SecretNotFound)
        }

        fn delete_secret(&self, key: &str) -> IdentityResult<()> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| IdentityError::Internal("store state lock poisoned".to_owned()))?;
            state.remove(key);
            Ok(())
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    fn sample_device_id() -> &'static str {
        "01ARZ3NDEKTSV4RRFFQ69G5FAV"
    }

    fn sample_second_device_id() -> &'static str {
        "01ARZ3NDEKTSV4RRFFQ69G5FAW"
    }

    fn start_pin_pairing(
        manager: &mut IdentityManager,
        proof: &str,
    ) -> (DeviceIdentity, PairingSession, DevicePairingHello) {
        start_pin_pairing_for_device(manager, sample_device_id(), proof)
    }

    fn start_pin_pairing_for_device(
        manager: &mut IdentityManager,
        device_id: &str,
        proof: &str,
    ) -> (DeviceIdentity, PairingSession, DevicePairingHello) {
        let device = DeviceIdentity::generate(device_id).expect("device should generate");
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
    fn pairing_method_debug_redacts_pin_secret_material() {
        let formatted = format!("{:?}", PairingMethod::Pin { code: "123456".to_owned() });
        assert!(!formatted.contains("123456"), "debug output must not contain raw pin");
        assert!(formatted.contains("code_len"), "debug output should retain diagnostic shape");
    }

    #[test]
    fn pairing_session_debug_redacts_embedded_method_secret_material() {
        let session = PairingSession {
            session_id: "session-test".to_owned(),
            protocol_version: 1,
            client_kind: PairingClientKind::Desktop,
            method: PairingMethod::Qr { token: "SECRET_TOKEN_VALUE".to_owned() },
            gateway_ephemeral_public: [0_u8; 32],
            challenge: [1_u8; 32],
            expires_at_unix_ms: 1,
        };
        let formatted = format!("{session:?}");
        assert!(!formatted.contains("SECRET_TOKEN_VALUE"), "debug output must redact raw token");
        assert!(formatted.contains("token_len"), "debug output should preserve safe diagnostics");
    }

    #[test]
    fn device_pairing_hello_debug_redacts_secret_material() {
        let mut manager = IdentityManager::with_memory_store().expect("manager should initialize");
        let (_, _, hello) = start_pin_pairing(&mut manager, "123456");

        let formatted = format!("{hello:?}");
        assert!(!formatted.contains("123456"), "debug output must redact proof");
        assert!(formatted.contains("<redacted>"), "sensitive binary fields should be redacted");
        assert!(formatted.contains("proof_len"), "debug output should preserve safe diagnostics");
    }

    #[cfg(not(windows))]
    #[test]
    fn filesystem_lock_is_not_reclaimed_when_owner_pid_is_live() {
        let root = TempDir::new().expect("temp directory should be created");
        let store = Arc::new(
            FilesystemSecretStore::new(root.path())
                .expect("filesystem secret store should initialize"),
        );
        let mut manager = IdentityManager::with_store(store)
            .expect("manager with filesystem store should initialize");
        let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
        fs::write(&lock_path, format!("pid={} ts_ms=0\n", std::process::id()))
            .expect("lock marker should be written");

        let started = Instant::now();
        let result = manager.issue_gateway_server_certificate("localhost");
        assert!(result.is_err(), "operation should fail when lock cannot be acquired");
        assert!(lock_path.exists(), "existing lock file must not be force deleted");
        assert!(
            started.elapsed() >= IDENTITY_STATE_LOCK_TIMEOUT,
            "lock acquisition should wait for timeout before failing"
        );
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_lock_reclaims_stale_dead_owner_marker() {
        let root = TempDir::new().expect("temp directory should be created");
        let store = Arc::new(
            FilesystemSecretStore::new(root.path())
                .expect("filesystem secret store should initialize"),
        );
        let mut manager = IdentityManager::with_store(store)
            .expect("manager with filesystem store should initialize");
        let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
        fs::write(&lock_path, format!("pid={} ts_ms=0\n", i32::MAX))
            .expect("stale lock marker should be written");

        let issued = manager.issue_gateway_server_certificate("localhost");
        assert!(issued.is_ok(), "stale dead lock marker should be reclaimed");
        assert!(!lock_path.exists(), "stale lock file should be removed after successful mutation");
    }

    #[cfg(not(windows))]
    #[test]
    fn malformed_recent_filesystem_lock_marker_is_not_reclaimed() {
        let root = TempDir::new().expect("temp directory should be created");
        let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
        fs::write(&lock_path, "invalid lock marker payload")
            .expect("malformed lock marker should be written");

        let reclaimed = try_reclaim_stale_filesystem_lock(
            &lock_path,
            SystemTime::now(),
            Duration::from_secs(60),
        )
        .expect("reclaim evaluation should succeed");
        assert!(!reclaimed, "fresh malformed lock marker should not be reclaimed");
        assert!(lock_path.exists(), "fresh malformed lock marker must remain in place");
    }

    #[cfg(not(windows))]
    #[test]
    fn malformed_stale_filesystem_lock_marker_is_reclaimed() {
        let root = TempDir::new().expect("temp directory should be created");
        let lock_path = root.path().join(IDENTITY_STATE_LOCK_FILENAME);
        fs::write(&lock_path, "invalid lock marker payload")
            .expect("malformed lock marker should be written");

        let reclaimed =
            try_reclaim_stale_filesystem_lock(&lock_path, SystemTime::now(), Duration::ZERO)
                .expect("reclaim evaluation should succeed");
        assert!(reclaimed, "stale malformed lock marker should be reclaimed");
        assert!(!lock_path.exists(), "stale malformed lock marker should be removed");
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
    fn stale_manager_instance_does_not_clobber_existing_pairings() {
        let store = Arc::new(InMemorySecretStore::new());
        let mut first =
            IdentityManager::with_store(store.clone()).expect("first manager should initialize");
        let mut stale =
            IdentityManager::with_store(store.clone()).expect("second manager should initialize");

        let (_, _, first_hello) =
            start_pin_pairing_for_device(&mut first, sample_device_id(), "123456");
        first
            .complete_pairing(first_hello, SystemTime::now())
            .expect("first pairing should complete");

        let (_, _, second_hello) =
            start_pin_pairing_for_device(&mut stale, sample_second_device_id(), "123456");
        stale
            .complete_pairing(second_hello, SystemTime::now())
            .expect("stale manager pairing should still preserve existing state");

        let reloaded = IdentityManager::with_store(store).expect("manager should reload");
        assert!(
            reloaded.paired_device(sample_device_id()).is_some(),
            "first pairing must remain present after stale manager write"
        );
        assert!(
            reloaded.paired_device(sample_second_device_id()).is_some(),
            "second pairing must be persisted"
        );
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

    #[test]
    fn rotate_if_due_reissues_certificate_when_private_key_is_not_persisted() {
        let store = Arc::new(InMemorySecretStore::new());
        let mut first =
            IdentityManager::with_store(store.clone()).expect("manager should initialize");
        let (_, _, hello) = start_pin_pairing(&mut first, "123456");
        let pairing =
            first.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
        drop(first);

        let mut second =
            IdentityManager::with_store(store).expect("manager should reload from store");
        let before = second
            .paired_device(sample_device_id())
            .expect("paired device should be available")
            .current_certificate
            .clone();
        assert!(
            before.private_key_pem.is_empty(),
            "private key should be absent after state rehydration"
        );
        let before_fingerprint =
            certificate_fingerprint_hex(&before.certificate_pem).expect("fingerprint should parse");

        let rotated = second
            .rotate_device_certificate_if_due(sample_device_id(), SystemTime::now())
            .expect("certificate should be reissued when private key is unavailable");
        assert!(
            !rotated.private_key_pem.is_empty(),
            "rotated certificate must include private key"
        );
        assert!(
            rotated.sequence > pairing.device.current_certificate.sequence,
            "reissued certificate sequence must advance"
        );
        assert!(
            second.revoked_certificate_fingerprints().contains(&before_fingerprint),
            "previous certificate should be revoked after keyless reissue"
        );
    }

    #[test]
    fn rotate_if_due_reloads_revocation_state_before_returning_cached_certificate() {
        let store = Arc::new(InMemorySecretStore::new());
        let mut stale =
            IdentityManager::with_store(store.clone()).expect("manager should initialize");
        let (_, _, hello) = start_pin_pairing(&mut stale, "123456");
        stale.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");
        let mut revoker =
            IdentityManager::with_store(store.clone()).expect("revoker should initialize");
        revoker
            .revoke_device(sample_device_id(), "operator revoked", SystemTime::now())
            .expect("revocation should succeed");

        let result = stale.rotate_device_certificate_if_due(sample_device_id(), SystemTime::now());
        assert!(
            matches!(result, Err(IdentityError::DeviceRevoked)),
            "rotate_if_due must refresh persisted revocation state before returning a certificate"
        );
    }

    #[test]
    fn failed_bundle_write_does_not_persist_partial_rotation_state() {
        let store = Arc::new(ToggleFailSecretStore::new());
        let mut manager =
            IdentityManager::with_store(store.clone()).expect("manager should initialize");
        let (_, _, hello) = start_pin_pairing(&mut manager, "123456");
        let pairing =
            manager.complete_pairing(hello, SystemTime::now()).expect("pairing should complete");

        let baseline_sequence = pairing.device.current_certificate.sequence;
        let baseline_revoked = manager.revoked_certificate_fingerprints();

        store.set_fail_writes(true);
        let rotation = manager.force_rotate_device_certificate(sample_device_id());
        assert!(rotation.is_err(), "rotation should fail when bundle persistence fails");
        store.set_fail_writes(false);

        let reloaded =
            IdentityManager::with_store(store).expect("manager should reload from persisted state");
        let restored = reloaded
            .paired_device(sample_device_id())
            .expect("paired device should remain persisted");
        assert_eq!(
            restored.current_certificate.sequence, baseline_sequence,
            "failed write must not persist rotated certificate sequence"
        );
        assert_eq!(
            reloaded.revoked_certificate_fingerprints(),
            baseline_revoked,
            "failed write must not persist revocation side effects"
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
