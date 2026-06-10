//! Data types exchanged and persisted during pairing.
//!
//! Several types carry secret material (PINs, QR tokens, MACs), so their `Debug` impls
//! are hand-written to redact it — tests pin that behavior. Serde shapes here are
//! on-disk contracts; renaming fields breaks existing identity stores.

use std::collections::{HashMap, HashSet};

use x25519_dalek::StaticSecret;

use crate::ca::{IssuedCertificate, StoredCertificateAuthority};

/// Kind of client requesting to pair; both sides must agree on it for a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PairingClientKind {
    /// Operator CLI (`palyra`).
    Cli,
    /// Desktop control center.
    Desktop,
    /// Headless node daemon.
    Node,
}

impl PairingClientKind {
    /// Returns the stable wire label baked into pairing signature payloads.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cli => "cli",
            Self::Desktop => "desktop",
            Self::Node => "node",
        }
    }
}

/// Out-of-band proof channel for pairing; the carried value is secret material.
#[derive(Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum PairingMethod {
    /// Six-digit numeric PIN shown to the operator.
    Pin { code: String },
    /// Opaque token transported via QR code (16–128 characters).
    Qr { token: String },
}

impl PairingMethod {
    pub(crate) fn proof(&self) -> &str {
        match self {
            Self::Pin { code } => code,
            Self::Qr { token } => token,
        }
    }

    /// Returns a log-safe label: the PIN is fully masked and the QR token truncated.
    #[must_use]
    pub fn display_label(&self) -> String {
        match self {
            Self::Pin { code } => format!("pin:{}", "*".repeat(code.len())),
            Self::Qr { token } => format!("qr:{}...", token.chars().take(6).collect::<String>()),
        }
    }
}

// INTENTIONAL: manual Debug emits only secret lengths, never the PIN/token itself.
impl std::fmt::Debug for PairingMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pin { code } => f.debug_struct("Pin").field("code_len", &code.len()).finish(),
            Self::Qr { token } => f.debug_struct("Qr").field("token_len", &token.len()).finish(),
        }
    }
}

/// Public half of a pending pairing session, handed to the device side.
///
/// Contains the expected proof inside [`PairingMethod`] (secret) but not the gateway's
/// ephemeral X25519 secret, which never leaves the gateway-private session record.
#[derive(Debug, Clone)]
pub struct PairingSession {
    /// Unique session identifier (ULID).
    pub session_id: String,
    /// Protocol version the session was opened with; a hello must match it exactly.
    pub protocol_version: u32,
    /// Client kind this session was opened for; a hello must match it exactly.
    pub client_kind: PairingClientKind,
    /// Proof channel (PIN/QR) the device must echo back.
    pub method: PairingMethod,
    /// Gateway's ephemeral X25519 public key for this session.
    pub gateway_ephemeral_public: [u8; 32],
    /// Random challenge the device must sign.
    pub challenge: [u8; 32],
    /// Session expiry in milliseconds since the Unix epoch.
    pub expires_at_unix_ms: u64,
}

/// Gateway-side session record: the public session plus the ephemeral DH secret.
#[derive(Clone)]
pub(super) struct ActivePairingSession {
    pub(super) public: PairingSession,
    pub(super) gateway_ephemeral_secret: StaticSecret,
}

/// Device's signed response to a [`PairingSession`], verified by the gateway.
#[derive(Clone)]
pub struct DevicePairingHello {
    /// Session this hello answers.
    pub session_id: String,
    /// Protocol version the device speaks; must equal the session's.
    pub protocol_version: u32,
    /// Canonical device identifier.
    pub device_id: String,
    /// Client kind claimed by the device; must equal the session's.
    pub client_kind: PairingClientKind,
    /// Out-of-band proof (PIN/QR value) entered on the device — secret material.
    pub proof: String,
    /// Device's Ed25519 public key.
    pub device_signing_public: [u8; 32],
    /// Device's X25519 public key for the session key agreement.
    pub device_x25519_public: [u8; 32],
    /// Ed25519 signature over the canonical pairing payload.
    pub challenge_signature: [u8; 64],
    /// HKDF transcript MAC binding the DH shared secret to the session transcript.
    pub transcript_mac: [u8; 32],
}

// INTENTIONAL: manual Debug redacts the proof and MAC material; tests pin this.
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

/// Persisted record of a successfully paired device.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PairedDevice {
    /// Canonical device identifier.
    pub device_id: String,
    /// Client kind established during pairing.
    pub client_kind: PairingClientKind,
    /// Most recently issued certificate. After deserialization its `private_key_pem`
    /// is empty (serde-skipped), which triggers reissue on the next rotation check.
    pub current_certificate: IssuedCertificate,
    /// Fingerprints of every certificate ever issued to this device, used to revoke
    /// superseded certificates on rotation, re-pairing, and revocation.
    #[serde(default)]
    pub certificate_fingerprints: Vec<String>,
}

/// Outcome of a completed pairing, including everything the device needs to connect.
#[derive(Debug, Clone)]
pub struct PairingResult {
    /// The newly persisted device record (with leaf private key still present).
    pub device: PairedDevice,
    /// Hex SHA-256 of the device signing key, for operator confirmation.
    pub identity_fingerprint: String,
    /// Hex-encoded device Ed25519 public key.
    pub signing_public_key_hex: String,
    /// Hex SHA-256 of the transcript MAC, usable for out-of-band comparison.
    pub transcript_hash_hex: String,
    /// Gateway CA certificate the device must pin as its trust root.
    pub gateway_ca_certificate_pem: String,
}

/// Cryptographically verified pairing awaiting certificate issuance.
///
/// Produced by [`verify_pairing`] and consumed by [`finalize_verified_pairing`],
/// letting callers run approval steps between verification and persistence.
///
/// [`verify_pairing`]: super::IdentityManager::verify_pairing
/// [`finalize_verified_pairing`]: super::IdentityManager::finalize_verified_pairing
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VerifiedPairing {
    /// Canonical device identifier.
    pub device_id: String,
    /// Client kind established during verification.
    pub client_kind: PairingClientKind,
    /// Hex SHA-256 of the device signing key.
    pub identity_fingerprint: String,
    /// Hex-encoded device Ed25519 public key.
    pub signing_public_key_hex: String,
    /// Hex SHA-256 of the transcript MAC.
    pub transcript_hash_hex: String,
}

/// Persisted tombstone for a revoked device; its ID can never pair again while present.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RevokedDevice {
    /// Canonical device identifier.
    pub device_id: String,
    /// Operator-supplied revocation reason.
    pub reason: String,
    /// Revocation time in milliseconds since the Unix epoch.
    pub revoked_at_unix_ms: u64,
}

/// In-memory view of all durable identity state, assembled from the store.
#[derive(Debug, Clone)]
pub(super) struct PersistedIdentityState {
    pub(super) generation: u64,
    pub(super) ca: StoredCertificateAuthority,
    pub(super) paired_devices: HashMap<String, PairedDevice>,
    pub(super) revoked_devices: HashMap<String, RevokedDevice>,
    pub(super) revoked_certificate_fingerprints: HashSet<String>,
}

/// Current on-disk bundle shape (`identity/state.v1.json`).
///
/// Deliberately excludes the CA so its private key lives only in the separately sealed
/// CA state document.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct PersistedIdentityStateBundle {
    #[serde(default)]
    pub(super) generation: u64,
    pub(super) paired_devices: HashMap<String, PairedDevice>,
    pub(super) revoked_devices: HashMap<String, RevokedDevice>,
    pub(super) revoked_certificate_fingerprints: HashSet<String>,
}

/// Pre-split bundle shape that still embedded the CA (and its private key); read-only
/// for migration in `persistence::load_identity_state_bundle`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct LegacyPersistedIdentityStateBundle {
    #[serde(default)]
    pub(super) generation: u64,
    pub(super) ca: StoredCertificateAuthority,
    pub(super) paired_devices: HashMap<String, PairedDevice>,
    pub(super) revoked_devices: HashMap<String, RevokedDevice>,
    pub(super) revoked_certificate_fingerprints: HashSet<String>,
}
