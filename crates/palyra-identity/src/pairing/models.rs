use std::collections::{HashMap, HashSet};

use x25519_dalek::StaticSecret;

use crate::ca::{IssuedCertificate, StoredCertificateAuthority};

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
pub(super) struct ActivePairingSession {
    pub(super) public: PairingSession,
    pub(super) gateway_ephemeral_secret: StaticSecret,
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
    pub current_certificate: IssuedCertificate,
    #[serde(default)]
    pub certificate_fingerprints: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct PairingResult {
    pub device: PairedDevice,
    pub identity_fingerprint: String,
    pub signing_public_key_hex: String,
    pub transcript_hash_hex: String,
    pub gateway_ca_certificate_pem: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RevokedDevice {
    pub device_id: String,
    pub reason: String,
    pub revoked_at_unix_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(super) struct PersistedIdentityState {
    #[serde(default)]
    pub(super) generation: u64,
    pub(super) ca: StoredCertificateAuthority,
    pub(super) paired_devices: HashMap<String, PairedDevice>,
    pub(super) revoked_devices: HashMap<String, RevokedDevice>,
    pub(super) revoked_certificate_fingerprints: HashSet<String>,
}
