//! Error contract for the identity crate.
//!
//! Pairing rejections are deliberately split into distinct variants (proof, signature,
//! transcript, version, revocation) so callers can audit-log the exact denial reason
//! without parsing message strings.

use thiserror::Error;

/// Failure modes of identity, pairing, certificate, and secret-store operations.
///
/// `Display` messages are stable and may be pinned by golden fixtures; change them only
/// as a deliberate contract update. Crypto-adjacent variants never embed secret material.
#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("invalid canonical device ID: {0}")]
    InvalidCanonicalDeviceId(String),
    /// The pairing proof was malformed at session start or did not match at completion.
    #[error("invalid pairing proof")]
    InvalidPairingProof,
    #[error("pairing session not found")]
    PairingSessionNotFound,
    #[error("pairing session expired")]
    PairingSessionExpired,
    #[error("active pairing session capacity exceeded (limit: {limit})")]
    PairingSessionCapacityExceeded { limit: usize },
    #[error("pairing session start rate limit exceeded (max {max} per {window_ms} ms)")]
    PairingSessionRateLimited { max: usize, window_ms: u64 },
    #[error("pairing protocol version mismatch (expected {expected}, got {got})")]
    PairingVersionMismatch { expected: u32, got: u32 },
    #[error("pairing client kind mismatch")]
    PairingClientKindMismatch,
    #[error("device signature verification failed")]
    SignatureVerificationFailed,
    #[error("pairing transcript verification failed")]
    TranscriptVerificationFailed,
    #[error("device is revoked and cannot pair")]
    DeviceRevoked,
    #[error("device is not paired")]
    DeviceNotPaired,
    #[error("invalid secret-store key")]
    InvalidSecretStoreKey,
    #[error("secret not found")]
    SecretNotFound,
    #[error("certificate parsing failed")]
    CertificateParsingFailed,
    #[error("private key parsing failed")]
    PrivateKeyParsingFailed,
    /// A key, cipher, certificate-signing, or randomness operation failed.
    #[error("cryptographic operation failed: {0}")]
    Cryptographic(String),
    /// Catch-all for storage, serialization, locking, and other infrastructure failures.
    #[error("{0}")]
    Internal(String),
}

/// Convenience alias used by every fallible operation in this crate.
pub type IdentityResult<T> = Result<T, IdentityError>;
