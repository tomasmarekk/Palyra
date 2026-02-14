use thiserror::Error;

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("invalid canonical device ID: {0}")]
    InvalidCanonicalDeviceId(String),
    #[error("invalid pairing proof")]
    InvalidPairingProof,
    #[error("pairing session not found")]
    PairingSessionNotFound,
    #[error("pairing session expired")]
    PairingSessionExpired,
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
    #[error("cryptographic operation failed: {0}")]
    Cryptographic(String),
    #[error("{0}")]
    Internal(String),
}

pub type IdentityResult<T> = Result<T, IdentityError>;
