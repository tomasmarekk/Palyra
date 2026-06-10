//! Typed error surface for skill packaging, verification, trust, and lifecycle.
//!
//! Display strings are user-visible through the CLI and pinned by golden
//! fixtures; treat every `#[error(...)]` message as frozen.

use thiserror::Error;

/// Failure modes of skill manifest parsing, artifact packaging, signature and
/// trust verification, runtime compatibility checks, and lifecycle transitions.
///
/// The `#[error]` attribute on each variant documents the user-visible message;
/// signature and trust failures intentionally carry no cryptographic detail
/// beyond the publisher identity, so verification errors cannot leak key
/// material or oracle information.
#[derive(Debug, Error)]
pub enum SkillPackagingError {
    #[error("manifest parse failed: {0}")]
    ManifestParse(String),
    #[error("manifest validation failed: {0}")]
    ManifestValidation(String),
    #[error("artifact size exceeds limit ({actual} > {limit})")]
    ArtifactTooLarge { actual: usize, limit: usize },
    #[error("artifact contains too many entries ({actual} > {limit})")]
    ArtifactTooManyEntries { actual: usize, limit: usize },
    #[error("artifact entry '{path}' exceeds limit ({actual} > {limit})")]
    ArtifactEntryTooLarge { path: String, actual: usize, limit: usize },
    #[error("artifact is missing required entry '{0}'")]
    MissingArtifactEntry(String),
    #[error("artifact contains duplicate entry '{0}'")]
    DuplicateArtifactEntry(String),
    #[error("artifact entry path is invalid: {0}")]
    InvalidArtifactPath(String),
    #[error("invalid SBOM payload: {0}")]
    InvalidSbom(String),
    #[error("invalid provenance payload: {0}")]
    InvalidProvenance(String),
    #[error("artifact payload hash mismatch")]
    PayloadHashMismatch,
    #[error("artifact signature verification failed")]
    SignatureVerificationFailed,
    #[error("signing key length is invalid: {actual}")]
    InvalidSigningKeyLength { actual: usize },
    #[error("untrusted publisher '{publisher}'")]
    UntrustedPublisher { publisher: String },
    #[error("trusted publisher key mismatch for '{publisher}'")]
    TrustedPublisherKeyMismatch { publisher: String },
    #[error("TOFU pinned key mismatch for '{publisher}'")]
    TofuKeyMismatch { publisher: String },
    #[error("requested min protocol major {requested} is higher than current {current}")]
    UnsupportedProtocolMajor { requested: u32, current: u32 },
    #[error("requested min runtime version {requested} is higher than current {current}")]
    UnsupportedRuntimeVersion { requested: String, current: String },
    #[error("current runtime version {current} is higher than supported maximum {supported_max}")]
    RuntimeVersionAboveSupportedMaximum { supported_max: String, current: String },
    #[error("I/O failed: {0}")]
    Io(String),
    #[error("zip handling failed: {0}")]
    Zip(String),
    #[error("serialization failed: {0}")]
    Serialization(String),
    #[error("extension lifecycle failed: {0}")]
    ExtensionLifecycle(String),
}
