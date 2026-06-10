//! Typed error surface for auth profile registry operations.
//!
//! Display strings are user-visible through the CLI and daemon APIs and are pinned by
//! tests/fixtures; treat the message text as part of the contract.

use std::path::PathBuf;

use thiserror::Error;

/// Errors returned by auth profile registry operations.
///
/// Messages never embed secret material; field values interpolated into messages are
/// limited to paths, field names, and identifiers.
#[derive(Debug, Error)]
pub enum AuthProfileError {
    /// A registry mutex was poisoned by a panic in another thread.
    #[error("auth profile registry lock poisoned")]
    LockPoisoned,
    /// An env-configured path (state root or registry path) failed validation.
    #[error("invalid path in {field}: {message}")]
    InvalidPath { field: &'static str, message: String },
    /// Reading a registry or runtime-state file from disk failed.
    #[error("failed to read auth profile registry {path}: {source}")]
    ReadRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// A registry or runtime-state file contained invalid TOML.
    #[error("failed to parse auth profile registry {path}: {source}")]
    ParseRegistry {
        path: PathBuf,
        #[source]
        source: Box<toml::de::Error>,
    },
    /// Persisting a registry or runtime-state file to disk failed.
    #[error("failed to write auth profile registry {path}: {source}")]
    WriteRegistry {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Serializing the registry document to TOML failed.
    #[error("failed to serialize auth profile registry: {0}")]
    SerializeRegistry(#[from] toml::ser::Error),
    /// A persisted document declares a schema version this build does not support.
    #[error("unsupported auth profile registry version {0}")]
    UnsupportedVersion(u32),
    /// A request or stored field failed validation or normalization.
    #[error("invalid field '{field}': {message}")]
    InvalidField { field: &'static str, message: String },
    /// The referenced profile id does not exist in the registry.
    #[error("auth profile not found: {0}")]
    ProfileNotFound(String),
    /// The registry refuses to grow beyond `MAX_PROFILE_COUNT` entries.
    #[error("auth profile registry exceeds maximum entries")]
    RegistryLimitExceeded,
    /// The system clock reported a time before the unix epoch.
    #[error("system time before unix epoch: {0}")]
    InvalidSystemTime(#[from] std::time::SystemTimeError),
}
