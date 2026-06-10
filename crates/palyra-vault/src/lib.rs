//! Scoped secret storage with envelope encryption and OS-native backends.
//!
//! [`Vault`] stores secrets per [`VaultScope`], sealed with a device-bound KEK derived from the
//! `palyra-identity` CA key, in an OS keystore (Keychain, Secret Service, DPAPI) or an
//! encrypted-file fallback. [`SecretResolver`] resolves `palyra-common` `SecretRef` sources
//! (vault/env/file/exec) into exposure-controlled [`SensitiveBytes`]. Consumed by the daemon and
//! CLI; error/message strings are pinned by downstream fixtures — keep them byte-identical.

mod api;
mod backend;
mod crypto;
mod envelope;
mod filesystem;
mod metadata;
mod scope;
mod secret_resolver;

#[cfg(test)]
mod tests;

pub use api::{SecretMetadata, Vault, VaultConfig, VaultError, VaultRef};
pub use backend::{BackendKind, BackendPreference};
pub use crypto::SensitiveBytes;
pub use filesystem::{ensure_owner_only_dir, ensure_owner_only_file};
pub use scope::{VaultScope, MAX_SCOPE_SEGMENT_BYTES};
pub use secret_resolver::{
    SecretResolution, SecretResolutionMetadata, SecretResolutionStatus, SecretResolveError,
    SecretResolveErrorKind, SecretResolver,
};

pub(crate) use crypto::{build_aad, current_unix_ms, normalize_storage_object_id, object_id_for};
pub(crate) use filesystem::{canonicalize_existing_dir, ensure_path_within_root};
