//! Device identity, pairing, certificate rotation, and mTLS trust for Palyra.
//!
//! [`IdentityManager`] owns the gateway certificate authority and the paired-device
//! lifecycle (pairing handshake, certificate rotation, revocation). [`DeviceIdentity`]
//! holds a device's long-lived key material, and the `build_*` helpers produce rustls
//! configurations that enforce the gateway's revocation index during mTLS handshakes.
//! State persists through a [`SecretStore`] so multiple daemon processes stay consistent.

mod ca;
mod device;
mod error;
mod mtls;
mod pairing;
mod store;

use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub use ca::{CertificateAuthority, IssuedCertificate};
pub use device::DeviceIdentity;
pub use error::{IdentityError, IdentityResult};
pub use mtls::{
    build_node_rpc_server_mtls_config, build_paired_device_client_mtls_config,
    build_revocation_aware_client_verifier, build_unpaired_client_config, MemoryRevocationIndex,
    RevocationIndex,
};
pub use pairing::{
    build_device_pairing_hello, should_rotate_certificate, DevicePairingHello, IdentityManager,
    PairedDevice, PairingClientKind, PairingMethod, PairingResult, PairingSession, RevokedDevice,
    VerifiedPairing,
};
pub use store::{
    default_identity_storage_path, FilesystemSecretStore, InMemorySecretStore, SecretStore,
};

/// Pairing protocol version embedded in every signed handshake payload.
///
/// A [`DevicePairingHello`] carrying any other version is rejected with
/// [`IdentityError::PairingVersionMismatch`] to prevent downgrade attempts.
pub const PAIRING_PROTOCOL_VERSION: u32 = 1;
/// Policy flag: node RPC surfaces must always require mutual TLS.
pub const NODE_RPC_MTLS_REQUIRED: bool = true;
/// Default lifetime of a pairing session before the challenge expires.
pub const DEFAULT_PAIRING_WINDOW: Duration = Duration::from_secs(5 * 60);
/// Default validity period for issued leaf certificates (24 hours).
pub const DEFAULT_CERT_VALIDITY: Duration = Duration::from_secs(24 * 60 * 60);
/// Default remaining-lifetime threshold below which a certificate is due for rotation.
pub const DEFAULT_ROTATION_THRESHOLD: Duration = Duration::from_secs(10 * 60);

/// Converts a [`SystemTime`] to milliseconds since the Unix epoch.
fn unix_ms(value: SystemTime) -> IdentityResult<u64> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|error| IdentityError::Internal(error.to_string()))?;
    duration
        .as_millis()
        .try_into()
        .map_err(|_| IdentityError::Internal("timestamp overflow".to_owned()))
}
