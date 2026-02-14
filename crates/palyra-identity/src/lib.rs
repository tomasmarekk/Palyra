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
    build_unpaired_client_config,
};
pub use pairing::{
    should_rotate_certificate, DevicePairingHello, IdentityManager, PairedDevice,
    PairingClientKind, PairingMethod, PairingResult, PairingSession, RevokedDevice,
};
pub use store::{
    default_identity_storage_path, FilesystemSecretStore, InMemorySecretStore, SecretStore,
};

pub const PAIRING_PROTOCOL_VERSION: u32 = 1;
pub const NODE_RPC_MTLS_REQUIRED: bool = true;
pub const DEFAULT_PAIRING_WINDOW: Duration = Duration::from_secs(5 * 60);
pub const DEFAULT_CERT_VALIDITY: Duration = Duration::from_secs(24 * 60 * 60);
pub const DEFAULT_ROTATION_THRESHOLD: Duration = Duration::from_secs(10 * 60);

fn unix_ms(value: SystemTime) -> IdentityResult<u64> {
    let duration = value
        .duration_since(UNIX_EPOCH)
        .map_err(|error| IdentityError::Internal(error.to_string()))?;
    duration
        .as_millis()
        .try_into()
        .map_err(|_| IdentityError::Internal("timestamp overflow".to_owned()))
}
