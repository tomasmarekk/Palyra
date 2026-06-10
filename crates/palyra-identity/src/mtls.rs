//! rustls configuration builders for node-RPC mutual TLS.
//!
//! Wraps the standard WebPKI client verifier with a revocation check keyed by the
//! SHA-256 fingerprint of the end-entity certificate DER. Fingerprints must stay in
//! sync with `pairing::helpers::certificate_fingerprint_hex`, which feeds the
//! revocation index from the PEM side.

use std::{
    collections::HashSet,
    sync::{Arc, RwLock},
};

use rustls::{
    client::danger::HandshakeSignatureValid,
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer, UnixTime},
    server::{
        danger::{ClientCertVerified, ClientCertVerifier},
        WebPkiClientVerifier,
    },
    CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, Error as RustlsError,
    RootCertStore, ServerConfig, SignatureScheme,
};
use sha2::{Digest, Sha256};

use crate::{
    ca::IssuedCertificate,
    error::{IdentityError, IdentityResult},
};

/// Lookup of revoked client certificates consulted on every mTLS handshake.
///
/// Implementations must fail closed: when revocation state cannot be determined,
/// report the certificate as revoked rather than admitting it.
pub trait RevocationIndex: Send + Sync {
    /// Returns whether the certificate with this hex SHA-256 DER fingerprint is revoked.
    fn is_revoked(&self, certificate_fingerprint_hex: &str) -> bool;
}

/// In-memory [`RevocationIndex`] refreshed wholesale from persisted identity state.
#[derive(Default)]
pub struct MemoryRevocationIndex {
    revoked_fingerprints: RwLock<HashSet<String>>,
}

impl MemoryRevocationIndex {
    /// Creates an index pre-populated with the given revoked fingerprints.
    #[must_use]
    pub fn from_fingerprints(fingerprints: HashSet<String>) -> Self {
        Self { revoked_fingerprints: RwLock::new(fingerprints) }
    }

    /// Replaces the entire revocation set, e.g. after a rotation or revocation event.
    ///
    /// # Errors
    /// Returns [`IdentityError::Internal`] if the lock is poisoned.
    pub fn replace_all(&self, fingerprints: HashSet<String>) -> IdentityResult<()> {
        let mut guard = self
            .revoked_fingerprints
            .write()
            .map_err(|_| IdentityError::Internal("revocation index lock poisoned".to_owned()))?;
        *guard = fingerprints;
        Ok(())
    }
}

impl std::fmt::Debug for MemoryRevocationIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("MemoryRevocationIndex")
    }
}

impl RevocationIndex for MemoryRevocationIndex {
    fn is_revoked(&self, certificate_fingerprint_hex: &str) -> bool {
        // Fail closed: a poisoned lock means revocation state is unknown, so treat the
        // certificate as revoked instead of admitting a possibly revoked client.
        self.revoked_fingerprints
            .read()
            .map(|fingerprints| fingerprints.contains(certificate_fingerprint_hex))
            .unwrap_or(true)
    }
}

/// [`ClientCertVerifier`] that delegates chain validation to `base` and then rejects
/// end-entity certificates listed in the revocation index.
struct RevocationAwareClientVerifier {
    base: Arc<dyn ClientCertVerifier>,
    revocation_index: Arc<dyn RevocationIndex>,
}

impl RevocationAwareClientVerifier {
    fn new(base: Arc<dyn ClientCertVerifier>, revocation_index: Arc<dyn RevocationIndex>) -> Self {
        Self { base, revocation_index }
    }
}

impl std::fmt::Debug for RevocationAwareClientVerifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("RevocationAwareClientVerifier")
    }
}

impl ClientCertVerifier for RevocationAwareClientVerifier {
    fn offer_client_auth(&self) -> bool {
        self.base.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.base.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.base.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        // Chain validation runs first so revocation only applies to certificates that
        // are otherwise trusted; an unrelated forged cert still fails as untrusted.
        let verified = self.base.verify_client_cert(end_entity, intermediates, now)?;
        let fingerprint = certificate_fingerprint_hex(end_entity);
        if self.revocation_index.is_revoked(&fingerprint) {
            return Err(RustlsError::InvalidCertificate(CertificateError::Revoked));
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.base.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        self.base.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.base.supported_verify_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        self.base.requires_raw_public_keys()
    }
}

/// Builds a client-certificate verifier that trusts the gateway CA and enforces the
/// revocation index after standard WebPKI chain validation.
///
/// # Errors
/// Returns [`IdentityError::CertificateParsingFailed`] if the CA PEM cannot be parsed
/// or added to the root store, and [`IdentityError::Internal`] if the underlying WebPKI
/// verifier cannot be constructed.
pub fn build_revocation_aware_client_verifier(
    gateway_ca_certificate_pem: &str,
    revocation_index: Arc<dyn RevocationIndex>,
) -> IdentityResult<Arc<dyn ClientCertVerifier>> {
    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certs(gateway_ca_certificate_pem)? {
        roots.add(cert).map_err(|_| IdentityError::CertificateParsingFailed)?;
    }

    let base_verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|error| IdentityError::Internal(error.to_string()))?;
    Ok(Arc::new(RevocationAwareClientVerifier::new(base_verifier, revocation_index)))
}

/// Builds the node-RPC server config: client certificates are mandatory, must chain to
/// the gateway CA, and must not be revoked.
///
/// # Errors
/// Returns certificate/key parsing variants of [`IdentityError`] for malformed PEM
/// inputs and [`IdentityError::Internal`] if rustls rejects the server configuration.
pub fn build_node_rpc_server_mtls_config(
    gateway_ca_certificate_pem: &str,
    server_certificate: &IssuedCertificate,
    revocation_index: Arc<dyn RevocationIndex>,
) -> IdentityResult<ServerConfig> {
    let verifier =
        build_revocation_aware_client_verifier(gateway_ca_certificate_pem, revocation_index)?;

    let cert_chain = parse_pem_certs(&server_certificate.certificate_pem)?;
    let private_key = parse_private_key(&server_certificate.private_key_pem)?;

    ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, private_key)
        .map_err(|error| IdentityError::Internal(error.to_string()))
}

/// Builds the client config a paired device uses to dial the gateway: the gateway CA is
/// the sole trust root and the device's leaf certificate is presented for client auth.
///
/// # Errors
/// Returns certificate/key parsing variants of [`IdentityError`] for malformed PEM
/// inputs and [`IdentityError::Internal`] if rustls rejects the client configuration.
pub fn build_paired_device_client_mtls_config(
    gateway_ca_certificate_pem: &str,
    device_certificate: &IssuedCertificate,
) -> IdentityResult<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certs(gateway_ca_certificate_pem)? {
        roots.add(cert).map_err(|_| IdentityError::CertificateParsingFailed)?;
    }

    let cert_chain = parse_pem_certs(&device_certificate.certificate_pem)?;
    let private_key = parse_private_key(&device_certificate.private_key_pem)?;

    ClientConfig::builder()
        .with_root_certificates(roots)
        .with_client_auth_cert(cert_chain, private_key)
        .map_err(|error| IdentityError::Internal(error.to_string()))
}

/// Builds a TLS-only client config (no client certificate) for not-yet-paired devices.
///
/// Servers built by [`build_node_rpc_server_mtls_config`] will reject this config; it
/// exists for pre-pairing endpoints that only need to authenticate the gateway.
///
/// # Errors
/// Returns [`IdentityError::CertificateParsingFailed`] if the CA PEM cannot be parsed
/// or added to the root store.
pub fn build_unpaired_client_config(
    gateway_ca_certificate_pem: &str,
) -> IdentityResult<ClientConfig> {
    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certs(gateway_ca_certificate_pem)? {
        roots.add(cert).map_err(|_| IdentityError::CertificateParsingFailed)?;
    }
    Ok(ClientConfig::builder().with_root_certificates(roots).with_no_client_auth())
}

fn parse_pem_certs(pem: &str) -> IdentityResult<Vec<CertificateDer<'static>>> {
    CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| IdentityError::CertificateParsingFailed)
}

fn parse_private_key(pem: &str) -> IdentityResult<PrivateKeyDer<'static>> {
    PrivateKeyDer::from_pem_slice(pem.as_bytes())
        .map_err(|_| IdentityError::PrivateKeyParsingFailed)
}

/// Hex SHA-256 over the raw certificate DER.
///
/// Must stay byte-compatible with `pairing::helpers::certificate_fingerprint_hex`
/// (PEM input), which produces the fingerprints stored in the revocation index.
fn certificate_fingerprint_hex(certificate: &CertificateDer<'_>) -> String {
    hex::encode(Sha256::digest(certificate.as_ref()))
}

#[cfg(test)]
mod tests {
    use super::MemoryRevocationIndex;
    use std::{
        collections::HashSet,
        panic::{self, AssertUnwindSafe},
        sync::Arc,
        thread,
    };

    #[test]
    fn replace_all_returns_error_when_lock_is_poisoned() {
        let index = Arc::new(MemoryRevocationIndex::default());
        let poisoned = index.clone();
        let _ = panic::catch_unwind(AssertUnwindSafe(|| {
            let _guard =
                poisoned.revoked_fingerprints.write().expect("write lock should be acquired");
            panic!("intentional lock poisoning for test");
        }));

        thread::yield_now();
        let result = index.replace_all(HashSet::from([String::from("deadbeef")]));
        assert!(result.is_err(), "poisoned lock should return an explicit error");
    }
}
