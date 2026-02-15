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

pub trait RevocationIndex: Send + Sync {
    fn is_revoked(&self, certificate_fingerprint_hex: &str) -> bool;
}

#[derive(Default)]
pub struct MemoryRevocationIndex {
    revoked_fingerprints: RwLock<HashSet<String>>,
}

impl MemoryRevocationIndex {
    #[must_use]
    pub fn from_fingerprints(fingerprints: HashSet<String>) -> Self {
        Self { revoked_fingerprints: RwLock::new(fingerprints) }
    }

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
        self.revoked_fingerprints
            .read()
            .map(|fingerprints| fingerprints.contains(certificate_fingerprint_hex))
            .unwrap_or(true)
    }
}

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

pub fn build_node_rpc_server_mtls_config(
    gateway_ca_certificate_pem: &str,
    server_certificate: &IssuedCertificate,
    revocation_index: Arc<dyn RevocationIndex>,
) -> IdentityResult<ServerConfig> {
    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certs(gateway_ca_certificate_pem)? {
        roots.add(cert).map_err(|_| IdentityError::CertificateParsingFailed)?;
    }

    let base_verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|error| IdentityError::Internal(error.to_string()))?;
    let verifier: Arc<dyn ClientCertVerifier> =
        Arc::new(RevocationAwareClientVerifier::new(base_verifier, revocation_index));

    let cert_chain = parse_pem_certs(&server_certificate.certificate_pem)?;
    let private_key = parse_private_key(&server_certificate.private_key_pem)?;

    ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(cert_chain, private_key)
        .map_err(|error| IdentityError::Internal(error.to_string()))
}

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
