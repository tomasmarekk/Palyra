use std::sync::Arc;

use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    server::WebPkiClientVerifier,
    ClientConfig, RootCertStore, ServerConfig,
};

use crate::{
    ca::IssuedCertificate,
    error::{IdentityError, IdentityResult},
};

pub fn build_node_rpc_server_mtls_config(
    gateway_ca_certificate_pem: &str,
    server_certificate: &IssuedCertificate,
) -> IdentityResult<ServerConfig> {
    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certs(gateway_ca_certificate_pem)? {
        roots.add(cert).map_err(|_| IdentityError::CertificateParsingFailed)?;
    }

    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(|error| IdentityError::Internal(error.to_string()))?;

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
