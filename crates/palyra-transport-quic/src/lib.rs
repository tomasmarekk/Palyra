use std::{any::Any, net::SocketAddr, sync::Arc, time::Duration};

use quinn::{Connection, Endpoint, RecvStream, SendStream};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    RootCertStore,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const PROTOCOL_VERSION: u16 = 1;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 512 * 1024;
const DEFAULT_ALPN: &[u8] = b"palyra-quic-v1";

#[derive(Debug, Clone)]
pub struct QuicTransportLimits {
    pub handshake_timeout: Duration,
    pub idle_timeout: Duration,
    pub keep_alive_interval: Duration,
    pub max_concurrent_bidi_streams: u32,
    pub max_concurrent_uni_streams: u32,
}

impl Default for QuicTransportLimits {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(5),
            max_concurrent_bidi_streams: 32,
            max_concurrent_uni_streams: 32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct QuicServerTlsConfig {
    pub ca_cert_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub require_client_auth: bool,
}

#[derive(Debug, Clone)]
pub struct QuicClientTlsConfig {
    pub ca_cert_pem: String,
    pub client_cert_pem: Option<String>,
    pub client_key_pem: Option<String>,
    pub server_name: String,
    pub pinned_server_fingerprint_sha256: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpFallbackPolicy {
    Disabled,
    AllowExplicit,
}

#[derive(Debug)]
pub enum QuicConnectOutcome {
    Connected(Connection),
    FallbackRequired { reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum QuicTransportError {
    #[error("certificate parsing failed")]
    CertificateParsingFailed,
    #[error("private key parsing failed")]
    PrivateKeyParsingFailed,
    #[error("invalid QUIC server name '{server_name}'")]
    InvalidServerName { server_name: String },
    #[error("client certificate and key must be provided together for mTLS")]
    ClientIdentityIncomplete,
    #[error("client certificate is required when server enforces mTLS")]
    MissingClientIdentity,
    #[error("invalid QUIC transport idle timeout: {timeout_ms}ms")]
    InvalidIdleTimeout { timeout_ms: u64 },
    #[error("failed to configure QUIC TLS stack: {message}")]
    TlsConfigurationFailed { message: String },
    #[error("failed to bind QUIC endpoint on {bind_addr}: {message}")]
    EndpointBindFailed { bind_addr: SocketAddr, message: String },
    #[error("failed to start QUIC connect attempt to {remote_addr}: {message}")]
    ConnectStartFailed { remote_addr: SocketAddr, message: String },
    #[error("QUIC handshake timed out after {timeout_ms}ms")]
    HandshakeTimeout { timeout_ms: u64 },
    #[error("QUIC connection to {remote_addr} failed: {message}")]
    ConnectFailed { remote_addr: SocketAddr, message: String },
    #[error("pinned server certificate mismatch: expected {expected}, got {actual}")]
    PinnedCertificateMismatch { expected: String, actual: String },
    #[error("peer certificate identity metadata is unavailable")]
    MissingPeerIdentity,
    #[error("unexpected peer certificate identity metadata type")]
    UnexpectedPeerIdentityType,
    #[error("peer certificate chain is empty")]
    EmptyPeerCertificateChain,
    #[error("frame payload exceeds limit ({size} bytes > {max} bytes)")]
    FrameTooLarge { size: usize, max: usize },
    #[error("failed to read frame: {message}")]
    FrameReadFailed { message: String },
    #[error("failed to write frame: {message}")]
    FrameWriteFailed { message: String },
}

pub fn build_server_endpoint(
    bind_addr: SocketAddr,
    tls: &QuicServerTlsConfig,
    limits: &QuicTransportLimits,
) -> Result<Endpoint, QuicTransportError> {
    let cert_chain = parse_pem_certs(tls.cert_pem.as_str())?;
    let private_key = parse_private_key(tls.key_pem.as_str())?;

    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certs(tls.ca_cert_pem.as_str())? {
        roots.add(cert).map_err(|error| QuicTransportError::TlsConfigurationFailed {
            message: format!("failed to add CA certificate to server roots: {error}"),
        })?;
    }

    let mut tls_server = if tls.require_client_auth {
        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|error| QuicTransportError::TlsConfigurationFailed {
                message: format!("failed to build mTLS client verifier: {error}"),
            })?;
        rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(cert_chain, private_key)
            .map_err(|error| QuicTransportError::TlsConfigurationFailed {
                message: format!("failed to build server certificate chain: {error}"),
            })?
    } else {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_chain, private_key)
            .map_err(|error| QuicTransportError::TlsConfigurationFailed {
                message: format!("failed to build server certificate chain: {error}"),
            })?
    };
    tls_server.alpn_protocols = vec![DEFAULT_ALPN.to_vec()];
    tls_server.max_early_data_size = 0;

    let quic_crypto =
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_server).map_err(|error| {
            QuicTransportError::TlsConfigurationFailed {
                message: format!("failed to convert rustls server config to QUIC: {error}"),
            }
        })?;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport = Arc::new(build_transport_config(limits)?);

    quinn::Endpoint::server(server_config, bind_addr).map_err(|error| {
        QuicTransportError::EndpointBindFailed { bind_addr, message: error.to_string() }
    })
}

pub fn build_client_endpoint(
    bind_addr: SocketAddr,
    tls: &QuicClientTlsConfig,
    limits: &QuicTransportLimits,
) -> Result<Endpoint, QuicTransportError> {
    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certs(tls.ca_cert_pem.as_str())? {
        roots.add(cert).map_err(|error| QuicTransportError::TlsConfigurationFailed {
            message: format!("failed to add CA certificate to client roots: {error}"),
        })?;
    }

    let mut tls_client = match (&tls.client_cert_pem, &tls.client_key_pem) {
        (Some(cert_pem), Some(key_pem)) => {
            let cert_chain = parse_pem_certs(cert_pem.as_str())?;
            let private_key = parse_private_key(key_pem.as_str())?;
            rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_client_auth_cert(cert_chain, private_key)
                .map_err(|error| QuicTransportError::TlsConfigurationFailed {
                    message: format!("failed to configure client certificate identity: {error}"),
                })?
        }
        (None, None) => {
            rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth()
        }
        _ => return Err(QuicTransportError::ClientIdentityIncomplete),
    };

    tls_client.enable_early_data = false;
    tls_client.alpn_protocols = vec![DEFAULT_ALPN.to_vec()];

    let quic_crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls_client).map_err(|error| {
            QuicTransportError::TlsConfigurationFailed {
                message: format!("failed to convert rustls client config to QUIC: {error}"),
            }
        })?;
    let mut client_config = quinn::ClientConfig::new(Arc::new(quic_crypto));
    client_config.transport_config(Arc::new(build_transport_config(limits)?));

    let mut endpoint = quinn::Endpoint::client(bind_addr).map_err(|error| {
        QuicTransportError::EndpointBindFailed { bind_addr, message: error.to_string() }
    })?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

pub async fn connect_quic(
    endpoint: &Endpoint,
    remote_addr: SocketAddr,
    tls: &QuicClientTlsConfig,
    limits: &QuicTransportLimits,
) -> Result<Connection, QuicTransportError> {
    let connecting = endpoint.connect(remote_addr, tls.server_name.as_str()).map_err(|error| {
        QuicTransportError::ConnectStartFailed { remote_addr, message: error.to_string() }
    })?;
    let connection = tokio::time::timeout(limits.handshake_timeout, connecting)
        .await
        .map_err(|_| QuicTransportError::HandshakeTimeout {
            timeout_ms: limits.handshake_timeout.as_millis().try_into().unwrap_or(u64::MAX),
        })?
        .map_err(|error| QuicTransportError::ConnectFailed {
            remote_addr,
            message: error.to_string(),
        })?;
    verify_pinned_server_certificate(&connection, tls.pinned_server_fingerprint_sha256.as_deref())?;
    Ok(connection)
}

pub async fn connect_with_explicit_fallback(
    endpoint: &Endpoint,
    remote_addr: SocketAddr,
    tls: &QuicClientTlsConfig,
    limits: &QuicTransportLimits,
    fallback_policy: TcpFallbackPolicy,
    privileged_path: bool,
) -> Result<QuicConnectOutcome, QuicTransportError> {
    match connect_quic(endpoint, remote_addr, tls, limits).await {
        Ok(connection) => Ok(QuicConnectOutcome::Connected(connection)),
        Err(error) => {
            if matches!(fallback_policy, TcpFallbackPolicy::AllowExplicit) && !privileged_path {
                return Ok(QuicConnectOutcome::FallbackRequired { reason: error.to_string() });
            }
            Err(error)
        }
    }
}

pub async fn write_frame(
    stream: &mut SendStream,
    payload: &[u8],
    max_frame_bytes: usize,
) -> Result<(), QuicTransportError> {
    if payload.len() > max_frame_bytes {
        return Err(QuicTransportError::FrameTooLarge {
            size: payload.len(),
            max: max_frame_bytes,
        });
    }
    stream
        .write_u32(payload.len().try_into().map_err(|_| QuicTransportError::FrameTooLarge {
            size: payload.len(),
            max: max_frame_bytes,
        })?)
        .await
        .map_err(|error| QuicTransportError::FrameWriteFailed { message: error.to_string() })?;
    stream
        .write_all(payload)
        .await
        .map_err(|error| QuicTransportError::FrameWriteFailed { message: error.to_string() })?;
    stream
        .flush()
        .await
        .map_err(|error| QuicTransportError::FrameWriteFailed { message: error.to_string() })?;
    Ok(())
}

pub async fn read_frame(
    stream: &mut RecvStream,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, QuicTransportError> {
    let size = stream
        .read_u32()
        .await
        .map_err(|error| QuicTransportError::FrameReadFailed { message: error.to_string() })?
        as usize;
    if size > max_frame_bytes {
        return Err(QuicTransportError::FrameTooLarge { size, max: max_frame_bytes });
    }
    let mut payload = vec![0_u8; size];
    stream
        .read_exact(payload.as_mut_slice())
        .await
        .map_err(|error| QuicTransportError::FrameReadFailed { message: error.to_string() })?;
    Ok(payload)
}

fn build_transport_config(
    limits: &QuicTransportLimits,
) -> Result<quinn::TransportConfig, QuicTransportError> {
    let mut transport = quinn::TransportConfig::default();
    let idle_timeout = quinn::IdleTimeout::try_from(limits.idle_timeout).map_err(|_| {
        QuicTransportError::InvalidIdleTimeout {
            timeout_ms: limits.idle_timeout.as_millis().try_into().unwrap_or(u64::MAX),
        }
    })?;
    transport.max_idle_timeout(Some(idle_timeout));
    transport.keep_alive_interval(Some(limits.keep_alive_interval));
    transport.max_concurrent_bidi_streams(limits.max_concurrent_bidi_streams.into());
    transport.max_concurrent_uni_streams(limits.max_concurrent_uni_streams.into());
    Ok(transport)
}

fn verify_pinned_server_certificate(
    connection: &Connection,
    expected_fingerprint_sha256: Option<&str>,
) -> Result<(), QuicTransportError> {
    let Some(expected) = expected_fingerprint_sha256 else {
        return Ok(());
    };
    let expected = expected.trim().to_ascii_lowercase();
    if expected.is_empty() {
        return Ok(());
    }
    let peer_identity =
        connection.peer_identity().ok_or(QuicTransportError::MissingPeerIdentity)?;
    let peer_certs = downcast_peer_certificates(peer_identity.as_ref())?;
    let peer_leaf = peer_certs.first().ok_or(QuicTransportError::EmptyPeerCertificateChain)?;
    let actual = hex::encode(Sha256::digest(peer_leaf.as_ref()));
    if actual.eq_ignore_ascii_case(expected.as_str()) {
        return Ok(());
    }
    Err(QuicTransportError::PinnedCertificateMismatch { expected, actual })
}

fn downcast_peer_certificates(
    identity: &dyn Any,
) -> Result<&Vec<CertificateDer<'static>>, QuicTransportError> {
    identity
        .downcast_ref::<Vec<CertificateDer<'static>>>()
        .ok_or(QuicTransportError::UnexpectedPeerIdentityType)
}

fn parse_pem_certs(pem: &str) -> Result<Vec<CertificateDer<'static>>, QuicTransportError> {
    CertificateDer::pem_slice_iter(pem.as_bytes())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| QuicTransportError::CertificateParsingFailed)
}

fn parse_private_key(pem: &str) -> Result<PrivateKeyDer<'static>, QuicTransportError> {
    PrivateKeyDer::from_pem_slice(pem.as_bytes())
        .map_err(|_| QuicTransportError::PrivateKeyParsingFailed)
}
