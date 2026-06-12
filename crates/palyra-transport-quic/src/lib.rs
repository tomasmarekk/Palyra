//! QUIC transport primitives with secure-by-default TLS for Palyra node links.
//!
//! Builds mutually authenticated quinn/rustls endpoints, frames payloads with a
//! length prefix, and enforces optional pinned server fingerprints. TCP fallback
//! is never silent: callers receive an explicit
//! [`QuicConnectOutcome::FallbackRequired`] and decide per policy. Consumed by
//! `palyra-daemon` (`quic_runtime`) for node RPC.

use std::{any::Any, fmt, net::SocketAddr, sync::Arc, time::Duration};

use quinn::{Connection, Endpoint, RecvStream, SendStream};
use rustls::{
    pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer},
    server::danger::ClientCertVerifier,
    RootCertStore,
};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Version tag embedded in payloads exchanged over this transport; peers must
/// reject mismatches explicitly.
pub const PROTOCOL_VERSION: u16 = 1;
/// Default upper bound for a single framed payload (512 KiB).
pub const DEFAULT_MAX_FRAME_BYTES: usize = 512 * 1024;
const DEFAULT_ALPN: &[u8] = b"palyra-quic-v1";

/// Timeout and concurrency limits applied to QUIC endpoints and connections.
///
/// The [`Default`] values favor failing fast over hanging connections; loosen
/// them deliberately rather than removing limits.
#[derive(Debug, Clone)]
pub struct QuicTransportLimits {
    /// Maximum time to wait for the QUIC/TLS handshake to complete.
    pub handshake_timeout: Duration,
    /// Connections are closed after this period without traffic.
    pub idle_timeout: Duration,
    /// Interval between keep-alive pings; keep below `idle_timeout` so healthy
    /// idle connections stay open.
    pub keep_alive_interval: Duration,
    /// Maximum concurrently open bidirectional streams per connection.
    pub max_concurrent_bidi_streams: u32,
    /// Maximum concurrently open unidirectional streams per connection.
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

/// PEM-encoded TLS material and client-auth policy for a QUIC server endpoint.
///
/// `Debug` redacts all PEM fields so configurations can be logged safely.
#[derive(Clone)]
pub struct QuicServerTlsConfig {
    /// PEM bundle of CA certificates trusted for client authentication.
    pub ca_cert_pem: String,
    /// PEM certificate chain presented by the server.
    pub cert_pem: String,
    /// PEM private key matching `cert_pem`.
    pub key_pem: String,
    /// When `true`, clients must present a certificate accepted by the
    /// configured verifier (mTLS).
    pub require_client_auth: bool,
    /// Custom client-certificate verifier; when absent and client auth is
    /// required, a WebPKI verifier is built from `ca_cert_pem`.
    pub client_cert_verifier: Option<Arc<dyn ClientCertVerifier>>,
}

// Manual impl so private-key and certificate material in the PEM fields can
// never leak through logs.
impl std::fmt::Debug for QuicServerTlsConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuicServerTlsConfig")
            .field("ca_cert_pem", &"<redacted>")
            .field("cert_pem", &"<redacted>")
            .field("key_pem", &"<redacted>")
            .field("require_client_auth", &self.require_client_auth)
            .field("has_client_cert_verifier", &self.client_cert_verifier.is_some())
            .finish()
    }
}

/// PEM-encoded TLS material and verification settings for a QUIC client endpoint.
///
/// `Debug` redacts all PEM fields so configurations can be logged safely.
#[derive(Clone)]
pub struct QuicClientTlsConfig {
    /// PEM bundle of CA certificates trusted for server verification.
    pub ca_cert_pem: String,
    /// Optional PEM client certificate chain for mTLS; must be set together
    /// with `client_key_pem`.
    pub client_cert_pem: Option<String>,
    /// Optional PEM private key matching `client_cert_pem`.
    pub client_key_pem: Option<String>,
    /// Expected server name used for SNI and certificate hostname verification.
    pub server_name: String,
    /// Optional hex-encoded SHA-256 fingerprint of the server's leaf
    /// certificate DER; when set, connections to servers presenting any other
    /// leaf are rejected even if the chain otherwise validates.
    pub pinned_server_fingerprint_sha256: Option<String>,
}

impl fmt::Debug for QuicClientTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QuicClientTlsConfig")
            .field("ca_cert_pem", &"<redacted>")
            .field("client_cert_pem", &self.client_cert_pem.as_ref().map(|_| "<redacted>"))
            .field("client_key_pem", &self.client_key_pem.as_ref().map(|_| "<redacted>"))
            .field("server_name", &self.server_name)
            .field("pinned_server_fingerprint_sha256", &self.pinned_server_fingerprint_sha256)
            .finish()
    }
}

/// Policy governing whether a failed QUIC connect may surface a TCP fallback.
///
/// This crate never performs the fallback itself; at most it *reports* one via
/// [`QuicConnectOutcome::FallbackRequired`] for the caller to act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpFallbackPolicy {
    /// QUIC failures are always surfaced as errors.
    Disabled,
    /// QUIC failures on non-privileged paths may be reported as a fallback request.
    AllowExplicit,
}

/// Result of a connect attempt made through [`connect_with_explicit_fallback`].
#[derive(Debug)]
pub enum QuicConnectOutcome {
    /// QUIC connected and any configured certificate pin was verified.
    Connected(Connection),
    /// QUIC failed and policy permits an explicit TCP fallback; `reason`
    /// carries the rendered QUIC error for diagnostics.
    FallbackRequired { reason: String },
}

/// Errors produced by QUIC endpoint construction, connection, and framing.
#[derive(Debug, thiserror::Error)]
pub enum QuicTransportError {
    /// A PEM certificate bundle could not be parsed.
    #[error("certificate parsing failed")]
    CertificateParsingFailed,
    /// A PEM private key could not be parsed.
    #[error("private key parsing failed")]
    PrivateKeyParsingFailed,
    /// The configured server name is not a valid TLS server name. Reserved for
    /// callers validating configuration; not produced by this crate itself.
    #[error("invalid QUIC server name '{server_name}'")]
    InvalidServerName { server_name: String },
    /// Only one half of the client certificate/key pair was provided.
    #[error("client certificate and key must be provided together for mTLS")]
    ClientIdentityIncomplete,
    /// The server enforces mTLS but no client identity was configured. Reserved
    /// for callers validating configuration; not produced by this crate itself.
    #[error("client certificate is required when server enforces mTLS")]
    MissingClientIdentity,
    /// The configured idle timeout exceeds what QUIC can encode on the wire.
    #[error("invalid QUIC transport idle timeout: {timeout_ms}ms")]
    InvalidIdleTimeout { timeout_ms: u64 },
    /// The rustls/quinn TLS stack rejected the supplied configuration.
    #[error("failed to configure QUIC TLS stack: {message}")]
    TlsConfigurationFailed { message: String },
    /// Binding the UDP socket for the endpoint failed.
    #[error("failed to bind QUIC endpoint on {bind_addr}: {message}")]
    EndpointBindFailed { bind_addr: SocketAddr, message: String },
    /// The connect attempt could not be initiated (for example, an invalid
    /// server name or endpoint configuration).
    #[error("failed to start QUIC connect attempt to {remote_addr}: {message}")]
    ConnectStartFailed { remote_addr: SocketAddr, message: String },
    /// The handshake did not finish within [`QuicTransportLimits::handshake_timeout`].
    #[error("QUIC handshake timed out after {timeout_ms}ms")]
    HandshakeTimeout { timeout_ms: u64 },
    /// The QUIC connection attempt failed after being initiated.
    #[error("QUIC connection to {remote_addr} failed: {message}")]
    ConnectFailed { remote_addr: SocketAddr, message: String },
    /// The server's leaf certificate did not match the pinned SHA-256 fingerprint.
    #[error("pinned server certificate mismatch: expected {expected}, got {actual}")]
    PinnedCertificateMismatch { expected: String, actual: String },
    /// The connection exposed no peer identity to verify the pin against.
    #[error("peer certificate identity metadata is unavailable")]
    MissingPeerIdentity,
    /// The peer identity was not the rustls certificate chain this crate expects.
    #[error("unexpected peer certificate identity metadata type")]
    UnexpectedPeerIdentityType,
    /// The peer presented an empty certificate chain.
    #[error("peer certificate chain is empty")]
    EmptyPeerCertificateChain,
    /// A frame payload exceeded the configured maximum size.
    #[error("frame payload exceeds limit ({size} bytes > {max} bytes)")]
    FrameTooLarge { size: usize, max: usize },
    /// Reading a length-prefixed frame from the stream failed.
    #[error("failed to read frame: {message}")]
    FrameReadFailed { message: String },
    /// Writing a length-prefixed frame to the stream failed.
    #[error("failed to write frame: {message}")]
    FrameWriteFailed { message: String },
}

/// Builds a QUIC server endpoint bound to `bind_addr` with the given TLS material.
///
/// The endpoint accepts only the Palyra ALPN, disables 0-RTT early data, and
/// applies `limits` to every accepted connection. When
/// [`QuicServerTlsConfig::require_client_auth`] is set, clients must present a
/// certificate accepted by the configured verifier (default: WebPKI against
/// [`QuicServerTlsConfig::ca_cert_pem`]).
///
/// # Errors
///
/// Returns [`QuicTransportError::CertificateParsingFailed`] or
/// [`QuicTransportError::PrivateKeyParsingFailed`] for malformed PEM input,
/// [`QuicTransportError::TlsConfigurationFailed`] when the TLS stack rejects
/// the material, [`QuicTransportError::InvalidIdleTimeout`] for an unencodable
/// idle timeout, and [`QuicTransportError::EndpointBindFailed`] when the UDP
/// socket cannot be bound.
pub fn build_server_endpoint(
    bind_addr: SocketAddr,
    tls: &QuicServerTlsConfig,
    limits: &QuicTransportLimits,
) -> Result<Endpoint, QuicTransportError> {
    let cert_chain = parse_pem_certs(tls.cert_pem.as_str())?;
    let private_key = parse_private_key(tls.key_pem.as_str())?;

    // CA roots are parsed even when client auth is off (or a custom verifier is
    // supplied) so a malformed CA bundle is rejected at configuration time.
    let mut roots = RootCertStore::empty();
    for cert in parse_pem_certs(tls.ca_cert_pem.as_str())? {
        roots.add(cert).map_err(|error| QuicTransportError::TlsConfigurationFailed {
            message: format!("failed to add CA certificate to server roots: {error}"),
        })?;
    }

    let mut tls_server = if tls.require_client_auth {
        let client_verifier: Arc<dyn ClientCertVerifier> =
            if let Some(verifier) = tls.client_cert_verifier.as_ref() {
                verifier.clone()
            } else {
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build().map_err(
                    |error| QuicTransportError::TlsConfigurationFailed {
                        message: format!("failed to build mTLS client verifier: {error}"),
                    },
                )?
            };
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
    // 0-RTT early data is replayable by an attacker; keep it disabled
    // regardless of rustls defaults.
    tls_server.max_early_data_size = 0;

    let quic_crypto =
        quinn::crypto::rustls::QuicServerConfig::try_from(tls_server).map_err(|error| {
            QuicTransportError::TlsConfigurationFailed {
                message: format!("failed to convert rustls server config to QUIC: {error}"),
            }
        })?;

    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_crypto));
    server_config.transport = Arc::new(build_transport_config(limits)?);

    Endpoint::server(server_config, bind_addr).map_err(|error| {
        QuicTransportError::EndpointBindFailed { bind_addr, message: error.to_string() }
    })
}

/// Builds a QUIC client endpoint bound to `bind_addr` that trusts
/// [`QuicClientTlsConfig::ca_cert_pem`] for server verification.
///
/// Client identity is all-or-nothing: provide both
/// [`QuicClientTlsConfig::client_cert_pem`] and
/// [`QuicClientTlsConfig::client_key_pem`] for mTLS, or neither. The endpoint
/// offers only the Palyra ALPN, disables 0-RTT early data, and applies
/// `limits` to outgoing connections.
///
/// # Errors
///
/// Returns [`QuicTransportError::ClientIdentityIncomplete`] when only one half
/// of the client identity is set, [`QuicTransportError::CertificateParsingFailed`]
/// or [`QuicTransportError::PrivateKeyParsingFailed`] for malformed PEM input,
/// [`QuicTransportError::TlsConfigurationFailed`] when the TLS stack rejects
/// the material, [`QuicTransportError::InvalidIdleTimeout`] for an unencodable
/// idle timeout, and [`QuicTransportError::EndpointBindFailed`] when the UDP
/// socket cannot be bound.
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

    // 0-RTT early data is replayable by an attacker; never enable it.
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

    let mut endpoint = Endpoint::client(bind_addr).map_err(|error| {
        QuicTransportError::EndpointBindFailed { bind_addr, message: error.to_string() }
    })?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

/// Connects to `remote_addr` over QUIC and enforces the optional certificate pin.
///
/// The handshake is bounded by [`QuicTransportLimits::handshake_timeout`]. When
/// [`QuicClientTlsConfig::pinned_server_fingerprint_sha256`] is set, the
/// server's leaf certificate DER must hash (SHA-256, hex) to that value; a
/// mismatched connection is rejected and dropped after the handshake.
///
/// # Errors
///
/// Returns [`QuicTransportError::ConnectStartFailed`] when the attempt cannot
/// be initiated, [`QuicTransportError::HandshakeTimeout`] on deadline expiry,
/// [`QuicTransportError::ConnectFailed`] for handshake or transport failures,
/// and the pin-verification errors
/// [`QuicTransportError::PinnedCertificateMismatch`],
/// [`QuicTransportError::MissingPeerIdentity`],
/// [`QuicTransportError::UnexpectedPeerIdentityType`], and
/// [`QuicTransportError::EmptyPeerCertificateChain`].
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

/// Connects over QUIC, reporting an explicit fallback request instead of an
/// error when policy allows.
///
/// Fallback is opt-in and never automatic: a QUIC failure becomes
/// [`QuicConnectOutcome::FallbackRequired`] only when `fallback_policy` is
/// [`TcpFallbackPolicy::AllowExplicit`] *and* `privileged_path` is `false`.
/// Privileged paths always fail closed so they can never be downgraded to TCP.
///
/// # Errors
///
/// Propagates the underlying [`connect_quic`] error whenever fallback is not
/// permitted.
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

/// Writes one length-prefixed frame (big-endian `u32` length, then payload) and
/// flushes the stream.
///
/// # Errors
///
/// Returns [`QuicTransportError::FrameTooLarge`] when `payload` exceeds
/// `max_frame_bytes` or cannot fit the `u32` length prefix, and
/// [`QuicTransportError::FrameWriteFailed`] on stream IO errors.
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
    // The length prefix is a u32, so payloads beyond u32::MAX are unframeable
    // even when `max_frame_bytes` would allow them.
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

/// Reads one length-prefixed frame, rejecting payloads larger than `max_frame_bytes`.
///
/// The announced size is validated before the payload buffer is allocated, so
/// a hostile peer cannot force an oversized allocation via the length prefix.
///
/// # Errors
///
/// Returns [`QuicTransportError::FrameTooLarge`] when the announced size
/// exceeds `max_frame_bytes`, and [`QuicTransportError::FrameReadFailed`] when
/// the stream ends early or another IO error occurs.
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
    let Some(expected) = normalize_pinned_server_fingerprint(expected_fingerprint_sha256)? else {
        return Ok(());
    };
    let peer_identity =
        connection.peer_identity().ok_or(QuicTransportError::MissingPeerIdentity)?;
    let peer_certs = downcast_peer_certificates(peer_identity.as_ref())?;
    // rustls orders the chain leaf-first, so the pin applies to the
    // end-entity certificate.
    let peer_leaf = peer_certs.first().ok_or(QuicTransportError::EmptyPeerCertificateChain)?;
    let actual = hex::encode(Sha256::digest(peer_leaf.as_ref()));
    if actual.eq_ignore_ascii_case(expected.as_str()) {
        return Ok(());
    }
    Err(QuicTransportError::PinnedCertificateMismatch { expected, actual })
}

fn normalize_pinned_server_fingerprint(
    expected_fingerprint_sha256: Option<&str>,
) -> Result<Option<String>, QuicTransportError> {
    let Some(expected) = expected_fingerprint_sha256 else {
        return Ok(None);
    };
    let expected = expected.trim().to_ascii_lowercase();
    // An empty pin is a configuration error, not "no pinning": failing closed
    // prevents a misrendered config from silently disabling pin enforcement.
    if expected.is_empty() {
        return Err(QuicTransportError::TlsConfigurationFailed {
            message: "pinned server fingerprint cannot be empty".to_owned(),
        });
    }
    Ok(Some(expected))
}

fn downcast_peer_certificates(
    identity: &dyn Any,
) -> Result<&[CertificateDer<'static>], QuicTransportError> {
    // quinn exposes the peer identity as `Box<dyn Any>`; with the rustls
    // backend it is the presented certificate chain.
    identity
        .downcast_ref::<Vec<CertificateDer<'static>>>()
        .map(Vec::as_slice)
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

#[cfg(test)]
mod tests {
    use super::{normalize_pinned_server_fingerprint, QuicClientTlsConfig, QuicTransportError};

    #[test]
    fn missing_pinned_server_fingerprint_is_allowed() {
        assert_eq!(
            normalize_pinned_server_fingerprint(None).expect("missing pin should be allowed"),
            None
        );
    }

    #[test]
    fn empty_pinned_server_fingerprint_is_rejected() {
        let error = normalize_pinned_server_fingerprint(Some("  "))
            .expect_err("empty pin should fail closed");
        assert!(matches!(
            error,
            QuicTransportError::TlsConfigurationFailed { message }
                if message == "pinned server fingerprint cannot be empty"
        ));
    }

    #[test]
    fn pinned_server_fingerprint_is_trimmed_and_lowercased() {
        assert_eq!(
            normalize_pinned_server_fingerprint(Some("  ABCDEF0123  "))
                .expect("pin normalization should succeed"),
            Some("abcdef0123".to_owned())
        );
    }

    #[test]
    fn quic_client_tls_config_debug_redacts_pem_material() {
        let config = QuicClientTlsConfig {
            ca_cert_pem: "raw-ca-cert-pem".to_owned(),
            client_cert_pem: Some("raw-client-cert-pem".to_owned()),
            client_key_pem: Some("raw-client-key-pem".to_owned()),
            server_name: "daemon.example.test".to_owned(),
            pinned_server_fingerprint_sha256: Some("abcdef0123".to_owned()),
        };

        let rendered = format!("{config:?}");

        assert!(!rendered.contains("raw-ca-cert-pem"));
        assert!(!rendered.contains("raw-client-cert-pem"));
        assert!(!rendered.contains("raw-client-key-pem"));
        assert!(rendered.contains("<redacted>"));
        assert!(rendered.contains("daemon.example.test"));
        assert!(rendered.contains("abcdef0123"));
    }
}
