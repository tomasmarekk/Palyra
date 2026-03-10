use std::{net::SocketAddr, sync::Arc};

use anyhow::{anyhow, Context, Result};
use palyra_transport_quic::{
    build_server_endpoint, read_frame, write_frame, QuicServerTlsConfig, QuicTransportLimits,
    DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION,
};
use rustls::server::danger::ClientCertVerifier;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::{debug, warn};

const METHOD_HEALTH: &str = "node.health";
const METHOD_STREAM_EVENTS: &str = "node.stream_events";
const MAX_STREAM_SEQUENCE: u64 = 5;
const MAX_CONCURRENT_CONNECTIONS: usize = 256;

#[derive(Clone)]
pub struct QuicRuntimeTlsMaterial {
    pub ca_cert_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub require_client_auth: bool,
    pub client_cert_verifier: Option<Arc<dyn ClientCertVerifier>>,
}

impl std::fmt::Debug for QuicRuntimeTlsMaterial {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("QuicRuntimeTlsMaterial")
            .field("ca_cert_pem", &"<redacted>")
            .field("cert_pem", &"<redacted>")
            .field("key_pem", &"<redacted>")
            .field("require_client_auth", &self.require_client_auth)
            .field("has_client_cert_verifier", &self.client_cert_verifier.is_some())
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuicRuntimeRequest {
    protocol_version: u16,
    method: String,
    #[serde(default)]
    resume_from: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct QuicRuntimeResponse {
    protocol_version: u16,
    kind: String,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    seq: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_rpc_mtls_required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

pub fn bind_endpoint(
    bind_addr: SocketAddr,
    tls_material: &QuicRuntimeTlsMaterial,
    limits: &QuicTransportLimits,
) -> Result<quinn::Endpoint> {
    build_server_endpoint(
        bind_addr,
        &QuicServerTlsConfig {
            ca_cert_pem: tls_material.ca_cert_pem.clone(),
            cert_pem: tls_material.cert_pem.clone(),
            key_pem: tls_material.key_pem.clone(),
            require_client_auth: tls_material.require_client_auth,
            client_cert_verifier: tls_material.client_cert_verifier.clone(),
        },
        limits,
    )
    .map_err(|error| anyhow!(error))
    .with_context(|| format!("failed to initialize QUIC endpoint on {bind_addr}"))
}

pub async fn serve(endpoint: quinn::Endpoint, node_rpc_mtls_required: bool) -> Result<()> {
    serve_with_connection_limit(endpoint, node_rpc_mtls_required, MAX_CONCURRENT_CONNECTIONS).await
}

async fn serve_with_connection_limit(
    endpoint: quinn::Endpoint,
    node_rpc_mtls_required: bool,
    max_concurrent_connections: usize,
) -> Result<()> {
    let connection_slots = Arc::new(Semaphore::new(max_concurrent_connections.max(1)));
    while let Some(connecting) = endpoint.accept().await {
        let permit = match connection_slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                warn!(
                    max_concurrent_connections = max_concurrent_connections.max(1),
                    "QUIC connection dropped: global concurrency limit reached"
                );
                continue;
            }
        };
        tokio::spawn(async move {
            let _permit = permit;
            let connection = match connecting.await {
                Ok(connection) => connection,
                Err(error) => {
                    warn!(error = %error, "QUIC handshake rejected");
                    return;
                }
            };
            debug!(
                remote_address = %connection.remote_address(),
                "accepted QUIC connection"
            );
            if let Err(error) = handle_connection(connection, node_rpc_mtls_required).await {
                warn!(error = %error, "QUIC connection handling failed");
            }
        });
    }
    Ok(())
}

async fn handle_connection(
    connection: quinn::Connection,
    node_rpc_mtls_required: bool,
) -> Result<()> {
    loop {
        let (mut send_stream, mut recv_stream) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(_) => break,
        };
        if let Err(error) =
            handle_stream(&mut send_stream, &mut recv_stream, node_rpc_mtls_required).await
        {
            warn!(error = %error, "QUIC stream handling failed");
            let _ = send_protocol_error(&mut send_stream, "stream_failure").await;
        }
        let _ = send_stream.finish();
    }
    Ok(())
}

async fn handle_stream(
    send_stream: &mut quinn::SendStream,
    recv_stream: &mut quinn::RecvStream,
    node_rpc_mtls_required: bool,
) -> Result<()> {
    let payload = read_frame(recv_stream, DEFAULT_MAX_FRAME_BYTES)
        .await
        .map_err(|error| anyhow!(error))
        .context("failed to read QUIC request frame")?;
    let request = match serde_json::from_slice::<QuicRuntimeRequest>(payload.as_slice()) {
        Ok(request) => request,
        Err(_) => {
            send_protocol_error(send_stream, "invalid_request").await?;
            return Ok(());
        }
    };
    if request.protocol_version != PROTOCOL_VERSION {
        send_protocol_error(send_stream, "protocol_mismatch").await?;
        return Ok(());
    }

    match request.method.as_str() {
        METHOD_HEALTH => {
            send_response(
                send_stream,
                QuicRuntimeResponse {
                    protocol_version: PROTOCOL_VERSION,
                    kind: "health".to_owned(),
                    ok: true,
                    seq: None,
                    node_rpc_mtls_required: Some(node_rpc_mtls_required),
                    error: None,
                },
            )
            .await
        }
        METHOD_STREAM_EVENTS => {
            let start = request.resume_from.unwrap_or(0).saturating_add(1);
            for sequence in start..=MAX_STREAM_SEQUENCE {
                send_response(
                    send_stream,
                    QuicRuntimeResponse {
                        protocol_version: PROTOCOL_VERSION,
                        kind: "event".to_owned(),
                        ok: true,
                        seq: Some(sequence),
                        node_rpc_mtls_required: None,
                        error: None,
                    },
                )
                .await?;
            }
            Ok(())
        }
        _ => send_protocol_error(send_stream, "unsupported_method").await,
    }
}

async fn send_protocol_error(send_stream: &mut quinn::SendStream, reason: &str) -> Result<()> {
    send_response(
        send_stream,
        QuicRuntimeResponse {
            protocol_version: PROTOCOL_VERSION,
            kind: "error".to_owned(),
            ok: false,
            seq: None,
            node_rpc_mtls_required: None,
            error: Some(reason.to_owned()),
        },
    )
    .await
}

async fn send_response(
    send_stream: &mut quinn::SendStream,
    response: QuicRuntimeResponse,
) -> Result<()> {
    let payload = serde_json::to_vec(&response)
        .context("failed to serialize QUIC runtime response payload")?;
    write_frame(send_stream, payload.as_slice(), DEFAULT_MAX_FRAME_BYTES)
        .await
        .map_err(|error| anyhow!(error))
        .context("failed to write QUIC runtime response frame")
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
        time::Duration,
    };

    use palyra_identity::{
        build_revocation_aware_client_verifier, CertificateAuthority, MemoryRevocationIndex,
    };
    use palyra_transport_quic::{
        build_client_endpoint, connect_quic, read_frame, write_frame, QuicClientTlsConfig,
        QuicTransportLimits, DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION,
    };
    use rustls::pki_types::{pem::PemObject, CertificateDer};
    use sha2::{Digest, Sha256};

    use super::{
        bind_endpoint, serve, serve_with_connection_limit, QuicRuntimeRequest, QuicRuntimeResponse,
        QuicRuntimeTlsMaterial, METHOD_HEALTH, METHOD_STREAM_EVENTS,
    };

    struct TestPki {
        ca_cert_pem: String,
        server_cert_pem: String,
        server_key_pem: String,
        client_cert_pem: String,
        client_key_pem: String,
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quic_runtime_supports_health_and_stream_resume() {
        let pki = build_test_pki();
        let limits = QuicTransportLimits::default();
        let server_endpoint = bind_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            &QuicRuntimeTlsMaterial {
                ca_cert_pem: pki.ca_cert_pem.clone(),
                cert_pem: pki.server_cert_pem.clone(),
                key_pem: pki.server_key_pem.clone(),
                require_client_auth: true,
                client_cert_verifier: None,
            },
            &limits,
        )
        .expect("QUIC endpoint should bind");
        let server_addr =
            server_endpoint.local_addr().expect("bound QUIC endpoint should expose listen address");
        let server_task = tokio::spawn(serve(server_endpoint, true));

        let client_tls = QuicClientTlsConfig {
            ca_cert_pem: pki.ca_cert_pem.clone(),
            client_cert_pem: Some(pki.client_cert_pem.clone()),
            client_key_pem: Some(pki.client_key_pem.clone()),
            server_name: "localhost".to_owned(),
            pinned_server_fingerprint_sha256: None,
        };
        let client_endpoint = build_client_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            &client_tls,
            &limits,
        )
        .expect("client endpoint should bind");
        let connection = connect_quic(&client_endpoint, server_addr, &client_tls, &limits)
            .await
            .expect("client should connect over mTLS");

        let (mut health_send, mut health_recv) =
            connection.open_bi().await.expect("health stream should open");
        send_request(
            &mut health_send,
            QuicRuntimeRequest {
                protocol_version: PROTOCOL_VERSION,
                method: METHOD_HEALTH.to_owned(),
                resume_from: None,
            },
        )
        .await;
        health_send.finish().expect("health stream should finish");
        let health_payload = read_frame(&mut health_recv, DEFAULT_MAX_FRAME_BYTES)
            .await
            .expect("health response should be readable");
        let health: QuicRuntimeResponse =
            serde_json::from_slice(health_payload.as_slice()).expect("health JSON should parse");
        assert!(health.ok, "health request should succeed");
        assert_eq!(health.kind, "health");
        assert_eq!(health.node_rpc_mtls_required, Some(true));

        let (mut stream_send, mut stream_recv) =
            connection.open_bi().await.expect("stream request should open");
        send_request(
            &mut stream_send,
            QuicRuntimeRequest {
                protocol_version: PROTOCOL_VERSION,
                method: METHOD_STREAM_EVENTS.to_owned(),
                resume_from: Some(2),
            },
        )
        .await;
        stream_send.finish().expect("stream request should finish");

        let mut sequences = Vec::new();
        loop {
            let payload = match read_frame(&mut stream_recv, DEFAULT_MAX_FRAME_BYTES).await {
                Ok(payload) => payload,
                Err(_) => break,
            };
            let response: QuicRuntimeResponse = match serde_json::from_slice(payload.as_slice()) {
                Ok(response) => response,
                Err(_) => break,
            };
            if let Some(sequence) = response.seq {
                sequences.push(sequence);
            }
        }
        assert_eq!(sequences, vec![3, 4, 5]);

        connection.close(0_u32.into(), b"test complete");
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quic_runtime_rejects_connections_above_global_limit() {
        let pki = build_test_pki();
        let limits = QuicTransportLimits {
            handshake_timeout: Duration::from_millis(750),
            ..QuicTransportLimits::default()
        };
        let server_endpoint = bind_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            &QuicRuntimeTlsMaterial {
                ca_cert_pem: pki.ca_cert_pem.clone(),
                cert_pem: pki.server_cert_pem.clone(),
                key_pem: pki.server_key_pem.clone(),
                require_client_auth: true,
                client_cert_verifier: None,
            },
            &limits,
        )
        .expect("QUIC endpoint should bind");
        let server_addr =
            server_endpoint.local_addr().expect("bound QUIC endpoint should expose listen address");
        let server_task = tokio::spawn(serve_with_connection_limit(server_endpoint, true, 1));

        let client_tls = QuicClientTlsConfig {
            ca_cert_pem: pki.ca_cert_pem.clone(),
            client_cert_pem: Some(pki.client_cert_pem.clone()),
            client_key_pem: Some(pki.client_key_pem.clone()),
            server_name: "localhost".to_owned(),
            pinned_server_fingerprint_sha256: None,
        };
        let client_endpoint = build_client_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            &client_tls,
            &limits,
        )
        .expect("client endpoint should bind");

        let first_connection = connect_quic(&client_endpoint, server_addr, &client_tls, &limits)
            .await
            .expect("first client should connect within limit");

        let second_connection =
            connect_quic(&client_endpoint, server_addr, &client_tls, &limits).await;
        assert!(
            second_connection.is_err(),
            "second client should be rejected once global connection limit is reached"
        );

        let (mut health_send, mut health_recv) =
            first_connection.open_bi().await.expect("health stream should open");
        send_request(
            &mut health_send,
            QuicRuntimeRequest {
                protocol_version: PROTOCOL_VERSION,
                method: METHOD_HEALTH.to_owned(),
                resume_from: None,
            },
        )
        .await;
        health_send.finish().expect("health stream should finish");
        let health_payload = read_frame(&mut health_recv, DEFAULT_MAX_FRAME_BYTES)
            .await
            .expect("health response should be readable");
        let health: QuicRuntimeResponse =
            serde_json::from_slice(health_payload.as_slice()).expect("health JSON should parse");
        assert!(health.ok, "first connection should remain healthy within limit");

        first_connection.close(0_u32.into(), b"test complete");
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quic_runtime_reports_protocol_mismatch() {
        let pki = build_test_pki();
        let limits = QuicTransportLimits::default();
        let server_endpoint = bind_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            &QuicRuntimeTlsMaterial {
                ca_cert_pem: pki.ca_cert_pem.clone(),
                cert_pem: pki.server_cert_pem.clone(),
                key_pem: pki.server_key_pem.clone(),
                require_client_auth: true,
                client_cert_verifier: None,
            },
            &limits,
        )
        .expect("QUIC endpoint should bind");
        let server_addr =
            server_endpoint.local_addr().expect("bound QUIC endpoint should expose listen address");
        let server_task = tokio::spawn(serve(server_endpoint, true));

        let client_tls = QuicClientTlsConfig {
            ca_cert_pem: pki.ca_cert_pem.clone(),
            client_cert_pem: Some(pki.client_cert_pem.clone()),
            client_key_pem: Some(pki.client_key_pem.clone()),
            server_name: "localhost".to_owned(),
            pinned_server_fingerprint_sha256: None,
        };
        let client_endpoint = build_client_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            &client_tls,
            &limits,
        )
        .expect("client endpoint should bind");
        let connection = connect_quic(&client_endpoint, server_addr, &client_tls, &limits)
            .await
            .expect("client should connect over mTLS");

        let (mut request_send, mut request_recv) =
            connection.open_bi().await.expect("request stream should open");
        send_request(
            &mut request_send,
            QuicRuntimeRequest {
                protocol_version: PROTOCOL_VERSION.saturating_add(1),
                method: METHOD_HEALTH.to_owned(),
                resume_from: None,
            },
        )
        .await;
        request_send.finish().expect("request stream should finish");
        let payload = read_frame(&mut request_recv, DEFAULT_MAX_FRAME_BYTES)
            .await
            .expect("error response should be readable");
        let response: QuicRuntimeResponse =
            serde_json::from_slice(payload.as_slice()).expect("error response JSON should parse");
        assert!(!response.ok);
        assert_eq!(response.kind, "error");
        assert_eq!(response.error.as_deref(), Some("protocol_mismatch"));

        connection.close(0_u32.into(), b"test complete");
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quic_runtime_rejects_revoked_client_certificate() {
        let pki = build_test_pki();
        let limits = QuicTransportLimits::default();
        let revoked_client_fingerprint = certificate_fingerprint_hex(&pki.client_cert_pem);
        let client_cert_verifier = build_revocation_aware_client_verifier(
            &pki.ca_cert_pem,
            Arc::new(MemoryRevocationIndex::from_fingerprints(HashSet::from([
                revoked_client_fingerprint,
            ]))),
        )
        .expect("revocation-aware verifier should build");
        let server_endpoint = bind_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            &QuicRuntimeTlsMaterial {
                ca_cert_pem: pki.ca_cert_pem.clone(),
                cert_pem: pki.server_cert_pem.clone(),
                key_pem: pki.server_key_pem.clone(),
                require_client_auth: true,
                client_cert_verifier: Some(client_cert_verifier),
            },
            &limits,
        )
        .expect("QUIC endpoint should bind");
        let server_addr =
            server_endpoint.local_addr().expect("bound QUIC endpoint should expose listen address");
        let server_task = tokio::spawn(serve(server_endpoint, true));

        let client_tls = QuicClientTlsConfig {
            ca_cert_pem: pki.ca_cert_pem.clone(),
            client_cert_pem: Some(pki.client_cert_pem.clone()),
            client_key_pem: Some(pki.client_key_pem.clone()),
            server_name: "localhost".to_owned(),
            pinned_server_fingerprint_sha256: None,
        };
        let client_endpoint = build_client_endpoint(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            &client_tls,
            &limits,
        )
        .expect("client endpoint should bind");
        match connect_quic(&client_endpoint, server_addr, &client_tls, &limits).await {
            Err(error) => {
                let message = error.to_string().to_ascii_lowercase();
                assert!(
                    message.contains("revoked")
                        || message.contains("invalid peer certificate")
                        || message.contains("handshake failed"),
                    "revoked QUIC client certificate must fail closed during connect: {error}"
                );
            }
            Ok(connection) => match connection.open_bi().await {
                Err(error) => {
                    let message = error.to_string().to_ascii_lowercase();
                    assert!(
                        message.contains("closed")
                            || message.contains("revoked")
                            || message.contains("invalid peer certificate")
                            || message.contains("handshake failed"),
                        "revoked QUIC connection should be closed before any runtime method access: {error}"
                    );
                }
                Ok((mut send_stream, mut recv_stream)) => {
                    let payload = serde_json::to_vec(&QuicRuntimeRequest {
                        protocol_version: PROTOCOL_VERSION,
                        method: METHOD_HEALTH.to_owned(),
                        resume_from: None,
                    })
                    .expect("request should serialize");
                    match write_frame(&mut send_stream, payload.as_slice(), DEFAULT_MAX_FRAME_BYTES)
                        .await
                    {
                        Err(error) => {
                            let message = error.to_string().to_ascii_lowercase();
                            assert!(
                                message.contains("connection lost")
                                    || message.contains("closed")
                                    || message.contains("revoked")
                                    || message.contains("invalid peer certificate")
                                    || message.contains("handshake failed"),
                                "revoked client certificate must fail closed before request frame write completes: {error}"
                            );
                        }
                        Ok(()) => {
                            let finish_result = send_stream.finish();
                            assert!(
                                finish_result
                                    .as_ref()
                                    .err()
                                    .map(|error| {
                                        let message = error.to_string().to_ascii_lowercase();
                                        message.contains("connection lost")
                                            || message.contains("closed")
                                            || message.contains("revoked")
                                            || message.contains("invalid peer certificate")
                                            || message.contains("handshake failed")
                                    })
                                    .unwrap_or(true),
                                "revoked client certificate must not keep the request stream writable: {finish_result:?}"
                            );
                        }
                    }
                    let response = read_frame(&mut recv_stream, DEFAULT_MAX_FRAME_BYTES).await;
                    assert!(
                        response
                            .as_ref()
                            .err()
                            .map(|error| {
                                let message = error.to_string().to_ascii_lowercase();
                                message.contains("connection lost")
                                    || message.contains("closed")
                                    || message.contains("revoked")
                            })
                            .unwrap_or(false),
                        "revoked client certificate must not receive a QUIC runtime response: {response:?}"
                    );
                }
            },
        }

        server_task.abort();
        let _ = server_task.await;
    }

    async fn send_request(send_stream: &mut quinn::SendStream, request: QuicRuntimeRequest) {
        let payload = serde_json::to_vec(&request).expect("request should serialize");
        write_frame(send_stream, payload.as_slice(), DEFAULT_MAX_FRAME_BYTES)
            .await
            .expect("request frame should write");
    }

    fn certificate_fingerprint_hex(certificate_pem: &str) -> String {
        let certificate = CertificateDer::from_pem_slice(certificate_pem.as_bytes())
            .expect("certificate PEM should parse");
        hex::encode(Sha256::digest(certificate.as_ref()))
    }

    fn build_test_pki() -> TestPki {
        let mut certificate_authority =
            CertificateAuthority::new("palyra-daemon-quic-runtime-test")
                .expect("test certificate authority should initialize");
        let server = certificate_authority
            .issue_server_certificate("localhost", Duration::from_secs(3_600))
            .expect("server certificate should issue");
        let client = certificate_authority
            .issue_client_certificate(
                "test-device",
                "a".repeat(64).as_str(),
                Duration::from_secs(3_600),
            )
            .expect("client certificate should issue");
        TestPki {
            ca_cert_pem: certificate_authority.certificate_pem,
            server_cert_pem: server.certificate_pem,
            server_key_pem: server.private_key_pem,
            client_cert_pem: client.certificate_pem,
            client_key_pem: client.private_key_pem,
        }
    }
}
