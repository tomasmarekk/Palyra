use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    time::Duration,
};

use palyra_identity::CertificateAuthority;
use palyra_transport_quic::{
    build_client_endpoint, build_server_endpoint, connect_quic, connect_with_explicit_fallback,
    read_frame, write_frame, QuicClientTlsConfig, QuicConnectOutcome, QuicServerTlsConfig,
    QuicTransportLimits, TcpFallbackPolicy, DEFAULT_MAX_FRAME_BYTES, PROTOCOL_VERSION,
};
use rcgen::{CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, KeyPair};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
struct TestPki {
    ca_cert_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    untrusted_server_cert_pem: String,
    untrusted_server_key_pem: String,
    expired_server_cert_pem: String,
    expired_server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamRequest {
    protocol_version: u16,
    kind: String,
    resume_from: Option<u64>,
    force_disconnect_after: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StreamResponse {
    protocol_version: u16,
    kind: String,
    ok: bool,
    seq: Option<u64>,
    error: Option<String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_transport_roundtrip_stream_and_reconnect_resume() {
    let pki = build_test_pki();
    let limits = QuicTransportLimits::default();
    let server_tls = QuicServerTlsConfig {
        ca_cert_pem: pki.ca_cert_pem.clone(),
        cert_pem: pki.server_cert_pem.clone(),
        key_pem: pki.server_key_pem.clone(),
        require_client_auth: true,
    };
    let server_endpoint = build_server_endpoint(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        &server_tls,
        &limits,
    )
    .expect("test server endpoint should bind");
    let server_addr = server_endpoint.local_addr().expect("server endpoint should expose addr");

    tokio::spawn(run_test_server(server_endpoint));

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
    .expect("test client endpoint should bind");

    let connection = connect_quic(&client_endpoint, server_addr, &client_tls, &limits)
        .await
        .expect("initial QUIC connection should succeed");
    let (mut send, mut recv) = connection.open_bi().await.expect("initial stream should open");
    let health_request = serde_json::to_vec(&StreamRequest {
        protocol_version: PROTOCOL_VERSION,
        kind: "health".to_owned(),
        resume_from: None,
        force_disconnect_after: None,
    })
    .expect("health request should serialize");
    write_frame(&mut send, health_request.as_slice(), DEFAULT_MAX_FRAME_BYTES)
        .await
        .expect("health request write should succeed");
    send.finish().expect("health stream should finish cleanly");
    let health_response =
        read_frame(&mut recv, DEFAULT_MAX_FRAME_BYTES).await.expect("health response should parse");
    let health: StreamResponse =
        serde_json::from_slice(health_response.as_slice()).expect("health response JSON");
    assert!(health.ok, "health request should return success");

    let (mut send_stream, mut recv_stream) =
        connection.open_bi().await.expect("stream request should open");
    let stream_request = serde_json::to_vec(&StreamRequest {
        protocol_version: PROTOCOL_VERSION,
        kind: "stream".to_owned(),
        resume_from: Some(0),
        force_disconnect_after: Some(2),
    })
    .expect("stream request should serialize");
    write_frame(&mut send_stream, stream_request.as_slice(), DEFAULT_MAX_FRAME_BYTES)
        .await
        .expect("stream request should write");
    send_stream.finish().expect("stream request stream should finish");

    let first = read_stream_seq(&mut recv_stream).await;
    let second = read_stream_seq(&mut recv_stream).await;
    assert_eq!(first, 1, "first event sequence should be 1");
    assert_eq!(second, 2, "second event sequence should be 2");
    let third = read_frame(&mut recv_stream, DEFAULT_MAX_FRAME_BYTES).await;
    assert!(third.is_err(), "forced disconnect should terminate stream");

    let resumed_connection = connect_quic(&client_endpoint, server_addr, &client_tls, &limits)
        .await
        .expect("resume QUIC connection should succeed");
    let (mut resume_send, mut resume_recv) =
        resumed_connection.open_bi().await.expect("resume stream should open");
    let resume_request = serde_json::to_vec(&StreamRequest {
        protocol_version: PROTOCOL_VERSION,
        kind: "stream".to_owned(),
        resume_from: Some(2),
        force_disconnect_after: None,
    })
    .expect("resume request should serialize");
    write_frame(&mut resume_send, resume_request.as_slice(), DEFAULT_MAX_FRAME_BYTES)
        .await
        .expect("resume request should write");
    resume_send.finish().expect("resume stream should finish");
    let resumed = collect_stream_sequences(&mut resume_recv).await;
    assert_eq!(resumed, vec![3, 4, 5], "resume should avoid replaying already-acked events");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_transport_rejects_invalid_server_certificate() {
    let pki = build_test_pki();
    let limits = QuicTransportLimits::default();
    let server_endpoint = build_server_endpoint(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        &QuicServerTlsConfig {
            ca_cert_pem: pki.ca_cert_pem.clone(),
            cert_pem: pki.untrusted_server_cert_pem.clone(),
            key_pem: pki.untrusted_server_key_pem.clone(),
            require_client_auth: false,
        },
        &limits,
    )
    .expect("test server endpoint should bind");
    let server_addr = server_endpoint.local_addr().expect("server endpoint addr");
    tokio::spawn(run_test_server(server_endpoint));

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
    .expect("test client endpoint should bind");
    let result = connect_quic(&client_endpoint, server_addr, &client_tls, &limits).await;
    assert!(result.is_err(), "untrusted server certificate should fail TLS verification");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_transport_rejects_expired_server_certificate() {
    let pki = build_test_pki();
    let limits = QuicTransportLimits::default();
    let server_endpoint = build_server_endpoint(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        &QuicServerTlsConfig {
            ca_cert_pem: pki.ca_cert_pem.clone(),
            cert_pem: pki.expired_server_cert_pem.clone(),
            key_pem: pki.expired_server_key_pem.clone(),
            require_client_auth: false,
        },
        &limits,
    )
    .expect("test server endpoint should bind");
    let server_addr = server_endpoint.local_addr().expect("server endpoint addr");
    tokio::spawn(run_test_server(server_endpoint));

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
    .expect("test client endpoint should bind");
    let result = connect_quic(&client_endpoint, server_addr, &client_tls, &limits).await;
    assert!(result.is_err(), "expired server certificate should fail TLS verification");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn quic_transport_protocol_mismatch_returns_structured_error_response() {
    let pki = build_test_pki();
    let limits = QuicTransportLimits::default();
    let server_endpoint = build_server_endpoint(
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        &QuicServerTlsConfig {
            ca_cert_pem: pki.ca_cert_pem.clone(),
            cert_pem: pki.server_cert_pem.clone(),
            key_pem: pki.server_key_pem.clone(),
            require_client_auth: true,
        },
        &limits,
    )
    .expect("test server endpoint should bind");
    let server_addr = server_endpoint.local_addr().expect("server endpoint addr");
    tokio::spawn(run_test_server(server_endpoint));

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
    .expect("test client endpoint should bind");
    let connection =
        connect_quic(&client_endpoint, server_addr, &client_tls, &limits).await.expect("connect");

    let (mut send, mut recv) = connection.open_bi().await.expect("stream should open");
    let mismatch_request = serde_json::to_vec(&StreamRequest {
        protocol_version: PROTOCOL_VERSION.saturating_add(1),
        kind: "health".to_owned(),
        resume_from: None,
        force_disconnect_after: None,
    })
    .expect("request serialization should succeed");
    write_frame(&mut send, mismatch_request.as_slice(), DEFAULT_MAX_FRAME_BYTES)
        .await
        .expect("mismatch request write should succeed");
    send.finish().expect("mismatch request stream should finish");

    let payload =
        read_frame(&mut recv, DEFAULT_MAX_FRAME_BYTES).await.expect("response should be readable");
    let response: StreamResponse =
        serde_json::from_slice(payload.as_slice()).expect("response should deserialize");
    assert!(!response.ok, "protocol mismatch should return explicit error response");
    assert_eq!(response.kind, "error");
    assert!(
        response.error.as_deref().map(|value| value.contains("protocol_mismatch")).unwrap_or(false),
        "error response should include protocol_mismatch marker"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fallback_is_explicit_and_disallowed_for_privileged_paths() {
    let pki = build_test_pki();
    let limits = QuicTransportLimits::default();
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
    .expect("test client endpoint should bind");
    let unreachable_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 9);

    let fallback = connect_with_explicit_fallback(
        &client_endpoint,
        unreachable_addr,
        &client_tls,
        &limits,
        TcpFallbackPolicy::AllowExplicit,
        false,
    )
    .await
    .expect("non-privileged connect with fallback should not throw");
    match fallback {
        QuicConnectOutcome::FallbackRequired { .. } => {}
        QuicConnectOutcome::Connected(_) => {
            panic!("unexpected successful QUIC connection to unreachable endpoint")
        }
    }

    let privileged = connect_with_explicit_fallback(
        &client_endpoint,
        unreachable_addr,
        &client_tls,
        &limits,
        TcpFallbackPolicy::AllowExplicit,
        true,
    )
    .await;
    assert!(
        privileged.is_err(),
        "privileged path must fail closed instead of silently downgrading"
    );
}

async fn run_test_server(endpoint: quinn::Endpoint) {
    while let Some(connecting) = endpoint.accept().await {
        tokio::spawn(async move {
            let connection = match connecting.await {
                Ok(connection) => connection,
                Err(_) => return,
            };
            loop {
                let (mut send, mut recv) = match connection.accept_bi().await {
                    Ok(stream) => stream,
                    Err(_) => break,
                };
                let request_payload = match read_frame(&mut recv, DEFAULT_MAX_FRAME_BYTES).await {
                    Ok(payload) => payload,
                    Err(_) => break,
                };
                let request: StreamRequest =
                    match serde_json::from_slice(request_payload.as_slice()) {
                        Ok(request) => request,
                        Err(_) => break,
                    };
                if request.protocol_version != PROTOCOL_VERSION {
                    let _ = send_response(
                        &mut send,
                        StreamResponse {
                            protocol_version: PROTOCOL_VERSION,
                            kind: "error".to_owned(),
                            ok: false,
                            seq: None,
                            error: Some("protocol_mismatch".to_owned()),
                        },
                    )
                    .await;
                    let _ = send.finish();
                    continue;
                }
                if request.kind == "health" {
                    let _ = send_response(
                        &mut send,
                        StreamResponse {
                            protocol_version: PROTOCOL_VERSION,
                            kind: "health".to_owned(),
                            ok: true,
                            seq: None,
                            error: None,
                        },
                    )
                    .await;
                    let _ = send.finish();
                    continue;
                }
                if request.kind != "stream" {
                    let _ = send_response(
                        &mut send,
                        StreamResponse {
                            protocol_version: PROTOCOL_VERSION,
                            kind: "error".to_owned(),
                            ok: false,
                            seq: None,
                            error: Some("unsupported_request".to_owned()),
                        },
                    )
                    .await;
                    let _ = send.finish();
                    continue;
                }

                let resume_from = request.resume_from.unwrap_or(0);
                let disconnect_after = request.force_disconnect_after.unwrap_or(0);
                let mut sent = 0_u64;
                for seq in 1_u64..=5 {
                    if seq <= resume_from {
                        continue;
                    }
                    if send_response(
                        &mut send,
                        StreamResponse {
                            protocol_version: PROTOCOL_VERSION,
                            kind: "event".to_owned(),
                            ok: true,
                            seq: Some(seq),
                            error: None,
                        },
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                    sent = sent.saturating_add(1);
                    if disconnect_after > 0 && sent >= disconnect_after {
                        break;
                    }
                }
                let _ = send.finish();
            }
        });
    }
}

async fn send_response(
    send: &mut quinn::SendStream,
    response: StreamResponse,
) -> Result<(), palyra_transport_quic::QuicTransportError> {
    let payload = serde_json::to_vec(&response).expect("response should serialize");
    write_frame(send, payload.as_slice(), DEFAULT_MAX_FRAME_BYTES).await
}

async fn read_stream_seq(recv_stream: &mut quinn::RecvStream) -> u64 {
    let payload =
        read_frame(recv_stream, DEFAULT_MAX_FRAME_BYTES).await.expect("event frame should parse");
    let response: StreamResponse =
        serde_json::from_slice(payload.as_slice()).expect("event response should deserialize");
    response.seq.expect("stream response should contain sequence")
}

async fn collect_stream_sequences(recv_stream: &mut quinn::RecvStream) -> Vec<u64> {
    let mut sequences = Vec::new();
    loop {
        let payload = match read_frame(recv_stream, DEFAULT_MAX_FRAME_BYTES).await {
            Ok(payload) => payload,
            Err(_) => break,
        };
        let response: StreamResponse = match serde_json::from_slice(payload.as_slice()) {
            Ok(response) => response,
            Err(_) => break,
        };
        if let Some(seq) = response.seq {
            sequences.push(seq);
        }
    }
    sequences
}

fn build_test_pki() -> TestPki {
    let mut trusted_ca = CertificateAuthority::new("palyra-test-ca").expect("CA should build");
    let server = trusted_ca
        .issue_server_certificate("localhost", Duration::from_secs(3_600))
        .expect("server cert should issue");
    let client = trusted_ca
        .issue_client_certificate(
            "test-device",
            "a".repeat(64).as_str(),
            Duration::from_secs(3_600),
        )
        .expect("client cert should issue");

    let mut untrusted_ca =
        CertificateAuthority::new("palyra-untrusted-ca").expect("untrusted CA should build");
    let untrusted_server = untrusted_ca
        .issue_server_certificate("localhost", Duration::from_secs(3_600))
        .expect("untrusted server cert should issue");

    let expired_server = build_expired_server_cert(&trusted_ca);
    TestPki {
        ca_cert_pem: trusted_ca.certificate_pem.clone(),
        server_cert_pem: server.certificate_pem,
        server_key_pem: server.private_key_pem,
        untrusted_server_cert_pem: untrusted_server.certificate_pem,
        untrusted_server_key_pem: untrusted_server.private_key_pem,
        expired_server_cert_pem: expired_server.0,
        expired_server_key_pem: expired_server.1,
        client_cert_pem: client.certificate_pem,
        client_key_pem: client.private_key_pem,
    }
}

fn build_expired_server_cert(ca: &CertificateAuthority) -> (String, String) {
    let stored = ca.to_stored();
    let ca_key =
        KeyPair::from_pem(stored.private_key_pem.as_str()).expect("stored CA key should parse");
    let ca_issuer = rcgen::Issuer::from_ca_cert_pem(stored.certificate_pem.as_str(), &ca_key)
        .expect("stored CA issuer should parse");

    let mut params =
        CertificateParams::new(vec!["localhost".to_owned()]).expect("server params should init");
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CommonName, "localhost");
    params.distinguished_name = distinguished_name;
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.not_before = rcgen::date_time_ymd(2019, 1, 1);
    params.not_after = rcgen::date_time_ymd(2019, 1, 2);

    let expired_key = KeyPair::generate().expect("expired cert key should generate");
    let expired_cert =
        params.signed_by(&expired_key, &ca_issuer).expect("expired cert should sign");
    (expired_cert.pem(), expired_key.serialize_pem())
}
