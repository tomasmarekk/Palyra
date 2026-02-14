use std::{net::SocketAddr, sync::Arc, time::SystemTime};

use anyhow::{Context, Result};
use palyra_identity::{
    build_node_rpc_server_mtls_config, build_paired_device_client_mtls_config,
    build_unpaired_client_config, DeviceIdentity, IdentityError, IdentityManager,
    PairingClientKind, PairingMethod,
};
use rustls::pki_types::ServerName;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

async fn spawn_mtls_echo_server(
    config: rustls::ServerConfig,
) -> Result<(SocketAddr, oneshot::Sender<()>, tokio::task::JoinHandle<()>)> {
    let listener =
        TcpListener::bind("127.0.0.1:0").await.context("failed to bind test listener")?;
    let address = listener.local_addr().context("failed to read listener address")?;
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => break,
                incoming = listener.accept() => {
                    if let Ok((stream, _)) = incoming {
                        let acceptor = acceptor.clone();
                        tokio::spawn(async move {
                            if let Ok(mut tls_stream) = acceptor.accept(stream).await {
                                let mut buf = [0_u8; 4];
                                if tls_stream.read_exact(&mut buf).await.is_ok() && &buf == b"ping" {
                                    let _ = tls_stream.write_all(b"pong").await;
                                }
                            }
                        });
                    }
                }
            }
        }
    });

    Ok((address, shutdown_tx, task))
}

async fn send_ping(address: SocketAddr, config: rustls::ClientConfig) -> Result<()> {
    let stream = TcpStream::connect(address).await.context("failed to connect to test server")?;
    let connector = TlsConnector::from(Arc::new(config));
    let server_name =
        ServerName::try_from("localhost").context("failed to parse server name")?.to_owned();
    let mut tls_stream =
        connector.connect(server_name, stream).await.context("mTLS handshake failed")?;
    tls_stream.write_all(b"ping").await.context("failed to write ping")?;
    let mut response = [0_u8; 4];
    tls_stream.read_exact(&mut response).await.context("failed to read pong")?;
    if &response != b"pong" {
        anyhow::bail!("unexpected server response");
    }
    Ok(())
}

#[tokio::test]
async fn pairing_connect_rotate_flow_requires_valid_client_cert() -> Result<()> {
    let mut manager =
        IdentityManager::with_memory_store().context("failed to build identity manager")?;
    let device_id = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
    let device =
        DeviceIdentity::generate(device_id).context("failed to generate device identity")?;

    let session = manager
        .start_pairing(
            PairingClientKind::Node,
            PairingMethod::Pin { code: "123456".to_owned() },
            SystemTime::now(),
        )
        .context("failed to start pairing session")?;
    let hello = manager
        .build_device_hello(&session, &device, "123456")
        .context("failed to build device hello")?;
    let pairing_result =
        manager.complete_pairing(hello, SystemTime::now()).context("failed to complete pairing")?;

    let server_certificate = manager
        .issue_gateway_server_certificate("palyrad-node-rpc")
        .context("failed to issue server certificate")?;
    let server_config = build_node_rpc_server_mtls_config(
        &pairing_result.gateway_ca_certificate_pem,
        &server_certificate,
    )
    .context("failed to build server mTLS config")?;
    let (address, shutdown_tx, server_task) = spawn_mtls_echo_server(server_config).await?;

    let paired_client = build_paired_device_client_mtls_config(
        &pairing_result.gateway_ca_certificate_pem,
        &pairing_result.device.current_certificate,
    )
    .context("failed to build paired client mTLS config")?;
    send_ping(address, paired_client).await.context("paired device should connect over mTLS")?;

    let unpaired_client = build_unpaired_client_config(&pairing_result.gateway_ca_certificate_pem)
        .context("failed to build unpaired client config")?;
    let unpaired_result = send_ping(address, unpaired_client).await;
    assert!(unpaired_result.is_err(), "unpaired device unexpectedly connected");

    let rotated_certificate = manager
        .force_rotate_device_certificate(device_id)
        .context("failed to rotate device cert")?;
    let rotated_client = build_paired_device_client_mtls_config(
        &pairing_result.gateway_ca_certificate_pem,
        &rotated_certificate,
    )
    .context("failed to build rotated client config")?;
    send_ping(address, rotated_client)
        .await
        .context("rotated paired device should connect over mTLS")?;

    manager
        .revoke_device(device_id, "lost device", SystemTime::now())
        .context("failed to revoke device")?;
    let revoked_rotate = manager.force_rotate_device_certificate(device_id);
    assert!(
        matches!(revoked_rotate, Err(IdentityError::DeviceRevoked)),
        "revoked device should not be rotated"
    );

    let _ = shutdown_tx.send(());
    let _ = server_task.await;
    Ok(())
}
