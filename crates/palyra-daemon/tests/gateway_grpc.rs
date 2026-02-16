use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    process::{Child, ChildStdout, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use tokio_stream::StreamExt;
use tonic::Code;

const ADMIN_TOKEN: &str = "test-admin-token";
const DEVICE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAV";
const SESSION_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAW";
const RUN_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAX";
const ENVELOPE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAY";

pub mod proto {
    pub mod palyra {
        pub mod common {
            pub mod v1 {
                tonic::include_proto!("palyra.common.v1");
            }
        }

        pub mod gateway {
            pub mod v1 {
                tonic::include_proto!("palyra.gateway.v1");
            }
        }
    }
}

use proto::palyra::{common::v1 as common_v1, gateway::v1 as gateway_v1};

#[tokio::test(flavor = "multi_thread")]
async fn grpc_gateway_enforces_auth_and_streams_status() -> Result<()> {
    let (child, admin_port, grpc_port) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");

    let mut unauthorized_client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect unauthorized gRPC client")?;
    let denied = unauthorized_client
        .run_stream(tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request()])))
        .await
        .expect_err("run_stream should reject requests without auth context");
    assert_eq!(denied.code(), Code::PermissionDenied);

    let mut client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect gRPC client")?;
    let health = client
        .get_health(gateway_v1::HealthRequest { v: 1 })
        .await
        .context("failed to call GetHealth")?
        .into_inner();
    assert_eq!(health.status, "ok");
    assert_eq!(health.service, "palyrad");

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request()]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut saw_accepted = false;
    let mut saw_done = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body {
            if status.kind == common_v1::stream_status::StatusKind::Accepted as i32 {
                saw_accepted = true;
            }
            if status.kind == common_v1::stream_status::StatusKind::Done as i32 {
                saw_done = true;
            }
        }
    }
    assert!(saw_accepted, "run stream should emit accepted status");
    assert!(saw_done, "run stream should emit done status");

    Ok(())
}

fn sample_run_stream_request() -> common_v1::RunStreamRequest {
    common_v1::RunStreamRequest {
        v: 1,
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        run_id: Some(common_v1::CanonicalId { ulid: RUN_ID.to_owned() }),
        input: Some(common_v1::MessageEnvelope {
            v: 1,
            envelope_id: Some(common_v1::CanonicalId { ulid: ENVELOPE_ID.to_owned() }),
            content: Some(common_v1::MessageContent {
                text: "hello from grpc integration".to_owned(),
                attachments: Vec::new(),
            }),
            ..Default::default()
        }),
        allow_sensitive_tools: false,
    }
}

fn spawn_palyrad_with_dynamic_ports() -> Result<(Child, u16, u16)> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_palyrad"))
        .args([
            "--bind",
            "127.0.0.1",
            "--port",
            "0",
            "--grpc-bind",
            "127.0.0.1",
            "--grpc-port",
            "0",
        ])
        .env("PALYRA_ADMIN_TOKEN", ADMIN_TOKEN)
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port))
}

fn wait_for_listen_ports(stdout: ChildStdout, daemon: &mut Child) -> Result<(u16, u16)> {
    let (sender, receiver) = mpsc::channel::<Result<(u16, u16), String>>();
    thread::spawn(move || {
        let mut sender = Some(sender);
        let mut admin_port = None::<u16>;
        let mut grpc_port = None::<u16>;
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err("failed to read palyrad stdout line".to_owned()));
                }
                return;
            };

            if admin_port.is_none() {
                admin_port = parse_port_from_log(&line, "\"listen_addr\":\"");
            }
            if grpc_port.is_none() {
                grpc_port = parse_port_from_log(&line, "\"grpc_listen_addr\":\"");
            }

            if let (Some(admin_port), Some(grpc_port)) = (admin_port, grpc_port) {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Ok((admin_port, grpc_port)));
                }
                return;
            }
        }

        if let Some(sender) = sender.take() {
            let _ = sender.send(Err(
                "palyrad stdout closed before admin and gRPC listen addresses were published"
                    .to_owned(),
            ));
        }
    });

    let timeout_at = Instant::now() + Duration::from_secs(10);
    loop {
        match receiver.recv_timeout(Duration::from_millis(100)) {
            Ok(Ok(ports)) => return Ok(ports),
            Ok(Err(message)) => anyhow::bail!("{message}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                anyhow::bail!("listen-address reader disconnected before publishing ports");
            }
        }

        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad listen address logs");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
            anyhow::bail!(
                "palyrad exited before publishing listen addresses with status: {status}"
            );
        }
    }
}

fn parse_port_from_log(line: &str, prefix: &str) -> Option<u16> {
    let start = line.find(prefix)? + prefix.len();
    let rest = &line[start..];
    let end = rest.find('"')?;
    rest[..end].parse::<SocketAddr>().ok().map(|address| address.port())
}

fn wait_for_health(port: u16, daemon: &mut Child) -> Result<()> {
    let timeout_at = Instant::now() + Duration::from_secs(10);
    let request = b"GET /healthz HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";

    loop {
        if Instant::now() > timeout_at {
            anyhow::bail!("timed out waiting for palyrad health endpoint");
        }
        if let Some(status) = daemon.try_wait().context("failed to check palyrad status")? {
            anyhow::bail!("palyrad exited before becoming healthy with status: {status}");
        }
        if let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = stream.set_write_timeout(Some(Duration::from_millis(300)));
            let _ = stream.set_read_timeout(Some(Duration::from_millis(300)));
            if stream.write_all(request).is_ok() {
                let mut response = String::new();
                if stream.read_to_string(&mut response).is_ok()
                    && response.starts_with("HTTP/1.1 200")
                {
                    return Ok(());
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
}

struct ChildGuard {
    child: Child,
}

impl ChildGuard {
    fn new(child: Child) -> Self {
        Self { child }
    }

    fn child_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}
