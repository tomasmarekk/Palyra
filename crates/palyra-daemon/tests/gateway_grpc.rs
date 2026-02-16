use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    process::{Child, ChildStdout, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use reqwest::blocking::Client;
use rusqlite::Connection;
use serde_json::Value;
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
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
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

#[tokio::test(flavor = "multi_thread")]
async fn grpc_gateway_run_stream_emits_at_most_sixteen_model_tokens() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let long_input = (0..64).map(|index| format!("token{index}")).collect::<Vec<_>>().join(" ");
    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            long_input,
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let mut model_tokens = Vec::new();
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(common_v1::run_stream_event::Body::ModelToken(token)) = event.body {
            model_tokens.push(token);
        }
    }

    assert_eq!(model_tokens.len(), 16, "run stream should emit at most 16 model tokens");
    assert!(
        model_tokens.last().map(|token| token.is_final).unwrap_or(false),
        "last emitted token should be final"
    );
    assert!(
        model_tokens.iter().take(15).all(|token| !token.is_final),
        "only the last emitted token should be marked final"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_event_persists_redacted_payload_and_hash_chain() -> Result<()> {
    let (child, admin_port, grpc_port, journal_db_path) =
        spawn_palyrad_with_dynamic_ports_and_hash_chain(true)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let first_request = authorized_append_event_request(sample_journal_event(
        "01ARZ3NDEKTSV4RRFFQ69G5FB0",
        br#"{"stage":"first","note":"safe"}"#,
    ))?;
    client.append_event(first_request).await.context("failed to append first journal event")?;

    let second_request = authorized_append_event_request(sample_journal_event(
        "01ARZ3NDEKTSV4RRFFQ69G5FB1",
        br#"{"api_token":"SECRET_TOKEN_VALUE","nested":{"password":"123456"}}"#,
    ))?;
    client.append_event(second_request).await.context("failed to append second journal event")?;

    let connection =
        Connection::open(journal_db_path).context("failed to open journal sqlite db")?;
    let mut statement = connection
        .prepare(
            r#"
                SELECT payload_json, redacted, hash, prev_hash
                FROM journal_events
                ORDER BY seq ASC
            "#,
        )
        .context("failed to prepare journal query")?;
    let mut rows = statement.query([]).context("failed to query journal rows")?;
    let first = rows
        .next()
        .context("failed to read first row")?
        .context("first journal row should exist")?;
    let first_hash: Option<String> = first.get(2).context("first hash should be readable")?;
    let first_prev_hash: Option<String> =
        first.get(3).context("first prev_hash should be readable")?;
    assert!(first_hash.is_some(), "hash-chain mode should generate first hash");
    assert!(first_prev_hash.is_none(), "first event must not have prev_hash");

    let second = rows
        .next()
        .context("failed to read second row")?
        .context("second journal row should exist")?;
    let second_payload: String = second.get(0).context("second payload should be readable")?;
    let second_redacted: i64 = second.get(1).context("second redaction flag should be readable")?;
    let second_hash: Option<String> = second.get(2).context("second hash should be readable")?;
    let second_prev_hash: Option<String> =
        second.get(3).context("second prev_hash should be readable")?;

    assert_eq!(second_redacted, 1, "secret-bearing payload must be marked redacted");
    assert!(
        !second_payload.contains("SECRET_TOKEN_VALUE") && !second_payload.contains("123456"),
        "journal payload must not contain raw secret values"
    );
    assert!(
        second_payload.contains("<redacted>"),
        "journal payload should include redaction marker"
    );
    assert!(second_hash.is_some(), "second event should include hash");
    assert_eq!(
        second_prev_hash, first_hash,
        "second event prev_hash must reference first event hash"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_event_duplicate_event_id_returns_already_exists() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let duplicate_event_id = "01ARZ3NDEKTSV4RRFFQ69G5FB7";
    let first_request = authorized_append_event_request(sample_journal_event(
        duplicate_event_id,
        br#"{"attempt":1}"#,
    ))?;
    client.append_event(first_request).await.context("first append should succeed")?;

    let second_request = authorized_append_event_request(sample_journal_event(
        duplicate_event_id,
        br#"{"attempt":2}"#,
    ))?;
    let error =
        client.append_event(second_request).await.expect_err("duplicate append must be rejected");
    assert_eq!(error.code(), Code::AlreadyExists, "duplicate event IDs should be deterministic");
    assert!(
        error.message().contains(duplicate_event_id),
        "duplicate error should include conflicting event id"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_append_event_rejects_mismatched_embedded_event_version() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut event = sample_journal_event("01ARZ3NDEKTSV4RRFFQ69G5FB8", br#"{"state":"invalid"}"#);
    event.v = 0;
    let request = authorized_append_event_request(event)?;
    let error =
        client.append_event(request).await.expect_err("mismatched event.v should be rejected");
    assert_eq!(error.code(), Code::FailedPrecondition, "event.v mismatch should fail precondition");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_persists_orchestrator_snapshot_and_matches_golden_tape() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "alpha beta gamma".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    while let Some(event) = response_stream.next().await {
        let _event = event.context("failed to read RunStream event")?;
    }

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "done"
    );
    assert_eq!(
        run_snapshot
            .get("prompt_tokens")
            .and_then(Value::as_u64)
            .context("run snapshot missing prompt_tokens")?,
        3
    );
    assert_eq!(
        run_snapshot
            .get("completion_tokens")
            .and_then(Value::as_u64)
            .context("run snapshot missing completion_tokens")?,
        3
    );

    let expected_tape = load_golden_json("run_tape_basic.json")?;
    assert_eq!(
        run_snapshot.get("tape").cloned().context("run snapshot missing tape")?,
        expected_tape
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_honors_cancel_command_and_marks_run_cancelled() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request = tonic::Request::new(tokio_stream::iter(vec![
        sample_run_stream_request_with_text(
            "one two three four five six seven eight nine ten".to_owned(),
        ),
        sample_run_stream_request_with_text("/cancel".to_owned()),
    ]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    let mut saw_failed = false;
    let mut saw_done = false;
    while let Some(event) = response_stream.next().await {
        let event = event.context("failed to read RunStream event")?;
        if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body {
            if status.kind == common_v1::stream_status::StatusKind::Failed as i32 {
                saw_failed = true;
            }
            if status.kind == common_v1::stream_status::StatusKind::Done as i32 {
                saw_done = true;
            }
        }
    }
    assert!(saw_failed, "cancelled run should emit failed status");
    assert!(!saw_done, "cancelled run must not emit done status");

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "cancelled"
    );
    assert!(
        run_snapshot
            .get("cancel_requested")
            .and_then(Value::as_bool)
            .context("run snapshot missing cancel_requested")?,
        "cancelled run should persist cancel_requested=true"
    );
    Ok(())
}

fn sample_run_stream_request() -> common_v1::RunStreamRequest {
    sample_run_stream_request_with_text("hello from grpc integration".to_owned())
}

fn sample_run_stream_request_with_text(text: String) -> common_v1::RunStreamRequest {
    common_v1::RunStreamRequest {
        v: 1,
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        run_id: Some(common_v1::CanonicalId { ulid: RUN_ID.to_owned() }),
        input: Some(common_v1::MessageEnvelope {
            v: 1,
            envelope_id: Some(common_v1::CanonicalId { ulid: ENVELOPE_ID.to_owned() }),
            content: Some(common_v1::MessageContent { text, attachments: Vec::new() }),
            ..Default::default()
        }),
        allow_sensitive_tools: false,
    }
}

fn admin_get_json(admin_port: u16, path: &str) -> Result<Value> {
    let endpoint = format!("http://127.0.0.1:{admin_port}{path}");
    Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build admin HTTP client")?
        .get(endpoint)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .send()
        .context("failed to call daemon admin endpoint")?
        .error_for_status()
        .context("daemon admin endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon admin JSON response")
}

async fn admin_get_json_async(admin_port: u16, path: String) -> Result<Value> {
    tokio::task::spawn_blocking(move || admin_get_json(admin_port, path.as_str()))
        .await
        .context("admin JSON worker panicked")?
}

fn load_golden_json(name: &str) -> Result<Value> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests").join("golden").join(name);
    let content =
        fs::read_to_string(&path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_str(content.as_str())
        .with_context(|| format!("failed to parse golden JSON {}", path.display()))
}

fn spawn_palyrad_with_dynamic_ports() -> Result<(Child, u16, u16, PathBuf)> {
    spawn_palyrad_with_dynamic_ports_and_hash_chain(false)
}

fn spawn_palyrad_with_dynamic_ports_and_hash_chain(
    hash_chain_enabled: bool,
) -> Result<(Child, u16, u16, PathBuf)> {
    let journal_db_path = unique_temp_journal_db_path();
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
        .env("PALYRA_JOURNAL_DB_PATH", journal_db_path.to_string_lossy().to_string())
        .env("PALYRA_JOURNAL_HASH_CHAIN_ENABLED", if hash_chain_enabled { "true" } else { "false" })
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port, journal_db_path))
}

fn unique_temp_journal_db_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("palyra-gateway-grpc-{nonce}-{}.sqlite3", std::process::id()))
}

fn sample_journal_event(event_id: &str, payload_json: &[u8]) -> common_v1::JournalEvent {
    common_v1::JournalEvent {
        v: 1,
        event_id: Some(common_v1::CanonicalId { ulid: event_id.to_owned() }),
        session_id: Some(common_v1::CanonicalId { ulid: SESSION_ID.to_owned() }),
        run_id: Some(common_v1::CanonicalId { ulid: RUN_ID.to_owned() }),
        kind: common_v1::journal_event::EventKind::ToolExecuted as i32,
        actor: common_v1::journal_event::EventActor::User as i32,
        timestamp_unix_ms: 1_730_000_000_000,
        payload_json: payload_json.to_vec(),
        hash: String::new(),
        prev_hash: String::new(),
    }
}

fn authorized_append_event_request(
    event: common_v1::JournalEvent,
) -> Result<tonic::Request<gateway_v1::AppendEventRequest>> {
    let mut request =
        tonic::Request::new(gateway_v1::AppendEventRequest { v: 1, event: Some(event) });
    request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);
    Ok(request)
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
