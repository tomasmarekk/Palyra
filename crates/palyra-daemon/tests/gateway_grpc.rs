use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    path::PathBuf,
    process::{Child, ChildStdout, Command, Stdio},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc, Arc,
    },
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
const RUN_ID_ALT: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAZ";
const ENVELOPE_ID: &str = "01ARZ3NDEKTSV4RRFFQ69G5FAY";
const OPENAI_API_KEY: &str = "sk-openai-integration-test";
static TEMP_JOURNAL_COUNTER: AtomicU64 = AtomicU64::new(0);

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
async fn grpc_run_stream_uses_openai_compatible_provider_when_configured() -> Result<()> {
    let (openai_base_url, request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::immediate(
            200,
            r#"{"choices":[{"message":{"content":"provider says hello"}}]}"#.to_owned(),
        )])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider(openai_base_url.as_str(), OPENAI_API_KEY)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "ignored by deterministic fallback".to_owned(),
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
            model_tokens.push(token.token);
        }
    }
    assert_eq!(model_tokens, vec!["provider", "says", "hello"]);
    assert_eq!(
        request_count.load(Ordering::Relaxed),
        1,
        "openai-compatible provider should perform one upstream call"
    );

    let status_snapshot = admin_get_json_async(admin_port, "/admin/v1/status".to_owned()).await?;
    assert_eq!(
        status_snapshot.pointer("/model_provider/kind").and_then(Value::as_str),
        Some("openai_compatible")
    );
    assert_eq!(
        status_snapshot.pointer("/model_provider/api_key_configured").and_then(Value::as_bool),
        Some(true)
    );
    let serialized_status =
        serde_json::to_string(&status_snapshot).context("failed to serialize status snapshot")?;
    assert!(
        !serialized_status.contains(OPENAI_API_KEY),
        "admin status snapshot must not leak provider API key"
    );

    server_handle.join().expect("scripted openai server thread should exit");
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_admin_cancel_preempts_inflight_provider_call() -> Result<()> {
    let (openai_base_url, request_count, server_handle) =
        spawn_scripted_openai_server(vec![ScriptedOpenAiResponse::delayed(
            200,
            r#"{"choices":[{"message":{"content":"slow provider response"}}]}"#.to_owned(),
            Duration::from_secs(5),
        )])?;
    let (child, admin_port, grpc_port, _journal_db_path) =
        spawn_palyrad_with_openai_provider(openai_base_url.as_str(), OPENAI_API_KEY)?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_text(
            "this request should be cancelled while provider call is in-flight".to_owned(),
        )]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();

    let in_progress_kind = common_v1::stream_status::StatusKind::InProgress as i32;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(2), response_stream.next())
            .await
            .context("run stream did not emit in-progress status before timeout")?;
        let Some(event) = next_event else {
            anyhow::bail!("run stream ended before entering in-progress state");
        };
        let event = event.context("failed to read RunStream event while waiting in-progress")?;
        if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body {
            if status.kind == in_progress_kind {
                break;
            }
        }
    }

    let cancel_started_at = Instant::now();
    let cancel_snapshot = admin_post_json_async(
        admin_port,
        format!("/admin/v1/runs/{RUN_ID}/cancel"),
        serde_json::json!({ "reason": "integration_cancel_request" }),
    )
    .await?;
    assert_eq!(
        cancel_snapshot.get("cancel_requested").and_then(Value::as_bool),
        Some(true),
        "admin cancel endpoint should persist cancel flag"
    );

    let mut saw_failed = false;
    let mut saw_done = false;
    let failed_kind = common_v1::stream_status::StatusKind::Failed as i32;
    let done_kind = common_v1::stream_status::StatusKind::Done as i32;
    loop {
        let next_event = tokio::time::timeout(Duration::from_secs(2), response_stream.next())
            .await
            .context("run stream did not terminate quickly after cancellation")?;
        let Some(event) = next_event else {
            break;
        };
        let event = event.context("failed to read RunStream event after cancellation")?;
        if let Some(common_v1::run_stream_event::Body::Status(status)) = event.body {
            if status.kind == failed_kind {
                saw_failed = true;
                break;
            }
            if status.kind == done_kind {
                saw_done = true;
                break;
            }
        }
    }

    assert!(
        cancel_started_at.elapsed() < Duration::from_secs(2),
        "run cancellation should preempt in-flight provider call without waiting for upstream timeout"
    );
    assert!(saw_failed, "cancelled run should emit failed status");
    assert!(!saw_done, "cancelled run must not emit done status");
    assert_eq!(
        request_count.load(Ordering::Relaxed),
        1,
        "cancel preemption should not trigger extra upstream retries in this scenario"
    );

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "cancelled"
    );

    server_handle.join().expect("scripted openai server thread should exit");
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
        run_snapshot
            .get("tape_events")
            .and_then(Value::as_u64)
            .context("run snapshot missing tape_events")?,
        expected_tape.as_array().context("golden tape must be a JSON array")?.len() as u64
    );
    assert!(
        run_snapshot.get("tape").is_none(),
        "run status endpoint should not include full tape payload"
    );

    let tape_snapshot =
        admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}/tape")).await?;
    assert_eq!(
        tape_snapshot.get("events").cloned().context("run tape snapshot missing events")?,
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

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_rejects_session_identity_mismatch_as_failed_precondition() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint.clone())
            .await
            .context("failed to connect gRPC client for initial stream")?;

    let mut first_stream =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_ids(
            SESSION_ID,
            RUN_ID,
            "seed-session".to_owned(),
        )]));
    first_stream.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    first_stream.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    first_stream.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    first_stream.metadata_mut().insert("x-palyra-channel", "cli".parse()?);
    let mut first_response = client
        .run_stream(first_stream)
        .await
        .context("failed to call first RunStream")?
        .into_inner();
    while let Some(event) = first_response.next().await {
        let _ = event.context("first run stream should finish without errors")?;
    }

    let mut second_client =
        gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
            .await
            .context("failed to connect gRPC client for mismatch stream")?;
    let mut second_stream =
        tonic::Request::new(tokio_stream::iter(vec![sample_run_stream_request_with_ids(
            SESSION_ID,
            RUN_ID_ALT,
            "mismatch-session".to_owned(),
        )]));
    second_stream.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    second_stream.metadata_mut().insert("x-palyra-principal", "user:other".parse()?);
    second_stream.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    second_stream.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut second_response = second_client
        .run_stream(second_stream)
        .await
        .context("failed to call second RunStream")?
        .into_inner();
    let mismatch_error = second_response
        .next()
        .await
        .transpose()
        .expect_err("second run stream should fail before emitting any events");
    assert_eq!(mismatch_error.code(), Code::FailedPrecondition);
    assert!(
        mismatch_error.message().contains("session identity mismatch"),
        "expected session identity mismatch message, got: {}",
        mismatch_error.message()
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_protocol_error_after_accept_marks_run_failed() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut invalid_protocol_message =
        sample_run_stream_request_with_ids(SESSION_ID, RUN_ID, "second-message".to_owned());
    invalid_protocol_message.v = 0;
    let mut stream_request = tonic::Request::new(tokio_stream::iter(vec![
        sample_run_stream_request_with_ids(SESSION_ID, RUN_ID, "first-message".to_owned()),
        invalid_protocol_message,
    ]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    let mut terminal_error = None;
    while let Some(event) = response_stream.next().await {
        if let Err(status) = event {
            terminal_error = Some(status);
            break;
        }
    }
    let status = terminal_error.context("run stream should terminate with a protocol error")?;
    assert_eq!(status.code(), Code::FailedPrecondition);

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "failed"
    );
    assert!(
        run_snapshot
            .get("last_error")
            .and_then(Value::as_str)
            .context("run snapshot missing last_error")?
            .contains("unsupported protocol major version"),
        "failure reason should be persisted for protocol errors"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn grpc_run_stream_mid_stream_run_id_switch_marks_original_run_failed() -> Result<()> {
    let (child, admin_port, grpc_port, _journal_db_path) = spawn_palyrad_with_dynamic_ports()?;
    let mut daemon = ChildGuard::new(child);
    wait_for_health(admin_port, daemon.child_mut())?;

    let endpoint = format!("http://127.0.0.1:{grpc_port}");
    let mut client = gateway_v1::gateway_service_client::GatewayServiceClient::connect(endpoint)
        .await
        .context("failed to connect gRPC client")?;

    let mut stream_request = tonic::Request::new(tokio_stream::iter(vec![
        sample_run_stream_request_with_ids(SESSION_ID, RUN_ID, "first-message".to_owned()),
        sample_run_stream_request_with_ids(SESSION_ID, RUN_ID_ALT, "switch-run".to_owned()),
    ]));
    stream_request.metadata_mut().insert("authorization", format!("Bearer {ADMIN_TOKEN}").parse()?);
    stream_request.metadata_mut().insert("x-palyra-principal", "user:ops".parse()?);
    stream_request.metadata_mut().insert("x-palyra-device-id", DEVICE_ID.parse()?);
    stream_request.metadata_mut().insert("x-palyra-channel", "cli".parse()?);

    let mut response_stream =
        client.run_stream(stream_request).await.context("failed to call RunStream")?.into_inner();
    let mut terminal_error = None;
    while let Some(event) = response_stream.next().await {
        if let Err(status) = event {
            terminal_error = Some(status);
            break;
        }
    }
    let status =
        terminal_error.context("run stream should terminate with a run_id mismatch error")?;
    assert_eq!(status.code(), Code::InvalidArgument);

    let run_snapshot = admin_get_json_async(admin_port, format!("/admin/v1/runs/{RUN_ID}")).await?;
    assert_eq!(
        run_snapshot.get("state").and_then(Value::as_str).context("run snapshot missing state")?,
        "failed"
    );
    assert!(
        run_snapshot
            .get("last_error")
            .and_then(Value::as_str)
            .context("run snapshot missing last_error")?
            .contains("cannot switch run_id mid-stream"),
        "failure reason should be persisted for run_id mismatch"
    );
    Ok(())
}

fn sample_run_stream_request() -> common_v1::RunStreamRequest {
    sample_run_stream_request_with_text("hello from grpc integration".to_owned())
}

fn sample_run_stream_request_with_text(text: String) -> common_v1::RunStreamRequest {
    sample_run_stream_request_with_ids(SESSION_ID, RUN_ID, text)
}

fn sample_run_stream_request_with_ids(
    session_id: &str,
    run_id: &str,
    text: String,
) -> common_v1::RunStreamRequest {
    common_v1::RunStreamRequest {
        v: 1,
        session_id: Some(common_v1::CanonicalId { ulid: session_id.to_owned() }),
        run_id: Some(common_v1::CanonicalId { ulid: run_id.to_owned() }),
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

fn admin_post_json(admin_port: u16, path: &str, payload: Value) -> Result<Value> {
    let endpoint = format!("http://127.0.0.1:{admin_port}{path}");
    Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .context("failed to build admin HTTP client")?
        .post(endpoint)
        .header("Authorization", format!("Bearer {ADMIN_TOKEN}"))
        .header("x-palyra-principal", "user:ops")
        .header("x-palyra-device-id", DEVICE_ID)
        .header("x-palyra-channel", "cli")
        .json(&payload)
        .send()
        .context("failed to call daemon admin endpoint")?
        .error_for_status()
        .context("daemon admin endpoint returned non-success status")?
        .json()
        .context("failed to parse daemon admin JSON response")
}

async fn admin_post_json_async(admin_port: u16, path: String, payload: Value) -> Result<Value> {
    tokio::task::spawn_blocking(move || admin_post_json(admin_port, path.as_str(), payload))
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

fn spawn_palyrad_with_openai_provider(
    openai_base_url: &str,
    openai_api_key: &str,
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
        .env("PALYRA_ORCHESTRATOR_RUNLOOP_V1_ENABLED", "true")
        .env("PALYRA_MODEL_PROVIDER_KIND", "openai_compatible")
        .env("PALYRA_MODEL_PROVIDER_OPENAI_BASE_URL", openai_base_url)
        .env("PALYRA_MODEL_PROVIDER_OPENAI_API_KEY", openai_api_key)
        .env("PALYRA_MODEL_PROVIDER_MAX_RETRIES", "0")
        .env("PALYRA_MODEL_PROVIDER_RETRY_BACKOFF_MS", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "1")
        .env("PALYRA_MODEL_PROVIDER_CIRCUIT_BREAKER_COOLDOWN_MS", "30000")
        .env("RUST_LOG", "info")
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .context("failed to start palyrad with openai-compatible provider")?;
    let stdout = child.stdout.take().context("failed to capture palyrad stdout")?;
    let (admin_port, grpc_port) = wait_for_listen_ports(stdout, &mut child)?;
    Ok((child, admin_port, grpc_port, journal_db_path))
}

#[derive(Debug, Clone)]
struct ScriptedOpenAiResponse {
    status_code: u16,
    body: String,
    delay_before_response: Duration,
}

impl ScriptedOpenAiResponse {
    fn immediate(status_code: u16, body: String) -> Self {
        Self { status_code, body, delay_before_response: Duration::ZERO }
    }

    fn delayed(status_code: u16, body: String, delay_before_response: Duration) -> Self {
        Self { status_code, body, delay_before_response }
    }
}

fn spawn_scripted_openai_server(
    responses: Vec<ScriptedOpenAiResponse>,
) -> Result<(String, Arc<AtomicUsize>, thread::JoinHandle<()>)> {
    let listener =
        TcpListener::bind("127.0.0.1:0").context("failed to bind scripted openai listener")?;
    let address = listener.local_addr().context("failed to resolve scripted listener address")?;
    let request_count = Arc::new(AtomicUsize::new(0));
    let request_count_for_thread = Arc::clone(&request_count);
    let handle = thread::spawn(move || {
        for response_spec in responses {
            let (mut stream, _) =
                listener.accept().expect("scripted openai listener should accept connection");
            request_count_for_thread.fetch_add(1, Ordering::Relaxed);
            if read_http_request_for_scripted_server(&mut stream).is_err() {
                continue;
            }
            if !response_spec.delay_before_response.is_zero() {
                thread::sleep(response_spec.delay_before_response);
            }
            let reason = match response_spec.status_code {
                200 => "OK",
                429 => "Too Many Requests",
                500 => "Internal Server Error",
                502 => "Bad Gateway",
                503 => "Service Unavailable",
                504 => "Gateway Timeout",
                _ => "Error",
            };
            let response = format!(
                "HTTP/1.1 {} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_spec.status_code,
                response_spec.body.len(),
                response_spec.body
            );
            let _ = stream.write_all(response.as_bytes());
            let _ = stream.flush();
        }
    });
    Ok((format!("http://{address}/v1"), request_count, handle))
}

fn read_http_request_for_scripted_server(stream: &mut TcpStream) -> Result<()> {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .context("failed to configure scripted server read timeout")?;
    let mut reader = BufReader::new(stream);
    let mut content_length = 0_usize;
    loop {
        let mut line = String::new();
        let bytes_read =
            reader.read_line(&mut line).context("failed to read scripted request line")?;
        if bytes_read == 0 || line == "\r\n" {
            break;
        }
        let line_trimmed = line.trim_end_matches(['\r', '\n']);
        if let Some(value) = line_trimmed.strip_prefix("Content-Length:") {
            content_length =
                value.trim().parse::<usize>().context("invalid Content-Length in request")?;
        }
    }

    if content_length > 0 {
        let mut body = vec![0_u8; content_length];
        reader.read_exact(&mut body).context("failed to read scripted request body")?;
        if body.is_empty() {
            anyhow::bail!("scripted openai request body should not be empty");
        }
    }

    Ok(())
}

fn unique_temp_journal_db_path() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos();
    let counter = TEMP_JOURNAL_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir()
        .join(format!("palyra-gateway-grpc-{nonce}-{}-{counter}.sqlite3", std::process::id()))
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
