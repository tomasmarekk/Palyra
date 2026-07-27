//! Cross-platform conformance for the process-backed runtime transport.
//!
//! Tests use the real fixture child, exact process lease, actor queue, framing,
//! cancellation, quarantine, and structured cleanup path.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use palyra_daemon::application::managed_runtime::{
    ManagedRuntimeDescriptor, ManagedRuntimeHealthState, ManagedRuntimeStartRequest,
    RuntimeTransport, RuntimeTransportCommand, RuntimeTransportEvent, StdioRuntimeTransport,
};
use serde_json::json;

fn fixture_transport() -> StdioRuntimeTransport {
    let descriptor = ManagedRuntimeDescriptor {
        runtime_id: "managed_runtime_fixture".to_owned(),
        protocol_version: "palyra.managed-runtime.fixture.v1".to_owned(),
        capability_digest: "a".repeat(64),
        executable: PathBuf::from(env!("CARGO_BIN_EXE_palyra-managed-runtime-fixture")),
        args: Vec::new(),
        cwd: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        env: BTreeMap::new(),
        handshake_timeout: Duration::from_secs(5),
        command_timeout: Duration::from_secs(5),
        lease_duration: Duration::from_secs(30),
    };
    StdioRuntimeTransport::new(descriptor).expect("fixture descriptor")
}

fn start_request(generation: u64) -> ManagedRuntimeStartRequest {
    ManagedRuntimeStartRequest {
        session_id: "session-managed-runtime".to_owned(),
        generation,
        resume_metadata_json: Some(r#"{"resume":"fixture"}"#.to_owned()),
    }
}

fn command(method: &str, generation: u64) -> RuntimeTransportCommand {
    RuntimeTransportCommand {
        command_id: format!("command-{method}"),
        generation,
        method: method.to_owned(),
        payload: json!({"input":"fixture"}),
        deadline_unix_ms: now_unix_ms() + 5_000,
    }
}

#[tokio::test]
async fn real_child_handshake_events_and_cleanup_are_conformant() {
    let transport = fixture_transport();
    let binding = transport.start(start_request(3)).await.expect("start fixture");
    binding.lease.validate().expect("valid exact process lease");
    assert_eq!(binding.generation, 3);
    let mut events = transport.event_stream().expect("event stream");

    transport.send_command(command("run", 3)).await.expect("send command");
    let accepted = recv_event(&mut events).await;
    let event = recv_event(&mut events).await;
    let terminal = recv_event(&mut events).await;

    assert!(matches!(accepted, RuntimeTransportEvent::Accepted { sequence: 1, .. }));
    assert!(matches!(event, RuntimeTransportEvent::Event { sequence: 2, .. }));
    assert!(matches!(terminal, RuntimeTransportEvent::Terminal { sequence: 3, .. }));
    let cleanup = transport.close().await.expect("close fixture");
    cleanup.validate().expect("valid cleanup evidence");
    assert_eq!(transport.health().state, ManagedRuntimeHealthState::Closed);
}

#[tokio::test]
async fn priority_cancel_and_stale_generation_fail_closed() {
    let transport = fixture_transport();
    transport.start(start_request(9)).await.expect("start fixture");
    let mut events = transport.event_stream().expect("event stream");
    assert!(transport.send_command(command("hang", 8)).await.is_err());

    transport.send_command(command("hang", 9)).await.expect("start hanging command");
    transport.cancel("command-hang", 9).await.expect("priority cancellation");
    let accepted = recv_event(&mut events).await;
    assert!(matches!(accepted, RuntimeTransportEvent::Accepted { generation: 9, sequence: 1, .. }));
    let terminal = recv_event(&mut events).await;
    assert!(
        matches!(
            &terminal,
            RuntimeTransportEvent::Terminal {
                generation: 9,
                outcome,
                ..
            } if outcome == "cancelled"
        ),
        "unexpected cancellation event: {terminal:?}"
    );
    transport.close().await.expect("close fixture").validate().expect("cleanup");
}

#[tokio::test]
async fn malformed_child_frame_quarantines_and_cleans_up() {
    let transport = fixture_transport();
    transport.start(start_request(11)).await.expect("start fixture");
    let mut events = transport.event_stream().expect("event stream");
    transport.send_command(command("malformed", 11)).await.expect("send malformed trigger");

    let first = recv_event(&mut events).await;
    assert!(matches!(first, RuntimeTransportEvent::ProtocolError { generation: 11, .. }));
    let cleanup = recv_event(&mut events).await;
    let RuntimeTransportEvent::Cleanup { report, .. } = cleanup else {
        panic!("protocol violation must clean the child");
    };
    report.validate().expect("valid cleanup evidence");
    let health = transport.health();
    assert_eq!(health.state, ManagedRuntimeHealthState::Quarantined);
    assert_eq!(health.protocol_strikes, 1);
}

#[tokio::test]
async fn child_exit_without_terminal_is_observed_and_disposed() {
    let transport = fixture_transport();
    transport.start(start_request(12)).await.expect("start fixture");
    let mut events = transport.event_stream().expect("event stream");
    transport.send_command(command("crash", 12)).await.expect("send crash trigger");

    let exited = recv_event(&mut events).await;
    assert!(matches!(
        exited,
        RuntimeTransportEvent::ChildExited { generation: 12, exit_code: Some(17) }
    ));
    let cleanup = recv_event(&mut events).await;
    assert!(matches!(cleanup, RuntimeTransportEvent::Cleanup { generation: 12, .. }));
    assert_eq!(transport.health().state, ManagedRuntimeHealthState::Crashed);
    transport.close().await.expect("close crashed fixture").validate().expect("cleanup evidence");
    assert_eq!(transport.health().state, ManagedRuntimeHealthState::Closed);
}

#[tokio::test]
async fn event_flood_quarantines_the_child_at_the_per_attempt_bound() {
    let transport = fixture_transport();
    transport.start(start_request(13)).await.expect("start fixture");
    let mut events = transport.event_stream().expect("event stream");
    transport.send_command(command("flood", 13)).await.expect("send flood trigger");

    let mut observed_protocol_error = false;
    let mut observed_cleanup = false;
    while !observed_cleanup {
        match recv_event_allow_lag(&mut events).await {
            RuntimeTransportEvent::ProtocolError { reason_code, .. } => {
                assert_eq!(reason_code, "runtime.transport.event_flood");
                observed_protocol_error = true;
            }
            RuntimeTransportEvent::Cleanup { report, .. } => {
                report.validate().expect("flood cleanup evidence");
                observed_cleanup = true;
            }
            _ => {}
        }
    }
    assert!(observed_protocol_error);
    assert_eq!(transport.health().state, ManagedRuntimeHealthState::Quarantined);
}

#[tokio::test]
async fn closed_transport_restarts_with_a_fresh_generation_and_sequence() {
    let transport = fixture_transport();
    for generation in [20, 21] {
        let binding = transport.start(start_request(generation)).await.expect("start fixture");
        assert_eq!(binding.generation, generation);
        let mut events = transport.event_stream().expect("event stream");
        transport.send_command(command("run", generation)).await.expect("send command");
        assert!(matches!(
            recv_event(&mut events).await,
            RuntimeTransportEvent::Accepted {
                generation: observed,
                sequence: 1,
                ..
            } if observed == generation
        ));
        let _ = recv_event(&mut events).await;
        let _ = recv_event(&mut events).await;
        transport.close().await.expect("close fixture").validate().expect("cleanup");
    }
}

async fn recv_event(
    receiver: &mut tokio::sync::broadcast::Receiver<RuntimeTransportEvent>,
) -> RuntimeTransportEvent {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("event deadline")
        .expect("event")
}

async fn recv_event_allow_lag(
    receiver: &mut tokio::sync::broadcast::Receiver<RuntimeTransportEvent>,
) -> RuntimeTransportEvent {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("event deadline")
        {
            Ok(event) => return event,
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                panic!("event stream closed before cleanup");
            }
        }
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
