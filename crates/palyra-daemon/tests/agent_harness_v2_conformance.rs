//! Shared async conformance for embedded and real process-backed harnesses.
//!
//! Both adapters use the same strict sink; the external lane additionally
//! proves host-authorized text projection, cancellation, and process cleanup.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use palyra_daemon::application::{
    agent_harness::{AgentHarnessDescriptor, EmbeddedPalyraHarness},
    agent_harness_host::{
        GuardedHarnessHost, HarnessCancellationContext, HarnessCapabilityStore, HarnessHostBackend,
        HarnessHostError, HarnessHostOperation,
    },
    agent_harness_v2::{
        execute_agent_harness_v2, full_external_harness_capabilities, AgentHarnessAcceptedV2,
        AgentHarnessAttemptRequestV2, AgentHarnessEventSinkV2, AgentHarnessEventV2,
        AgentHarnessTerminalOutcomeV2, AgentHarnessTerminalReceiptV2, AgentHarnessTerminalV2,
        AgentHarnessV2, AgentHarnessV2Error,
    },
    external_agent_harness::ManagedExternalAgentHarness,
    managed_runtime::{ManagedRuntimeDescriptor, StdioRuntimeTransport},
};
use serde_json::{json, Value};

#[derive(Debug)]
struct RecordingBackend;

#[async_trait]
impl HarnessHostBackend for RecordingBackend {
    async fn invoke(
        &self,
        _operation: HarnessHostOperation,
        _payload: Value,
        _cancellation: HarnessCancellationContext,
    ) -> Result<Value, HarnessHostError> {
        Ok(Value::Null)
    }
}

#[derive(Debug, Default)]
struct RecordingSink {
    accepted: usize,
    events: Vec<AgentHarnessEventV2>,
    terminal: Option<AgentHarnessTerminalV2>,
}

#[async_trait]
impl AgentHarnessEventSinkV2 for RecordingSink {
    async fn accepted(
        &mut self,
        _accepted: AgentHarnessAcceptedV2,
    ) -> Result<(), AgentHarnessV2Error> {
        self.accepted = self.accepted.saturating_add(1);
        Ok(())
    }

    async fn event(&mut self, event: AgentHarnessEventV2) -> Result<(), AgentHarnessV2Error> {
        self.events.push(event);
        Ok(())
    }

    async fn terminal(
        &mut self,
        terminal: AgentHarnessTerminalV2,
    ) -> Result<AgentHarnessTerminalReceiptV2, AgentHarnessV2Error> {
        self.terminal = Some(terminal.clone());
        Ok(AgentHarnessTerminalReceiptV2 {
            generation: terminal.generation,
            terminal_sequence: terminal.sequence,
            event_count: self.events.len(),
        })
    }
}

#[tokio::test]
async fn embedded_and_real_external_harness_share_one_contract() {
    let (host, handle) = host("managed_fixture", 5);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("model", 5, handle, cancellation);
    let embedded = EmbeddedPalyraHarness::default();
    let external = external_harness();

    let (_, embedded_sink) =
        execute_agent_harness_v2(&embedded, &request, &host, RecordingSink::default())
            .await
            .expect("embedded harness");
    let (_, external_sink) =
        execute_agent_harness_v2(&external, &request, &host, RecordingSink::default())
            .await
            .expect("real external harness");

    assert_eq!(embedded_sink.accepted, 1);
    assert_eq!(external_sink.accepted, 1);
    assert!(embedded_sink.terminal.is_some());
    assert!(external_sink.terminal.is_some());
    assert_eq!(external_sink.events.len(), 1);
    let audit = host.audit_records();
    assert!(audit.iter().any(|record| record.operation == HarnessHostOperation::EmitTextDelta));
    assert!(audit.iter().any(|record| record.operation == HarnessHostOperation::Checkpoint));
}

#[tokio::test]
async fn cancellation_terminalizes_and_disposes_real_external_child() {
    let (host, handle) = host("managed_fixture", 8);
    let (cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("hang", 8, handle, cancellation);
    let external = external_harness();
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.send(true).expect("cancellation receiver");
    });

    let (_, sink) = execute_agent_harness_v2(&external, &request, &host, RecordingSink::default())
        .await
        .expect("cancelled external harness");
    cancel_task.await.expect("cancel task");

    assert_eq!(sink.accepted, 1);
    assert!(sink
        .terminal
        .as_ref()
        .is_some_and(|terminal| matches!(
            terminal.outcome,
            palyra_daemon::application::agent_harness_v2::AgentHarnessTerminalOutcomeV2::Cancelled { .. }
        )));
}

#[tokio::test]
async fn attempt_deadline_hard_stops_and_disposes_real_external_child() {
    let (host, handle) = host("managed_fixture", 9);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let mut request = request("hang", 9, handle, cancellation);
    request.deadline_unix_ms = now_unix_ms() + 150;
    let external = external_harness();

    let (_, sink) = execute_agent_harness_v2(&external, &request, &host, RecordingSink::default())
        .await
        .expect("timed-out external harness");

    assert!(sink.terminal.as_ref().is_some_and(|terminal| matches!(
        &terminal.outcome,
        AgentHarnessTerminalOutcomeV2::TimedOut { reason_code }
            if reason_code == "harness.external.deadline_exceeded"
    )));
    external.dispose().await.expect("idempotent post-timeout dispose");
}

#[tokio::test]
async fn child_crash_fails_closed_and_dispose_remains_idempotent() {
    let (host, handle) = host("managed_fixture", 10);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("crash", 10, handle, cancellation);
    let external = external_harness();

    let error = execute_agent_harness_v2(&external, &request, &host, RecordingSink::default())
        .await
        .expect_err("crashed external harness");

    assert!(matches!(
        error,
        AgentHarnessV2Error::Transport { reason_code }
            if reason_code == "harness.transport.runtime_failed"
    ));
    external.dispose().await.expect("first post-crash dispose");
    external.dispose().await.expect("second post-crash dispose");
}

#[tokio::test]
async fn unsupported_capability_is_rejected_before_transport_dispatch() {
    let transport =
        StdioRuntimeTransport::new(fixture_transport_descriptor()).expect("fixture transport");
    let harness = ManagedExternalAgentHarness::new(
        AgentHarnessDescriptor::new("limited_fixture", "Limited fixture", false),
        Arc::new(transport),
    );

    assert_eq!(
        harness.steer(14, "change direction").await,
        Err(AgentHarnessV2Error::UnsupportedCapability { capability: "steering" })
    );
}

fn external_harness() -> ManagedExternalAgentHarness<StdioRuntimeTransport> {
    let transport =
        StdioRuntimeTransport::new(fixture_transport_descriptor()).expect("fixture transport");
    ManagedExternalAgentHarness::new(
        AgentHarnessDescriptor::with_capabilities(
            "managed_fixture",
            "Managed fixture",
            false,
            full_external_harness_capabilities(),
        ),
        Arc::new(transport),
    )
}

fn fixture_transport_descriptor() -> ManagedRuntimeDescriptor {
    ManagedRuntimeDescriptor {
        runtime_id: "managed_fixture".to_owned(),
        protocol_version: "palyra.managed-runtime.fixture.v1".to_owned(),
        capability_digest: "b".repeat(64),
        executable: PathBuf::from(env!("CARGO_BIN_EXE_palyra-managed-runtime-fixture")),
        args: Vec::new(),
        cwd: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        env: BTreeMap::new(),
        handshake_timeout: Duration::from_secs(5),
        command_timeout: Duration::from_secs(5),
        lease_duration: Duration::from_secs(30),
    }
}

fn host(
    harness_id: &str,
    generation: u64,
) -> (
    GuardedHarnessHost<RecordingBackend>,
    palyra_daemon::application::agent_harness_host::HarnessCapabilityHandle,
) {
    let capabilities = Arc::new(HarnessCapabilityStore::default());
    let handle = capabilities
        .issue(
            harness_id,
            generation,
            vec![
                HarnessHostOperation::EmitTextDelta,
                HarnessHostOperation::EmitProgress,
                HarnessHostOperation::ProposeToolCall,
                HarnessHostOperation::AwaitToolOutcome,
                HarnessHostOperation::Heartbeat,
                HarnessHostOperation::Checkpoint,
            ],
            now_unix_ms() + 30_000,
        )
        .expect("capability");
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    (
        GuardedHarnessHost::new(
            Arc::new(RecordingBackend),
            capabilities,
            cancellation,
            Duration::from_secs(5),
        ),
        handle,
    )
}

fn request(
    model_id: &str,
    generation: u64,
    host_capability: palyra_daemon::application::agent_harness_host::HarnessCapabilityHandle,
    cancellation: HarnessCancellationContext,
) -> AgentHarnessAttemptRequestV2 {
    AgentHarnessAttemptRequestV2 {
        run_id: "run-managed-harness".to_owned(),
        session_id: "session-managed-harness".to_owned(),
        generation,
        deadline_unix_ms: now_unix_ms() + 10_000,
        provider_id: "fixture-provider".to_owned(),
        model_id: model_id.to_owned(),
        context_token_budget: 8_192,
        reasoning_policy: Some("default".to_owned()),
        sanitized_transcript: vec![json!({"role":"user","content":"hello"})],
        tool_surface: json!({"tools":[]}),
        tool_catalog_epoch: 1,
        workspace_root: None,
        sandbox: "host_owned".to_owned(),
        trace_context: "trace-managed-harness".to_owned(),
        host_capability,
        cancellation,
    }
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}
