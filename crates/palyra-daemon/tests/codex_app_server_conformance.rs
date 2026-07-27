//! Real-process conformance for the managed Codex app-server adapter.
//!
//! A deterministic JSON-RPC fixture proves text, dynamic-tool authority,
//! cancellation, binding checkpoint, and process cleanup through one host path.

use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use palyra_daemon::application::{
    agent_harness_host::{
        GuardedHarnessHost, HarnessCancellationContext, HarnessCapabilityHandle,
        HarnessCapabilityStore, HarnessHostBackend, HarnessHostError, HarnessHostOperation,
    },
    agent_harness_v2::{
        execute_agent_harness_v2, AgentHarnessAcceptedV2, AgentHarnessAttemptRequestV2,
        AgentHarnessEventKindV2, AgentHarnessEventSinkV2, AgentHarnessEventV2,
        AgentHarnessTerminalOutcomeV2, AgentHarnessTerminalReceiptV2, AgentHarnessTerminalV2,
        AgentHarnessV2, AgentHarnessV2Error,
    },
    codex_app_server_bridge::{
        codex_agent_harness_descriptor, codex_managed_runtime_descriptor,
        CodexAppServerVersionPolicy, ManagedCodexAppServerConfig,
    },
    external_agent_harness::ManagedExternalAgentHarness,
    managed_runtime::{ManagedRuntimeHealthState, RuntimeTransport, StdioRuntimeTransport},
};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy)]
enum ApprovalMode {
    Deny,
    Timeout,
    Wait,
}

#[derive(Debug)]
struct CodexHostBackend {
    approval_mode: ApprovalMode,
    expected_tool_epoch: u64,
    observed_tool_epochs: Arc<Mutex<Vec<u64>>>,
}

type CodexGuardedHost = GuardedHarnessHost<CodexHostBackend>;
type ToolEpochAudit = Arc<Mutex<Vec<u64>>>;

#[async_trait]
impl HarnessHostBackend for CodexHostBackend {
    async fn invoke(
        &self,
        operation: HarnessHostOperation,
        payload: Value,
        _cancellation: HarnessCancellationContext,
    ) -> Result<Value, HarnessHostError> {
        Ok(match operation {
            HarnessHostOperation::ProposeToolCall => {
                let observed = payload
                    .get("tool_catalog_epoch")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| HarnessHostError::Backend {
                        reason_code: "harness.tool.missing_catalog_epoch".to_owned(),
                    })?;
                if observed != self.expected_tool_epoch {
                    return Err(HarnessHostError::Backend {
                        reason_code: "harness.tool.stale_catalog_epoch".to_owned(),
                    });
                }
                self.observed_tool_epochs
                    .lock()
                    .map_err(|_| HarnessHostError::Backend {
                        reason_code: "harness.tool.epoch_audit_unavailable".to_owned(),
                    })?
                    .push(observed);
                json!({"ok": true, "text": "host tool completed"})
            }
            HarnessHostOperation::AwaitToolOutcome => match self.approval_mode {
                ApprovalMode::Deny => json!({"outcome": "denied"}),
                ApprovalMode::Timeout => {
                    return Err(HarnessHostError::Backend {
                        reason_code: "harness.approval.timed_out".to_owned(),
                    });
                }
                ApprovalMode::Wait => {
                    return std::future::pending::<Result<Value, HarnessHostError>>().await;
                }
            },
            _ => Value::Null,
        })
    }
}

#[derive(Debug, Default)]
struct CodexSink {
    accepted: usize,
    events: Vec<AgentHarnessEventV2>,
    terminal: Option<AgentHarnessTerminalV2>,
}

#[async_trait]
impl AgentHarnessEventSinkV2 for CodexSink {
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
async fn codex_bridge_streams_text_and_routes_dynamic_tool_through_host() {
    let generation = 11;
    let (host, handle) = host(generation);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("fixture-model", generation, handle, cancellation);
    let harness = codex_harness();

    let (_, sink) = execute_agent_harness_v2(&harness, &request, &host, CodexSink::default())
        .await
        .expect("managed Codex harness");

    assert_eq!(sink.accepted, 1);
    assert!(sink.events.iter().any(
        |event| matches!(&event.event, AgentHarnessEventKindV2::TextDelta { text } if text == "codex fixture")
    ));
    assert!(sink.events.iter().any(
        |event| matches!(&event.event, AgentHarnessEventKindV2::ToolProposed {
            call_id,
            tool_name,
            ..
        } if call_id == "codex-tool-fixture" && tool_name == "palyra.fixture")
    ));
    assert!(sink.events.iter().any(
        |event| matches!(&event.event, AgentHarnessEventKindV2::ToolOutcome { outcome, .. } if outcome == "completed")
    ));
    assert!(sink.terminal.as_ref().is_some_and(|terminal| matches!(
        &terminal.outcome,
        AgentHarnessTerminalOutcomeV2::Completed { final_message }
            if final_message.as_deref() == Some("codex fixture")
    )));
    assert!(host.audit_records().iter().any(|record| {
        record.operation == HarnessHostOperation::ProposeToolCall && record.outcome == "completed"
    }));
    assert!(host
        .audit_records()
        .iter()
        .any(|record| record.operation == HarnessHostOperation::Checkpoint));
}

#[tokio::test]
async fn codex_bridge_preserves_event_received_before_turn_start_response() {
    let generation = 14;
    let (host, handle) = host(generation);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("early-event", generation, handle, cancellation);

    let (_, sink) =
        execute_agent_harness_v2(&codex_harness(), &request, &host, CodexSink::default())
            .await
            .expect("managed Codex harness with an early event");

    assert!(sink.events.iter().any(
        |event| matches!(&event.event, AgentHarnessEventKindV2::TextDelta { text } if text == "early codex fixture")
    ));
    assert!(sink.terminal.as_ref().is_some_and(|terminal| matches!(
        &terminal.outcome,
        AgentHarnessTerminalOutcomeV2::Completed { final_message }
            if final_message.as_deref() == Some("early codex fixture")
    )));
}

#[tokio::test]
async fn codex_text_only_and_unknown_notifications_remain_bounded() {
    for (generation, model_id) in [(15, "text-only"), (16, "unknown-event")] {
        let (host, handle) = host(generation);
        let (_cancel, cancellation) = HarnessCancellationContext::channel();
        let request = request(model_id, generation, handle, cancellation);
        let (_, sink) =
            execute_agent_harness_v2(&codex_harness(), &request, &host, CodexSink::default())
                .await
                .expect("bounded Codex notification handling");

        assert!(sink.terminal.as_ref().is_some_and(|terminal| matches!(
            terminal.outcome,
            AgentHarnessTerminalOutcomeV2::Completed { .. }
        )));
        if model_id == "unknown-event" {
            assert!(sink.events.iter().any(|event| matches!(
                &event.event,
                AgentHarnessEventKindV2::Progress { label, .. }
                    if label == "codex_unknown_event_ignored"
            )));
        }
    }
}

#[tokio::test]
async fn unsupported_codex_version_fails_before_runtime_acceptance() {
    let generation = 17;
    let (host, handle) = host(generation);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("text-only", generation, handle, cancellation);
    let mut codex_env = BTreeMap::new();
    codex_env.insert("PALYRA_FAKE_CODEX_VERSION".to_owned(), "codex-cli/9.0.0".to_owned());
    let (harness, transport) = codex_harness_with_env(codex_env);

    let error = execute_agent_harness_v2(&harness, &request, &host, CodexSink::default())
        .await
        .expect_err("unsupported Codex version");

    assert!(matches!(error, AgentHarnessV2Error::Transport { .. }));
    assert_ne!(transport.health().state, ManagedRuntimeHealthState::Ready);
}

#[tokio::test]
async fn codex_child_exit_and_stderr_secret_are_cleaned_and_redacted() {
    let generation = 18;
    let (crash_host, handle) = host(generation);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let crash_request = request("crash-after-start", generation, handle, cancellation);
    let (harness, transport) = codex_harness_with_env(BTreeMap::new());
    let result =
        execute_agent_harness_v2(&harness, &crash_request, &crash_host, CodexSink::default()).await;
    assert!(
        result.is_err()
            || result.as_ref().is_ok_and(|(_, sink)| {
                sink.terminal.as_ref().is_some_and(|terminal| {
                    matches!(terminal.outcome, AgentHarnessTerminalOutcomeV2::Failed { .. })
                })
            })
    );
    harness.dispose().await.expect("post-crash dispose");
    assert_eq!(transport.health().state, ManagedRuntimeHealthState::Closed);

    let generation = 19;
    let (stderr_host, handle) = host(generation);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let stderr_request = request("stderr-secret", generation, handle, cancellation);
    let (harness, transport) = codex_harness_with_env(BTreeMap::new());
    execute_agent_harness_v2(&harness, &stderr_request, &stderr_host, CodexSink::default())
        .await
        .expect("stderr redaction fixture");
    let stderr = transport.health().stderr_tail_redacted;
    assert!(!stderr.contains("fixture-secret-token"));
    assert!(!stderr.contains("Bearer fixture"));
}

#[tokio::test]
async fn codex_approval_denial_timeout_and_steer_are_host_controlled() {
    let generation = 20;
    let (denial_host, handle) = host(generation);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let denial_request = request("approval", generation, handle, cancellation);
    let (_, sink) = execute_agent_harness_v2(
        &codex_harness(),
        &denial_request,
        &denial_host,
        CodexSink::default(),
    )
    .await
    .expect("host-owned approval denial");
    assert!(sink
        .events
        .iter()
        .any(|event| matches!(event.event, AgentHarnessEventKindV2::ApprovalRequired { .. })));
    assert!(sink.events.iter().any(|event| matches!(
        &event.event,
        AgentHarnessEventKindV2::ApprovalResolved { outcome, .. } if outcome == "denied"
    )));

    let generation = 21;
    let (timeout_host, handle) = host_with_approval_mode(generation, ApprovalMode::Timeout);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let timeout_request = request("approval", generation, handle, cancellation);
    let harness = codex_harness();
    let timeout_error =
        execute_agent_harness_v2(&harness, &timeout_request, &timeout_host, CodexSink::default())
            .await
            .expect_err("host-owned approval timeout");
    assert!(matches!(
        timeout_error,
        AgentHarnessV2Error::Host(HarnessHostError::Backend { reason_code })
            if reason_code == "harness.approval.timed_out"
    ));
    harness.dispose().await.expect("approval-timeout cleanup");

    let generation = 22;
    let (host, handle) = host(generation);
    let host = Arc::new(host);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let request = Arc::new(request("steer", generation, handle, cancellation));
    let (harness, transport) = codex_harness_with_env(BTreeMap::new());
    let harness = Arc::new(harness);
    let run_harness = Arc::clone(&harness);
    let run_host = Arc::clone(&host);
    let run_request = Arc::clone(&request);
    let run = tokio::spawn(async move {
        execute_agent_harness_v2(
            run_harness.as_ref(),
            run_request.as_ref(),
            run_host.as_ref(),
            CodexSink::default(),
        )
        .await
    });
    for _ in 0..500 {
        if transport.health().state == ManagedRuntimeHealthState::Ready {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let health = transport.health();
    assert_eq!(
        health.state,
        ManagedRuntimeHealthState::Ready,
        "Codex steer fixture did not reach a steerable state: {health:?}"
    );
    let steer = harness
        .steer(generation, "Use the host-approved revised direction.")
        .await
        .expect("Codex steer");
    assert!(matches!(
        steer,
        palyra_daemon::application::agent_harness_v2::AgentHarnessSteerOutcomeV2::Accepted {
            generation: 22
        }
    ));
    let (_, sink) = run.await.expect("steered run task").expect("steered Codex run");
    assert!(sink.terminal.as_ref().is_some_and(|terminal| matches!(
        &terminal.outcome,
        AgentHarnessTerminalOutcomeV2::Completed { final_message }
            if final_message.as_deref() == Some("steered codex fixture")
    )));
}

#[tokio::test]
async fn codex_cancellation_interrupts_an_in_flight_approval_wait() {
    let generation = 23;
    let (host, handle) = host_with_approval_mode(generation, ApprovalMode::Wait);
    let (cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("approval", generation, handle, cancellation);
    let harness = codex_harness();
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel.send(true).expect("approval-wait cancellation receiver");
    });

    let (_, sink) = execute_agent_harness_v2(&harness, &request, &host, CodexSink::default())
        .await
        .expect("cancelled approval wait");
    cancel_task.await.expect("approval-wait cancellation task");

    assert!(sink.terminal.as_ref().is_some_and(|terminal| matches!(
        terminal.outcome,
        AgentHarnessTerminalOutcomeV2::Cancelled { .. }
    )));
    assert!(!harness.health_probe().await.expect("closed health").ready);
}

#[tokio::test]
async fn codex_crash_after_host_tool_result_does_not_repeat_the_side_effect() {
    let generation = 24;
    let (host, handle, observed_tool_epochs) = host_with_epoch(generation, 7);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("crash-after-tool", generation, handle, cancellation);
    let (harness, transport) = codex_harness_with_env(BTreeMap::new());

    let result = execute_agent_harness_v2(&harness, &request, &host, CodexSink::default()).await;

    assert!(
        result.is_err()
            || result.as_ref().is_ok_and(|(_, sink)| {
                sink.terminal.as_ref().is_some_and(|terminal| {
                    matches!(terminal.outcome, AgentHarnessTerminalOutcomeV2::Failed { .. })
                })
            })
    );
    assert_eq!(observed_tool_epochs.lock().expect("tool epoch audit").as_slice(), &[7]);
    assert_eq!(
        host.audit_records()
            .iter()
            .filter(|record| record.operation == HarnessHostOperation::ProposeToolCall)
            .count(),
        1
    );
    harness.dispose().await.expect("post-tool-crash dispose");
    assert_eq!(transport.health().state, ManagedRuntimeHealthState::Closed);
}

#[tokio::test]
async fn codex_turn_keeps_its_pinned_tool_epoch_when_the_next_epoch_differs() {
    let generation = 25;
    let pinned_epoch = 41;
    let next_catalog_epoch = 42;
    let (host, handle, observed_tool_epochs) = host_with_epoch(generation, pinned_epoch);
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let mut request = request("fixture-model", generation, handle, cancellation);
    request.tool_catalog_epoch = pinned_epoch;

    execute_agent_harness_v2(&codex_harness(), &request, &host, CodexSink::default())
        .await
        .expect("pinned catalog turn");

    assert_ne!(pinned_epoch, next_catalog_epoch);
    assert_eq!(observed_tool_epochs.lock().expect("tool epoch audit").as_slice(), &[pinned_epoch]);
}

#[tokio::test]
async fn codex_bridge_interrupts_and_cleans_up_hanging_turn() {
    let generation = 12;
    let (host, handle) = host(generation);
    let (cancel, cancellation) = HarnessCancellationContext::channel();
    let request = request("hang", generation, handle, cancellation);
    let harness = codex_harness();
    let cancel_task = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        cancel.send(true).expect("Codex cancellation receiver");
    });

    let (_, sink) = execute_agent_harness_v2(&harness, &request, &host, CodexSink::default())
        .await
        .expect("cancelled managed Codex harness");
    cancel_task.await.expect("Codex cancel task");

    assert!(sink.terminal.as_ref().is_some_and(|terminal| matches!(
        terminal.outcome,
        AgentHarnessTerminalOutcomeV2::Cancelled { .. }
    )));
}

#[tokio::test]
async fn codex_bridge_resumes_safe_thread_binding_on_the_next_turn() {
    let generation = 13;
    let (host, handle) = host(generation);
    let harness = codex_harness();

    let (_first_cancel, first_cancellation) = HarnessCancellationContext::channel();
    let first_request = request("fixture-model", generation, handle.clone(), first_cancellation);
    execute_agent_harness_v2(&harness, &first_request, &host, CodexSink::default())
        .await
        .expect("first managed Codex turn");

    let (_second_cancel, second_cancellation) = HarnessCancellationContext::channel();
    let second_request = request("fixture-model", generation, handle, second_cancellation);
    let (_, second_sink) =
        execute_agent_harness_v2(&harness, &second_request, &host, CodexSink::default())
            .await
            .expect("resumed managed Codex turn");

    assert!(second_sink.events.iter().any(|event| matches!(
        &event.event,
        AgentHarnessEventKindV2::Progress { label, .. }
            if label == "codex_thread_resumed"
    )));
}

fn codex_harness() -> ManagedExternalAgentHarness<StdioRuntimeTransport> {
    codex_harness_with_env(BTreeMap::new()).0
}

fn codex_harness_with_env(
    codex_env: BTreeMap<String, String>,
) -> (ManagedExternalAgentHarness<StdioRuntimeTransport>, Arc<StdioRuntimeTransport>) {
    let descriptor = codex_managed_runtime_descriptor(&ManagedCodexAppServerConfig {
        bridge_executable: PathBuf::from(env!("CARGO_BIN_EXE_palyrad")),
        codex_executable: PathBuf::from(env!("CARGO_BIN_EXE_palyra-fake-codex-app-server")),
        codex_args: Vec::new(),
        codex_env,
        cwd: PathBuf::from(env!("CARGO_MANIFEST_DIR")),
        version_policy: CodexAppServerVersionPolicy::default(),
    })
    .expect("Codex managed descriptor");
    let transport = Arc::new(StdioRuntimeTransport::new(descriptor).expect("Codex transport"));
    (
        ManagedExternalAgentHarness::new(codex_agent_harness_descriptor(), Arc::clone(&transport)),
        transport,
    )
}

fn host(generation: u64) -> (CodexGuardedHost, HarnessCapabilityHandle) {
    host_with_approval_mode(generation, ApprovalMode::Deny)
}

fn host_with_approval_mode(
    generation: u64,
    approval_mode: ApprovalMode,
) -> (CodexGuardedHost, HarnessCapabilityHandle) {
    let (host, handle, _observed_tool_epochs) =
        host_with_epoch_and_approval_mode(generation, 7, approval_mode);
    (host, handle)
}

fn host_with_epoch(
    generation: u64,
    expected_tool_epoch: u64,
) -> (CodexGuardedHost, HarnessCapabilityHandle, ToolEpochAudit) {
    host_with_epoch_and_approval_mode(generation, expected_tool_epoch, ApprovalMode::Deny)
}

fn host_with_epoch_and_approval_mode(
    generation: u64,
    expected_tool_epoch: u64,
    approval_mode: ApprovalMode,
) -> (CodexGuardedHost, HarnessCapabilityHandle, ToolEpochAudit) {
    let capabilities = Arc::new(HarnessCapabilityStore::default());
    let handle = capabilities
        .issue(
            "codex_app_server",
            generation,
            vec![
                HarnessHostOperation::EmitTextDelta,
                HarnessHostOperation::EmitProgress,
                HarnessHostOperation::ProposeToolCall,
                HarnessHostOperation::AwaitToolOutcome,
                HarnessHostOperation::RequestCompaction,
                HarnessHostOperation::SideQuestion,
                HarnessHostOperation::Checkpoint,
                HarnessHostOperation::Heartbeat,
            ],
            now_unix_ms().saturating_add(60_000),
        )
        .expect("Codex host capability");
    let (_cancel, cancellation) = HarnessCancellationContext::channel();
    let observed_tool_epochs = Arc::new(Mutex::new(Vec::new()));
    (
        GuardedHarnessHost::new(
            Arc::new(CodexHostBackend {
                approval_mode,
                expected_tool_epoch,
                observed_tool_epochs: Arc::clone(&observed_tool_epochs),
            }),
            capabilities,
            cancellation,
            Duration::from_secs(5),
        ),
        handle,
        observed_tool_epochs,
    )
}

fn request(
    model_id: &str,
    generation: u64,
    host_capability: HarnessCapabilityHandle,
    cancellation: HarnessCancellationContext,
) -> AgentHarnessAttemptRequestV2 {
    AgentHarnessAttemptRequestV2 {
        run_id: format!("run-codex-{generation}"),
        session_id: format!("session-codex-{generation}"),
        generation,
        deadline_unix_ms: now_unix_ms() + 10_000,
        provider_id: "codex-fixture".to_owned(),
        model_id: model_id.to_owned(),
        context_token_budget: 16_384,
        reasoning_policy: Some("default".to_owned()),
        sanitized_transcript: vec![json!({"role": "user", "content": "hello Codex"})],
        tool_surface: json!({
            "tools": [{
                "name": "palyra.fixture",
                "description": "Host-owned fixture tool.",
                "input_schema": {"type": "object"}
            }]
        }),
        tool_catalog_epoch: 7,
        workspace_root: None,
        sandbox: "host_owned".to_owned(),
        trace_context: "trace-codex-fixture".to_owned(),
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
