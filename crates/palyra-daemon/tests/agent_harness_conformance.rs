use palyra_common::runtime_contracts::{
    AgentHarnessAttemptTerminalStatus, AgentHarnessCallbackKind, AgentHarnessSelectionMode,
};
use palyra_daemon::application::{
    agent_harness::{
        select_agent_harness, AgentHarness, AgentHarnessCancellation, AgentHarnessDescriptor,
        AgentHarnessRunOutcome, AgentHarnessSupportDecision, AgentHarnessSupportRequest,
        EmbeddedPalyraHarness, PreparedAgentAttempt, PreparedAgentAttemptCallbacks,
    },
    agent_harness_callbacks::{
        HarnessCallbackCapabilityScope, HarnessCallbackProxy, HarnessCallbackRedactionPolicy,
        HarnessCallbackRequest,
    },
    agent_harness_lifecycle::run_selected_harness_attempt,
    agent_harness_tool_bridge::{
        evaluate_harness_tool_call, HarnessToolBridgePolicy, HarnessToolCallRequest,
        HarnessToolReplayMetadata,
    },
};
use serde_json::{json, Value};

#[derive(Debug)]
struct ConformanceHarness {
    descriptor: AgentHarnessDescriptor,
    status: &'static str,
}

impl ConformanceHarness {
    fn new(id: &str, status: &'static str) -> Self {
        Self { descriptor: AgentHarnessDescriptor::new(id, id, false), status }
    }
}

impl AgentHarness for ConformanceHarness {
    fn descriptor(&self) -> &AgentHarnessDescriptor {
        &self.descriptor
    }

    fn supports(&self, _request: &AgentHarnessSupportRequest<'_>) -> AgentHarnessSupportDecision {
        AgentHarnessSupportDecision::preferred("conformance.preferred")
    }

    fn run_attempt(&self, _attempt: PreparedAgentAttempt<'_>) -> AgentHarnessRunOutcome {
        AgentHarnessRunOutcome {
            status: self.status.to_owned(),
            emitted_callbacks: vec![AgentHarnessCallbackKind::FinalOutcome],
            final_message: Some("done".to_owned()),
        }
    }
}

fn request(
    selection_mode: AgentHarnessSelectionMode,
    explicit_harness_id: Option<&str>,
    mutating: bool,
) -> AgentHarnessSupportRequest<'_> {
    AgentHarnessSupportRequest {
        selection_mode,
        explicit_harness_id,
        provider_id: "openai",
        model_id: "gpt",
        runtime_policy: "default",
        channel_kind: "operator_cli",
        sandbox_mode: "host_owned",
        tool_policy_summary: "approval_required",
        model_capabilities: &["text"],
        mutating,
        replay_safe: true,
        fallback_allowed: true,
        replay_required: false,
    }
}

fn attempt<'a>(
    callbacks: PreparedAgentAttemptCallbacks,
    cancellation: AgentHarnessCancellation,
    auth: &'a Value,
    transcript: &'a [Value],
    tools: &'a Value,
    policy: &'a Value,
) -> PreparedAgentAttempt<'a> {
    PreparedAgentAttempt {
        run_id: "run-1",
        session_id: "session-1",
        provider_id: "openai",
        model_id: "gpt",
        auth_state_metadata: auth,
        context_token_budget: 4_096,
        reasoning_policy: Some("standard"),
        sanitized_transcript_view: transcript,
        tool_surface: tools,
        tool_policy: policy,
        workspace_root: None,
        sandbox: "host_owned",
        trace_context: "trace-1",
        callbacks,
        cancellation,
    }
}

#[test]
fn conformance_selects_embedded_and_records_lifecycle() {
    let embedded = EmbeddedPalyraHarness::default();
    let harnesses: [&dyn AgentHarness; 1] = [&embedded];
    let selected = select_agent_harness(
        &harnesses,
        &request(AgentHarnessSelectionMode::Embedded, None, false),
    )
    .expect("embedded selection should pass");
    let auth = json!({});
    let transcript = Vec::new();
    let tools = json!({});
    let policy = json!({});

    let trace = run_selected_harness_attempt(
        &selected,
        attempt(
            PreparedAgentAttemptCallbacks::host_controlled(),
            AgentHarnessCancellation::default(),
            &auth,
            transcript.as_slice(),
            &tools,
            &policy,
        ),
        true,
    );

    assert_eq!(trace.result.terminal_status, AgentHarnessAttemptTerminalStatus::Completed);
    assert_eq!(trace.events.len(), 2);
    assert!(!trace.fallback_used);
}

#[test]
fn conformance_blocks_explicit_missing_and_mutating_fallback() {
    let embedded = EmbeddedPalyraHarness::default();
    let harnesses: [&dyn AgentHarness; 1] = [&embedded];

    let explicit_error = match select_agent_harness(
        &harnesses,
        &request(AgentHarnessSelectionMode::ExplicitPlugin, Some("missing"), false),
    ) {
        Ok(selected) => {
            panic!("explicit missing harness must not select {}", selected.harness.descriptor().id)
        }
        Err(error) => error,
    };
    let mutating_error = match select_agent_harness(
        &harnesses,
        &request(AgentHarnessSelectionMode::PreferredPlugin, Some("missing"), true),
    ) {
        Ok(selected) => panic!(
            "mutating preferred fallback must not select {}",
            selected.harness.descriptor().id
        ),
        Err(error) => error,
    };

    assert_eq!(explicit_error.code, "explicit_harness_not_found");
    assert_eq!(mutating_error.code, "preferred_harness_unavailable_for_mutation");
}

#[test]
fn conformance_routes_tool_and_callback_through_host_boundaries() {
    let policy = HarnessToolBridgePolicy::new(["palyra.fs.read_file"], "catalog-1");
    let decision = evaluate_harness_tool_call(
        &HarnessToolCallRequest {
            harness_id: "embedded_palyra".to_owned(),
            run_id: "run-1".to_owned(),
            tool_call_id: "call-1".to_owned(),
            tool_name: "palyra.fs.read_file".to_owned(),
            raw_args: json!({"path":"README.md"}),
            catalog_snapshot_id: "catalog-1".to_owned(),
            replay_metadata: HarnessToolReplayMetadata {
                replay_safe: true,
                tool_surface_hash: "sha256:tools".to_owned(),
            },
            mutating: false,
        },
        &policy,
    )
    .expect("tool bridge should evaluate");
    let mut callbacks = HarnessCallbackProxy::new([AgentHarnessCallbackKind::ToolEvent], false);
    let record = callbacks
        .emit(HarnessCallbackRequest {
            callback_kind: AgentHarnessCallbackKind::ToolEvent,
            capability_scope: HarnessCallbackCapabilityScope::ToolBridge,
            redaction_policy: HarnessCallbackRedactionPolicy::RedactedPayload,
            idempotency_key: "tool:call-1".to_owned(),
            payload: json!({"api_key":"secret-token","summary":"ok"}),
        })
        .expect("callback should emit");
    let serialized = serde_json::to_string(&record).expect("record should serialize");

    assert!(decision.allowed);
    assert!(decision.execution_gate_required);
    assert!(!serialized.contains("secret-token"));
    assert_eq!(callbacks.records().len(), 1);
}

#[test]
fn conformance_distinguishes_cancellation_and_timeout_results() {
    let cancelled = AgentHarnessRunOutcome {
        status: "cancelled".to_owned(),
        emitted_callbacks: Vec::new(),
        final_message: None,
    }
    .to_attempt_result(false, "trace-1");
    let timed_out = ConformanceHarness::new("timeout.harness", "timed_out")
        .run_attempt(attempt(
            PreparedAgentAttemptCallbacks::host_controlled(),
            AgentHarnessCancellation::default(),
            &json!({}),
            &[],
            &json!({}),
            &json!({}),
        ))
        .to_attempt_result(false, "trace-2");

    assert_eq!(cancelled.terminal_status, AgentHarnessAttemptTerminalStatus::Cancelled);
    assert_eq!(timed_out.terminal_status, AgentHarnessAttemptTerminalStatus::TimedOut);
}
