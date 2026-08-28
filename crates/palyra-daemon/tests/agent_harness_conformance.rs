use palyra_common::runtime_contracts::{
    AgentHarnessAttemptTerminalStatus, AgentHarnessCallbackKind, AgentHarnessSelectionMode,
    AgentHarnessTerminalClassification,
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
        evaluate_harness_tool_call, project_harness_visible_tool_result, HarnessToolBridgePolicy,
        HarnessToolCallRequest, HarnessToolReplayMetadata,
    },
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;

#[derive(Debug)]
struct ConformanceHarness {
    descriptor: AgentHarnessDescriptor,
    status: &'static str,
}

#[derive(Debug, Deserialize)]
struct ConformanceMatrix {
    schema_version: u32,
    profiles: Vec<String>,
    common_adapters: Vec<String>,
    cases: Vec<ConformanceMatrixCase>,
    ci_profiles: Value,
}

#[derive(Debug, Deserialize)]
struct ConformanceMatrixCase {
    id: String,
    package: String,
    quick: bool,
    expected_terminal_status: String,
    expected_classification: String,
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
    assert_eq!(trace.events.len(), 3);
    assert!(!trace.fallback_used);
}

#[test]
fn conformance_selects_fake_plugin_subset_and_records_lifecycle() {
    let fake_plugin = ConformanceHarness::new("fake.plugin", "completed");
    let harnesses: [&dyn AgentHarness; 1] = [&fake_plugin];
    let selected = select_agent_harness(
        &harnesses,
        &request(AgentHarnessSelectionMode::ExplicitPlugin, Some("fake.plugin"), false),
    )
    .expect("fake plugin subset should select explicitly");
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
    assert_eq!(trace.events.len(), 3);
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
            run_id: "run-1".to_owned(),
            attempt_id: "attempt-1".to_owned(),
            sequence: 1,
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
fn conformance_returns_safe_projection_for_approval_denial() {
    let mut policy = HarnessToolBridgePolicy::new(["palyra.fs.apply_patch"], "catalog-1");
    policy.approval_required_for_mutation = true;
    policy.deny_approval_for("call-1");

    let decision = evaluate_harness_tool_call(
        &HarnessToolCallRequest {
            harness_id: "embedded_palyra".to_owned(),
            run_id: "run-1".to_owned(),
            tool_call_id: "call-1".to_owned(),
            tool_name: "palyra.fs.apply_patch".to_owned(),
            raw_args: json!({"patch":"*** Begin Patch"}),
            catalog_snapshot_id: "catalog-1".to_owned(),
            replay_metadata: HarnessToolReplayMetadata {
                replay_safe: false,
                tool_surface_hash: "sha256:tools".to_owned(),
            },
            mutating: true,
        },
        &policy,
    )
    .expect("approval denial should evaluate");
    let projected = project_harness_visible_tool_result(
        &json!({"status":"completed","summary":"api_key=secret-token should be hidden"}),
        false,
        128,
    );

    assert!(!decision.allowed);
    assert_eq!(decision.reason_code, "harness_tool.approval_denied");
    assert_eq!(
        decision.harness_visible_result.as_ref().map(|result| result.status.as_str()),
        Some("denied")
    );
    assert!(!projected.summary.contains("secret-token"));
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
    assert_eq!(
        cancelled.terminal_classification,
        Some(AgentHarnessTerminalClassification::Cancelled)
    );
    assert_eq!(
        timed_out.terminal_classification,
        Some(AgentHarnessTerminalClassification::Timeout)
    );
}

#[test]
fn conformance_matrix_fixture_covers_p0_recovery_cases() {
    let matrix: ConformanceMatrix = serde_json::from_str(include_str!(
        "../../../fixtures/golden/backend_harness_conformance_matrix.json"
    ))
    .expect("conformance matrix should parse");
    let packages = matrix.cases.iter().map(|case| case.package.as_str()).collect::<BTreeSet<_>>();
    let quick_count = matrix.cases.iter().filter(|case| case.quick).count();
    let quick_max = matrix
        .ci_profiles
        .pointer("/quick/max_cases")
        .and_then(Value::as_u64)
        .expect("quick max cases should be present") as usize;

    assert_eq!(matrix.schema_version, 1);
    assert!(matrix.profiles.contains(&"quick".to_owned()));
    assert!(matrix.profiles.contains(&"full".to_owned()));
    assert!(matrix.common_adapters.contains(&"embedded_palyra".to_owned()));
    assert!(matrix.common_adapters.contains(&"fake_plugin_subset".to_owned()));
    for required in [
        "agent_loop_recovery",
        "provider_stream_normalizer",
        "tool_guardrail_loop",
        "approval_mapping",
        "native_harness_relay",
        "codex_adapter",
        "lsp_service",
        "verification_evidence",
        "context_compaction",
        "hook_runner",
    ] {
        assert!(packages.contains(required), "missing conformance package {required}");
    }
    assert!(quick_count <= quick_max);
    assert!(matrix.cases.iter().any(|case| {
        case.id == "approval_denied"
            && case.expected_terminal_status == "denied"
            && case.expected_classification == "approval_denied"
    }));
    assert!(matrix.cases.iter().any(|case| {
        case.id == "cancel_during_tool" && case.expected_terminal_status == "cancelled"
    }));
}
