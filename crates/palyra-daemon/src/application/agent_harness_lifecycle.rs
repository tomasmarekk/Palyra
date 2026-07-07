//! Lifecycle wrapper for selected agent harness attempts.

use palyra_common::runtime_contracts::{
    AgentHarnessAttemptClassification, AgentHarnessAttemptReplaySafety, AgentHarnessAttemptResult,
    AgentHarnessAttemptTerminalStatus,
};
use serde::{Deserialize, Serialize};

use super::agent_harness::{
    AgentHarnessRegistry, AgentHarnessRegistryError, AgentHarnessSelectionError,
    AgentHarnessSupportRequest, PreparedAgentAttempt, SelectedAgentHarness,
};

/// Stable lifecycle event name for harness attempt start.
pub const HARNESS_RUN_STARTED_EVENT: &str = "harness.run.started";
/// Stable lifecycle event name for harness attempt completion.
pub const HARNESS_RUN_COMPLETED_EVENT: &str = "harness.run.completed";
/// Stable lifecycle event name for harness attempt failure.
pub const HARNESS_RUN_FAILED_EVENT: &str = "harness.run.failed";
/// Stable lifecycle event name for harness attempt cancellation.
pub const HARNESS_RUN_CANCELLED_EVENT: &str = "harness.run.cancelled";
/// Stable lifecycle event name for mandatory harness attempt cleanup.
pub const HARNESS_RUN_CLEANED_UP_EVENT: &str = "harness.run.cleaned_up";

/// Host-retained lifecycle event for a harness attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessLifecycleEvent {
    pub event_name: String,
    pub harness_id: String,
    pub descriptor_hash: String,
    pub trace_context: String,
    pub reason_code: String,
}

/// Full wrapper output for one selected harness attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentHarnessLifecycleTrace {
    pub harness_id: String,
    pub descriptor_hash: String,
    pub selection_reason_code: String,
    pub fallback_used: bool,
    pub events: Vec<AgentHarnessLifecycleEvent>,
    pub result: AgentHarnessAttemptResult,
}

/// Versioned runtime owner for selecting and executing a prepared harness attempt.
pub struct HarnessRuntimeV1 {
    registry: AgentHarnessRegistry,
}

impl HarnessRuntimeV1 {
    /// Builds a runtime with the embedded Palyra harness registered.
    ///
    /// # Errors
    /// Returns [`AgentHarnessRegistryError`] if the embedded descriptor cannot be registered.
    pub fn with_embedded_default() -> Result<Self, AgentHarnessRegistryError> {
        Ok(Self { registry: AgentHarnessRegistry::with_embedded_default()? })
    }

    /// Builds a runtime from an existing host-owned registry.
    #[must_use]
    pub fn with_registry(registry: AgentHarnessRegistry) -> Self {
        Self { registry }
    }

    /// Returns the underlying host-owned registry for descriptor activation.
    #[must_use]
    pub fn registry(&self) -> &AgentHarnessRegistry {
        &self.registry
    }

    /// Returns a mutable registry handle for pre-selection plugin/native descriptor activation.
    pub fn registry_mut(&mut self) -> &mut AgentHarnessRegistry {
        &mut self.registry
    }

    /// Selects a harness and executes the prepared attempt through lifecycle auditing.
    ///
    /// # Errors
    /// Returns [`AgentHarnessSelectionError`] when selection fails closed.
    pub fn run_attempt(
        &self,
        request: &AgentHarnessSupportRequest<'_>,
        attempt: PreparedAgentAttempt<'_>,
        replay_safe: bool,
    ) -> Result<AgentHarnessLifecycleTrace, AgentHarnessSelectionError> {
        let selected = self.registry.select(request)?;
        Ok(run_selected_harness_attempt(&selected, attempt, replay_safe))
    }
}

/// Runs a selected harness attempt through the lifecycle wrapper.
#[must_use]
pub fn run_selected_harness_attempt(
    selected: &SelectedAgentHarness<'_>,
    attempt: PreparedAgentAttempt<'_>,
    replay_safe: bool,
) -> AgentHarnessLifecycleTrace {
    let descriptor = selected.harness.descriptor();
    let trace_context = palyra_common::redaction::redact_diagnostic_text(attempt.trace_context);
    let mut events = vec![AgentHarnessLifecycleEvent {
        event_name: HARNESS_RUN_STARTED_EVENT.to_owned(),
        harness_id: descriptor.id.clone(),
        descriptor_hash: descriptor.descriptor_hash.clone(),
        trace_context: trace_context.clone(),
        reason_code: selected.decision.reason_code.clone(),
    }];

    let outcome = selected.harness.run_attempt(attempt);
    let mut result = outcome.to_attempt_result(replay_safe, trace_context.as_str());
    normalize_cancelled_classification(&mut result);
    events.push(AgentHarnessLifecycleEvent {
        event_name: terminal_event_name(result.terminal_status).to_owned(),
        harness_id: descriptor.id.clone(),
        descriptor_hash: descriptor.descriptor_hash.clone(),
        trace_context: trace_context.clone(),
        reason_code: terminal_reason_code(result.terminal_status).to_owned(),
    });
    events.push(AgentHarnessLifecycleEvent {
        event_name: HARNESS_RUN_CLEANED_UP_EVENT.to_owned(),
        harness_id: descriptor.id.clone(),
        descriptor_hash: descriptor.descriptor_hash.clone(),
        trace_context,
        reason_code: "harness.run.cleaned_up".to_owned(),
    });

    AgentHarnessLifecycleTrace {
        harness_id: descriptor.id.clone(),
        descriptor_hash: descriptor.descriptor_hash.clone(),
        selection_reason_code: selected.decision.reason_code.clone(),
        fallback_used: selected.fallback_used,
        events,
        result,
    }
}

fn normalize_cancelled_classification(result: &mut AgentHarnessAttemptResult) {
    if result.terminal_status == AgentHarnessAttemptTerminalStatus::Cancelled {
        result.classification = AgentHarnessAttemptClassification::NativeRuntimeError;
        result.replay_safety = AgentHarnessAttemptReplaySafety::NotReplaySafe;
    }
}

const fn terminal_event_name(status: AgentHarnessAttemptTerminalStatus) -> &'static str {
    match status {
        AgentHarnessAttemptTerminalStatus::Completed
        | AgentHarnessAttemptTerminalStatus::Yielded => HARNESS_RUN_COMPLETED_EVENT,
        AgentHarnessAttemptTerminalStatus::Cancelled => HARNESS_RUN_CANCELLED_EVENT,
        AgentHarnessAttemptTerminalStatus::Blocked
        | AgentHarnessAttemptTerminalStatus::Failed
        | AgentHarnessAttemptTerminalStatus::TimedOut => HARNESS_RUN_FAILED_EVENT,
    }
}

const fn terminal_reason_code(status: AgentHarnessAttemptTerminalStatus) -> &'static str {
    match status {
        AgentHarnessAttemptTerminalStatus::Completed => "harness.run.completed",
        AgentHarnessAttemptTerminalStatus::Yielded => "harness.run.yielded",
        AgentHarnessAttemptTerminalStatus::Cancelled => "harness.run.cancelled",
        AgentHarnessAttemptTerminalStatus::Blocked => "harness.run.blocked",
        AgentHarnessAttemptTerminalStatus::Failed => "harness.run.failed",
        AgentHarnessAttemptTerminalStatus::TimedOut => "harness.run.timed_out",
    }
}

#[cfg(test)]
mod tests {
    use palyra_common::runtime_contracts::{
        AgentHarnessCallbackKind, AgentHarnessSelectionMode, AgentHarnessSupportOutcome,
    };
    use serde_json::json;

    use super::*;
    use crate::application::agent_harness::{
        AgentHarness, AgentHarnessCancellation, AgentHarnessDescriptor,
        AgentHarnessSupportDecision, AgentHarnessSupportRequest, EmbeddedPalyraHarness,
        PreparedAgentAttemptCallbacks,
    };

    fn support_request() -> AgentHarnessSupportRequest<'static> {
        AgentHarnessSupportRequest {
            selection_mode: AgentHarnessSelectionMode::Embedded,
            explicit_harness_id: None,
            provider_id: "openai",
            model_id: "gpt",
            runtime_policy: "default",
            channel_kind: "operator_cli",
            sandbox_mode: "host_owned",
            tool_policy_summary: "approval_required",
            model_capabilities: &["text"],
            mutating: false,
            replay_safe: true,
            fallback_allowed: false,
            replay_required: false,
        }
    }

    fn attempt<'a>(
        cancellation: AgentHarnessCancellation,
        auth: &'a serde_json::Value,
        transcript: &'a [serde_json::Value],
        tools: &'a serde_json::Value,
        policy: &'a serde_json::Value,
    ) -> PreparedAgentAttempt<'a> {
        PreparedAgentAttempt {
            run_id: "run-1",
            session_id: "session-1",
            provider_id: "openai",
            model_id: "gpt",
            auth_state_metadata: auth,
            context_token_budget: 1,
            reasoning_policy: None,
            sanitized_transcript_view: transcript,
            tool_surface: tools,
            tool_policy: policy,
            workspace_root: None,
            sandbox: "host_owned",
            trace_context: "trace?access_token=secret",
            callbacks: PreparedAgentAttemptCallbacks::host_controlled(),
            cancellation,
        }
    }

    #[test]
    fn lifecycle_wrapper_emits_started_and_completed_events() {
        let harness = EmbeddedPalyraHarness::default();
        let decision = AgentHarnessSupportDecision {
            outcome: AgentHarnessSupportOutcome::Supported,
            reason_code: "harness.embedded_default".to_owned(),
        };
        let selected = SelectedAgentHarness {
            harness: &harness,
            decision,
            fallback_used: false,
            fallback_policy: "not_applicable".to_owned(),
            selection_mode: AgentHarnessSelectionMode::Embedded,
        };
        let auth = json!({});
        let transcript = Vec::new();
        let tools = json!({});
        let policy = json!({});

        let trace = run_selected_harness_attempt(
            &selected,
            attempt(
                AgentHarnessCancellation::default(),
                &auth,
                transcript.as_slice(),
                &tools,
                &policy,
            ),
            true,
        );
        let serialized = serde_json::to_string(&trace).expect("trace should serialize");

        assert_eq!(trace.events[0].event_name, HARNESS_RUN_STARTED_EVENT);
        assert_eq!(trace.events[1].event_name, HARNESS_RUN_COMPLETED_EVENT);
        assert_eq!(trace.events[2].event_name, HARNESS_RUN_CLEANED_UP_EVENT);
        assert_eq!(trace.result.terminal_status, AgentHarnessAttemptTerminalStatus::Completed);
        assert!(!serialized.contains("secret"));
    }

    #[test]
    fn lifecycle_wrapper_distinguishes_cancelled_from_timeout() {
        let harness = EmbeddedPalyraHarness::default();
        let cancellation = AgentHarnessCancellation::default();
        cancellation.cancel();
        let selected = SelectedAgentHarness {
            harness: &harness,
            decision: AgentHarnessSupportDecision::supported("harness.embedded_default"),
            fallback_used: false,
            fallback_policy: "not_applicable".to_owned(),
            selection_mode: AgentHarnessSelectionMode::Embedded,
        };
        let auth = json!({});
        let transcript = Vec::new();
        let tools = json!({});
        let policy = json!({});

        let trace = run_selected_harness_attempt(
            &selected,
            attempt(cancellation, &auth, transcript.as_slice(), &tools, &policy),
            true,
        );

        assert_eq!(trace.events[1].event_name, HARNESS_RUN_CANCELLED_EVENT);
        assert_eq!(trace.events[2].event_name, HARNESS_RUN_CLEANED_UP_EVENT);
        assert_eq!(trace.result.terminal_status, AgentHarnessAttemptTerminalStatus::Cancelled);
        assert_ne!(
            trace.result.classification,
            AgentHarnessAttemptClassification::ToolLoopGuardrail
        );
    }

    #[derive(Debug)]
    struct FailedHarness {
        descriptor: AgentHarnessDescriptor,
    }

    impl AgentHarness for FailedHarness {
        fn descriptor(&self) -> &AgentHarnessDescriptor {
            &self.descriptor
        }

        fn supports(
            &self,
            _request: &AgentHarnessSupportRequest<'_>,
        ) -> AgentHarnessSupportDecision {
            AgentHarnessSupportDecision::supported("harness.failed_test")
        }

        fn run_attempt(
            &self,
            _attempt: crate::application::agent_harness::PreparedAgentAttempt<'_>,
        ) -> crate::application::agent_harness::AgentHarnessRunOutcome {
            crate::application::agent_harness::AgentHarnessRunOutcome {
                status: "timed_out".to_owned(),
                emitted_callbacks: vec![AgentHarnessCallbackKind::LifecycleEvent],
                final_message: None,
            }
        }
    }

    #[test]
    fn lifecycle_wrapper_classifies_timeout_separately() {
        let harness = FailedHarness {
            descriptor: AgentHarnessDescriptor::new("timeout.harness", "Timeout harness", false),
        };
        let selected = SelectedAgentHarness {
            harness: &harness,
            decision: harness.supports(&support_request()),
            fallback_used: false,
            fallback_policy: "not_applicable".to_owned(),
            selection_mode: AgentHarnessSelectionMode::Explicit,
        };
        let auth = json!({});
        let transcript = Vec::new();
        let tools = json!({});
        let policy = json!({});

        let trace = run_selected_harness_attempt(
            &selected,
            attempt(
                AgentHarnessCancellation::default(),
                &auth,
                transcript.as_slice(),
                &tools,
                &policy,
            ),
            false,
        );

        assert_eq!(trace.events[1].event_name, HARNESS_RUN_FAILED_EVENT);
        assert_eq!(trace.result.terminal_status, AgentHarnessAttemptTerminalStatus::TimedOut);
        assert_eq!(trace.result.classification, AgentHarnessAttemptClassification::ProviderError);
    }

    #[test]
    fn harness_runtime_v1_selects_and_runs_embedded_attempt() {
        let runtime = HarnessRuntimeV1::with_embedded_default().expect("runtime should build");
        let auth = json!({});
        let transcript = Vec::new();
        let tools = json!({});
        let policy = json!({});

        let trace = runtime
            .run_attempt(
                &support_request(),
                attempt(
                    AgentHarnessCancellation::default(),
                    &auth,
                    transcript.as_slice(),
                    &tools,
                    &policy,
                ),
                true,
            )
            .expect("embedded harness should run");

        assert_eq!(trace.harness_id, crate::application::agent_harness::EMBEDDED_PALYRA_HARNESS_ID);
        assert_eq!(trace.events[0].event_name, HARNESS_RUN_STARTED_EVENT);
        assert_eq!(trace.events[2].event_name, HARNESS_RUN_CLEANED_UP_EVENT);
    }
}
