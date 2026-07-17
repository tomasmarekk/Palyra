//! Lifecycle wrapper for selected agent harness attempts.

use palyra_common::runtime_contracts::{
    AgentHarnessAttemptClassification, AgentHarnessAttemptReplaySafety, AgentHarnessAttemptResult,
    AgentHarnessAttemptTerminalStatus, RuntimeEventEnvelopeV2, RuntimeEventId, RuntimeEventName,
    RuntimeEventPayloadRef, RuntimeEventSequenceValidator, RuntimeIdentitySetV1,
    RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use thiserror::Error;

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

/// Host-issued authority required to project a harness trace into canonical V2 events.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentHarnessLifecycleRuntimeAuthority {
    /// Exact typed identities for the harness generation and attempt.
    pub identities: RuntimeIdentitySetV1,
    /// First host-allocated sequence reserved for this lifecycle trace.
    pub first_sequence: u64,
    /// Host timestamp applied to the bounded lifecycle projection.
    pub occurred_at_unix_ms: i64,
}

/// Failure to project a harness lifecycle trace into canonical V2 events.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AgentHarnessLifecycleProjectionError {
    /// The supplied typed identity set is malformed or has no attempt identity.
    #[error("harness lifecycle runtime authority is invalid: {0}")]
    InvalidAuthority(String),
    /// The custom compatibility event has no canonical V2 mapping.
    #[error("unsupported harness lifecycle event: {0}")]
    UnsupportedEvent(String),
    /// The host sequence range cannot represent the full lifecycle trace.
    #[error("harness lifecycle event sequence overflowed")]
    SequenceOverflow,
    /// One projected V2 event violated the shared event contract.
    #[error("projected harness lifecycle event is invalid: {0}")]
    InvalidEvent(String),
}

impl AgentHarnessLifecycleTrace {
    /// Projects this compatibility trace into the shared generation-aware event envelope.
    ///
    /// Raw trace context and harness identifiers are reduced to domain-separated hashes before
    /// entering the inline metadata boundary.
    ///
    /// # Errors
    /// Returns [`AgentHarnessLifecycleProjectionError`] when host authority, sequence allocation,
    /// event mapping, or the resulting V2 event stream is invalid.
    pub fn to_runtime_events_v2(
        &self,
        authority: &AgentHarnessLifecycleRuntimeAuthority,
    ) -> Result<Vec<RuntimeEventEnvelopeV2>, AgentHarnessLifecycleProjectionError> {
        authority.identities.validate().map_err(|error| {
            AgentHarnessLifecycleProjectionError::InvalidAuthority(error.to_string())
        })?;
        let attempt_id = authority.identities.attempt_id.as_ref().ok_or_else(|| {
            AgentHarnessLifecycleProjectionError::InvalidAuthority(
                "attempt_id is required".to_owned(),
            )
        })?;
        if authority.occurred_at_unix_ms < 0 {
            return Err(AgentHarnessLifecycleProjectionError::InvalidAuthority(
                "occurred_at_unix_ms must be non-negative".to_owned(),
            ));
        }

        let mut projected = Vec::with_capacity(self.events.len());
        let mut previous_event_id = None;
        let mut validator = RuntimeEventSequenceValidator::default();
        for (offset, lifecycle) in self.events.iter().enumerate() {
            let sequence = authority
                .first_sequence
                .checked_add(
                    u64::try_from(offset)
                        .map_err(|_| AgentHarnessLifecycleProjectionError::SequenceOverflow)?,
                )
                .ok_or(AgentHarnessLifecycleProjectionError::SequenceOverflow)?;
            let event_name =
                harness_runtime_event_name(lifecycle.event_name.as_str()).ok_or_else(|| {
                    AgentHarnessLifecycleProjectionError::UnsupportedEvent(
                        lifecycle.event_name.clone(),
                    )
                })?;
            let descriptor = event_name.descriptor();
            let event_id = RuntimeEventId::parse(
                format!("harness:{}:{sequence}", attempt_id.as_str()).as_str(),
            )
            .map_err(|error| {
                AgentHarnessLifecycleProjectionError::InvalidEvent(error.to_string())
            })?;
            let event = RuntimeEventEnvelopeV2 {
                schema_version: RUNTIME_EVENT_ENVELOPE_SCHEMA_VERSION,
                event_id: event_id.clone(),
                identities: authority.identities.clone(),
                sequence,
                causal_parent_event_id: previous_event_id,
                subsystem: descriptor.subsystem,
                phase: descriptor.phase,
                event_name,
                reason_code: lifecycle.reason_code.clone(),
                actor_kind: descriptor.actor_kind,
                retryability: descriptor.retryability,
                redaction_class: descriptor.redaction_class,
                terminal: descriptor.terminal,
                payload: RuntimeEventPayloadRef::Inline {
                    metadata: json!({
                        "harness_id_sha256": lifecycle_field_sha256(
                            "harness_id",
                            lifecycle.harness_id.as_str(),
                        ),
                        "descriptor_sha256": lifecycle_field_sha256(
                            "descriptor",
                            lifecycle.descriptor_hash.as_str(),
                        ),
                        "trace_context_sha256": lifecycle_field_sha256(
                            "trace_context",
                            lifecycle.trace_context.as_str(),
                        ),
                    }),
                },
                occurred_at_unix_ms: authority.occurred_at_unix_ms,
                extensions: Default::default(),
            };
            validator.observe(&event).map_err(|error| {
                AgentHarnessLifecycleProjectionError::InvalidEvent(error.to_string())
            })?;
            previous_event_id = Some(event_id);
            projected.push(event);
        }
        Ok(projected)
    }
}

fn harness_runtime_event_name(event_name: &str) -> Option<RuntimeEventName> {
    match event_name {
        HARNESS_RUN_STARTED_EVENT => Some(RuntimeEventName::HarnessAttemptStarted),
        HARNESS_RUN_COMPLETED_EVENT => Some(RuntimeEventName::HarnessAttemptCompleted),
        HARNESS_RUN_FAILED_EVENT => Some(RuntimeEventName::HarnessAttemptFailed),
        HARNESS_RUN_CANCELLED_EVENT => Some(RuntimeEventName::HarnessAttemptCancelled),
        HARNESS_RUN_CLEANED_UP_EVENT => Some(RuntimeEventName::HarnessAttemptCleanedUp),
        _ => None,
    }
}

fn lifecycle_field_sha256(domain: &str, value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"palyra.harness_lifecycle.v1");
    hasher.update(b"\0");
    hasher.update(domain.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.as_bytes());
    hex::encode(hasher.finalize())
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
        RuntimeAttemptId, RuntimeGeneration, RuntimeRunId, RuntimeSessionId, RuntimeTraceId,
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

        let mut identities = RuntimeIdentitySetV1::for_run(
            RuntimeTraceId::parse("trace_01").expect("trace id"),
            RuntimeSessionId::parse("session_01").expect("session id"),
            RuntimeRunId::parse("run_01").expect("run id"),
            RuntimeGeneration::new(3).expect("generation"),
        );
        identities.attempt_id = Some(RuntimeAttemptId::parse("attempt_01").expect("attempt id"));
        let projected = trace
            .to_runtime_events_v2(&AgentHarnessLifecycleRuntimeAuthority {
                identities,
                first_sequence: 11,
                occurred_at_unix_ms: 42,
            })
            .expect("harness lifecycle should project through V2");
        assert_eq!(
            projected.iter().map(|event| event.event_name).collect::<Vec<_>>(),
            vec![
                RuntimeEventName::HarnessAttemptStarted,
                RuntimeEventName::HarnessAttemptCompleted,
                RuntimeEventName::HarnessAttemptCleanedUp,
            ]
        );
        assert_eq!(projected[0].sequence, 11);
        assert_eq!(projected[1].causal_parent_event_id.as_ref(), Some(&projected[0].event_id));
        assert_eq!(projected[2].causal_parent_event_id.as_ref(), Some(&projected[1].event_id));
        assert_eq!(
            projected.iter().filter(|event| event.terminal).count(),
            1,
            "one harness generation must have exactly one terminal outcome"
        );
        let projected_json =
            serde_json::to_string(&projected).expect("projected events should serialize");
        assert!(!projected_json.contains(trace.harness_id.as_str()));
        assert!(!projected_json.contains(trace.descriptor_hash.as_str()));
        assert!(!projected_json.contains("trace?"));
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
